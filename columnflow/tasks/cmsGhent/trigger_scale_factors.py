from __future__ import annotations

import re
import law
from collections.abc import Iterator
import order as od
from itertools import product
import luigi

from columnflow.types import Any
from columnflow.tasks.framework.base import Requirements, ConfigTask
from columnflow.tasks.framework.mixins import (
    CalibratorsMixin, SelectorMixin, DatasetsMixin,
)
from columnflow.tasks.framework.plotting import (
    PlotBase, PlotBase2D, PlotBase1D,
)
from columnflow.tasks.cmsGhent.selection_hists import SelectionEfficiencyHistMixin
from columnflow.production.cmsGhent.trigger import TriggerSFConfig
import columnflow.production.cmsGhent.trigger.util as util
from columnflow.tasks.framework.remote import RemoteWorkflow
from columnflow.util import dev_sandbox, dict_add_strict, maybe_import


np = maybe_import("numpy")
hist = maybe_import("hist")

logger = law.logger.get_logger(__name__)


class TriggerConfigMixin(ConfigTask):
    exclude_index = True

    trigger_config = luigi.Parameter(description="Trigger config to use to measure", default=None)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.trigger_config_inst: TriggerSFConfig = self.get_trigger_config(self.config_inst, self.trigger_config)
        self.trigger_config = self.trigger_config_inst.config_name
        self.trigger = self.trigger_config_inst.tag
        self.ref_trigger = self.trigger_config_inst.ref_tag

    @classmethod
    def get_trigger_config(cls, config_inst, trigger_config=None):
        if trigger_config is None:
            return config_inst.x("trigger_sf", config_inst.x.trigger_sfs[0])
        elif type(trigger_config) is TriggerSFConfig:
            return trigger_config

        if hasattr(config_inst.x, "trigger_sf"):
            if config_inst.x.trigger_sf.config_name == trigger_config:
                return config_inst.x.trigger_sf
            else:
                AssertionError(
                    f"could not find trigger config {trigger_config}.\n"
                    f"The available trigger config in 'config.x.trigger_sf' is {config_inst.x.trigger_sf.config_name}",
                )
        elif hasattr(config_inst.x, "trigger_sfs"):
            for cfg in config_inst.x.trigger_sfs:
                if cfg.config_name == trigger_config:
                    return cfg
            AssertionError(
                f"could not find trigger config {trigger_config}.\n"
                "Available: " + ", ".join([cfg.config_name for cfg in config_inst.x.trigger_sfs]),
            )

    def loop_variables(self, include_1d=True) -> Iterator[tuple[str]]:
        tcfg = self.trigger_config_inst
        for var in tcfg.variable_names:
            # 1d efficiencies and sf
            if include_1d:
                yield var,

            # fully binned efficiency in  main variables with additional variables
            if var in tcfg.main_variables[1:] or len(tcfg.main_variables) == len(tcfg.variables) == 1:
                # don't repeat multiple times same combinations
                continue

            if var not in (vrs := tcfg.main_variables):
                vrs = sorted({var, *vrs}, key=tcfg.variables.index)
            yield tuple(vrs)


class TriggerDatasetsMixin(
    TriggerConfigMixin,
    DatasetsMixin,
):
    @property
    def datasets_repr(self):
        name = []
        for dt_type in ["mc", "data"]:
            datasets = [
                dt_name for dt_name in self.datasets
                if getattr(self.config_inst.get_dataset(dt_name), f"is_{dt_type}")
            ]
            assert datasets, f"could not find any datasets that are of type {dt_type}"
            if len(datasets) == 1:
                name.append(datasets[0])
            else:
                name.append(f"{len(datasets)}_{law.util.create_hash(sorted(datasets))}")
        return "__".join(name)

    def store_parts(self):
        parts = super().store_parts()
        name = f"trigger_{self.trigger}_ref_{self.ref_trigger}"
        parts.insert_before("version", "datasets", f"datasets_{self.datasets_repr}")
        parts.insert_before("datasets", "trigger", name)
        parts["task_family"] = TriggerScaleFactors.get_task_family()
        return parts

    @classmethod
    def resolve_param_values(cls, params: law.util.InsertableDict[str, Any]) -> law.util.InsertableDict[str, Any]:
        redo_default_datasets = False
        # when empty, use the config default
        if not params.get("datasets", None):
            redo_default_datasets = True

        params = super().resolve_param_values(params)
        if not redo_default_datasets:
            return params

        if not (config_inst := params.get("config_inst")):
            return params

        tcfg: TriggerSFConfig = cls.get_trigger_config(config_inst, params.get("trigger_config"))
        params["datasets"] = tcfg.datasets
        return params


class TriggerScaleFactors(
    TriggerDatasetsMixin,
    SelectionEfficiencyHistMixin,
    SelectorMixin,
    CalibratorsMixin,
    law.LocalWorkflow,
    RemoteWorkflow,
):
    exclude_index = False

    def output(self):
        out = {
            "json": self.target(f"{self.trigger_config}_sf.json"),
            "sf": self.target(f"{self.trigger_config}_sf.pickle"),
            "eff": self.target(f"{self.trigger_config}_eff.pickle"),
            "hist": self.target(f"{self.trigger_config}_hist.pickle"),
        }
        return out

    @law.decorator.log
    def run(self):
        import hist
        import correctionlib.convert

        calc_eff = lambda h: util.calculate_efficiency(h, self.trigger, self.ref_trigger, self.efficiency)
        tcfg = self.trigger_config_inst

        hist_name = self.trigger_config_inst.config_name + "_efficiencies"
        histograms = self.read_hist(self.trigger_config_inst.variables, hist_name)
        store_hists = dict()

        collect_hists = util.collect_hist(histograms)
        efficiencies = {}
        scale_factors: dict[str, hist.Hist] = {}
        triggers = (self.trigger, self.ref_trigger)  # needs to be tuple as vrs is tuple
        for vrs in self.loop_variables():
            key = "_".join(vrs)

            # calculate efficiency binned in given variables
            red_hist = {dt: util.reduce_hist(h, exclude=vrs + triggers) for dt, h in collect_hists.items()}
            efficiencies[key] = eff = {dt: calc_eff(h) for dt, h in red_hist.items()}

            # calculate sf from efficiencies
            eff_ct = {dt: eff[dt][{"systematic": "central"}] for dt in eff}
            sf = util.syst_hist(eff_ct["data"].axes, syst_name="central",
                                arrays=eff_ct["data"].values() / eff_ct["mc"].values())

            if set(law.util.make_list(vrs)) == set(tcfg.main_variables):
                # full uncertainties for main binning
                for unc_function in self.trigger_config_inst.uncertainties:
                    if (unc := unc_function(histograms, store_hists)) is not None:
                        sf = sf + unc
            elif tcfg.stat_unc is not None:
                # statistical only for other binnings
                sf = sf + tcfg.stat_unc(red_hist, store_hists)

            scale_factors[key] = sf

        # save efficiency and additional histograms
        self.output()["eff"].dump(efficiencies, formatter="pickle")
        self.output()["hist"].dump(store_hists, formatter="pickle")

        # add up all uncertainties for nominal
        for sf_type, hst in scale_factors.items():
            hst.name = "scale_factors"
            hst.label = (
                f"trigger scale factors for {self.trigger} trigger with reference {self.ref_trigger} "
                f"for year {self.config_inst.x.year}"
            )
            if len(hst.axes["systematic"]) > 1:
                get_syst = lambda syst: hst[{"systematic": syst}].values()
                ct = get_syst("central")
                # add quadratic sum of all uncertainties
                variance = {dr: 0 for dr in [od.Shift.DOWN, od.Shift.UP]}
                for err in hst.axes["systematic"]:
                    for dr in variance:
                        if dr in variance:
                            variance[dr] += (get_syst(err) - ct) ** 2

                var_hst = util.syst_hist(hst.axes, arrays=[ct - variance[od.Shift.DOWN], ct + variance[od.Shift.UP]])
                scale_factors[sf_type] = hst + var_hst

        # save sf histograms
        self.output()["sf"].dump(scale_factors, formatter="pickle")

        # save nominal as correctionlib file (not possible to specify the flow for each variable separately)
        nom_key = "_".join(tcfg.main_variables)
        clibcorr = correctionlib.convert.from_histogram(scale_factors[nom_key], flow="clamp")
        clibcorr.description = scale_factors[nom_key].label

        cset = correctionlib.schemav2.CorrectionSet(
            schema_version=2, description=scale_factors[nom_key].label, corrections=[clibcorr],
        )
        self.output()["json"].dump(cset.dict(exclude_unset=True), indent=4, formatter="json")


class TrigPlotLabelMixin(
    TriggerConfigMixin,
):

    baseline_label = luigi.Parameter(
        default="",
        description="Label for the baseline selection.",
    )

    def bin_label(self, index: dict[od.Variable | str, int]):
        index = {self.trigger_config_inst.get_variable(vr): bn for vr, bn in index.items()}
        return "\n".join([
            f"{vr.name}: bin {bn}" if vr.x_labels is None else vr.x_labels[bn]
            for vr, bn in index.items()
        ])

    def baseline_cat(self, add: str = None):
        p_cat = od.Category(name=self.baseline_label)
        if add is not None:
            p_cat.label += "\n" + add
        return p_cat


class OutputBranchWorkflow(
    law.LocalWorkflow,
    RemoteWorkflow,
):
    exclude_index = True

    def full_output(self) -> dict:
        return {}

    def create_branch_map(self):
        return list(self.full_output())

    def output(self):
        return self.full_output()[self.branch_data]


class PlotTriggerScaleFactorsBase(
    TrigPlotLabelMixin,
    TriggerDatasetsMixin,
    SelectorMixin,
    CalibratorsMixin,
    OutputBranchWorkflow,
):
    reqs = Requirements(
        RemoteWorkflow.reqs,
        TriggerScaleFactors=TriggerScaleFactors,
    )

    process = luigi.Parameter(
        default=None,
        description="process to represent MC",
        significant=False,
    )

    sandbox = dev_sandbox(law.config.get("analysis", "default_columnar_sandbox"))

    @classmethod
    def resolve_param_values(
        cls,
        params: law.util.InsertableDict[str, Any],
    ) -> law.util.InsertableDict[str, Any]:
        params = super().resolve_param_values(params)

        if (config_inst := params.get("config_inst")) and (datasets := params.get("datasets")):
            if params.get("process") is None:
                for dataset in datasets:
                    dataset_inst = config_inst.get_dataset(dataset)
                    if dataset_inst.is_mc:
                        params["process"] = dataset_inst.processes.get_first().name
                        continue
        return params

    def workflow_requires(self):
        reqs = super().workflow_requires()
        reqs["trig_sf"] = self.reqs.TriggerScaleFactors.req(
            self,
            branch=-1,
            _exclude={"branches"},
        )
        return reqs

    def requires(self):
        return self.reqs.TriggerScaleFactors.req(
            self,
            branch=-1,
            _exclude={"branches"},
        )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.var_bin_cats = {}  # for caching
        self.process_inst = self.config_inst.get_process(self.process)

    def get_plot_parameters(self):
        # convert parameters to usable values during plotting
        params = super().get_plot_parameters()
        dict_add_strict(params, "legend_title", "Processes")
        return params


class PlotTriggerScaleFactors2D(
    PlotTriggerScaleFactorsBase,
    PlotBase2D,
):
    exclude_index = False

    plot_function = PlotBase.plot_function.copy(
        default="columnflow.plotting.plot_functions_2d.plot_2d",
        add_default_to_description=True,
    )

    def full_output(self):
        out = {}
        for sys in ["central", "down", "up"]:
            for vrs in self.loop_variables(include_1d=False):
                vr_insts = [self.trigger_config_inst.get_variable(v) for v in vrs]
                for idx in product(*[range(vr.n_bins) for vr in vr_insts[2:]]):
                    key = (sys,) + vrs[:2] + tuple([(v, i) for v, i in zip(vrs[2:], idx)])
                    name = "_".join(vrs[:2]) + "__" + "_".join([f"{v}_{i}" for v, i in zip(vrs[2:], idx)])
                    out[key] = [self.target(name) for name in self.get_plot_names("sf_2d_" + name + "_" + sys)]
        return out

    @law.decorator.log
    def run(self):
        import numpy as np

        scale_factors = self.input()["collection"][0]["sf"].load(formatter="pickle")
        sys, vr1, vr2, *other_vars = self.branch_data

        index = dict(other_vars) | {"systematic": sys}
        key = "_".join([vr1, vr2, *dict(other_vars)])

        hist2d = scale_factors[key][index]

        label_values = np.round(hist2d.values(), decimals=2)
        style_config = {
            "plot2d_cfg": {"cmap": "PiYG", "labels": label_values},
            "annotate_cfg": {"bbox": dict(alpha=0.5, facecolor="white")},
        }

        p_cat = self.baseline_cat(add="\n".join([f"{vr}: bin {i}" for vr, i in other_vars]))
        fig, _ = self.call_plot_func(
            self.plot_function,
            hists={self.process_inst: hist2d},
            config_inst=self.config_inst,
            category_inst=p_cat,
            variable_insts=[self.trigger_config_inst.get_variable(vr).copy_shallow() for vr in [vr1, vr2]],
            style_config=style_config,
            **self.get_plot_parameters(),
        )
        for p in self.output():
            p.dump(fig, formatter="mpl")


class PlotTriggerScaleFactors1D(
    PlotTriggerScaleFactorsBase,
    PlotBase1D,
):
    exclude_index = False

    plot_function = PlotBase.plot_function.copy(
        default="columnflow.plotting.cmsGhent.plot_functions_1d.plot_1d_line",
        add_default_to_description=True,
    )

    def full_output(self):
        out = {}
        for vrs in self.loop_variables():
            is_nominal = set(vrs) == set(self.trigger_config_inst.main_variables)
            vr_name = "nominal" if is_nominal else "_".join(vrs)
            out[(tuple(vrs), "stat")] = [
                self.target(name) for name in
                self.get_plot_names("sf_1d_stat_" + vr_name)
            ]
            if is_nominal:
                out[(tuple(vrs), "")] = [
                    self.target(name) for name in
                    self.get_plot_names("sf_1d_full_" + vr_name)
                ]
        return out

    @law.decorator.log
    def run(self):
        vrs, syst = self.branch_data
        main_vrs = tuple(self.trigger_config_inst.main_variables)
        is_nominal = vrs == main_vrs

        scale_factors = self.input()["collection"][0]["sf"].load(formatter="pickle")
        sf_hist = scale_factors["_".join(vrs)]
        if len(vrs) == 1 or is_nominal:
            hists = {self.process_inst: sf_hist}
        else:
            sf_nom = scale_factors["_".join(main_vrs)]
            extra_var = self.trigger_config_inst.get_variable([vr for vr in vrs if vr not in main_vrs][0])
            labels = extra_var.x_labels or sf_hist.axes[extra_var.name]
            hists = {"nominal": sf_nom} | {
                lab: sf_hist[{extra_var.name: k}]
                for k, lab in enumerate(labels)
            }

        if syst:
            syst = syst.rstrip("_") + "_"

        for k, hs in hists.items():
            hs = [hs[{"systematic": sys}].values() for sys in ["central", f"{syst}down", f"{syst}up"]]
            # convert down and up variations to up and down errors
            hists[k] = [hs[0]] + [np.abs(h - hs[0]) for h in hs[1:]]

        
        kwargs = self.get_plot_parameters()
        if not kwargs.setdefault("skip_ratio"):
            kwargs["skip_ratio"] = len(hists) == 1
        fig, axes = self.call_plot_func(
            self.plot_function,
            hists=hists,
            config_inst=self.config_inst,
            category_inst=self.baseline_cat(),
            variable_insts=[self.trigger_config_inst.get_variable(v) for v in vrs],
            **kwargs,
        )

        for p in self.output():
            p.dump(fig, formatter="mpl")


class PlotTriggerEfficiencies1D(
    PlotTriggerScaleFactorsBase,
    PlotBase1D,
):
    exclude_index = False

    plot_function = PlotBase.plot_function.copy(
        default="columnflow.plotting.cmsGhent.plot_functions_1d.plot_1d_line",
        add_default_to_description=True,
    )


    def full_output(self):
        out = {}
        for vrs in self.loop_variables():
            is_nominal = set(vrs) == set(self.trigger_config_inst.main_variables)
            if not (is_nominal or len(vrs) == 1):
                continue
            vr_name = "nominal" if is_nominal else "_".join(vrs)
            out[(tuple(vrs), "")] = [
                self.target(name) for name in
                self.get_plot_names("eff_1d_stat_" + vr_name)
            ]
        return out

    @law.decorator.log
    def run(self):
        vrs, syst = self.branch_data

        efficiencies = self.input()["collection"][0]["eff"].load(formatter="pickle")
        hists = {}
        for k, hs in efficiencies["_".join(vrs)].items():
            hs = [hs[{"systematic": sys}].values() for sys in ["central", f"{syst}down", f"{syst}up"]]
            # convert down and up variations to up and down errors
            hists[k] = [hs[0]] + [np.abs(h - hs[0]) for h in hs[1:]]

        kwargs = dict(skip_ratio=len(hists) == 1) | self.get_plot_parameters()
        fig, axes = self.call_plot_func(
            self.plot_function,
            hists=hists,
            config_inst=self.config_inst,
            category_inst=self.baseline_cat(),
            variable_insts=[self.trigger_config_inst.get_variable(v) for v in vrs],
            **kwargs,
        )

        for p in self.output():
            p.dump(fig, formatter="mpl")


class PlotTriggerScaleFactorsHist(
    OutputBranchWorkflow,
    TrigPlotLabelMixin,
    TriggerDatasetsMixin,
    SelectionEfficiencyHistMixin,
    SelectorMixin,
    CalibratorsMixin,
    law.LocalWorkflow,
    RemoteWorkflow,
    PlotBase1D,
):
    exclude_index = False

    plot_function = PlotBase.plot_function.copy(
        default="columnflow.plotting.plot_functions_1d.plot_variable_per_process",
        add_default_to_description=True,
    )

    sandbox = dev_sandbox(law.config.get("analysis", "default_columnar_sandbox"))

    def full_output(self):
        out = {}
        for tr, vr in product(["ref", "trig"], self.trigger_config_inst.variables):
            name = f"proj_{tr}_{vr.name}"
            out[(tr, vr.name)] = [self.target(name) for name in self.get_plot_names(name)]
        return out

    def get_plot_parameters(self):
        # convert parameters to usable values during plotting
        params = super().get_plot_parameters()
        dict_add_strict(params, "legend_title", "Processes")
        return params

    @law.decorator.log
    def run(self):
        hist_name = self.trigger_config_inst.config_name + "_efficiencies"
        histograms = self.read_hist(self.trigger_config_inst.variables, hist_name)

        trig_label, vr = self.branch_data
        vr = self.trigger_config_inst.get_variable(vr)

        p_cat = self.baseline_cat()

        p_cat.label += "\n" + self.ref_trigger
        # reduce all variables but the one considered
        idx = {ivr.name: ivr.x("reduce", sum) for ivr in self.trigger_config_inst.variables if ivr != vr}
        idx[self.ref_trigger] = 1
        if trig_label == "trig":
            p_cat.label += " & " + self.trigger
            idx[self.trigger] = 1

        fig, axes = self.call_plot_func(
            self.plot_function,
            hists={p.processes.get_first(): histograms[p][idx].project(vr.name) for p in histograms},
            category_inst=p_cat,
            config_inst=self.config_inst,
            variable_insts=[vr],
            **self.get_plot_parameters(),
        )

        for p in self.output():
            p.dump(fig, formatter="mpl")
