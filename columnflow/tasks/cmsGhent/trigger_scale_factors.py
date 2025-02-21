from __future__ import annotations

import re
import law
from collections.abc import Iterator
from collections import defaultdict
import order as od
from itertools import product
import luigi

from columnflow.types import Any
from columnflow.tasks.framework.base import Requirements
from columnflow.tasks.framework.mixins import (
    CalibratorsMixin, SelectorMixin, DatasetsMixin,
)
from columnflow.tasks.framework.plotting import (
    PlotBase, PlotBase2D, PlotBase1D,
)
from columnflow.tasks.cmsGhent.selection_hists import SelectionEfficiencyHistMixin, CustomDefaultVariablesMixin
from columnflow.production.cmsGhent.trigger import TriggerSFConfig
import columnflow.production.cmsGhent.trigger.util as util
from columnflow.tasks.framework.remote import RemoteWorkflow
from columnflow.util import dev_sandbox, dict_add_strict, maybe_import


np = maybe_import("numpy")
hist = maybe_import("hist")

logger = law.logger.get_logger(__name__)


class TriggerScaleFactorsBase(
    CustomDefaultVariablesMixin,
    SelectorMixin,
    CalibratorsMixin,
    law.LocalWorkflow,
    RemoteWorkflow,
):
    exclude_index = True
    trigger_config = luigi.Parameter(description="Trigger config to use to measure", default=None)

    sandbox = dev_sandbox(law.config.get("analysis", "default_columnar_sandbox"))

    @classmethod
    def get_trigger_config(cls, config_inst, name=None):
        if name is None:
            return config_inst.x("trigger_sf", config_inst.x.trigger_sfs[0])
        for cfg in config_inst.x.trigger_sfs:
            if cfg.config_name == name:
                return cfg
        AssertionError(
            f"could not find trigger config {name}.\n"
            "Available: " + ", ".join([cfg.config_name for cfg in config_inst.x.trigger_sfs])
        )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.variable_insts: list[od.Variable] = list(map(self.config_inst.get_variable, self.variables))

        # auxiliary variables mapped to how they should be treated for the nominal scale factors
        # An integer means the nominal is calculated in the corresponding bin.
        # "sum" means the variable is auxiliary over.
        self.aux_variable_insts: dict[od.Variable, int | sum] = {
            v: v.x("auxiliary") for v in self.variable_insts
            if v.aux.get("auxiliary") is not None
        }

        # variable in which the nominal variables are binned
        self.nonaux_variable_insts = [v for v in self.variable_insts if v.aux.get("auxiliary") is None]

        self.trigger_config_inst: TriggerSFConfig = self.get_trigger_config(self.config_inst, self.trigger_config)
        self.trigger_config = self.trigger_config_inst.config_name
        self.trigger = self.trigger_config_inst.tag
        self.ref_trigger = self.trigger_config_inst.ref_tag

    def loop_variables(self) -> Iterator[tuple[str]]:
        tcfg = self.trigger_config_inst
        for var in tcfg.variable_names:
            # 1d efficiencies and sf
            yield var,

            # fully binned efficiency in  main variables with additional variables
            if var in tcfg.main_variables[1:] or len(tcfg.main_variables) == len(tcfg.variables) == 1:
                # don't repeat multiple times same combinations
                continue

            if var not in (vrs := tcfg.main_variables):
                vrs = sorted({var, *vrs}, key=tcfg.variables.index)
            yield tuple(vrs)

    def data_mc_keys(self, suff=""):
        """
        get data and mc key with suffix (omitted if empty)
        """
        return [f"{dt_type}{'' if not suff else '_' + suff}" for dt_type in ["data", "mc"]]


class TriggerDatasetsMixin(
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
    def get_default_variables(self, params):
        if not (config_inst := params.get("config_inst")):
            return []

        if (trigger_sf_cfg := config_inst.x("trigger_sf", config_inst.x("trigger_sfs", [None])[0])) is None:
            return []
        return trigger_sf_cfg.variable_names


class TriggerScaleFactors(
    TriggerDatasetsMixin,
    SelectionEfficiencyHistMixin,
    TriggerScaleFactorsBase,
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
        histograms = self.read_hist(self.variable_insts, hist_name)
        store_hists = dict()

        collect_hists = util.collect_hist(histograms)

        efficiencies = {}
        scale_factors: dict[str, hist.Hist] = {}
        triggers = [self.trigger, self.ref_trigger]
        for vrs in self.loop_variables():
            key = "_".join(vrs)

            # calculate efficiency binned in given variables
            red_hist = {dt: util.reduce_hist(h, exclude=vrs + triggers) for dt, h in collect_hists.items()}
            efficiencies[key] = eff = {dt: calc_eff(h) for dt, h in red_hist.items()}

            # calculate sf from efficiencies
            eff_ct = {dt: eff[dt][{"systematic": "central"}] for dt in eff}
            sf = util.syst_hist(eff_ct["data"].axes, syst_name="central",
                                arrays=eff_ct["data"].values() / eff_ct["mc"].values())

            if set(law.util.make_list(vars)) == set(tcfg.main_variables):
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


class TrigPlotLabelMixin():

    baseline_label = luigi.Parameter(
        default="",
        description="Label for the baseline selection.",
    )

    def bin_label(self, index: dict[od.Variable | str, int]):
        index = {self.config_inst.get_variable(vr): bn for vr, bn in index.items()}
        return "\n".join([
            f"{vr.name}: bin {bn}" if vr.x_labels is None else vr.x_labels[bn]
            for vr, bn in index.items()
        ])

    def baseline_cat(self, add: od.Category = None, exclude: list[str] = tuple()):
        p_cat = od.Category(name=self.baseline_label)
        if add is not None and add.label:
            p_cat.label += "\n" + add.label
        if not hasattr(self, "aux_variable_insts"):
            return p_cat
        if aux_label := self.bin_label({
            v: i
            for v, i in self.aux_variable_insts.items()
            if isinstance(i, int) and v.name not in exclude
        }):
            p_cat.label += "\n" + aux_label
        return p_cat


class OutputBranchMixin:
    def full_output(self):
        return []

    def create_branch_map(self):
        return list(self.full_output())

    def output(self):
        return self.full_output()[self.branch_data]


class TriggerScaleFactorsPlotBase(
    OutputBranchMixin,
    TrigPlotLabelMixin,
    TriggerDatasetsMixin,
    TriggerScaleFactorsBase,
    SelectorMixin,
    CalibratorsMixin,
    law.LocalWorkflow,
    RemoteWorkflow,
):
    exclude_index = True

    reqs = Requirements(
        RemoteWorkflow.reqs,
        TriggerScaleFactors=TriggerScaleFactors,
    )

    process = luigi.Parameter(
        default=None,
        description="process to represent MC",
        significant=False,
    )

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

    # def loop_variables(
    #     self,
    #     nonaux: slice | bool = True,
    #     aux: od.Variable | None = None,
    # ) -> Iterator[od.Category]:
    #     for index in super().loop_variables(nonaux, aux):
    #         cat_name = "__".join([f"{vr}_{bn}" for vr, bn in index.items()])
    #         if not cat_name:
    #             cat_name = "nominal"
    #         if cat_name not in self.var_bin_cats:
    #             self.var_bin_cats[cat_name] = od.Category(
    #                 name=cat_name,
    #                 selection=index,
    #                 label=self.bin_label(index),
    #             )
    #         yield self.var_bin_cats[cat_name]

    def get_plot_parameters(self):
        # convert parameters to usable values during plotting
        params = super().get_plot_parameters()
        dict_add_strict(params, "legend_title", "Processes")
        return params


class TriggerScaleFactors2D(
    TriggerScaleFactorsPlotBase,
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
            out |= {
                sys + "__" + cat.name: [self.target(name) for name in self.get_plot_names("sf_" + cat.name + "_" + sys)]
                for cat in self.loop_variables(nonaux=slice(2, None))
            } | {
                sys + "__" + cat.name: [self.target(name) for name in self.get_plot_names("sf_" + cat.name + "_" + sys)]
                for aux_var in self.aux_variable_insts
                for cat in self.loop_variables(nonaux=slice(2, None), aux=aux_var)
            }
        return out

    @law.decorator.log
    def run(self):
        import hist
        import numpy as np

        def make_plot2d(hist2d: hist.Hist, sys: str, cat: od.Category):
            label_values = np.round(hist2d.values(), decimals=2)
            style_config = {
                "plot2d_cfg": {"cmap": "PiYG", "labels": label_values},
                "annotate_cfg": {"bbox": dict(alpha=0.5, facecolor="white")},
            }
            p_cat = self.baseline_cat(add=cat, exclude=cat.name.split("__"))
            fig, _ = self.call_plot_func(
                self.plot_function,
                hists={self.process_inst: hist2d},
                config_inst=self.config_inst,
                category_inst=p_cat,
                variable_insts=[var_inst.copy_shallow() for var_inst in self.nonaux_variable_insts[:2]],
                style_config=style_config,
                **self.get_plot_parameters(),
            )
            for p in self.output():
                p.dump(fig, formatter="mpl")

        scale_factors = self.input()["collection"][0]["sf"].load(formatter="pickle")
        sys, cat_name = self.branch_data.split("__", maxsplit=1)
        cat = self.var_bin_cats[cat_name]
        index = cat.selection | {"systematic": sys}

        sf_key = "nominal"
        if any(auxs := [v.name for v in self.aux_variable_insts if v.name in index]):
            sf_key = auxs[0]
        # scale factor 2d plot
        make_plot2d(scale_factors[sf_key][index], sys, cat)


class TriggerScaleFactors1D(
    TriggerScaleFactorsPlotBase,
    PlotBase1D,
):
    make_plots = law.CSVParameter(
        default=("sf", "eff"),
        significant=False,
        description=("which plots to make. Choose from:\n"
                    "\tsf: scale factor plots\n",
                    "\teff: efficiency plots,\n"),
    )

    plot_function = PlotBase.plot_function.copy(
        default="columnflow.plotting.cmsGhent.plot_functions_1d.plot_1d_line",
        add_default_to_description=True,
    )

    reqs = Requirements(
        RemoteWorkflow.reqs,
        TriggerScaleFactors=TriggerScaleFactors,
    )

    sandbox = dev_sandbox(law.config.get("analysis", "default_columnar_sandbox"))

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if "all" in self.make_plots:
            self.make_plots = ("sf", "eff")

    def requires(self):
        return self.reqs.TriggerScaleFactors.req(
            self,
            branch=-1,
            _exclude={"branches"},
        )

    def full_output(self):
        out = {}
        for vrs in self.loop_variables():
            is_nominal = set(vrs) == set(self.trigger_config_inst.main_variables)
            vr_name = "nominal" if is_nominal else "_".join(vrs)
            for plot_type in self.make_plots:
                if not (is_nominal or len(vrs) == 1 or plot_type == "sf"):
                    continue
                out[(plot_type, tuple(vrs), "stat" if plot_type == "sf" else "")] = [
                    self.target(name) for name in
                    self.get_plot_names(plot_type + "_1d_stat_" + vr_name)
                ]
                if plot_type == "sf" and is_nominal:
                    out[(plot_type, tuple(vrs), "")] = [
                        self.target(name) for name in
                        self.get_plot_names(plot_type + "_1d_full_" + vr_name)
                    ]
        return out


    @law.decorator.log
    def run(self):
        def plot_1d(hists, syst="", **kwargs):
            if syst:
                syst = syst.rstrip("_") +  "_" 
            for k, hs in hists.items():
                hs = [hs[{"systematic": sys}].values() for sys in ["central", f"{syst}down", f"{syst}up"]]
                # convert down and up variations to up and down errors
                hists[k] = [hs[0]] + [np.abs(h - hs[0]) for h in hs[1:]]

            fig, axes = self.call_plot_func(
                self.plot_function,
                hists=hists,
                config_inst=self.config_inst,
                category_inst=self.baseline_cat(),
                variable_insts=[self.trigger_config_inst.get_variable(v) for v in vrs],
                skip_ratio=len(hists) == 1,
                **kwargs,
            )

            for p in self.output():
                p.dump(fig, formatter="mpl")

        plot_type, vrs, syst = self.branch_data
        main_vrs = tuple(self.trigger_config_inst.main_variables)
        is_nominal = vrs == main_vrs

        if plot_type == "sf":
            scale_factors = self.input()["collection"][0][plot_type].load(formatter="pickle")
            sf_hist = scale_factors["_".join(vrs)]
            if len(vrs) == 1 or is_nominal:
                return plot_1d({self.process_inst: sf_hist}, syst)

            sf_nom = scale_factors["_".join(main_vrs)]
            extra_var: od.Variable = self.trigger_config_inst.get_variable([vr for vr in vrs if vr not in main_vrs][0])
            labels = extra_var.x_labels or sf_hist.axes[extra_var.name]
            plot_1d(
                {"nominal": sf_nom} | {
                    lab: sf_hist[{extra_var.name: k}]
                    for k, lab in enumerate(labels)
                },
                syst,
            )
        elif plot_type == "eff":
            efficiencies = self.input()["collection"][0][plot_type].load(formatter="pickle")
            return plot_1d(efficiencies["_".join(vrs)], syst)


class TriggerScaleFactorsHist(
    OutputBranchMixin,
    TrigPlotLabelMixin,
    TriggerScaleFactors,
    PlotBase1D,
):

    plot_function = PlotBase.plot_function.copy(
        default="columnflow.plotting.plot_functions_1d.plot_variable_per_process",
        add_default_to_description=True,
    )

    baseline_label = TriggerScaleFactorsPlotBase.baseline_label.copy()

    sandbox = dev_sandbox(law.config.get("analysis", "default_columnar_sandbox"))

    def full_output(self):
        out = {}
        for tr, vr in product(["ref", "trig"], self.variable_insts):
            name = f"proj_{tr}_{vr.name}"
            out[name] = [self.target(name) for name in self.get_plot_names(name)]
        return out

    def get_plot_parameters(self):
        # convert parameters to usable values during plotting
        params = super().get_plot_parameters()
        dict_add_strict(params, "legend_title", "Processes")
        return params

    @law.decorator.log
    def run(self):
        hist_name = self.tag_name + "_ref_" + self.ref_trigger.lower() + "_efficiencies"
        histograms = self.read_hist(self.variable_insts, hist_name)

        trig_label, vr = re.findall("proj_(.*?)_(.*?)$", self.branch_data)[0]
        vr = self.config_inst.get_variable(vr)

        p_cat = self.baseline_cat(exclude=[vr])

        p_cat.label += "\n" + self.ref_trigger
        # reduce all variables but the one considered
        idx = {ivr.name: self.aux_variable_insts.get(ivr, sum) for ivr in self.variable_insts if ivr != vr}
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

        if (ll := vr.aux.get("lower_limit", None)) is not None:
            for ax in axes:
                ax.axvspan(-0.5, ll, color="grey", alpha=0.3)
        for p in self.output():
            p.dump(fig, formatter="mpl")
