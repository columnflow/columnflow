from __future__ import annotations

import re
import law
from collections.abc import Iterator
from collections import defaultdict
import order as od
from itertools import product
import luigi

from columnflow.tasks.framework.base import Requirements
from columnflow.tasks.framework.mixins import (
    CalibratorsMixin, VariablesMixin, SelectorMixin,
)
from columnflow.tasks.framework.plotting import (
    PlotBase, PlotBase2D, PlotBase1D,
)
from columnflow.tasks.cmsGhent.selection_hists import SelectionEfficiencyHistMixin


from columnflow.tasks.framework.remote import RemoteWorkflow
from columnflow.util import dev_sandbox, dict_add_strict, maybe_import

np = maybe_import("numpy")
hist = maybe_import("hist")


class TriggerScaleFactorsBase(
    SelectionEfficiencyHistMixin,
    VariablesMixin,
    SelectorMixin,
    CalibratorsMixin,
    law.LocalWorkflow,
    RemoteWorkflow,
):
    exclude_index = True
    trigger = luigi.Parameter(description="Trigger to measure")
    ref_trigger = luigi.Parameter(description="Reference to use", default=None)

    sandbox = dev_sandbox(law.config.get("analysis", "default_columnar_sandbox"))

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
        self.trigger_vr = "HLT." + self.trigger
        self.ref_trigger_vr = "HLT." + self.ref_trigger

    @classmethod
    def get_default_variables(self, params):
        if not (config_inst := params.get("config_inst")):
            return params
        return config_inst.x("analysis_triggers", dict()).get(params["trigger"], (None, None))[1]

    @classmethod
    def resolve_param_values(cls, params):
        if not (trig := params.get("trigger")):
            return params
        cls.tag_name = f"hlt_{trig.lower()}"
        params = super().resolve_param_values(params)
        if not (config_inst := params.get("config_inst")):
            return params
        if not (ref_trigger := params.get("ref_trigger")):
            ref_trigger = config_inst.x("analysis_triggers", dict()).get(trig, (None, None))[0]
            assert ref_trigger is not None, "no default trigger specified in config.x.analysis_triggers"
        params["ref_trigger"] = ref_trigger
        return params

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
                name.append(self.datasets[0])
            else:
                name.append(f"{len(datasets)}_{law.util.create_hash(sorted(datasets))}")
        return "__".join(name)

    def store_parts(self):
        parts = super().store_parts()
        name = f"trigger_{self.trigger}_ref_{self.ref_trigger}"
        parts.insert_before("datasets", "trigger", name)
        return parts

    def loop_variables(
        self,
        nonaux: slice | bool = True,
        aux: od.Variable | None = None,
    ) -> Iterator[tuple[dict[str, int], od.Category] | dict[str, int]]:
        loop_vars = []
        if nonaux:
            if nonaux is True:
                nonaux = slice(None, None)
            loop_vars += self.nonaux_variable_insts[nonaux]
        if aux is not None:
            loop_vars.append(aux)

        for index in np.ndindex(*[v.n_bins for v in loop_vars]):
            index = dict(zip(loop_vars, index))
            yield {vr.name: bn for vr, bn in index.items()}

    def data_mc_keys(self, suff=""):
        """
        get data and mc key with suffix (omitted if empty)
        """
        return [f"{dt_type}{'' if not suff else '_' + suff}" for dt_type in ["data", "mc"]]


class TriggerScaleFactors(TriggerScaleFactorsBase):
    exclude_index = False

    def output(self):
        out = {
            "json": self.target(f"{self.tag_name}_sf.json"),
            "sf": self.target(f"{self.tag_name}_sf.pickle"),
            "eff": self.target(f"{self.tag_name}_eff.pickle"),
            "cnt": self.target(f"{self.tag_name}_counts.pickle"),
            "corr": self.target(f"{self.tag_name}_corr.pickle"),
        }
        return out

    def correlation_efficiency_bias(self, passfailhist):
        """
        calculate efficiecy bias of trigger T w.r.t. reference trigger M based on frequency matrix.
        The bias is defined as Eff(T|M=1) / Eff(T|M=0)

        Returns: bias, bias - 1sig error, bias + 1sig error

        """
        import statsmodels.genmod.generalized_linear_model as glm
        from statsmodels.genmod.families import Binomial
        from statsmodels.genmod.families.links import Log
        from statsmodels.tools.sm_exceptions import DomainWarning

        total = passfailhist[sum, sum]
        passfailmatrix = passfailhist.values() * total.value / total.variance

        assert passfailmatrix.shape == (2, 2), "invalid pass-fail-matrix"

        if np.all(np.abs(passfailmatrix) < 1e-9):
            return (np.nan,) * 2

        trigger_values = np.array([0, 0, 1, 1])
        ref_trigger_values = np.array([0, 1, 0, 1])

        # log Eff(T|M) = b + a * M
        # Eff(T|M = 1) / Eff(T|M = 0) = exp(a)
        with np.testing.suppress_warnings() as sup:
            # Using the Log linking function with binomial values is convenient,
            # but can result in issues since the function does not map to [0, 1].
            # To avoid constant warnings about this, they are suppressed here
            sup.filter(DomainWarning)
            model = glm.GLM(trigger_values,
                            np.transpose([[1, 1, 1, 1],
                                          ref_trigger_values]),
                            freq_weights=passfailmatrix.flatten(),
                            family=Binomial(link=Log()))
            res = model.fit()

        # multiplicative bias = exp(a)
        mult_bias = np.exp(res.params[1])
        # d(exp a) = exp(a) da
        var = res.cov_params()[1, 1] * mult_bias ** 2

        return mult_bias - 1, var

    @law.decorator.log
    def run(self):
        import hist
        import numpy as np
        import correctionlib.convert
        from columnflow.tasks.cmsGhent.Koopman_test import koopman_confint

        bindim = [variable_inst.n_bins for variable_inst in self.nonaux_variable_insts]

        hist_name = self.tag_name + "_ref_" + self.ref_trigger.lower() + "_efficiencies"
        histograms = self.read_hist(self.variable_insts, hist_name)
        efficiencies = {}
        sum_histograms = {}

        def calc_eff(dt_type: str, vr: od.Variable = None, reduce_all=False):
            """
            sum histograms from datasets certain data type, reduce selected axes, and calculate efficiencies.
            Summed histograms are stored in the dict **sum_histograms**. Efficiencies in the dict **efficiencies**.
            Keys are of the form **{dt_type}_{vr.name}_proj** if **reduce_all** else **{dt_type}_{vr.name}**
            By default, all auxiliary variable axes are reduced, unless **reduce_all=True**.
            The axis of the variable **vr** is not reduced. Combine **vr** with **reduce_all** to make 1D efficiencies.

            @param dt_type: data type of the datasets (data or mc)
            @param vr: variable whose axis will not be reduced
            @param reduce_all: if True, reduce all variable axis, otherwise only auxiliaries (always except **vr**)
            """

            h_key = dt_type
            if vr is not None:
                h_key += f"_{vr.name}"
                if reduce_all:
                    h_key += "_proj"

            # sum all mc / data histograms
            sum_histograms[h_key] = sum([histograms[dt] for dt in histograms if getattr(dt, f"is_{dt_type}")])
            assert sum_histograms[h_key] != 0, f"did not find any {dt_type} histograms"

            # choose all variables or only the auxiliary ones to reduce
            all_vars = self.variable_insts if reduce_all else self.aux_variable_insts
            # apply reduction method specified in the dict **self.aux_variable_insts** (sum or slice)
            idx = {v.name: self.aux_variable_insts.get(v, sum) for v in all_vars if v != vr}
            sum_histograms[h_key] = sum_histograms[h_key][idx]

            # counts that pass both triggers
            selected_counts = sum_histograms[h_key][{self.ref_trigger_vr: 1, self.trigger_vr: 1}]
            # counts that pass reference triggers
            incl = sum_histograms[h_key][{self.ref_trigger_vr: 1, self.trigger_vr: sum}]
            # calculate efficiency
            efficiencies[h_key] = self.efficiency(selected_counts, incl)

        # multidim efficiencies with additional auxiliaries
        for dt_type, aux_vr in product(["data", "mc"], [None, *self.aux_variable_insts]):
            calc_eff(dt_type, aux_vr)

        # 1d efficiencies
        for dt_type, aux_vr in product(["data", "mc"], self.variable_insts):
            calc_eff(dt_type, aux_vr, reduce_all=True)

        # save efficiency  and summed hists
        self.output()["cnt"].dump(efficiencies, formatter="pickle")
        self.output()["eff"].dump(efficiencies, formatter="pickle")

        # scale factors

        def calc_sf(suff=""):
            """
            get central scale factor for efficiencies determined by the given suffix
            """
            data, mc = self.data_mc_keys(suff)
            ct_idx = {"systematic": "central"}
            return efficiencies[data][ct_idx] / efficiencies[mc][ct_idx].values()

        nom_centr_sf = calc_sf()
        scale_factors: dict[str, dict[str, hist.Hist]] = defaultdict(dict)
        scale_factors["nominal"] = {"central": nom_centr_sf}

        def up_down_dict(name, err):
            """convenience function"""
            return {f"{name}_down": nom_centr_sf - err, f"{name}_up": nom_centr_sf + err}

        # bias from auxiliary variables: e.g. HT in single electron trigger
        for aux_vr in self.aux_variable_insts:
            sf_aux = calc_sf(aux_vr.name)
            scale_factors[aux_vr.name]["central"] = sf_aux
            aux_idx = sf_aux.axes.name.index(aux_vr.name)
            dev = np.abs(sf_aux.values() - np.expand_dims(nom_centr_sf.values(), axis=aux_idx))
            scale_factors["nominal"] |= up_down_dict(aux_vr.name, np.max(dev, axis=aux_idx))

        # koopman-based statistical error
        def calc_staterr(aux_vr: od.Variable = None):
            """
            get scale factor uncertainties for efficiencies determined by the given suffix. Allow
            for one additional auxiliary variable in the binning.
            """
            container_dim = bindim
            if aux_vr is not None:
                container_dim = [vr.n_bins for vr in self.variable_insts if vr in [aux_vr, *self.nonaux_variable_insts]]

            container = np.ones((2, *container_dim))
            for idx in self.loop_variables(aux=aux_vr):
                data, mc = self.data_mc_keys("" if aux_vr is None else aux_vr.name)
                t_idx = idx | {self.ref_trigger_vr: 1}
                data = sum_histograms[data][t_idx]
                mc = sum_histograms[mc][t_idx]
                inputs = [x.value for x in [data[1], data[sum], mc[1], mc[sum]]]
                container[(..., *idx.values())] = koopman_confint(*inputs)
            out = {}
            for dr, c in zip(["stat_down", "stat_up"], container):
                out[dr] = scale_factors["nominal" if aux_vr is None else aux_vr.name]["central"].copy()
                out[dr].values()[:] = c
            return out

        scale_factors["nominal"] |= calc_staterr()
        for aux_vr in self.aux_variable_insts:
            scale_factors[aux_vr.name] |= calc_staterr(aux_vr=aux_vr)

        # bias in efficiency because of correlation
        corr_bias = {
            "all": hist.Hist(
                *scale_factors["nominal"]["central"].axes,
                name="correlation bias",
                label=f"correlation bias for {self.trigger} trigger with reference {self.ref_trigger} "
                      f"for year {self.config_inst.x.year} "
                      f"(binned in {', '.join([vr.name for vr in self.nonaux_variable_insts])})",
                storage=hist.storage.Weight(),
            ),
        }

        for idx in self.loop_variables(aux=None):
            pf = sum_histograms["mc"][idx]
            corr_bias["all"][idx] = self.correlation_efficiency_bias(pf)
            scale_factors["nominal"] |= up_down_dict("corr", corr_bias["all"].values() * nom_centr_sf.values())

        # correlation bias in 1D efficiencies
        for vr in self.nonaux_variable_insts:
            corr_bias[vr.name] = hist.Hist(
                scale_factors["nominal"]["central"].axes[vr.name],
                name=f"correlation bias ({vr.name})",
                label=f"correlation bias for {self.trigger} trigger with reference {self.ref_trigger} "
                      f"for year {self.config_inst.x.year} (binned in {vr.name})",
                storage=hist.storage.Weight(),
            )
            # trigger must be along first axis!
            pf = sum_histograms["mc"].project(self.trigger_vr, self.ref_trigger_vr, vr.name)

            for idx in range(vr.n_bins):
                corr_bias[vr.name][idx] = self.correlation_efficiency_bias(pf[{vr.name: idx}])

            # broad cast projected hist back to nominal binning
            vr_idx = nom_centr_sf.axes.name.index(vr.name)
            bias = np.expand_dims(corr_bias[vr.name], np.delete(np.arange(nom_centr_sf.ndim), vr_idx).tolist())
            bias = bias.value

            scale_factors["nominal"] |= up_down_dict("corr_" + vr.name, bias * nom_centr_sf.values())

        # inclusive correlations
        corr_bias["incl"] = hist.Hist(
            name="correlation bias (incl)",
            label=f"correlation bias for {self.trigger} trigger with reference {self.ref_trigger} "
                  f"for year {self.config_inst.x.year} (inclusive)",
            storage=hist.storage.Weight(),
        )
        pf = sum_histograms["mc"].project(self.trigger_vr, self.ref_trigger_vr)
        corr_bias["incl"][...] = self.correlation_efficiency_bias(pf)

        # store correlations
        self.output()["corr"].dump(corr_bias, formatter="pickle")

        scale_factors["nominal"] |= up_down_dict("corr_incl", corr_bias["incl"][...].value * nom_centr_sf.values())

        sf_hists = {}
        for sf_type, arrays in scale_factors.items():
            ct = arrays.pop("central")
            hst = hist.Hist(
                hist.axis.StrCategory(["central", "down", "up", *arrays], name="systematic"),
                *ct.axes,
                name="scale_factors",
                label=f"trigger scale factors for {self.trigger} trigger with reference {self.ref_trigger} "
                      f"for year {self.config_inst.x.year}",
                storage=hist.storage.Weight(),
            )
            hst.values()[(idx := 0)] = ct.values()
            # add quadratic sum of all uncertainties
            for dr in ["down", "up"]:
                variance = 0
                for err in [v.name for v in self.aux_variable_insts] + ["stat", "corr_incl"]:
                    if f"{err}_{dr}" not in arrays:
                        continue
                    variance = variance + (arrays[f"{err}_{dr}"].values() - ct.values()) ** 2
                hst.values()[(idx := idx + 1)] = ct.values() - (-1) ** idx * np.sqrt(variance)

            # add remaining uncertainties
            hst.values()[idx + 1:] = [h.values() for h in arrays.values()]

            # remove variances
            hst.variances()[:] = 1

            sf_hists[sf_type] = hst

        # save sf histograms
        self.output()["sf"].dump(sf_hists, formatter="pickle")

        # save nominal as correctionlib file (not possible to specify the flow for each variable separately)
        clibcorr = correctionlib.convert.from_histogram(sf_hists["nominal"], flow="clamp")
        clibcorr.description = sf_hists["nominal"].label

        cset = correctionlib.schemav2.CorrectionSet(
            schema_version=2, description=sf_hists["nominal"].label, corrections=[clibcorr],
        )
        self.output()["json"].dump(cset.dict(exclude_unset=True), indent=4, formatter="json")


class TriggerScaleFactorsPlotBase(
    TriggerScaleFactorsBase,
):
    exclude_index = True

    baseline_label = luigi.Parameter(
        default="",
        description="Label for the baseline selection.",
    )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.var_bin_cats = {}  # for caching

    def baseline_cat(self, add: od.Category = None, exclude: list[str] = tuple()):
        p_cat = od.Category(name=self.baseline_label)
        if add is not None and add.label:
            p_cat.label += "\n" + add.label
        if aux_label := self.bin_label({
            v: i
            for v, i in self.aux_variable_insts.items()
            if isinstance(i, int) and v.name not in exclude
        }):
            p_cat.label += "\n" + aux_label
        return p_cat

    def loop_variables(
        self,
        nonaux: slice | bool = True,
        aux: od.Variable | None = None,
    ) -> Iterator[od.Category]:
        for index in super().loop_variables(nonaux, aux):
            cat_name = "__".join([f"{vr}_{bn}" for vr, bn in index.items()])
            if not cat_name:
                cat_name = "nominal"
            if cat_name not in self.var_bin_cats:
                self.var_bin_cats[cat_name] = od.Category(
                    name=cat_name,
                    selection=index,
                    label=self.bin_label(index),
                )
            yield self.var_bin_cats[cat_name]

    def bin_label(self, index: dict[od.Variable | str, int]):
        index = {self.config_inst.get_variable(vr): bn for vr, bn in index.items()}
        return "\n".join([
            f"{vr.name}: bin {bn}" if vr.x_labels is None else vr.x_labels[bn]
            for vr, bn in index.items()
        ])

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

    reqs = Requirements(
        TriggerScaleFactorsPlotBase.reqs,
        TriggerScaleFactors=TriggerScaleFactors,
    )

    def requires(self):
        return self.reqs.TriggerScaleFactors.req(
            self,
            branch=-1,
            _exclude={"branches"},
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

    def create_branch_map(self):
        return list(self.full_output())

    def output(self):
        return self.full_output()[self.branch_data]

    @law.decorator.log
    def run(self):
        import hist
        import numpy as np

        plot_process: od.Process = self.config_inst.get_process(self.processes[-1])

        def make_plot2d(hist2d: hist.Hist, sys: str, cat: od.Category):
            label_values = np.round(hist2d.values(), decimals=2)
            style_config = {
                "plot2d_cfg": {"cmap": "PiYG", "labels": label_values},
                "annotate_cfg": {"bbox": dict(alpha=0.5, facecolor="white")},
            }
            p_cat = self.baseline_cat(add=cat, exclude=cat.name.split("__"))
            fig, _ = self.call_plot_func(
                self.plot_function,
                hists={plot_process: hist2d},
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
        default=tuple(),
        significant=False,
        description=("which plots to make. Choose from:\n"
                    "\tcorr: correlation plots\n",
                    "\tsf_1d: 1d scale factor plots\n",
                    "\teff_1d: 1d efficiency plots,\n"),
    )

    plot_function = PlotBase.plot_function.copy(
        default="singletop.plotting.plot_1d_line",
        add_default_to_description=True,
    )

    reqs = Requirements(
        TriggerScaleFactorsPlotBase.reqs,
        TriggerScaleFactors=TriggerScaleFactors,
    )

    sandbox = dev_sandbox(law.config.get("analysis", "default_columnar_sandbox"))

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if "all" in self.make_plots:
            self.make_plots = ("sf_1d", "eff_1d", "corr")

    def requires(self):
        return self.reqs.TriggerScaleFactors.req(
            self,
            branch=-1,
            _exclude={"branches"},
        )

    def full_output(self):
        out = {}
        if "sf_1d" in self.make_plots:
            out["sf_1d_stat"] = [self.target(name) for name in self.get_plot_names("sf_nominal_1d_stat")]
            out["sf_1d_full"] = [self.target(name) for name in self.get_plot_names("sf_nominal_1d_full")]

            for aux_var in self.aux_variable_insts:
                n_group = aux_var.aux.get("group_bins", 3)
                for i in range(0, aux_var.n_bins, n_group):
                    name = "sf_1d_" + aux_var.name
                    name += "" if aux_var.n_bins <= n_group else f"_{i}:{i + n_group}"
                    out[name] = [self.target(name) for name in self.get_plot_names(name)]

        if "eff_1d" in self.make_plots:
            out["eff_1d"] = [self.target(name) for name in self.get_plot_names("eff_1d")]
            for vr in self.variable_insts:
                out[f"eff_1d_proj_{vr.name}"] = [
                    self.target(name) for name in self.get_plot_names(f"eff_proj_{vr.name}")
                ]

        if "corr" in self.make_plots:
            for vr in ["all"] + [variable_inst.name for variable_inst in self.nonaux_variable_insts]:
                out[f"corr_{vr}"] = [self.target(name) for name in self.get_plot_names(f"corr_{vr}")]
        return out

    def create_branch_map(self):
        return list(self.full_output())

    def output(self):
        return self.full_output()[self.branch_data]

    def get_hists(self, h: hist.Hist, unc=""):
        if unc:
            unc += "_"
        hs = [h[{"systematic": sys}].values() for sys in ["central", f"{unc}down", f"{unc}up"]]
        # convert down and up variations to up and down errors
        return [hs[0], *[np.abs(h - hs[0]) for h in hs[1:]]]

    @law.decorator.log
    def run(self):
        import numpy as np

        plot_process: od.Process = self.config_inst.get_process(self.processes[-1])
        od.Category(name=self.baseline_label)

        def plot_1d(key: str, hists, vrs=None, **kwargs):
            vrs = self.nonaux_variable_insts if vrs is None else vrs

            # exclude auxiliary baseline selection variables if requested
            p_cat = self.baseline_cat(exclude=[vr.name for vr in vrs])

            fig, axes = self.call_plot_func(
                self.plot_function,
                hists=hists,
                config_inst=self.config_inst,
                category_inst=p_cat,
                variable_insts=vrs,
                skip_ratio=len(hists) == 1,
                **kwargs,
            )
            if (ll := vrs[0].aux.get("lower_limit", None)) is not None:
                if len(vrs) > 1:
                    ll = np.searchsorted(vrs[0].bin_edges, ll)
                    for vr in vrs[1:]:
                        ll *= vr.n_bins
                    ll -= 0.5
                for ax in axes:
                    ax.axvspan(-0.5, ll, color="grey", alpha=0.3)
            for p in self.output():
                p.dump(fig, formatter="mpl")

        if "sf_1d" in self.branch_data:
            scale_factors = self.input()["collection"][0]["sf"].load(formatter="pickle")

            # scale factor flat plot with full errors
            if self.branch_data == "sf_1d_full":
                plot_1d("sf_1d_full", {plot_process: self.get_hists(scale_factors["nominal"])})
                return
                # scale factor flat plot with stat errors
            sf_flat = self.get_hists(scale_factors["nominal"], unc="stat")
            if self.branch_data == "sf_1d_stat":
                plot_1d("sf_1d_stat", {plot_process: sf_flat})
                return

            # convert branch to aux variable and bin group
            aux_vr, group = re.findall("^sf_1d_(.*?)_*([\\d+:\\d+]*)$", self.branch_data, re.DOTALL)[0]
            i0, i1 = [int(x) for x in group.split(":")]
            aux_vr = self.config_inst.get_variable(aux_vr)
            aux_bins = list(self.loop_variables(nonaux=False, aux=aux_vr))[i0:i1]
            plot_1d(
                self.branch_data,
                {"nominal": sf_flat} | {
                    cat.label: self.get_hists(scale_factors[aux_vr.name][cat.selection]) for cat in aux_bins
                },
            )

        if "corr" in self.branch_data:
            corr_bias = self.input()["collection"][0]["corr"].load(formatter="pickle")
            get_arr = lambda h: [h.values(), np.sqrt(h.variances())]
            # correlation plot
            style_config = {"ax_cfg": {"ylim": (-0.1, 0.1)}}
            if self.branch_data == "corr_all":
                plot_1d(
                    "corr_all",
                    {plot_process: get_arr(corr_bias["all"])},
                    style_config=style_config,
                )
                return
            vr = re.findall("corr_(.*)", self.branch_data)[0]
            plot_1d(
                f"corr_{vr}",
                {plot_process: get_arr(corr_bias[vr])},
                vrs=[self.config_inst.get_variable(vr)],
                style_config=style_config,
            )

        if "eff_1d" in self.branch_data:
            efficiencies = self.input()["collection"][0]["eff"].load(formatter="pickle")
            if self.branch_data == "eff_1d":
                hists = {dt: self.get_hists(efficiencies[dt]) for dt in self.data_mc_keys()}
                plot_1d("eff_1d", hists[::-1])  # reverse to mc data (first entry is denominator)
                return

            vr = re.findall("eff_1d_proj_(.*)", self.branch_data)[0]
            vr_inst = self.config_inst.get_variable(vr)

            suff = f"{vr}_proj"

            hists = {dt[:-len(suff) - 1]: self.get_hists(efficiencies[dt]) for dt in self.data_mc_keys(suff)}

            fig, axes = self.call_plot_func(
                self.plot_function,
                hists=hists[::-1],
                skip_ratio=False,
                category_inst=self.baseline_cat(exclude=[vr]),
                config_inst=self.config_inst,
                variable_insts=[vr_inst],
            )
            if (ll := vr_inst.aux.get("lower_limit", None)) is not None:
                for ax in axes:
                    ax.axvspan(-0.5, ll, color="grey", alpha=0.3)
            for p in self.output():
                p.dump(fig, formatter="mpl")


class TriggerScaleFactorsHist(
    TriggerScaleFactorsBase,
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

    def create_branch_map(self):
        return list(self.full_output())

    def output(self):
        return self.full_output()[self.branch_data]

    def get_plot_parameters(self):
        # convert parameters to usable values during plotting
        params = super().get_plot_parameters()
        dict_add_strict(params, "legend_title", "Processes")
        return params

    @law.decorator.log
    def run(self):
        hist_name = self.tag_name + "_ref_" + self.ref_trigger.lower() + "_efficiencies"
        histograms = self.read_hist(self.variable_insts, hist_name)

        trig_label, vr = re.findall("proj_(.*?)_(.*?)", self.branch_data)[0]
        vr = self.config_inst.get_variable(vr)

        p_cat = od.Category(name=self.baseline_label)
        if not isinstance(self.aux_variable_insts.get(vr, None), int):
            p_cat.label += "\n" + self.aux_label

        p_cat.label += "\n" + self.ref_trigger
        # reduce all variables but the one considered
        idx = {ivr.name: self.aux_variable_insts.get(ivr, sum) for ivr in self.variable_insts if ivr != vr}
        idx[self.ref_trigger_vr] = 1
        if trig_label == "trig":
            p_cat.label += " & " + self.trigger
            idx[self.trigger_vr] = 1

        fig, axes = self.call_plot_func(
            self.plot_function,
            hists={p: histograms[p][idx].project(vr.name) for p in histograms},
            category_inst=p_cat,
            config_inst=self.config_inst,
            variable_insts=[vr],
            **self.get_plot_parameter(),
        )
        if (ll := vr.aux.get("lower_limit", None)) is not None:
            for ax in axes:
                ax.axvspan(-0.5, ll, color="grey", alpha=0.3)
        for p in self.output():
            p.dump(fig, formatter="mpl")
