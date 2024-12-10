from __future__ import annotations

import law
from collections.abc import Iterator
import order as od
from itertools import product
import luigi

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


class TriggerScaleFactors(
    SelectionEfficiencyHistMixin,
    VariablesMixin,
    SelectorMixin,
    CalibratorsMixin,
    law.LocalWorkflow,
    RemoteWorkflow,
    PlotBase2D,
):

    trigger = luigi.Parameter(description="Trigger to measure")
    ref_trigger = luigi.Parameter(description="Reference to use")

    make_plots = law.CSVParameter(
        default=tuple(),
        significant=False,
        description=("which plots to make. Choose from:\n"
                    "\tcorr: correlation plots\n",
                    "\tsf_2d: 2d scale factor plots\n",
                    "\tsf_1d: 1d scale factor plots\n",
                    "\teff_1d: 1d efficiency plots,\n"
                    "\thist: plots of event counts,\n"),
    )
    plot_function = PlotBase.plot_function.copy(
        default="columnflow.plotting.plot_functions_2d.plot_2d",
        add_default_to_description=True,
    )

    plot_function_1d_line = PlotBase.plot_function.copy(
        default="singletop.plotting.plot_1d_line",
        add_default_to_description=True,
    )

    plot_function_1d_hist = PlotBase.plot_function.copy(
        default="columnflow.plotting.plot_functions_1d.plot_variable_per_process",
        add_default_to_description=True,
    )

    hist1d_skip_ratio = PlotBase1D.skip_ratio.copy()
    hist1d_density = PlotBase1D.density.copy()
    hist1d_yscale = PlotBase1D.yscale.copy()
    hist1d_shape_norm = PlotBase1D.shape_norm.copy()
    hist1d_hide_errors = PlotBase1D.hide_errors.copy()

    sandbox = dev_sandbox(law.config.get("analysis", "default_columnar_sandbox"))

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.variable_insts: list[od.Variable] = list(map(self.config_inst.get_variable, self.variables))
        self.aux_variable_insts: dict[od.Variable, int | sum] = {
            v: v.x("auxiliary") for v in self.variable_insts
            if v.aux.get("auxiliary") is not None
        }
        self.aux_label = self.bin_label({v: i for v, i in self.aux_variable_insts.items() if isinstance(i, int)})
        self.nonaux_variable_insts = [v for v in self.variable_insts if v.aux.get("auxiliary") is None]
        self.var_bin_cats = {}
        if "all" in self.make_plots:
            self.make_plots = ("sf_1d", "sf_2d", "eff_1d", "corr", "hist")

        self.trigger_vr = "HLT." + self.trigger
        self.ref_trigger_vr = "HLT." + self.ref_trigger

    @classmethod
    def resolve_param_values(cls, params):
        cls.tag_name = f"hlt_{params['trigger'].lower()}"
        params = super().resolve_param_values(params)
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

    def bin_label(self, index: dict[od.Variable, int]):
        return "\n".join([
            f"{vr.name}: bin {bn}" if vr.x_labels is None else vr.x_labels[bn]
            for vr, bn in index.items()
        ])

    def store_parts(self):
        parts = super().store_parts()
        name = f"trigger_{self.trigger}_ref_{self.ref_trigger}"
        parts.insert_before("datasets", "trigger", name)
        return parts

    def loop_variables(
        self,
        nonaux: slice | bool = True,
        aux: od.Variable | None = None,
        make_cat=False,
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
            # create a dummy category for plotting
            if make_cat:
                cat_name = "__".join([f"{vr.name}_{bn}" for vr, bn in index.items()])
                if not cat_name:
                    cat_name = "nominal"
                if cat_name not in self.var_bin_cats:
                    self.var_bin_cats[cat_name] = od.Category(name=cat_name, label=self.bin_label(index))
                yield {vr.name: bn for vr, bn in index.items()}, self.var_bin_cats[cat_name]
            else:
                yield {vr.name: bn for vr, bn in index.items()}

    def output(self):
        out = {
            "stats": self.target(".".join(
                self.get_plot_names(f"{self.tag_name}_efficiency")[0].split(".")[:-1],
            ) + ".json"),
        }

        if "sf_2d" in self.make_plots:
            out["sf_2d"] = {
                cat.name: [self.target(name) for name in self.get_plot_names("sf_" + cat.name)]
                for _, cat in self.loop_variables(nonaux=slice(2, None), make_cat=True)
            } | {
                cat.name: [self.target(name) for name in self.get_plot_names("sf_" + cat.name)]
                for aux_var in self.aux_variable_insts
                for _, cat in self.loop_variables(nonaux=slice(2, None), make_cat=True, aux=aux_var)
            }

        if "sf_1d" in self.make_plots:
            out["sf_1d"] = [self.target(name) for name in self.get_plot_names("sf_nominal_1d")]
            for aux_var in self.aux_variable_insts:
                n_group = aux_var.aux.get("group_bins", 3)
                for i in range(0, aux_var.n_bins, n_group):
                    name = "sf_1d_" + aux_var.name
                    name += "" if aux_var.n_bins <= n_group else f"_{i}:{i + n_group}"
                    out[name] = [self.target(name) for name in self.get_plot_names(name)]

        if "eff_1d" in self.make_plots:
            out["eff_1d"] = [self.target(name) for name in self.get_plot_names("eff_1d")]
            for vr in self.variable_insts:
                out[f"eff_proj_{vr.name}"] = [self.target(name) for name in self.get_plot_names(f"eff_proj_{vr.name}")]

        if "hist" in self.make_plots:
            for tr, vr in product(["ref", "trig"], self.variable_insts):
                name = f"proj_{tr}_{vr.name}"
                out[name] = [self.target(name) for name in self.get_plot_names(name)]

        if "corr" in self.make_plots:
            for vr in ["all"] + [variable_inst.name for variable_inst in self.nonaux_variable_insts]:
                out[f"corr_{vr}"] = [self.target(name) for name in self.get_plot_names(f"corr_{vr}")]
        return out

    def get_plot_parameters(self):
        # convert parameters to usable values during plotting
        params = super().get_plot_parameters()
        dict_add_strict(params, "legend_title", "Processes")
        return params

    def get_hist1d_plot_parameter(self):
        params = PlotBase.get_plot_parameters(self)
        for pp in ["skip_ratio", "density", "shape_norm", "hide_errors"]:
            params[pp] = getattr(self, f"hist1d_{pp}")
        params["yscale"] = None if self.hist1d_yscale == law.NO_STR else self.control_yscale
        return params

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
        # Eff(T|M = 1) / Eff(T|M = 0) - 1 = exp(a) - 1
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

        # bias = exp(a) - 1
        bias = np.exp(res.params[1])
        # d(exp a) = exp(a) da
        err = (np.sqrt(res.cov_params()[1, 1])) * bias

        return bias - 1, err

    @law.decorator.log
    def run(self):
        import hist
        import numpy as np
        import correctionlib.convert
        from singletop.util.Koopman_test import koopman_confint

        bindim = [variable_inst.n_bins for variable_inst in self.nonaux_variable_insts]

        hist_name = self.tag_name + "_ref_" + self.ref_trigger.lower() + "_efficiencies"
        histograms = self.read_hist(self.variable_insts, hist_name)
        efficiencies = {}
        sum_histograms = {}

        def calc_eff(dt_type: str, vr: od.Variable, proj_all=False):
            """
            sum histograms certain datatype, and calculate efficiencies
            """
            h_key = dt_type
            if aux_vr is not None:
                h_key += f"_{vr.name}"
                if proj_all:
                    h_key += "_proj"
            sum_histograms[h_key] = sum([histograms[dt] for dt in histograms if getattr(dt, f"is_{dt_type}")])
            assert sum_histograms[h_key] != 0, f"did not find any {dt_type} histograms"

            all_vars = self.variable_insts if proj_all else self.aux_variable_insts
            idx = {v.name: self.aux_variable_insts.get(v, sum) for v in all_vars if v != vr}
            sum_histograms[h_key] = sum_histograms[h_key][idx]

            selected_counts = sum_histograms[h_key][{self.ref_trigger_vr: 1, self.trigger_vr: 1}]
            incl = sum_histograms[h_key][{self.ref_trigger_vr: 1, self.trigger_vr: sum}]
            efficiencies[h_key] = self.efficiency(selected_counts, incl)

        # multidim efficiencies with additional auxiliaries
        for dt_type, aux_vr in product(["data", "mc"], [None, *self.aux_variable_insts]):
            calc_eff(dt_type, aux_vr)

        # 1d efficiencies
        for dt_type, aux_vr in product(["data", "mc"], self.variable_insts):
            calc_eff(dt_type, aux_vr, proj_all=True)

        # scale factors
        def data_mc_keys(suff):
            """
            get data and mc key with suffix (omitted if empty)
            """
            return [f"{dt_type}{'' if not suff else '_' + suff}" for dt_type in ["data", "mc"]]

        def calc_sf(suff=""):
            """
            get scale factor for efficiencies determined by the given suffix
            """
            data, mc = data_mc_keys(suff)
            return efficiencies[data][0] / efficiencies[mc][0].values()

        scale_factors: dict[str, hist.Hist] = {"nominal": calc_sf()}

        # bias auxiliary
        aux_bias = {}
        for aux_vr in self.aux_variable_insts:
            scale_factors[aux_vr.name] = calc_sf(aux_vr.name)
            aux_idx = scale_factors[aux_vr.name].axes.name.index(aux_vr.name)
            dev = np.abs(scale_factors[aux_vr.name].values() -
                         np.expand_dims(scale_factors["nominal"].values(), axis=aux_idx))
            aux_bias[aux_vr.name] = np.max(dev, axis=aux_idx)

        # koopman-based statistical error
        def calc_staterr(aux_vr: od.Variable = None):
            """
            get scale factor uncertainties for efficiencies determined by the given suffix
            """
            container_dim = bindim
            if aux_vr is not None:
                container_dim = [vr.n_bins for vr in self.variable_insts if vr in [aux_vr, *self.nonaux_variable_insts]]

            container = np.ones((2, *container_dim))
            for idx in self.loop_variables(aux=aux_vr):
                data, mc = data_mc_keys("" if aux_vr is None else aux_vr.name)
                t_idx = idx | {self.ref_trigger_vr: 1}
                data = sum_histograms[data][t_idx]
                mc = sum_histograms[mc][t_idx]
                inputs = [x.value for x in [data[1], data[sum], mc[1], mc[sum]]]
                container[(..., *idx.values())] = koopman_confint(*inputs)
            return container

        scale_factors_staterr = {
            "nominal": calc_staterr(),
        }
        for aux_vr in self.aux_variable_insts:
            scale_factors_staterr[aux_vr.name] = calc_staterr(aux_vr=aux_vr)

        scale_factors_staterr = {x: np.abs(err - scale_factors[x].values()) for x, err in scale_factors_staterr.items()}

        # bias because of correlation
        corr_bias = {"all": np.zeros((2, *bindim))}
        for idx in self.loop_variables(aux=None):
            pf = sum_histograms["mc"][idx]
            corr_bias["all"][(..., *idx.values())] = self.correlation_efficiency_bias(pf)

        for vr in self.nonaux_variable_insts:
            corr_bias[vr.name] = np.zeros((2, vr.n_bins))
            for idx in range(vr.n_bins):
                # trigger must be along first axis!
                pf = sum_histograms["mc"].project(self.trigger_vr, self.ref_trigger_vr, vr.name)[{vr.name: idx}]
                corr_bias[vr.name][:, idx] = self.correlation_efficiency_bias(pf)

        # save as correctionlib file (unfortunately not possible to specify the flow for each variable separately)
        corr_lib_hist = hist.Hist(
            hist.axis.StrCategory(["central", "down", "up"], name="systematic"),
            *scale_factors["nominal"].axes,
            name="scale_factors",
            label=f"trigger scale factors for {self.trigger} trigger with reference {self.ref_trigger} "
                  f"for year {self.config_inst.x.year}",
            storage=hist.storage.Double(),
        )

        corr_lib_hist.values()[0] = scale_factors["nominal"].values()
        err = scale_factors_staterr["nominal"] ** 2 + corr_bias["all"][0][None] ** 2
        for aux_bias_vals in aux_bias.values():
            err += aux_bias_vals[None] ** 2
        sign = np.array([-1, 1]).reshape((2,) + (1,) * (err.ndim - 1))
        corr_lib_hist.values()[1:] = corr_lib_hist.values()[0][None] + sign * np.sqrt(err)

        clibcorr = correctionlib.convert.from_histogram(corr_lib_hist, flow="clamp")
        clibcorr.description = corr_lib_hist.label

        cset = correctionlib.schemav2.CorrectionSet(
            schema_version=2, description=corr_lib_hist.label, corrections=[clibcorr],
        )
        self.output()["stats"].dump(cset.dict(exclude_unset=True), indent=4, formatter="json")

        if not self.make_plots:
            return

        plot_process: od.Process = self.config_inst.get_process(self.processes[-1])
        dummy_cat = od.Category(name="1e1mu, $n_j > 1, n_b > 0$")

        def make_plot2d(hist2d: hist.Hist, cat: od.Category, aux_selection=True):
            label_values = np.around(hist2d.values(), decimals=2)
            style_config = {
                "plot2d_cfg": {"cmap": "PiYG", "labels": label_values},
                "annotate_cfg": {"bbox": dict(alpha=0.5, facecolor="white")},
            }
            p_cat = dummy_cat.copy_shallow()
            if cat.label:
                p_cat.label += "\n" + cat.label
            if aux_selection:
                p_cat.label += "\n" + self.aux_label

            fig, _ = self.call_plot_func(
                self.plot_function,
                hists={plot_process: hist2d},
                config_inst=self.config_inst,
                category_inst=p_cat,
                variable_insts=[var_inst.copy_shallow() for var_inst in self.nonaux_variable_insts[:2]],
                style_config=style_config,
                **self.get_plot_parameters(),
            )
            for p in self.output()["sf_2d"][cat.name]:
                p.dump(fig, formatter="mpl")

        if "sf_2d" in self.make_plots:
            # scale factor 2d plot
            for index, cat in self.loop_variables(nonaux=slice(2, None), aux=None, make_cat=True):
                make_plot2d(scale_factors["nominal"][index], cat)

            # scale factor 2d plot per aux bin
            for aux_vr, aux_idx in self.aux_variable_insts.items():
                for index, cat in self.loop_variables(make_cat=True, aux=aux_vr, nonaux=slice(2, None)):
                    make_plot2d(scale_factors[aux_vr.name][index], cat, aux_selection=not isinstance(aux_idx, int))

        def plot_1d(key: str, hists, vrs=None, aux_selection=True, **kwargs):
            p_cat = dummy_cat.copy_shallow()
            if aux_selection:
                p_cat.label += "\n" + self.aux_label
            vrs = self.nonaux_variable_insts if vrs is None else vrs
            fig, axes = self.call_plot_func(
                self.plot_function_1d_line,
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
            for p in self.output()[key]:
                p.dump(fig, formatter="mpl")

        if "sf_1d" in self.make_plots:
            # scale factor flat plot with stat errors
            sf_flat = [scale_factors["nominal"].values(), *scale_factors_staterr["nominal"]]
            plot_1d("sf_1d", {plot_process: sf_flat})

            for aux_vr, aux_idx in self.aux_variable_insts.items():
                ax_idx = scale_factors[aux_vr.name].axes.name.index(aux_vr.name)
                cat = dummy_cat.copy_shallow()
                if not isinstance(self.aux_variable_insts, int):
                    cat.label += "\n" + self.aux_label
                aux_bins = list(self.loop_variables(nonaux=False, aux=aux_vr, make_cat=True))
                n_group = aux_vr.aux.get("group_bins", 3)
                for i in range(0, len(aux_bins), n_group):
                    suff = "" if len(aux_bins) <= n_group else f"_{i}:{i + n_group}"
                    plot_1d("sf_1d_" + aux_vr.name + suff,
                        {"nominal": sf_flat} | {
                            cat.label: [
                                scale_factors[aux_vr.name][idx].values(),
                                *np.take(
                                    scale_factors_staterr[aux_vr.name],
                                    indices=tuple(idx.values())[0],
                                    axis=ax_idx + 1,  # + 1 because first axis is error
                                ),
                            ]
                            for idx, cat in aux_bins[i:i + n_group]
                        },
                        aux_selection=not isinstance(aux_idx, int),
                    )

        if "eff_1d" in self.make_plots:
            plot_1d("eff_1d", {dt: [x.values() for x in efficiencies[dt]] for dt in ["mc", "data"]})

        if "corr" in self.make_plots:
            # correlation plot
            style_config = {"ax_cfg": {"ylim": (-0.1, 0.1)}}
            plot_1d("corr_all", {plot_process: corr_bias["all"]}, style_config=style_config)
            for vr in self.nonaux_variable_insts:
                plot_1d(f"corr_{vr.name}", {plot_process: corr_bias[vr.name]}, vrs=[vr], style_config=style_config)

        for vr in self.variable_insts:
            suff = f"{vr.name}_proj"
            data, mc = data_mc_keys(suff)
            proj_kwargs = dict(
                config_inst=self.config_inst,
                variable_insts=[vr],
            )
            p_cat = dummy_cat.copy_shallow()
            ll = vr.aux.get("lower_limit", None)
            if not isinstance(self.aux_variable_insts.get(vr, None), int):
                p_cat.label += "\n" + self.aux_label
            if "eff_1d" in self.make_plots:
                fig, axes = self.call_plot_func(
                    self.plot_function_1d_line,
                    hists={dt[:-len(suff) - 1]: [x.values() for x in efficiencies[dt]] for dt in [mc, data]},
                    skip_ratio=False,
                    category_inst=dummy_cat,
                    **proj_kwargs,
                )
                if ll is not None:
                    for ax in axes:
                        ax.axvspan(-0.5, ll, color="grey", alpha=0.3)
                for p in self.output()[f"eff_proj_{vr.name}"]:
                    p.dump(fig, formatter="mpl")

            if "hist" in self.make_plots:
                # first with only reference trigger
                trig_cat = p_cat.copy_shallow()
                trig_cat.label += "\n" + self.ref_trigger
                idx = {ivr.name: self.aux_variable_insts.get(ivr, sum) for ivr in self.variable_insts if ivr != vr}
                idx[self.ref_trigger_vr] = 1
                label = "ref"
                for _ in range(2):
                    fig, axes = self.call_plot_func(
                        self.plot_function_1d_hist,
                        hists={p: histograms[p][idx].project(vr.name) for p in histograms},
                        category_inst=trig_cat,
                        **proj_kwargs,
                        **self.get_hist1d_plot_parameter(),
                    )
                    if ll is not None:
                        for ax in axes:
                            ax.axvspan(-0.5, ll, color="grey", alpha=0.3)
                    for p in self.output()[f"proj_{label}_{vr.name}"]:
                        p.dump(fig, formatter="mpl")

                    # repeat with trigger applied
                    trig_cat.label += " & " + self.trigger
                    idx[self.trigger_vr] = 1
                    label = "trig"
