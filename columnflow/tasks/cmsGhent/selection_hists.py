from __future__ import annotations

import luigi
import law
import order as od

from columnflow.tasks.framework.base import Requirements
from columnflow.tasks.framework.mixins import DatasetsMixin, VariablesMixin
from columnflow.tasks.selection import MergeSelectionStats
from columnflow.tasks.framework.remote import RemoteWorkflow
from columnflow.util import dev_sandbox, maybe_import

from columnflow.tasks.framework.plotting import (
    PlotBase, PlotBase1D, VariablePlotSettingMixin, ProcessPlotSettingMixin,
)

from columnflow.types import Any


hist = maybe_import("hist")


class CustomDefaultVariablesMixin(
    VariablesMixin,
):
    @classmethod
    def get_default_variables(cls, params):
        if not (config_inst := params.get("config_inst")):
            return params
        return config_inst.x("default_variables", tuple())

    @classmethod
    def resolve_param_values(
            cls,
            params: law.util.InsertableDict[str, Any],
    ) -> law.util.InsertableDict[str, Any]:
        f"""
        Resolve values *params* and check against possible default values

        Check the values in *params* against the default value in the current config inst.
        See {cls.__class__}.get_default_variables for where the default value should be stored
        For more information, see
        :py:meth:`~columnflow.tasks.framework.base.ConfigTask.resolve_config_default_and_groups`.
        """
        redo_default_variables = False
        # when empty, use the config default
        if not params.get("variables", None):
            redo_default_variables = True

        params = super().resolve_param_values(params)

        config_inst = params.get("config_inst")
        if not config_inst:
            return params

        if redo_default_variables:
            # when empty, use the config default
            if default_variables := cls.get_default_variables(params):
                params["variables"] = tuple(default_variables)
            elif cls.default_variables:
                params["variables"] = tuple(cls.default_variables)
            else:
                raise AssertionError(f"define default {cls.tag_name} variables "
                                     f"in {cls.__class__} or config {config_inst.name}")

        return params


class SelectionEfficiencyHistMixin(
    DatasetsMixin,
    CustomDefaultVariablesMixin,
):

    tag_name = "tag"
    exclude_index = True

    # upstream requirements
    reqs = Requirements(
        RemoteWorkflow.reqs,
        MergeSelectionStats=MergeSelectionStats,
    )

    sandbox = dev_sandbox("bash::$CF_BASE/sandboxes/venv_selection_eff.sh")

    @classmethod
    def get_default_variables(cls, params):
        if not (config_inst := params.get("config_inst")):
            return params
        return config_inst.x(f"default_{cls.tag_name}_variables", tuple())

    def workflow_requires(self):
        reqs = super().workflow_requires()
        for d in self.datasets:
            reqs[d] = self.reqs.MergeSelectionStats.req(
                self,
                tree_index=0,
                branch=-1,
                dataset=d,
                _exclude=MergeSelectionStats.exclude_params_forest_merge,
            )
        return reqs

    def requires(self):
        return {
            d: self.reqs.MergeSelectionStats.req(
                self,
                tree_index=0,
                branch=-1,
                dataset=d,
                _exclude=MergeSelectionStats.exclude_params_forest_merge,
            )
            for d in self.datasets
        }

    def create_branch_map(self):
        # create a dummy branch map so that this task could be submitted as a job
        return {0: None}

    def store_parts(self):
        parts = super().store_parts()
        parts.insert_before("version", "datasets", f"datasets_{self.datasets_repr}")
        return parts

    @classmethod
    def get_process_xsec(cls, dataset: od.Dataset | str, config_inst: od.Config) -> tuple[od.Process, float]:
        """
        link dataset to a process and a xsec. Implemented as class method so it can be easily adapted to the
        the needs of a specific analysis using a patch.
        """
        dataset_inst = dataset if isinstance(dataset, od.Dataset) else config_inst.get_dataset(dataset)
        process_inst = dataset_inst.processes.get_first()
        if config_inst.has_process(process_inst):
            process_inst = config_inst.get_process(process_inst)
        xsec = 1 if process_inst.is_data else process_inst.get_xsec(config_inst.campaign.ecm).nominal
        return process_inst, xsec

    def read_hist(self, variable_insts=tuple(), name=None) -> dict[od.Process, hist.Hist]:
        """
        read the histograms calculated in MergeSelectionStats task, properly normalize,
        and apply flow and merged bin settings from the given variables.
        """
        import numpy as np
        from columnflow.plotting.plot_util import use_flow_bins

        if name is None:
            name = f"{self.tag_name}_efficiencies"
        # histogram for the tagged and all jets (combine all datasets)

        histograms = {}
        for dataset, inp in self.input().items():
            # map dataset to one of to requested processes
            _, xsec = self.get_process_xsec(dataset, self.config_inst)
            dataset_inst = self.config_inst.get_dataset(dataset)

            h_in = inp["collection"][0]["hists"].load(formatter="pickle")[name]
            for variable_inst in variable_insts:
                v_idx = h_in.axes.name.index(variable_inst.name)
                for mi, mj in variable_inst.x("merge_bins", []):
                    merged_bins = h_in[{variable_inst.name: slice(mi, mj + 1, sum)}]
                    new_arr = np.moveaxis(h_in.view(), v_idx, 0)
                    new_arr[mi:mj + 1] = merged_bins.view()[np.newaxis]
                    h_in.view()[:] = np.moveaxis(new_arr, 0, v_idx)

                if any(flows := [
                    getattr(variable_inst, f + "flow", variable_inst.x(f + "flow", False))
                    for f in ["under", "over"]
                ]):
                    h_in = use_flow_bins(h_in, variable_inst.name, *flows)

            if dataset_inst.is_mc:
                norm = inp["collection"][0]["stats"].load()["sum_mc_weight"]
                h_in = h_in * xsec / norm * self.config_inst.x.luminosity.nominal

            histograms[dataset_inst] = histograms.get(dataset_inst, 0) + h_in

        if not histograms:
            raise Exception(
                "no histograms found to plot; possible reasons:\n" +
                "  - requested variable requires columns that were missing during histogramming\n" +
                "  - selected --datasets did not match any value on the process axis of the input histogram",
            )

        return histograms

    @classmethod
    def efficiency(cls, selected_counts: hist.Hist, incl: hist.Hist, **kwargs) -> hist.Hist:
        """
        calculate efficiencies with uncertainties given two histograms **selected_counts** and **incl**
        containing  the counts before and after selecton. The uncertainties are calculated using the method
        **proportion_confint**  from the **statsmodels.stats.proportion** modul. The default output are
        two-sided 65% level confidence intervals according to the Clopper-Pearson (beta) method, but other settings
        can be chosen through the keyword arguments. For more documentation see

        https://www.statsmodels.org/dev/generated/statsmodels.stats.proportion.proportion_confint.html

        @param selected_counts: histogram with event counts after selection
        @param incl: histogram with event counts before selection
        @param kwargs: keyword arguments passed to **proportion_confint**
        """
        from statsmodels.stats.proportion import proportion_confint
        efficiency = selected_counts / incl.values()
        eff_sample_size_corr = incl.values() / incl.variances()
        eff_selected = selected_counts.values() * eff_sample_size_corr
        eff_incl = incl.values() * eff_sample_size_corr
        kwargs = dict(alpha=0.35, method="beta") | kwargs
        error = proportion_confint(eff_selected, eff_incl, **kwargs)
        eff_full = hist.Hist(
            hist.axis.StrCategory(["central", "down", "up"], name="systematic"),
            *selected_counts.axes,
            storage=hist.storage.Weight(),
            name=selected_counts.name,
        )
        eff_full.values()[:] = [efficiency.values(), *error]
        eff_full.variances()[:] = 1
        return eff_full


class SelectionHistPlot(
    SelectionEfficiencyHistMixin,
    VariablePlotSettingMixin,
    ProcessPlotSettingMixin,
    PlotBase1D,
):
    exclude_index = False

    hist_name = luigi.Parameter(
        description="name of the selection histograms to plot",
        significant=True,
    )
    cat_label = luigi.Parameter(
        default="",
        description="category label for the plot",
        significant=False,
    )

    plot_function = PlotBase.plot_function.copy(
        default="columnflow.plotting.plot_functions_1d.plot_variable_per_process",
        add_default_to_description=True,
    )

    @law.workflow_property(setter=True, cache=True, empty_value=0)
    def hist_variables(self):
        # check if the merging stats are present
        all_hists = []
        for d in self.datasets:
            hists = self.reqs.MergeSelectionStats.req(self, dataset=d).output()["hists"]
            if not hists.exists():
                return []
            all_hists.append(hists.load(formatter="pickle")[self.hist_name])
        if not all([h.axes == all_hists[0].axes for h in all_hists]):
            return []
        return all_hists[0].axes.name

    @law.dynamic_workflow_condition
    def workflow_condition(self):
        # the workflow shape can be constructed as soon as the histogram variables are know
        return self.hist_variables

    @workflow_condition.output
    def output(self):
        return {
            vr: [self.target(name) for name in self.get_plot_names(vr)]
            for vr in self.hist_variables
        }

    @law.decorator.log
    def run(self):
        variable_insts = list(map(self.config_inst.get_variable, self.variables))
        histograms = self.read_hist(variable_insts)

        for variable_name in self.hist_variables:
            variable_inst = self.config_inst.get_variable(
                variable_name,
                default=od.Variable(variable_name),
            )

            fig, _ = self.call_plot_func(
                self.control_plot_function,
                hists={process_inst: h.project(variable_name) for process_inst, h in histograms.items()},
                config_inst=self.config_inst,
                category_inst=od.Category(name=self.cat_label),
                variable_insts=[variable_inst.copy_shallow()],
                **self.get_control_plot_parameter(),
            )
            for p in self.output()[variable_name]:
                p.dump(fig, formatter="mpl")
