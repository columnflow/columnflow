# coding: utf-8

"""
Tasks to plot different types of histograms.
"""

from collections import OrderedDict

import law

from ap.tasks.framework.base import ShiftTask
from ap.tasks.framework.mixins import (
    CalibratorsMixin, SelectorStepsMixin, ProducersMixin, MLModelsMixin, PlotMixin, CategoriesMixin,
    VariablesMixin, DatasetsProcessesMixin, ShiftSourcesMixin,
)
from ap.tasks.framework.remote import HTCondorWorkflow
from ap.tasks.histograms import MergeHistograms, MergeShiftedHistograms
from ap.util import DotDict


class ProcessPlotBase(
    CategoriesMixin,
    DatasetsProcessesMixin,
    PlotMixin,
):
    """
    Base class for tasks creating plots where contributions of different processes are shown.
    """

    def store_parts(self):
        parts = super().store_parts()
        part = f"datasets_{self.datasets_repr}__processes_{self.processes_repr}"
        parts.insert_before("version", "plot", part)
        return parts


class PlotVariables(
    ShiftTask,
    MLModelsMixin,
    ProducersMixin,
    SelectorStepsMixin,
    CalibratorsMixin,
    VariablesMixin,
    ProcessPlotBase,
    law.LocalWorkflow,
    HTCondorWorkflow,
):

    sandbox = "bash::$AP_BASE/sandboxes/cmssw_default.sh"

    shifts = set(MergeHistograms.shifts)

    # default plot function
    plot_function_name = "ap.plotting.variables.plot_variables"

    def create_branch_map(self):
        return [
            DotDict({"category": cat_name, "variable": var_name})
            for cat_name in sorted(self.categories)
            for var_name in sorted(self.variables)
        ]

    def workflow_requires(self):
        reqs = super().workflow_requires()
        reqs["merged_hists"] = self.requires_from_branch()
        return reqs

    def requires(self):
        return {
            d: MergeHistograms.req(
                self,
                dataset=d,
                branch=-1,
                tree_index=0,
                _exclude={"branches"},
                _prefer_cli={"variables"},
            )
            for d in self.datasets
        }

    def output(self):
        b = self.branch_data
        return self.local_target(f"plot__cat_{b.category}__var_{b.variable}.pdf")

    @PlotMixin.view_output_plots
    def run(self):
        import hist

        # prepare config objects
        variable_inst = self.config_inst.get_variable(self.branch_data.variable)
        category_inst = self.config_inst.get_category(self.branch_data.category)
        leaf_category_insts = category_inst.get_leaf_categories() or [category_inst]
        process_insts = list(map(self.config_inst.get_process, self.processes))
        sub_process_insts = {
            proc: [sub for sub, _, _ in proc.walk_processes(include_self=True)]
            for proc in process_insts
        }

        # histogram data per process
        hists = {}

        with self.publish_step(f"plotting {variable_inst.name} in {category_inst.name}"):
            for dataset, inp in self.input().items():
                dataset_inst = self.config_inst.get_dataset(dataset)
                h_in = inp["collection"][0].load(formatter="pickle")[variable_inst.name]

                # sanity checks
                n_shifts = len(h_in.axes["shift"])
                if n_shifts != 1:
                    raise Exception(f"shift axis is supposed to only contain 1 bin, found {n_shifts}")

                # loop and extract one histogram per process
                for process_inst in process_insts:
                    # skip when the dataset is already known to not contain any sub process
                    if not any(map(dataset_inst.has_process, sub_process_insts[process_inst])):
                        continue

                    # work on a copy
                    h = h_in.copy()

                    # axis selections
                    h = h[{
                        "process": [
                            hist.loc(p.id)
                            for p in sub_process_insts[process_inst]
                            if p.id in h.axes["process"]
                        ],
                        "category": [
                            hist.loc(c.id)
                            for c in leaf_category_insts
                            if c.id in h.axes["category"]
                        ],
                    }]

                    # axis reductions
                    h = h[{"process": sum, "category": sum, "shift": sum}]

                    # add the histogram
                    if process_inst in hists:
                        hists[process_inst] += h
                    else:
                        hists[process_inst] = h

            # there should be hists to plot
            if not hists:
                raise Exception("no histograms found to plot")

            # sort hists by process order
            hists = OrderedDict(
                (process_inst, hists[process_inst])
                for process_inst in sorted(hists, key=process_insts.index)
            )

            # call the plot function
            fig = self.call_plot_func(
                self.plot_function_name,
                hists=hists,
                config_inst=self.config_inst,
                variable_inst=variable_inst,
            )

            # save the plot
            self.output().dump(fig, formatter="mpl")


class PlotShiftedVariables(
    MLModelsMixin,
    ProducersMixin,
    SelectorStepsMixin,
    CalibratorsMixin,
    VariablesMixin,
    ProcessPlotBase,
    ShiftSourcesMixin,
    law.LocalWorkflow,
    HTCondorWorkflow,
):

    sandbox = "bash::$AP_BASE/sandboxes/cmssw_default.sh"

    # default plot function
    plot_function_name = "ap.plotting.variables.plot_shifted_variables"

    def store_parts(self):
        parts = super().store_parts()
        parts["plot"] += f"__shifts_{self.shift_sources_repr}"
        return parts

    def create_branch_map(self):
        return [
            DotDict({"category": cat_name, "variable": var_name, "shift_source": source})
            for cat_name in sorted(self.categories)
            for var_name in sorted(self.variables)
            for source in sorted(self.shift_sources)
        ]

    def workflow_requires(self):
        reqs = super().workflow_requires()
        reqs["merged_hists"] = self.requires_from_branch()
        return reqs

    def requires(self):
        return {
            d: MergeShiftedHistograms.req(
                self,
                dataset=d,
                branch=-1,
                _exclude={"branches"},
                _prefer_cli={"variables"},
            )
            for d in self.datasets
        }

    def output(self):
        b = self.branch_data
        return self.local_target(f"plot__cat_{b.category}__var_{b.variable}.pdf")

    @PlotMixin.view_output_plots
    def run(self):
        import hist

        # prepare config objects
        variable_inst = self.config_inst.get_variable(self.branch_data.variable)
        category_inst = self.config_inst.get_category(self.branch_data.category)
        leaf_category_insts = category_inst.get_leaf_categories() or [category_inst]
        process_insts = list(map(self.config_inst.get_process, self.processes))
        sub_process_insts = {
            proc: [sub for sub, _, _ in proc.walk_processes(include_self=True)]
            for proc in process_insts
        }

        # histogram data per process
        hists = OrderedDict()

        with self.publish_step(f"plotting {variable_inst.name} in {category_inst.name}"):
            for dataset, inp in self.input().items():
                dataset_inst = self.config_inst.get_dataset(dataset)
                h_in = inp["collection"][0].load(formatter="pickle")[variable_inst.name]

                # loop and extract one histogram per process
                for process_inst in process_insts:
                    # skip when the dataset is already known to not contain any sub process
                    if not any(map(dataset_inst.has_process, sub_process_insts[process_inst])):
                        continue

                    # work on a copy
                    h = h_in.copy()

                    # axis selections
                    h = h[{
                        "process": [
                            hist.loc(p.id)
                            for p in sub_process_insts[process_inst]
                            if p.id in h.axes["process"]
                        ],
                        "category": [
                            hist.loc(c.id)
                            for c in leaf_category_insts
                            if c.id in h.axes["category"]
                        ],
                    }]

                    # axis reductions
                    h = h[{"process": sum, "category": sum}]

                    # add the histogram
                    if process_inst in hists:
                        hists[process_inst] += h
                    else:
                        hists[process_inst] = h

            # there should be hists to plot
            if not hists:
                raise Exception("no histograms found to plot")

            # call the plot function
            fig = self.call_plot_func(
                self.plot_function_name,
                hists=hists,
                config_inst=self.config_inst,
                process_inst=process_inst,
                variable_inst=variable_inst,
            )

            # save the plot
            self.output().dump(fig, formatter="mpl")
