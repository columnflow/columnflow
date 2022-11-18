# coding: utf-8

"""
Tasks to plot different types of histograms.
"""

from collections import OrderedDict

import law
import luigi

from columnflow.tasks.framework.base import ShiftTask
from columnflow.tasks.framework.mixins import (
    CalibratorsMixin, SelectorStepsMixin, ProducersMixin, MLModelsMixin,
    VariablesMixin, CategoriesMixin, ShiftSourcesMixin, EventWeightMixin,
)
from columnflow.tasks.framework.plotting import PlotBase, PlotBase1d, PlotBase2d, ProcessPlotSettingMixin
from columnflow.tasks.framework.remote import RemoteWorkflow
from columnflow.tasks.histograms import MergeHistograms, MergeShiftedHistograms
from columnflow.util import DotDict, dev_sandbox


class PlotVariablesBase(
    ShiftTask,
    VariablesMixin,
    MLModelsMixin,
    ProducersMixin,
    SelectorStepsMixin,
    CalibratorsMixin,
    EventWeightMixin,
    CategoriesMixin,
    PlotBase,
    law.LocalWorkflow,
    RemoteWorkflow,
):
    exclude_index = True

    sandbox = dev_sandbox("bash::$CF_BASE/sandboxes/venv_columnar.sh")

    shifts = set(MergeHistograms.shifts)

    # default upstream dependency task classes
    dep_MergeHistograms = MergeHistograms

    def create_branch_map(self):
        return [
            DotDict({"category": cat_name, "variable": var_name})
            for cat_name in sorted(self.categories)
            for var_name in sorted(self.variables)
        ]

    def workflow_requires(self, only_super: bool = False):
        reqs = super().workflow_requires()
        if only_super:
            return reqs

        reqs["merged_hists"] = self.requires_from_branch()

        return reqs

    def requires(self):
        return {
            d: self.dep_MergeHistograms.req(
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
        return [
            self.target(name)
            for name in self.get_plot_names(f"plot__cat_{b.category}__var_{b.variable}")
        ]

    @law.decorator.log
    @PlotBase.view_output_plots
    def run(self):
        import hist

        # prepare config objects
        variable_tuple = self.variable_tuples[self.branch_data.variable]
        variable_insts = [
            self.config_inst.get_variable(var_name)
            for var_name in variable_tuple
        ]
        category_inst = self.config_inst.get_category(self.branch_data.category)
        leaf_category_insts = category_inst.get_leaf_categories() or [category_inst]
        process_insts = list(map(self.config_inst.get_process, self.processes))
        sub_process_insts = {
            proc: [sub for sub, _, _ in proc.walk_processes(include_self=True)]
            for proc in process_insts
        }

        # histogram data per process
        hists = {}

        with self.publish_step(f"plotting {self.branch_data.variable} in {category_inst.name}"):
            for dataset, inp in self.input().items():
                dataset_inst = self.config_inst.get_dataset(dataset)
                h_in = inp["collection"][0].targets[self.branch_data.variable].load(formatter="pickle")

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

            # determine the correct plot function for this variable
            plot_function_name = (
                self.plot_function_name_1d if len(variable_insts) == 1 and "plot_function_name_1d" in dir(self) else
                self.plot_function_name_2d if len(variable_insts) == 2 and "plot_function_name_2d" in dir(self) else
                None
            )
            if not plot_function_name:
                raise NotImplementedError(
                    f"No Plotting function for {len(variable_insts)} variables implemented of task {self.cls_name}",
                )

            # call the plot function
            fig = self.call_plot_func(
                plot_function_name,
                hists=hists,
                config_inst=self.config_inst,
                variable_insts=variable_insts,
                **self.get_plot_parameters(),
            )

            # save the plot
            for outp in self.output():
                outp.dump(fig, formatter="mpl")


class PlotVariables1d(
    PlotVariablesBase,
    PlotBase1d,
    ProcessPlotSettingMixin,
):

    pass


class PlotVariables2d(
    PlotVariablesBase,
    PlotBase2d,
    ProcessPlotSettingMixin,
):

    pass


class PlotShiftedVariables1d(
    VariablesMixin,
    ShiftSourcesMixin,
    MLModelsMixin,
    ProducersMixin,
    SelectorStepsMixin,
    CalibratorsMixin,
    CategoriesMixin,
    PlotBase1d,
    ProcessPlotSettingMixin,
    law.LocalWorkflow,
    RemoteWorkflow,
):

    per_process = luigi.BoolParameter(
        default=False,
        significant=True,
        description="when True, one plot per process is produced; default: False",
    )

    legend_title = luigi.Parameter(
        default=law.NO_STR,
        significant=False,
        description="sets the title of the legend; when empty and only one process is present in "
        "the plot, the process_inst label is used; empty default",
    )

    sandbox = dev_sandbox("bash::$CF_BASE/sandboxes/venv_columnar.sh")

    # default plot function
    plot_function_name = "columnflow.plotting.example.plot_shifted_variable"

    # default upstream dependency task classes
    dep_MergeShiftedHistograms = MergeShiftedHistograms

    def get_plot_parameters(self):
        # convert parameters to usable values during plotting
        params = super().get_plot_parameters()

        params["legend_title"] = None if self.legend_title == law.NO_STR else self.legend_title

        return params

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

    def workflow_requires(self, only_super: bool = False):
        reqs = super().workflow_requires()
        if only_super:
            return reqs

        reqs["merged_hists"] = self.requires_from_branch()
        return reqs

    def requires(self):
        return {
            d: self.dep_MergeShiftedHistograms.req(
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
        if self.per_process:
            # one output per process
            return law.SiblingFileCollection({
                p: [
                    self.target(name) for name in self.get_plot_names(
                        f"plot__proc_{p}__unc_{b.shift_source}__cat_{b.category}"
                        f"__var_{b.variable}",
                    )
                ]
                for p in self.processes
            })
        else:
            # a single output
            return [
                self.target(name)
                for name in self.get_plot_names(
                    f"plot__unc_{b.shift_source}__cat_{b.category}__var_{b.variable}",
                )
            ]

    @law.decorator.log
    @PlotBase.view_output_plots
    def run(self):
        import hist

        # prepare config objects
        variable_inst = self.config_inst.get_variable(self.branch_data.variable)
        category_inst = self.config_inst.get_category(self.branch_data.category)
        shift_insts = [
            self.config_inst.get_shift(s) for s in
            ["nominal", f"{self.branch_data.shift_source}_up", f"{self.branch_data.shift_source}_down"]
        ]
        leaf_category_insts = category_inst.get_leaf_categories() or [category_inst]
        process_insts = list(map(self.config_inst.get_process, self.processes))
        sub_process_insts = {
            proc: [sub for sub, _, _ in proc.walk_processes(include_self=True)]
            for proc in process_insts
        }

        # histogram data per process
        hists = OrderedDict()
        outputs = self.output()

        with self.publish_step(f"plotting {variable_inst.name} in {category_inst.name}"):
            for dataset, inp in self.input().items():
                dataset_inst = self.config_inst.get_dataset(dataset)
                h_in = inp["collection"][0].targets[variable_inst.name].load(formatter="pickle")

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
                        "shift": [
                            hist.loc(s.id)
                            for s in shift_insts
                            if s.id in h.axes["shift"]
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

            if self.per_process:
                for process_inst, h in hists.items():
                    # call the plot function once per process
                    fig = self.call_plot_func(
                        self.plot_function_name,
                        hists={process_inst: h},
                        config_inst=self.config_inst,
                        variable_inst=variable_inst,
                        **self.get_plot_parameters(),
                    )
                    # save the plot
                    for outp in outputs[process_inst.name]:
                        outp.dump(fig, formatter="mpl")
            else:
                # call the plot function once
                fig = self.call_plot_func(
                    self.plot_function_name,
                    hists=hists,
                    config_inst=self.config_inst,
                    variable_inst=variable_inst,
                    **self.get_plot_parameters(),
                )

                # save the plot
                for outp in outputs:
                    outp.dump(fig, formatter="mpl")
