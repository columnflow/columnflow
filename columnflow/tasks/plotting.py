# coding: utf-8

"""
Tasks to plot different types of histograms.
"""

from collections import OrderedDict
from abc import abstractmethod

import law
import luigi
import order as od

from columnflow.tasks.framework.base import Requirements, ShiftTask
from columnflow.tasks.framework.mixins import (
    CalibratorsMixin, SelectorStepsMixin, ProducersMixin, MLModelsMixin, WeightProducerMixin,
    CategoriesMixin, ShiftSourcesMixin, HistHookMixin,
)
from columnflow.tasks.framework.plotting import (
    PlotBase, PlotBase1D, PlotBase2D, ProcessPlotSettingMixin, VariablePlotSettingMixin,
)
from columnflow.tasks.framework.decorators import view_output_plots
from columnflow.tasks.framework.remote import RemoteWorkflow
from columnflow.tasks.histograms import MergeHistograms, MergeShiftedHistograms
from columnflow.util import DotDict, dev_sandbox, dict_add_strict


class PlotVariablesBase(
    HistHookMixin,
    VariablePlotSettingMixin,
    ProcessPlotSettingMixin,
    CategoriesMixin,
    MLModelsMixin,
    WeightProducerMixin,
    ProducersMixin,
    SelectorStepsMixin,
    CalibratorsMixin,
    law.LocalWorkflow,
    RemoteWorkflow,
):
    sandbox = dev_sandbox(law.config.get("analysis", "default_columnar_sandbox"))

    exclude_index = True

    # upstream requirements
    reqs = Requirements(
        RemoteWorkflow.reqs,
        MergeHistograms=MergeHistograms,
    )

    def store_parts(self):
        parts = super().store_parts()
        parts.insert_before("version", "datasets", f"datasets_{self.datasets_repr}")
        return parts

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

    @abstractmethod
    def get_plot_shifts(self):
        return

    @law.decorator.log
    @view_output_plots
    def run(self):
        import hist

        # get the shifts to extract and plot
        plot_shifts = law.util.make_list(self.get_plot_shifts())

        # copy process instances once so that their auxiliary data fields can be used as a storage
        # for process-specific plot parameters later on in plot scripts without affecting the
        # original instances
        fake_root = od.Process(
            name=f"{hex(id(object()))[2:]}",
            id="+",
            processes=list(map(self.config_inst.get_process, self.processes)),
        ).copy()
        process_insts = list(fake_root.processes)
        fake_root.processes.clear()

        # prepare other config objects
        variable_tuple = self.variable_tuples[self.branch_data.variable]
        variable_insts = [
            self.config_inst.get_variable(var_name)
            for var_name in variable_tuple
        ]
        category_inst = self.config_inst.get_category(self.branch_data.category)
        leaf_category_insts = category_inst.get_leaf_categories() or [category_inst]
        sub_process_insts = {
            process_inst: [sub for sub, _, _ in process_inst.walk_processes(include_self=True)]
            for process_inst in process_insts
        }

        # histogram data per process copy
        hists = {}

        with self.publish_step(f"plotting {self.branch_data.variable} in {category_inst.name}"):
            for dataset, inp in self.input().items():
                dataset_inst = self.config_inst.get_dataset(dataset)
                h_in = inp["collection"][0]["hists"].targets[self.branch_data.variable].load(formatter="pickle")

                # loop and extract one histogram per process
                for process_inst in process_insts:
                    # skip when the dataset is already known to not contain any sub process
                    if not any(
                        dataset_inst.has_process(sub_process_inst.name)
                        for sub_process_inst in sub_process_insts[process_inst]
                    ):
                        continue

                    # select processes and reduce axis
                    h = h_in.copy()
                    h = h[{
                        "process": [
                            hist.loc(p.id)
                            for p in sub_process_insts[process_inst]
                            if p.id in h.axes["process"]
                        ],
                    }]
                    h = h[{"process": sum}]

                    # add the histogram
                    if process_inst in hists:
                        hists[process_inst] += h
                    else:
                        hists[process_inst] = h

            # there should be hists to plot
            if not hists:
                raise Exception(
                    "no histograms found to plot; possible reasons:\n"
                    "  - requested variable requires columns that were missing during histogramming\n"
                    "  - selected --processes did not match any value on the process axis of the input histogram",
                )

            # update histograms using a custom hook
            hists = self.invoke_hist_hook(hists)

            # add new processes to the end of the list
            for process_inst in hists:
                if process_inst not in process_insts:
                    process_insts.append(process_inst)

            # axis selections and reductions, including sorting by process order
            _hists = OrderedDict()
            for process_inst in sorted(hists, key=process_insts.index):
                h = hists[process_inst]
                # selections
                h = h[{
                    "category": [
                        hist.loc(c.id)
                        for c in leaf_category_insts
                        if c.id in h.axes["category"]
                    ],
                    "shift": [
                        hist.loc(s.id)
                        for s in plot_shifts
                        if s.id in h.axes["shift"]
                    ],
                }]
                # reductions
                h = h[{"category": sum}]
                # store
                _hists[process_inst] = h
            hists = _hists

            # call the plot function
            fig, _ = self.call_plot_func(
                self.plot_function,
                hists=hists,
                config_inst=self.config_inst,
                category_inst=category_inst.copy_shallow(),
                variable_insts=[var_inst.copy_shallow() for var_inst in variable_insts],
                **self.get_plot_parameters(),
            )

            # save the plot
            for outp in self.output()["plots"]:
                outp.dump(fig, formatter="mpl")


class PlotVariablesBaseSingleShift(
    PlotVariablesBase,
    ShiftTask,
):
    exclude_index = True

    # upstream requirements
    reqs = Requirements(
        PlotVariablesBase.reqs,
        MergeHistograms=MergeHistograms,
    )

    def create_branch_map(self):
        return [
            DotDict({"category": cat_name, "variable": var_name})
            for var_name in sorted(self.variables)
            for cat_name in sorted(self.categories)
        ]

    def requires(self):
        return {
            d: self.reqs.MergeHistograms.req(
                self,
                dataset=d,
                branch=-1,
                _exclude={"branches"},
                _prefer_cli={"variables"},
            )
            for d in self.datasets
        }

    def plot_parts(self) -> law.util.InsertableDict:
        parts = super().plot_parts()

        parts["processes"] = f"proc_{self.processes_repr}"
        parts["category"] = f"cat_{self.branch_data.category}"
        parts["variable"] = f"var_{self.branch_data.variable}"

        if self.hist_hook not in ("", law.NO_STR, None):
            parts["hook"] = f"hook_{self.hist_hook}"

        return parts

    def output(self):
        return {
            "plots": [self.target(name) for name in self.get_plot_names("plot")],
        }

    def store_parts(self):
        parts = super().store_parts()
        if "shift" in parts:
            parts.insert_before("datasets", "shift", parts.pop("shift"))
        return parts

    def get_plot_shifts(self):
        return [self.global_shift_inst]


class PlotVariables1D(
    PlotVariablesBaseSingleShift,
    PlotBase1D,
):
    plot_function = PlotBase.plot_function.copy(
        default="columnflow.plotting.plot_functions_1d.plot_variable_per_process",
        add_default_to_description=True,
    )


class PlotVariables2D(
    PlotVariablesBaseSingleShift,
    PlotBase2D,
):
    plot_function = PlotBase.plot_function.copy(
        default="columnflow.plotting.plot_functions_2d.plot_2d",
        add_default_to_description=True,
    )


class PlotVariablesPerProcess2D(
    law.WrapperTask,
    PlotVariables2D,
):
    # force this one to be a local workflow
    workflow = "local"

    def requires(self):
        return {
            process: PlotVariables2D.req(self, processes=(process,))
            for process in self.processes
        }


class PlotVariablesBaseMultiShifts(
    PlotVariablesBase,
    ShiftSourcesMixin,
):
    legend_title = luigi.Parameter(
        default=law.NO_STR,
        significant=False,
        description="sets the title of the legend; when empty and only one process is present in "
        "the plot, the process_inst label is used; empty default",
    )

    exclude_index = True

    # upstream requirements
    reqs = Requirements(
        PlotVariablesBase.reqs,
        MergeShiftedHistograms=MergeShiftedHistograms,
    )

    def create_branch_map(self):
        return [
            DotDict({"category": cat_name, "variable": var_name, "shift_source": source})
            for var_name in sorted(self.variables)
            for cat_name in sorted(self.categories)
            for source in sorted(self.shift_sources)
        ]

    def requires(self):
        return {
            d: self.reqs.MergeShiftedHistograms.req(
                self,
                dataset=d,
                branch=-1,
                _exclude={"branches"},
                _prefer_cli={"variables"},
            )
            for d in self.datasets
        }

    def plot_parts(self) -> law.util.InsertableDict:
        parts = super().plot_parts()

        parts["processes"] = f"proc_{self.processes_repr}"
        parts["shift_source"] = f"unc_{self.branch_data.shift_source}"
        parts["category"] = f"cat_{self.branch_data.category}"
        parts["variable"] = f"var_{self.branch_data.variable}"

        if self.hist_hook not in ("", law.NO_STR, None):
            parts["hook"] = f"hook_{self.hist_hook}"

        return parts

    def output(self):
        return {
            "plots": [self.target(name) for name in self.get_plot_names("plot")],
        }

    def store_parts(self):
        parts = super().store_parts()
        parts.insert_before("datasets", "shifts", f"shifts_{self.shift_sources_repr}")
        return parts

    def get_plot_shifts(self):
        return [
            self.config_inst.get_shift(s) for s in [
                "nominal",
                f"{self.branch_data.shift_source}_up",
                f"{self.branch_data.shift_source}_down",
            ]
        ]

    def get_plot_parameters(self):
        # convert parameters to usable values during plotting
        params = super().get_plot_parameters()
        dict_add_strict(params, "legend_title", None if self.legend_title == law.NO_STR else self.legend_title)
        return params


class PlotShiftedVariables1D(
    PlotBase1D,
    PlotVariablesBaseMultiShifts,
):
    plot_function = PlotBase.plot_function.copy(
        default="columnflow.plotting.plot_functions_1d.plot_shifted_variable",
        add_default_to_description=True,
    )


class PlotShiftedVariablesPerProcess1D(
    law.WrapperTask,
    PlotShiftedVariables1D,
):
    # force this one to be a local workflow
    workflow = "local"

    # upstream requirements
    reqs = Requirements(
        PlotShiftedVariables1D.reqs,
        PlotShiftedVariables1D=PlotShiftedVariables1D,
    )

    def requires(self):
        return {
            process: self.reqs.PlotShiftedVariables1D.req(self, processes=(process,))
            for process in self.processes
        }
