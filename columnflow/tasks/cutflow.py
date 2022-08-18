# coding: utf-8

"""
Tasks to be implemented: MergeSelectionMasks, PlotCutflow
"""

import functools
from collections import OrderedDict

import luigi
import law

from columnflow.tasks.framework.base import AnalysisTask, DatasetTask, ShiftTask, wrapper_factory
from columnflow.tasks.framework.mixins import (
    CalibratorsMixin, SelectorStepsMixin, VariablesMixin, PlotMixin,
)
from columnflow.tasks.framework.remote import HTCondorWorkflow
from columnflow.tasks.plotting import ProcessPlotBase
from columnflow.tasks.selection import MergeSelectionMasks
from columnflow.util import dev_sandbox, DotDict


class CreateCutflowHistograms(
    DatasetTask,
    SelectorStepsMixin,
    CalibratorsMixin,
    VariablesMixin,
    law.LocalWorkflow,
    HTCondorWorkflow,
):

    sandbox = dev_sandbox("bash::$CF_BASE/sandboxes/venv_columnar.sh")

    shifts = set(MergeSelectionMasks.shifts)

    selector_steps_order_sensitive = True

    default_variables = ("lhe_weight", "cf_*")

    # default upstream dependency task classes
    dep_MergeSelectionMasks = MergeSelectionMasks

    def create_branch_map(self):
        # dummy branch map
        return [None]

    def workflow_requires(self, only_super: bool = False):
        reqs = super(CreateCutflowHistograms, self).workflow_requires()
        if only_super:
            return reqs

        reqs["masks"] = self.dep_MergeSelectionMasks.req(self, tree_index=0, _exclude={"branches"})
        return reqs

    def requires(self):
        return {
            "masks": self.dep_MergeSelectionMasks.req(self, tree_index=0, branch=0),
        }

    def output(self):
        return {var: self.target(f"cutflow_hist__var_{var}.pickle") for var in self.variables}

    @law.decorator.log
    @law.decorator.localize(input=True, output=False)
    @law.decorator.safe_output
    def run(self):
        import hist
        import awkward as ak
        from columnflow.columnar_util import ChunkedReader, Route, add_ak_aliases

        # prepare inputs and outputs
        inputs = self.input()

        # create a temp dir for saving intermediate files
        tmp_dir = law.LocalDirectoryTarget(is_tmp=True)
        tmp_dir.touch()

        # get shift dependent aliases
        aliases = self.shift_inst.x("column_aliases", {})

        # define a list of variables to create the histograms for
        variable_insts = [self.config_inst.get_variable(v) for v in self.variables]

        # get the expression per variable and when it's a string, parse it to extract index lookups
        expressions = {}
        for variable_inst in variable_insts:
            expr = variable_inst.expression
            if isinstance(expr, str):
                route = Route(expr)
                expr = functools.partial(route.apply, null_value=variable_inst.null_value)
            expressions[variable_inst.name] = expr

        # create histograms per variable
        histograms = {}
        for variable_inst in variable_insts:
            histograms[variable_inst.name] = (
                hist.Hist.new
                .IntCat([], name="category", growth=True)
                .IntCat([], name="process", growth=True)
                .StrCat([], name="step", growth=True)
                .IntCat([], name="shift", growth=True)
                .Var(
                    variable_inst.bin_edges,
                    name=variable_inst.name,
                    label=variable_inst.get_full_x_title(),
                )
                .Weight()
            )

        with ChunkedReader(
            inputs["masks"].path,
            source_type="awkward_parquet",
        ) as reader:
            msg = f"iterate through {reader.n_entries} events ..."
            for arr, pos in self.iter_progress(reader, reader.n_chunks, msg=msg):
                events = arr.events

                # add aliases
                add_ak_aliases(events, aliases, remove_src=True)

                # pad the category_ids when the event is not categorized at all
                category_ids = ak.fill_none(ak.pad_none(events.category_ids, 1, axis=-1), -1)

                for variable_inst in variable_insts:
                    var_name = variable_inst.name
                    # helper to build the point for filling, except for the step which does
                    # not support broadcasting
                    def get_point(mask=Ellipsis):
                        return {
                            variable_inst.name: expressions[var_name](events)[mask],
                            "process": events.process_id[mask],
                            "category": category_ids[mask],
                            "shift": self.shift_inst.id,
                            "weight": events.normalization_weight[mask],
                        }

                    # fill the raw point
                    fill_kwargs = get_point()
                    arrays = (ak.flatten(a) for a in ak.broadcast_arrays(*fill_kwargs.values()))
                    histograms[var_name].fill(step="Initial", **dict(zip(fill_kwargs, arrays)))

                    # fill all other steps
                    steps = self.selector_steps or arr.steps.fields

                    mask = True
                    for step in steps:
                        if step not in arr.steps.fields:
                            raise ValueError(
                                f"step '{step}' is not defined by selector {self.selector}",
                            )
                        # incrementally update the mask and fill the point
                        mask = mask & arr.steps[step]
                        fill_kwargs = get_point(mask)
                        arrays = (ak.flatten(a) for a in ak.broadcast_arrays(*fill_kwargs.values()))
                        histograms[var_name].fill(step=step, **dict(zip(fill_kwargs, arrays)))

        # dump the histograms
        for var_name in histograms.keys():
            self.output()[var_name].dump(histograms[var_name], formatter="pickle")


CreateCutflowHistogramsWrapper = wrapper_factory(
    base_cls=AnalysisTask,
    require_cls=CreateCutflowHistograms,
    enable=["configs", "skip_configs", "datasets", "skip_datasets", "shifts", "skip_shifts"],
)


class PlotCutflow(
    ShiftTask,
    SelectorStepsMixin,
    CalibratorsMixin,
    ProcessPlotBase,
    law.LocalWorkflow,
    HTCondorWorkflow,
):

    sandbox = "bash::$CF_BASE/sandboxes/cmssw_default.sh"

    shifts = set(CreateCutflowHistograms.shifts)

    plot_function_name = "columnflow.plotting.example.plot_cutflow"

    selector_steps_order_sensitive = True

    # default upstream dependency task classes
    dep_CreateCutflowHistograms = CreateCutflowHistograms

    def create_branch_map(self):
        # one category per branch
        if not self.categories:
            raise Exception(
                f"{self.__class__.__name__} task cannot build branch map when no categories are "
                "set",
            )

        return list(self.categories)

    def workflow_requires(self, only_super: bool = False):
        reqs = super(PlotCutflow, self).workflow_requires()
        if only_super:
            return reqs

        reqs["hists"] = [
            self.dep_CreateCutflowHistograms.req(
                self,
                dataset=d,
                variables=("lhe_weight",),
                _prefer_cli={"variables"},
                _exclude={"branches"},
            )
            for d in self.datasets
        ]
        return reqs

    def requires(self):
        return {
            d: self.dep_CreateCutflowHistograms.req(
                self,
                branch=0,
                dataset=d,
                variables=("lhe_weight",),
                _prefer_cli={"variables"},
            )
            for d in self.datasets
        }

    def output(self):
        return self.target(f"cutflow__cat_{self.branch_data}.pdf")

    @law.decorator.log
    @PlotMixin.view_output_plots
    def run(self):
        import hist

        # prepare config objects
        category_inst = self.config_inst.get_category(self.branch_data)
        leaf_category_insts = category_inst.get_leaf_categories() or [category_inst]
        process_insts = list(map(self.config_inst.get_process, self.processes))
        sub_process_insts = {
            proc: [sub for sub, _, _ in proc.walk_processes(include_self=True)]
            for proc in process_insts
        }

        # histogram data per process
        hists = {}

        with self.publish_step(f"plotting cutflow in {category_inst.name}"):
            for dataset, inp in self.input().items():
                dataset_inst = self.config_inst.get_dataset(dataset)
                h_in = inp["lhe_weight"].load(formatter="pickle")

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
                    h = h[{"process": sum, "category": sum, "shift": sum, "lhe_weight": sum}]

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
                **self.get_plot_parameters(),

            )

            # save the plot
            self.output().dump(fig, formatter="mpl")


PlotCutflowWrapper = wrapper_factory(
    base_cls=AnalysisTask,
    require_cls=PlotCutflow,
    enable=["configs", "skip_configs", "shifts", "skip_shifts"],
)


class PlotCutflowVariables(
    ShiftTask,
    SelectorStepsMixin,
    CalibratorsMixin,
    VariablesMixin,
    ProcessPlotBase,
    law.LocalWorkflow,
    HTCondorWorkflow,
):

    per_plot = luigi.ChoiceParameter(
        choices=("processes", "steps"),
        default="processes",
        description="what to show per plot; choices: 'processes' (one plot per selection step, all "
        "processes in one plot); 'steps' (one plot per process, all selection steps in one plot); "
        "default: processes",
    )
    only_final_step = luigi.BoolParameter(
        default=False,
        significant=False,
        description="when True, only create plots for the final selector step; default: False",
    )

    sandbox = "bash::$CF_BASE/sandboxes/cmssw_default.sh"

    shifts = set(CreateCutflowHistograms.shifts)

    selector_steps_order_sensitive = True
    initial_step = "Initial"

    default_variables = ("cf_*",)

    # default plot functions
    plot_function_name_per_process = "columnflow.plotting.example.plot_variable_per_process"
    plot_function_name_per_step = "columnflow.plotting.example.plot_variable_variants"

    # default upstream dependency task classes
    dep_CreateCutflowHistograms = CreateCutflowHistograms

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # chosen selectors steps, including the initial one
        self.chosen_steps = [self.initial_step] + list(self.selector_steps)
        if self.only_final_step:
            del self.chosen_steps[:-1]

    def create_branch_map(self):
        if not self.categories:
            raise Exception(
                f"{self.__class__.__name__} task cannot build branch map when no categories are "
                "set",
            )
        if not self.variables:
            raise Exception(
                f"{self.__class__.__name__} task cannot build branch map when no variables are"
                "set",
            )

        return [
            DotDict({"category": cat_name, "variable": var_name})
            for cat_name in sorted(self.categories)
            for var_name in sorted(self.variables)
        ]

    def workflow_requires(self):
        reqs = super(PlotCutflowVariables, self).workflow_requires()
        reqs["hists"] = [
            self.dep_CreateCutflowHistograms.req(self, dataset=d, _exclude={"branches"})
            for d in self.datasets
        ]
        return reqs

    def requires(self):
        return {
            d: self.dep_CreateCutflowHistograms.req(self, dataset=d, branch=0)
            for d in self.datasets
        }

    def output(self):
        b = self.branch_data
        if self.per_plot == "processes":
            return law.SiblingFileCollection({
                s: self.local_target(f"plot__cat_{b.category}__var_{b.variable}__step{i}_{s}.pdf")
                for i, s in enumerate(self.chosen_steps)
            })
        else:  # per_plot == "steps"
            return law.SiblingFileCollection({
                p: self.local_target(f"plot__cat_{b.category}__var_{b.variable}__proc_{p}.pdf")
                for p in self.processes
            })

    @law.decorator.log
    @PlotMixin.view_output_plots
    def run(self):
        import hist

        outputs = self.output()

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
                h_in = inp[variable_inst.name].load(formatter="pickle")

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

            if self.per_plot == "processes":
                for step in self.chosen_steps:
                    # sort hists by process order
                    step_hists = OrderedDict(
                        (process_inst, hists[process_inst][{"step": hist.loc(step)}])
                        for process_inst in sorted(hists, key=process_insts.index)
                    )

                    # call the plot function
                    fig = self.call_plot_func(
                        self.plot_function_name_per_process,
                        hists=step_hists,
                        config_inst=self.config_inst,
                        variable_inst=variable_inst,
                        style_config={"legend_cfg": {"title": f"Step '{step}'"}},
                        **self.get_plot_parameters(),
                    )

                    # save the plot
                    outputs[step].dump(fig, formatter="mpl")

            else:  # per_plot == "steps"
                for process_inst, h in hists.items():
                    process_hists = OrderedDict(
                        (step, h[{"step": hist.loc(step)}])
                        for step in self.chosen_steps
                    )

                    # call the plot function
                    fig = self.call_plot_func(
                        self.plot_function_name_per_step,
                        hists=process_hists,
                        config_inst=self.config_inst,
                        variable_inst=variable_inst,
                        style_config={"legend_cfg": {"title": process_inst.label}},
                        **self.get_plot_parameters(),
                    )

                    # save the plot
                    outputs[process_inst.name].dump(fig, formatter="mpl")
