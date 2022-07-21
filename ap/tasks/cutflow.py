# coding: utf-8

"""
Tasks to be implemented: MergeSelectionMasks, PlotCutflow
"""

import functools
from collections import OrderedDict

import law

from ap.tasks.framework.base import AnalysisTask, DatasetTask, ShiftTask, wrapper_factory
from ap.tasks.framework.mixins import CalibratorsMixin, SelectorMixin, SelectorStepsMixin, PlotMixin
from ap.tasks.framework.remote import HTCondorWorkflow
from ap.tasks.plotting import ProcessPlotBase
from ap.tasks.selection import SelectEvents
from ap.production import Producer
from ap.util import dev_sandbox, ensure_proxy
from ap.plotting.variables import plot_cutflow


class MergeSelectionMasks(
    DatasetTask,
    SelectorMixin,
    CalibratorsMixin,
    law.tasks.ForestMerge,
    HTCondorWorkflow,
):

    sandbox = dev_sandbox("bash::$AP_BASE/sandboxes/venv_columnar.sh")

    shifts = set(SelectEvents.shifts)

    # recursively merge 8 files into one
    merge_factor = 8

    # default upstream dependency task classes
    dep_SelectEvents = SelectEvents

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # store the normalization weight producer
        self.norm_weight_producer = Producer.get_cls("normalization_weights")(
            inst_dict=self.get_producer_kwargs(self),
        )

    def create_branch_map(self):
        # DatasetTask implements a custom branch map, but we want to use the one in ForestMerge
        return law.tasks.ForestMerge.create_branch_map(self)

    def merge_workflow_requires(self):
        return {
            "selection": self.dep_SelectEvents.req(self, _exclude={"branches"}),
            "normalization": self.norm_weight_producer.run_requires(),
        }

    def merge_requires(self, start_branch, end_branch):
        return {
            "selection": [
                self.dep_SelectEvents.req(self, branch=b)
                for b in range(start_branch, end_branch)
            ],
            "normalization": self.norm_weight_producer.run_requires(),
        }

    def trace_merge_workflow_inputs(self, inputs):
        return super().trace_merge_workflow_inputs(inputs["selection"])

    def trace_merge_inputs(self, inputs):
        return super().trace_merge_inputs(inputs["selection"])

    def merge_output(self):
        return self.local_target("masks.parquet")

    def merge(self, inputs, output):
        # in the lowest (leaf) stage, zip selection results with additional columns first
        if self.is_leaf():
            # create a temp dir for saving intermediate files
            tmp_dir = law.LocalDirectoryTarget(is_tmp=True)
            tmp_dir.touch()
            inputs = self.zip_results_and_columns(inputs, tmp_dir)

        law.pyarrow.merge_parquet_task(self, inputs, output, local=True)

    def zip_results_and_columns(self, inputs, tmp_dir):
        import awkward as ak
        from ap.columnar_util import RouteFilter, sorted_ak_to_parquet

        chunks = []

        # setup the normalization weights producer
        self.norm_weight_producer.run_setup(self.input()["forest_merge"]["normalization"])

        # get columns to keep
        keep_columns = set(self.config_inst.x.keep_columns[self.task_family])
        route_filter = RouteFilter(keep_columns)

        for inp in inputs:
            events = inp["columns"].load(formatter="awkward")
            steps = inp["results"].load(formatter="awkward").steps

            # add normalization weight
            self.norm_weight_producer(events)

            # remove columns
            events = route_filter(events)

            # zip them
            out = ak.zip({"steps": steps, "events": events})

            chunk = tmp_dir.child(f"tmp_{inp['results'].basename}", type="f")
            chunks.append(chunk)
            sorted_ak_to_parquet(out, chunk.path)

        return chunks


MergeSelectionMasksWrapper = wrapper_factory(
    base_cls=AnalysisTask,
    require_cls=MergeSelectionMasks,
    enable=["configs", "skip_configs", "datasets", "skip_datasets", "shifts", "skip_shifts"],
)


class CreateCutflowHistograms(
    DatasetTask,
    SelectorStepsMixin,
    CalibratorsMixin,
    law.LocalWorkflow,
    HTCondorWorkflow,
):

    sandbox = dev_sandbox("bash::$AP_BASE/sandboxes/venv_columnar.sh")

    shifts = set(MergeSelectionMasks.shifts)

    selector_steps_order_sensitive = True

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
        return self.local_target("cutflow_hist.pickle")

    @law.decorator.safe_output
    @law.decorator.localize
    @ensure_proxy
    def run(self):
        import hist
        import awkward as ak
        from ap.columnar_util import ChunkedReader, Route, add_ak_aliases

        # prepare inputs and outputs
        inputs = self.input()

        # create a temp dir for saving intermediate files
        tmp_dir = law.LocalDirectoryTarget(is_tmp=True)
        tmp_dir.touch()

        # get shift dependent aliases
        aliases = self.shift_inst.x("column_aliases", {})

        # define a list of variables to create the histograms for
        # TODO: for the moment, this is just the lhe_weight but it could be extended in the future
        #       and made configurable from the command line
        variable_insts = [self.config_inst.get_variable("lhe_weight")]

        # get the expression per variable and when it's a string, parse it to extract index lookups
        expressions = {}
        for variable_inst in variable_insts:
            expr = variable_inst.expression
            if isinstance(expr, str):
                route = Route.check(expr)
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

        # dump the histogram
        self.output().dump(histograms, formatter="pickle")


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

    sandbox = "bash::$AP_BASE/sandboxes/cmssw_default.sh"

    shifts = set(CreateCutflowHistograms.shifts)

    selector_steps_order_sensitive = True

    # default upstream dependency task classes
    dep_CreateCutflowHistograms = CreateCutflowHistograms

    def create_branch_map(self):
        # one category per branch
        return list(self.categories)

    def workflow_requires(self, only_super: bool = False):
        reqs = super(PlotCutflow, self).workflow_requires()
        if only_super:
            return reqs

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
        return self.local_target(f"cutflow__cat_{self.branch_data}.pdf")

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
                h_in = inp.load(formatter="pickle")["lhe_weight"]

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

            fig = plot_cutflow(hists, self.config_inst)

            self.output().dump(fig, formatter="mpl")


PlotCutflowWrapper = wrapper_factory(
    base_cls=AnalysisTask,
    require_cls=PlotCutflow,
    enable=["configs", "skip_configs", "shifts", "skip_shifts"],
)
