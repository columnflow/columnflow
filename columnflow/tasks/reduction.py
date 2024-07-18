# coding: utf-8

"""
Tasks related to reducing events for use on further tasks.
"""

from __future__ import annotations

import math
import functools
from collections import OrderedDict, defaultdict

import law
import luigi

from columnflow.tasks.framework.base import Requirements, AnalysisTask, DatasetTask, wrapper_factory
from columnflow.tasks.framework.mixins import (
    CalibratorsMixin, SelectorStepsMixin, ChunkedIOMixin,
)
from columnflow.tasks.framework.remote import RemoteWorkflow
from columnflow.tasks.external import GetDatasetLFNs
from columnflow.tasks.selection import CalibrateEvents, SelectEvents
from columnflow.util import maybe_import, ensure_proxy, dev_sandbox, safe_div

ak = maybe_import("awkward")


# default parameters
default_keep_reduced_events = law.config.get_expanded("analysis", "default_keep_reduced_events")


class ReduceEvents(
    SelectorStepsMixin,
    CalibratorsMixin,
    ChunkedIOMixin,
    DatasetTask,
    law.LocalWorkflow,
    RemoteWorkflow,
):
    sandbox = dev_sandbox(law.config.get("analysis", "default_columnar_sandbox"))

    # upstream requirements
    reqs = Requirements(
        RemoteWorkflow.reqs,
        GetDatasetLFNs=GetDatasetLFNs,
        CalibrateEvents=CalibrateEvents,
        SelectEvents=SelectEvents,
    )

    # strategy for handling missing source columns when adding aliases on event chunks
    missing_column_alias_strategy = "original"

    # register shifts found in the chosen selector upstream
    register_selector_shifts = True

    def workflow_requires(self):
        reqs = super().workflow_requires()

        reqs["lfns"] = self.reqs.GetDatasetLFNs.req(self)

        if not self.pilot:
            reqs["calibrations"] = [
                self.reqs.CalibrateEvents.req(self, calibrator=calibrator_inst.cls_name)
                for calibrator_inst in self.calibrator_insts
                if calibrator_inst.produced_columns
            ]
            reqs["selection"] = self.reqs.SelectEvents.req(self)
        else:
            # pass-through pilot workflow requirements of upstream task
            t = self.reqs.SelectEvents.req(self)
            reqs = law.util.merge_dicts(reqs, t.workflow_requires(), inplace=True)

        return reqs

    def requires(self):
        return {
            "lfns": self.reqs.GetDatasetLFNs.req(self),
            "calibrations": [
                self.reqs.CalibrateEvents.req(self, calibrator=calibrator_inst.cls_name)
                for calibrator_inst in self.calibrator_insts
                if calibrator_inst.produced_columns
            ],
            "selection": self.reqs.SelectEvents.req(self),
        }

    def output(self):
        return {"events": self.target(f"events_{self.branch}.parquet")}

    @ensure_proxy
    @law.decorator.localize(input=False)
    @law.decorator.safe_output
    def run(self):
        from columnflow.columnar_util import (
            Route, RouteFilter, mandatory_coffea_columns, update_ak_array, add_ak_aliases,
            sorted_ak_to_parquet,
        )
        from columnflow.selection.util import create_collections_from_masks

        # prepare inputs and outputs
        inputs = self.input()
        lfn_task = self.requires()["lfns"]
        output = self.output()
        output_chunks = {}

        # create a temp dir for saving intermediate files
        tmp_dir = law.LocalDirectoryTarget(is_tmp=True)
        tmp_dir.touch()

        # get shift dependent aliases
        aliases = self.local_shift_inst.x("column_aliases", {})

        # define columns that will be written
        write_columns: set[Route] = set()
        skip_columns: set[str] = set()
        for c in self.config_inst.x.keep_columns.get(self.task_family, ["*"]):
            for r in self._expand_keep_column(c):
                if r.has_tag("skip"):
                    skip_columns.add(r.column)
                else:
                    write_columns.add(r)
        write_columns = {
            r for r in write_columns
            if not law.util.multi_match(r.column, skip_columns, mode=any)
        }
        route_filter = RouteFilter(write_columns)

        # map routes to write to their top level column
        write_columns_groups = defaultdict(set)
        for route in write_columns:
            if len(route) > 1:
                write_columns_groups[route[0]].add(route)

        # define columns that need to be read
        read_columns = write_columns | set(mandatory_coffea_columns) | set(aliases.values())
        read_columns = {Route(c) for c in read_columns}

        # define columns to read for the differently structured selection masks
        read_sel_columns = set()
        # open either selector steps of the full event selection mask
        read_sel_columns.add(Route("steps.*" if self.selector_steps else "event"))
        # add object masks, depending on the columns to write
        # (as object masks are dynamic and deeply nested, preload the meta info to access fields)
        sel_results = inputs["selection"]["results"].load(formatter="dask_awkward")
        if "objects" in sel_results.fields:
            for src_field in sel_results.objects.fields:
                for dst_field in sel_results.objects[src_field].fields:
                    # nothing to do in case the top level column does not need to be loaded
                    if not law.util.multi_match(dst_field, write_columns_groups.keys()):
                        continue
                    # register the object masks
                    read_sel_columns.add(Route(f"objects.{src_field}.{dst_field}"))
                    # in case new collections are created and configured to be written, make sure
                    # that the corresponding columns of the source collection are loaded
                    if src_field != dst_field:
                        read_columns |= {
                            src_field + route[1:]
                            for route in write_columns_groups[dst_field]
                        }
        del sel_results

        # event counters
        n_all = 0
        n_reduced = 0

        # let the lfn_task prepare the nano file (basically determine a good pfn)
        [(lfn_index, input_file)] = lfn_task.iter_nano_files(self)

        # collect input targets
        input_targets = [input_file]
        input_targets.append(inputs["selection"]["results"])
        input_targets.extend([inp["columns"] for inp in inputs["calibrations"]])
        if self.selector_inst.produced_columns:
            input_targets.append(inputs["selection"]["columns"])

        # prepare inputs for localization
        with law.localize_file_targets(input_targets, mode="r") as inps:
            # iterate over chunks of events and diffs
            for (events, sel, *diffs), pos in self.iter_chunked_io(
                [inp.abspath for inp in inps],
                source_type=["coffea_root"] + (len(inps) - 1) * ["awkward_parquet"],
                read_columns=[read_columns, read_sel_columns] + (len(inps) - 2) * [read_columns],
            ):
                # optional check for overlapping inputs within diffs
                if self.check_overlapping_inputs:
                    self.raise_if_overlapping(list(diffs))

                # add the calibrated diffs and potentially new columns
                events = update_ak_array(events, *diffs)

                # add aliases
                events = add_ak_aliases(
                    events,
                    aliases,
                    remove_src=True,
                    missing_strategy=self.missing_column_alias_strategy,
                )

                # build the event mask
                if self.selector_steps:
                    # check if all steps are present
                    missing_steps = set(self.selector_steps) - set(sel.steps.fields)
                    if missing_steps:
                        raise Exception(
                            f"selector steps {','.join(missing_steps)} are not produced by "
                            f"selector '{self.selector}'",
                        )
                    event_mask = functools.reduce(
                        (lambda a, b: a & b),
                        (sel["steps", step] for step in self.selector_steps),
                    )
                else:
                    event_mask = sel.event if "event" in sel.fields else Ellipsis

                # apply the mask
                n_all += len(events)
                events = events[event_mask]
                n_reduced += len(events)

                # loop through all object selection, go through their masks
                # and create new collections if required
                if "objects" in sel.fields:
                    # apply the event mask
                    events = create_collections_from_masks(events, sel.objects[event_mask])

                # remove columns
                events = route_filter(events)

                # save as parquet via a thread in the same pool
                chunk = tmp_dir.child(f"file_{lfn_index}_{pos.index}.parquet", type="f")
                output_chunks[pos.index] = chunk
                self.chunked_io.queue(sorted_ak_to_parquet, (events, chunk.abspath))

        # some logs
        self.publish_message(
            f"reduced {n_all} to {n_reduced} events ({safe_div(n_reduced, n_all) * 100:.2f}%)",
        )

        # merge output files
        sorted_chunks = [output_chunks[key] for key in sorted(output_chunks)]
        law.pyarrow.merge_parquet_task(
            self, sorted_chunks, output["events"], local=True, writer_opts=self.get_parquet_writer_opts(),
        )


# overwrite class defaults
check_overlap_tasks = law.config.get_expanded("analysis", "check_overlapping_inputs", [], split_csv=True)
ReduceEvents.check_overlapping_inputs = ChunkedIOMixin.check_overlapping_inputs.copy(
    default=ReduceEvents.task_family in check_overlap_tasks,
    add_default_to_description=True,
)

ReduceEventsWrapper = wrapper_factory(
    base_cls=AnalysisTask,
    require_cls=ReduceEvents,
    enable=["configs", "skip_configs", "datasets", "skip_datasets", "shifts", "skip_shifts"],
)


class MergeReductionStats(
    SelectorStepsMixin,
    CalibratorsMixin,
    DatasetTask,
):

    n_inputs = luigi.IntParameter(
        default=10,
        significant=True,
        description="minimal number of input files for sufficient statistics to infer merging "
        "factors; default: 10",
    )
    merged_size = law.BytesParameter(
        default=law.NO_FLOAT,
        unit="MB",
        significant=False,
        description="the maximum file size of merged files; default unit is MB; when 0, the "
        "merging factor is not actually calculated from input files, but it is assumed to be 1 "
        "(= no merging); default: config value 'reduced_file_size' or 512MB'",
    )

    # upstream requirements
    reqs = Requirements(
        ReduceEvents=ReduceEvents,
    )

    @classmethod
    def resolve_param_values(cls, params):
        params = super().resolve_param_values(params)

        # check for the default merged size
        if "merged_size" in params:
            if params["merged_size"] in (None, law.NO_FLOAT):
                merged_size = 512.0
                if "config_inst" in params:
                    merged_size = params["config_inst"].x("reduced_file_size", merged_size)
                params["merged_size"] = float(merged_size)
            elif params["merged_size"] == 0:
                params["n_inputs"] = 0

        return params

    def requires(self):
        if self.merged_size == 0:
            return []
        return self.reqs.ReduceEvents.req(self, branches=((0, self.n_inputs),))

    def output(self):
        return {"stats": self.target(f"stats_n{self.n_inputs}.json")}

    @law.decorator.safe_output
    def run(self):
        # structure for statistics to save
        stats = OrderedDict([
            ("n_test_files", 0),
            ("tot_size", 0),
            ("avg_size", 0),
            ("std_size", 0),
            ("max_size_merged", 0),
            ("merge_factor", 1),
        ])

        # assume a merging factor of 1 when the merged size is 0
        if self.merged_size == 0:
            self.output()["stats"].dump(stats, indent=4, formatter="json")
            return

        # get all file sizes in bytes
        coll = self.input()["collection"]
        n = len(coll)
        sizes = [
            inp["events"].stat().st_size
            for inp in self.iter_progress(coll.targets.values(), n, msg=f"loading {n} stats ...")
        ]

        # helpers for avg and mean computation
        def get_avg_std(values):
            n = len(values)
            if n < 1:
                return 0.0, 0.0
            avg = sum(values) / n
            if n < 2:
                return avg, 0.0
            std = (sum((v - avg)**2 for v in values) / (n - 1))**0.5
            return avg, std

        # compute some stats
        stats["n_test_files"] = n
        stats["tot_size"] = sum(sizes)
        stats["avg_size"], stats["std_size"] = get_avg_std(sizes)
        stats["max_size_merged"] = self.merged_size * 1024**2  # MB to bytes

        # determine the number of files after merging, allowing a possible ~15% increase per file
        n_total = self.dataset_info_inst.n_files
        if n_total > 1:
            extrapolation = n_total / n
            n_merged_files = extrapolation * stats["tot_size"] / stats["max_size_merged"]
            rnd = math.ceil if n_merged_files % 1.0 > 0.15 else math.floor
            n_merged_files = max(int(rnd(n_merged_files)), 1)
            stats["merge_factor"] = max(math.ceil(n_total / n_merged_files), 1)
        else:
            # trivial case, no merging needed
            n_merged_files = 1
            stats["merge_factor"] = 1

        # save them
        self.output()["stats"].dump(stats, indent=4, formatter="json")

        # print them
        self.publish_message(f" stats of {n} input files ".center(40, "-"))
        self.publish_message(f"average size: {law.util.human_bytes(stats['avg_size'], fmt=True)}")
        deviation = stats["std_size"] / stats["avg_size"]
        self.publish_message(f"deviation   : {deviation * 100:.2f} % (std / avg)")
        self.publish_message(" merging info ".center(40, "-"))
        self.publish_message(f"target size : {self.merged_size} MB")
        self.publish_message(f"merging     : {stats['merge_factor']} into 1")
        self.publish_message(f"files before: {n_total}")
        self.publish_message(f"files after : {n_merged_files}")
        self.publish_message(40 * "-")


MergeReductionStatsWrapper = wrapper_factory(
    base_cls=AnalysisTask,
    require_cls=MergeReductionStats,
    enable=["configs", "skip_configs", "datasets", "skip_datasets", "shifts", "skip_shifts"],
)


class MergeReducedEvents(
    SelectorStepsMixin,
    CalibratorsMixin,
    DatasetTask,
    law.tasks.ForestMerge,
    RemoteWorkflow,
):

    keep_reduced_events = luigi.BoolParameter(
        default=default_keep_reduced_events,
        significant=False,
        description="whether to keep reduced input files after merging; when False, they are "
        f"removed after successful merging; default: {default_keep_reduced_events}",
    )

    sandbox = dev_sandbox(law.config.get("analysis", "default_columnar_sandbox"))

    # upstream requirements
    reqs = Requirements(
        RemoteWorkflow.reqs,
        MergeReductionStats=MergeReductionStats,
        ReduceEvents=ReduceEvents,
    )

    @property
    def merge_factor(self) -> int:
        """
        Defines the number of inputs to be merged per output at any point in the merging forest.
        Required by law.tasks.ForestMerge.
        """
        # return as many inputs as leafs are present to create the output of this tree, capped at 50
        return min(self.file_merging, 50)

    def is_sandboxed(self):
        # when the task is a merge forest, consider it sandboxed
        if self.is_forest():
            return True

        return super().is_sandboxed()

    @law.workflow_property(setter=True, cache=True, empty_value=0)
    def file_merging(self):
        # check if the merging stats are present
        stats = self.reqs.MergeReductionStats.req(self).output()["stats"]
        return stats.load(formatter="json")["merge_factor"] if stats.exists() else 0

    @law.dynamic_workflow_condition
    def workflow_condition(self):
        # the workflow shape can be constructed as soon as a file_merging is known
        return self.file_merging > 0

    @workflow_condition.create_branch_map
    def create_branch_map(self):
        # DatasetTask implements a custom branch map, but we want to use the one in ForestMerge
        return law.tasks.ForestMerge.create_branch_map(self)

    def merge_workflow_requires(self):
        return {
            "stats": self.reqs.MergeReductionStats.req(self),
            "events": self.reqs.ReduceEvents.req(self, _exclude={"branches"}),
        }

    def merge_requires(self, start_branch, end_branch):
        return {
            "stats": self.reqs.MergeReductionStats.req(self),
            "events": self.reqs.ReduceEvents.req(
                self,
                branches=((start_branch, end_branch),),
                workflow="local",
                _exclude={"branch"},
            ),
        }

    def trace_merge_workflow_inputs(self, inputs):
        return super().trace_merge_workflow_inputs(inputs["events"])

    def trace_merge_inputs(self, inputs):
        return super().trace_merge_inputs(inputs["events"]["collection"].targets.values())

    def reduced_dummy_output(self):
        # mark the dummy output as a placeholder for the ForestMerge task
        dummy = super().reduced_dummy_output()
        self._mark_merge_output_placeholder(dummy)
        return dummy

    @workflow_condition.output
    def output(self):
        return super().output()

    def merge_output(self):
        # use the branch_map defined in DatasetTask to compute the number of files after merging
        n_merged = len(DatasetTask.create_branch_map(self))
        return law.SiblingFileCollection([
            {"events": self.target(f"events_{i}.parquet")}
            for i in range(n_merged)
        ])

    def merge(self, inputs, output):
        inputs = [inp["events"] for inp in inputs]
        law.pyarrow.merge_parquet_task(
            self, inputs, output["events"], writer_opts=self.get_parquet_writer_opts(),
        )

        # optionally remove initial inputs
        if not self.keep_reduced_events and self.is_leaf():
            with self.publish_step("removing reduced inputs ..."):
                for inp in inputs:
                    inp.remove()


MergeReducedEventsWrapper = wrapper_factory(
    base_cls=AnalysisTask,
    require_cls=MergeReducedEvents,
    enable=["configs", "skip_configs", "datasets", "skip_datasets", "shifts", "skip_shifts"],
)


class ProvideReducedEvents(
    SelectorStepsMixin,
    CalibratorsMixin,
    DatasetTask,
    law.LocalWorkflow,
):

    skip_merging = luigi.BoolParameter(
        default=False,
        description="bypass MergedReducedEvents and directly require ReduceEvents with same "
        "workflow branching; default: False",
    )

    force_merging = luigi.BoolParameter(
        default=False,
        description="force requiring MergedReducedEvents, regardless of the merging factor "
        "obtained by MergeReductionStats; default: False",
    )

    # upstream requirements
    reqs = Requirements(
        ReduceEvents=ReduceEvents,
        MergeReductionStats=MergeReductionStats,
        MergeReducedEvents=MergeReducedEvents,
    )

    @law.workflow_property(setter=True, cache=True, empty_value=0)
    def file_merging(self):
        if self.skip_merging or self.dataset_info_inst.n_files == 1:
            return 1

        # check if the merging stats are present
        stats = self.reqs.MergeReductionStats.req(self).output()["stats"]
        return stats.load(formatter="json")["merge_factor"] if stats.exists() else 0

    @law.dynamic_workflow_condition
    def workflow_condition(self):
        # the workflow shape can be constructed as soon as a file_merging is known
        return self.file_merging > 0

    def _req_reduced_events(self, **params) -> law.Task:
        return self.reqs.ReduceEvents.req(self, **params)

    def _req_merged_reduced_events(self, **params) -> law.Task:
        if self.is_workflow():
            # require the full merging forest
            params["tree_index"] = -1
            params["branch"] = 0
        else:
            # require a single merging tree identified by the tree_index via a local workflow
            _exclude = law.util.make_set(params.pop("_exclude", None) or set())
            _exclude |= {"branch"}
            params["_exclude"] = _exclude
            params["tree_index"] = self.branch
            params["workflow"] = "local"

        return self.reqs.MergeReducedEvents.req(self, **params)

    def workflow_requires(self):
        reqs = super().workflow_requires()

        # strategy:
        # - when it is clear that the reduced events are being used directly, require them when not
        #   in pilot mode
        # - otherwise, always require the reduction stats as they are needed to make a decision
        # - when merging is forced, require it
        # - otherwise, and if the merging is already known, require either reduced or merged events
        if self.skip_merging or (not self.force_merging and self.dataset_info_inst.n_files == 1):
            # reduced events are used directly without having to look into the file merging factor
            if not self.pilot:
                reqs["events"] = self._req_reduced_events()
        else:
            # here, the merging is unclear so require the stats
            reqs["reduction_stats"] = self.reqs.MergeReductionStats.req(self)

            if self.force_merging:
                # require merged events when forced
                reqs["events"] = self._req_merged_reduced_events()
            else:
                # require either when the file merging is known, and nothing otherwise to let the
                # dynamic dependency definition resolve it at runtime
                file_merging = self.file_merging
                if file_merging > 1:
                    reqs["events"] = self._req_merged_reduced_events()
                elif file_merging == 1 and not self.pilot:
                    reqs["events"] = self._req_reduced_events()

        return reqs

    def requires(self):
        # same as for workflow requirements without optional pilot check
        reqs = {}
        if self.skip_merging or (not self.force_merging and self.dataset_info_inst.n_files == 1):
            reqs["events"] = self._req_reduced_events()
        else:
            reqs["reduction_stats"] = self.reqs.MergeReductionStats.req(self)

            if self.force_merging:
                reqs["events"] = self._req_merged_reduced_events()
            else:
                file_merging = self.file_merging
                if file_merging > 1:
                    reqs["events"] = self._req_merged_reduced_events()
                elif file_merging == 1:
                    reqs["events"] = self._req_reduced_events()

        return reqs

    @workflow_condition.output
    def output(self):
        # the "events" requirement is known at this point
        req = self.requires()["events"]

        # to simplify the handling for downstream tasks, extract the single output from workflows
        output = req.output()
        return list(output.collection.targets.values())[0] if req.is_workflow() else output

    def _yield_dynamic_deps(self):
        # do nothing if a decision was pre-set in which case requirements were already triggered
        if self.skip_merging or (not self.force_merging and self.dataset_info_inst.n_files == 1):
            return

        # yield the appropriate requirement
        yield (
            self._req_reduced_events()
            if self.file_merging == 1
            else self._req_merged_reduced_events()
        )

    def local_workflow_pre_run(self):
        return self._yield_dynamic_deps()

    def run(self):
        return self._yield_dynamic_deps()


ProvideReducedEventsWrapper = wrapper_factory(
    base_cls=AnalysisTask,
    require_cls=ProvideReducedEvents,
    enable=["configs", "skip_configs", "datasets", "skip_datasets", "shifts", "skip_shifts"],
)


class ReducedEventsUser(
    SelectorStepsMixin,
    CalibratorsMixin,
    DatasetTask,
    law.BaseWorkflow,
):
    # upstream requirements
    reqs = Requirements(
        ProvideReducedEvents=ProvideReducedEvents,
    )

    @law.workflow_property(setter=True, cache=True, empty_value=0)
    def file_merging(self):
        return self.reqs.ProvideReducedEvents.req(self).file_merging

    @law.dynamic_workflow_condition
    def workflow_condition(self):
        return self.reqs.ProvideReducedEvents.req(self).workflow_condition()

    @workflow_condition.create_branch_map
    def create_branch_map(self):
        return super().create_branch_map()
