# coding: utf-8

"""
Tasks related to reducing events for use on further tasks.
"""

import functools
from collections import OrderedDict

import law
import luigi

from columnflow.tasks.framework.base import AnalysisTask, DatasetTask, wrapper_factory
from columnflow.tasks.framework.mixins import CalibratorsMixin, SelectorStepsMixin
from columnflow.tasks.framework.remote import HTCondorWorkflow
from columnflow.tasks.external import GetDatasetLFNs
from columnflow.tasks.selection import CalibrateEvents, SelectEvents
from columnflow.util import maybe_import, ensure_proxy, dev_sandbox, safe_div


ak = maybe_import("awkward")


class ReduceEvents(DatasetTask, SelectorStepsMixin, CalibratorsMixin, law.LocalWorkflow, HTCondorWorkflow):

    sandbox = dev_sandbox("bash::$CF_BASE/sandboxes/venv_columnar.sh")

    shifts = GetDatasetLFNs.shifts | CalibrateEvents.shifts | SelectEvents.shifts

    # default upstream dependency task classes
    dep_GetDatasetLFNs = GetDatasetLFNs
    dep_CalibrateEvents = CalibrateEvents
    dep_SelectEvents = SelectEvents

    def workflow_requires(self, only_super: bool = False):
        reqs = super().workflow_requires()
        if only_super:
            return reqs

        reqs["lfns"] = self.dep_GetDatasetLFNs.req(self)
        if not self.pilot:
            reqs["calibrations"] = [
                self.dep_CalibrateEvents.req(self, calibrator=c)
                for c in self.calibrators
            ]
            reqs["selection"] = self.dep_SelectEvents.req(self)
        return reqs

    def requires(self):
        return {
            "lfns": self.dep_GetDatasetLFNs.req(self),
            "calibrations": [
                self.dep_CalibrateEvents.req(self, calibrator=c)
                for c in self.calibrators
            ],
            "selection": self.dep_SelectEvents.req(self),
        }

    def output(self):
        return self.target(f"events_{self.branch}.parquet")

    @ensure_proxy
    @law.decorator.localize
    @law.decorator.safe_output
    def run(self):
        from columnflow.columnar_util import (
            Route, RouteFilter, ChunkedReader, mandatory_coffea_columns, update_ak_array,
            add_ak_aliases, sorted_ak_to_parquet, set_ak_column,
        )

        # prepare inputs and outputs
        inputs = self.input()
        lfn_task = self.requires()["lfns"]
        output = self.output()
        output_chunks = {}

        # create a temp dir for saving intermediate files
        tmp_dir = law.LocalDirectoryTarget(is_tmp=True)
        tmp_dir.touch()

        # get shift dependent aliases
        aliases = self.shift_inst.x("column_aliases", {})

        # define nano columns that should be kept, and that need to be loaded
        keep_columns = set(self.config_inst.x.keep_columns[self.task_family])
        load_columns = keep_columns | set(mandatory_coffea_columns)
        load_columns_nano = [Route(column).nano_column for column in load_columns]
        route_filter = RouteFilter(keep_columns)

        # event counters
        n_all = 0
        n_reduced = 0

        # let the lfn_task prepare the nano file (basically determine a good pfn)
        [(lfn_index, input_file)] = lfn_task.iter_nano_files(self)

        # open the input file with uproot
        with self.publish_step("load and open ..."):
            nano_file = input_file.load(formatter="uproot")

        # iterate over chunks of events and diffs
        input_paths = [nano_file]
        input_paths.append(inputs["selection"]["results"].path)
        input_paths.extend([inp.path for inp in inputs["calibrations"]])
        if self.selector_inst.produced_columns:
            input_paths.append(inputs["selection"]["columns"].path)
        with ChunkedReader(
            input_paths,
            source_type=["coffea_root"] + (len(input_paths) - 1) * ["awkward_parquet"],
            read_options=[{"iteritems_options": {"filter_name": load_columns_nano}}] + (len(input_paths) - 1) * [None],
        ) as reader:
            msg = f"iterate through {reader.n_entries} events ..."
            for (events, sel, *diffs), pos in self.iter_progress(reader, reader.n_chunks, msg=msg):
                # shallow-copy the events chunk first due to some coffea/awkward peculiarity which
                # would result in the coffea behavior being partially lost after new columns are
                # added, which - for some reason - does not happen on copies
                events = ak.copy(events)

                # add the calibrated diffs and potentially new columns
                events = update_ak_array(events, *diffs)

                # add aliases
                events = add_ak_aliases(events, aliases, remove_src=True)

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
                for src_name in sel.objects.fields:
                    # get all destination collections, handling those named identically to the
                    # source collection last
                    dst_names = sel["objects", src_name].fields
                    if src_name in dst_names:
                        dst_names.remove(src_name)
                        dst_names.append(src_name)
                    for dst_name in dst_names:
                        object_mask = sel.objects[src_name, dst_name][event_mask]
                        dst_collection = events[src_name][object_mask]
                        set_ak_column(events, dst_name, dst_collection)

                # remove columns
                events = route_filter(events)

                # save as parquet via a thread in the same pool
                chunk = tmp_dir.child(f"file_{pos.index}.parquet", type="f")
                output_chunks[pos.index] = chunk
                reader.add_task(sorted_ak_to_parquet, (events, chunk.path))

        # some logs
        self.publish_message(
            f"reduced {n_all} to {n_reduced} events ({safe_div(n_reduced, n_all) * 100:.2f}%)",
        )

        # merge output files
        sorted_chunks = [output_chunks[key] for key in sorted(output_chunks)]
        law.pyarrow.merge_parquet_task(self, sorted_chunks, output, local=True)


ReduceEventsWrapper = wrapper_factory(
    base_cls=AnalysisTask,
    require_cls=ReduceEvents,
    enable=["configs", "skip_configs", "datasets", "skip_datasets", "shifts", "skip_shifts"],
)


class MergeReductionStats(DatasetTask, SelectorStepsMixin, CalibratorsMixin):

    n_inputs = luigi.IntParameter(
        default=20,
        significant=True,
        description="minimal number of input files for sufficient statistics to infer merging "
        "factors; default: 20",
    )
    merged_size = law.BytesParameter(
        default=1024.0,
        unit="MB",
        significant=False,
        description="the maximum file size of merged files; default unit is MB; default: '1024MB'",
    )

    shifts = set(ReduceEvents.shifts)

    # default upstream dependency task classes
    dep_ReduceEvents = ReduceEvents

    def requires(self):
        return self.dep_ReduceEvents.req(self, branches=((0, self.n_inputs),))

    def output(self):
        return self.target(f"stats_n{self.n_inputs}.json")

    @law.decorator.safe_output
    def run(self):
        # get all file sizes in bytes
        coll = self.input()["collection"]
        n = len(coll)
        sizes = [
            inp.stat().st_size
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
        max_size_merged = self.merged_size * 1024**2
        tot_size = sum(sizes)
        avg_size, std_size = get_avg_std(sizes)
        std_size = (sum((s - avg_size)**2 for s in sizes) / n)**0.5
        merge_factor = max(1, int(round(max_size_merged / avg_size)))
        merged_sizes = [sum(chunk) for chunk in law.util.iter_chunks(sizes, merge_factor)]
        n_merged = len(merged_sizes)
        avg_size_merged, std_size_merged = get_avg_std(merged_sizes)

        # save them
        stats = OrderedDict([
            ("n_files", n),
            ("tot_size", tot_size),
            ("avg_size", avg_size),
            ("std_size", std_size),
            ("max_size_merged", max_size_merged),
            ("merge_factor", merge_factor),
            ("n_merged_files", n_merged),
            ("avg_size_merged", avg_size_merged),
            ("std_size_merged", std_size_merged),
        ])
        self.output().dump(stats, indent=4, formatter="json")

        # print them
        self.publish_message(" files before merging ".center(40, "-"))
        self.publish_message(f"# files  : {n}")
        self.publish_message(f"tot. size: {law.util.human_bytes(tot_size, fmt=True)}")
        self.publish_message(f"avg. size: {law.util.human_bytes(avg_size, fmt=True)}")
        self.publish_message(f"std. size: {law.util.human_bytes(std_size, fmt=True)}")
        self.publish_message(" files after merging ".center(40, "-"))
        self.publish_message(f"merging  : {merge_factor} into 1")
        self.publish_message(f"# files  : {n_merged}")
        self.publish_message(f"max. size: {law.util.human_bytes(max_size_merged, fmt=True)}")
        self.publish_message(f"avg. size: {law.util.human_bytes(avg_size_merged, fmt=True)}")
        self.publish_message(f"std. size: {law.util.human_bytes(std_size_merged, fmt=True)}")


MergeReductionStatsWrapper = wrapper_factory(
    base_cls=AnalysisTask,
    require_cls=MergeReductionStats,
    enable=["configs", "skip_configs", "datasets", "skip_datasets", "shifts", "skip_shifts"],
)


class MergeReducedEventsUser(DatasetTask):

    # recursively merge 8 files into one
    merge_factor = 8

    # default upstream dependency task classes
    dep_MergeReductionStats = MergeReductionStats

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # cached value of the file_merging until it's positive
        self._cached_file_merging = -1

        # in case this is a workflow, do not cache branches by default
        # (this is enabled in reduced_file_merging once positive)
        self._cache_branches = False

    @property
    def file_merging(self):
        """
        Needed by DatasetTask to define the default branch map.
        """
        if self._cached_file_merging < 0:
            # reset the forest in case this is a forest ForestMerge task
            self._forest_built = False

            # check of the merging stats is present and of so, set the cached file merging value
            output = self.dep_MergeReductionStats.req(self).output()
            if output.exists():
                self._cached_file_merging = output.load(formatter="json")["merge_factor"]
                self._cache_branches = True

        return self._cached_file_merging

    @property
    def merging_stats_exist(self):
        return self.file_merging >= 1

    def reduced_dummy_output(self):
        # dummy output to be returned in case the merging stats are not present yet
        return self.target("DUMMY_UNTIL_REDUCED_MERGING_STATS_EXIST")

    @classmethod
    def maybe_dummy(cls, func):
        # meant to wrap output methods of tasks depending on merging stats
        # to inject a dummy output in case the status are not there yet
        @functools.wraps(func)
        def wrapper(self):
            # when the merging stats do not exist yet, return a dummy target
            if not self.merging_stats_exist:
                return self.reduced_dummy_output()

            # otherwise, bind the wrapped function and call it
            return func.__get__(self, self.__class__)()

        return wrapper


class MergeReducedEvents(
    MergeReducedEventsUser,
    SelectorStepsMixin,
    CalibratorsMixin,
    law.tasks.ForestMerge,
    HTCondorWorkflow,
):

    sandbox = dev_sandbox("bash::$CF_BASE/sandboxes/venv_columnar.sh")

    shifts = ReduceEvents.shifts | MergeReductionStats.shifts

    # default upstream dependency task classes
    dep_MergeReductionStats = MergeReductionStats
    dep_ReduceEvents = ReduceEvents

    def create_branch_map(self):
        # DatasetTask implements a custom branch map, but we want to use the one in ForestMerge
        return law.tasks.ForestMerge.create_branch_map(self)

    def merge_workflow_requires(self):
        return {
            "stats": self.dep_MergeReductionStats.req(self),
            "events": self.dep_ReduceEvents.req(self, _exclude={"branches"}),
        }

    def merge_requires(self, start_branch, end_branch):
        return {
            "stats": self.dep_MergeReductionStats.req(self),
            "events": [
                self.dep_ReduceEvents.req(self, branch=b)
                for b in range(start_branch, end_branch)
            ],
        }

    def trace_merge_workflow_inputs(self, inputs):
        return super().trace_merge_workflow_inputs(inputs["events"])

    def trace_merge_inputs(self, inputs):
        return super().trace_merge_inputs(inputs["events"])

    @MergeReducedEventsUser.maybe_dummy
    def merge_output(self):
        # use the branch_map defined in DatasetTask to compute the number of files after merging
        n_merged = len(DatasetTask.create_branch_map(self))
        return law.SiblingFileCollection([
            self.target(f"events_{i}.parquet")
            for i in range(n_merged)
        ])

    def merge(self, inputs, output):
        law.pyarrow.merge_parquet_task(self, inputs, output)


MergeReducedEventsWrapper = wrapper_factory(
    base_cls=AnalysisTask,
    require_cls=MergeReducedEvents,
    enable=["configs", "skip_configs", "datasets", "skip_datasets", "shifts", "skip_shifts"],
)
