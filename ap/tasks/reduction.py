# coding: utf-8

"""
Tasks related to reducing events for use on further tasks.
"""

from collections import OrderedDict

import law

from ap.tasks.framework.base import AnalysisTask, DatasetTask, wrapper_factory
from ap.tasks.framework.mixins import CalibratorsSelectorMixin
from ap.tasks.framework.remote import HTCondorWorkflow
from ap.tasks.external import GetDatasetLFNs
from ap.tasks.selection import CalibrateEvents, SelectEvents
from ap.util import ensure_proxy, dev_sandbox


class ReduceEvents(DatasetTask, CalibratorsSelectorMixin, law.LocalWorkflow, HTCondorWorkflow):

    sandbox = dev_sandbox("bash::$AP_BASE/sandboxes/venv_columnar.sh")

    shifts = CalibrateEvents.shifts | SelectEvents.shifts

    def workflow_requires(self):
        reqs = super().workflow_requires()
        reqs["lfns"] = GetDatasetLFNs.req(self)
        if not self.pilot:
            reqs["calib"] = CalibrateEvents.req(self)
            reqs["sel"] = SelectEvents.req(self)
        return reqs

    def requires(self):
        return {
            "lfns": GetDatasetLFNs.req(self),
            "calib": CalibrateEvents.req(self),
            "sel": SelectEvents.req(self),
        }

    def output(self):
        return self.local_target(f"events_{self.branch}.parquet")

    @law.decorator.safe_output
    @law.decorator.localize
    @ensure_proxy
    def run(self):
        from ap.columnar_util import (
            ChunkedReader, mandatory_coffea_columns, get_ak_routes, update_ak_array,
            add_ak_aliases, remove_ak_column, sorted_ak_to_parquet,
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
        keep_columns = set(self.config_inst.x.keep_columns[self.__class__.__name__])
        load_columns = keep_columns | set(mandatory_coffea_columns)
        remove_routes = None

        # let the lfn_task prepare the nano file (basically determine a good pfn)
        [(lfn_index, input_file)] = lfn_task.iter_nano_files(self)

        # open the input file with uproot
        with self.publish_step("load and open ..."):
            nano_file = input_file.load(formatter="uproot")

        # iterate over chunks of events and diffs
        with ChunkedReader(
            [nano_file, inputs["calib"].path, inputs["sel"]["res"].path],
            source_type=["coffea_root", "awkward_parquet", "awkward_parquet"],
            read_options=[{"iteritems_options": {"filter_name": load_columns}}, None, None],
        ) as reader:
            msg = f"iterate through {reader.n_entries} events ..."
            for (events, diff, sel), pos in self.iter_progress(reader, reader.n_chunks, msg=msg):
                # here, we would simply apply the mask from the selection results
                # to filter events and objects

                # add the calibrated diff and new columns from selection results
                events = update_ak_array(events, diff, sel.columns)

                # add aliases
                events = add_ak_aliases(events, aliases, remove_src=True)

                # apply the event mask
                events = events[sel.event]

                # apply all object masks whose names are present
                for name in sel.objects.fields:
                    if name in events.fields:
                        # apply the event mask to the object mask first
                        object_mask = sel.objects[name][sel.event]
                        events[name] = events[name][object_mask]

                # manually remove colums that should not be kept
                if not remove_routes:
                    remove_routes = {
                        route
                        for route in get_ak_routes(events)
                        if not law.util.multi_match("_".join(route), keep_columns)
                    }
                for route in remove_routes:
                    events = remove_ak_column(events, route)

                # save as parquet via a thread in the same pool
                chunk = tmp_dir.child(f"file_{pos.index}.parquet", type="f")
                output_chunks[pos.index] = chunk
                reader.add_task(sorted_ak_to_parquet, (events, chunk.path))

        # merge output files
        sorted_chunks = [output_chunks[key] for key in sorted(output_chunks)]
        law.pyarrow.merge_parquet_task(self, sorted_chunks, output, local=True)


ReduceEventsWrapper = wrapper_factory(
    base_cls=AnalysisTask,
    require_cls=ReduceEvents,
    enable=["configs", "skip_configs", "datasets", "skip_datasets", "shifts", "skip_shifts"],
)


class GatherReductionStats(DatasetTask, CalibratorsSelectorMixin):

    merged_size = law.BytesParameter(
        default=500.0,
        unit="MB",
        significant=False,
        description="the maximum file size of merged files; default unit is MB; default: '500MB'",
    )

    shifts = set(ReduceEvents.shifts)

    def requires(self):
        return ReduceEvents.req(self)

    def output(self):
        return self.local_target("stats.json")

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


GatherReductionStatsWrapper = wrapper_factory(
    base_cls=AnalysisTask,
    require_cls=GatherReductionStats,
    enable=["configs", "skip_configs", "datasets", "skip_datasets", "shifts", "skip_shifts"],
)


class MergeReducedEvents(DatasetTask, CalibratorsSelectorMixin, law.tasks.ForestMerge, HTCondorWorkflow):

    sandbox = dev_sandbox("bash::$AP_BASE/sandboxes/venv_columnar.sh")

    shifts = set(SelectEvents.shifts)

    # recursively merge 8 files into one
    merge_factor = 8

    # the key of the config that defines the merging in the branch map of DatasetTask
    file_merging = "after_reduction"

    @classmethod
    def modify_param_values(cls, params):
        params = cls._call_super_cls_method(DatasetTask.modify_param_values, params)
        params = cls._call_super_cls_method(law.tasks.ForestMerge.modify_param_values, params)
        return params

    def create_branch_map(self):
        # DatasetTask implements a custom branch map, but we want to use the one in ForestMerge
        return law.tasks.ForestMerge.create_branch_map(self)

    def merge_workflow_requires(self):
        return ReduceEvents.req(self, _exclude={"branches"})

    def merge_requires(self, start_branch, end_branch):
        return [ReduceEvents.req(self, branch=b) for b in range(start_branch, end_branch)]

    def merge_output(self):
        # use the branch_map defined in DatasetTask to compute the number of files after merging
        n_merged = len(DatasetTask.create_branch_map(self))
        return law.SiblingFileCollection([
            self.local_target(f"events_{i}.parquet")
            for i in range(n_merged)
        ])

    def merge(self, inputs, output):
        law.pyarrow.merge_parquet_task(self, inputs, output)


MergeReducedEventsWrapper = wrapper_factory(
    base_cls=AnalysisTask,
    require_cls=MergeReducedEvents,
    enable=["configs", "skip_configs", "datasets", "skip_datasets", "shifts", "skip_shifts"],
)
