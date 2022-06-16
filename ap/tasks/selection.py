# coding: utf-8

"""
Tasks related to selecting events and performing post-selection bookkeeping.
"""

from collections import defaultdict

import law

from ap.tasks.framework.base import AnalysisTask, DatasetTask, wrapper_factory
from ap.tasks.framework.mixins import CalibratorsSelectorMixin
from ap.tasks.framework.remote import HTCondorWorkflow
from ap.tasks.external import GetDatasetLFNs
from ap.tasks.calibration import CalibrateEvents
from ap.util import ensure_proxy, dev_sandbox


class SelectEvents(DatasetTask, CalibratorsSelectorMixin, law.LocalWorkflow, HTCondorWorkflow):

    sandbox = dev_sandbox("bash::$AP_BASE/sandboxes/venv_columnar.sh")

    shifts = {CalibrateEvents, "jec_up", "jec_down"}

    def workflow_requires(self):
        # workflow super classes might already define requirements, so extend them
        reqs = super().workflow_requires()
        reqs["lfns"] = GetDatasetLFNs.req(self)
        if not self.pilot:
            reqs["calib"] = [CalibrateEvents.req(self, calibrator=c) for c in self.calibrators]

        # add selector dependent requirements
        reqs["selector"] = self.selector_func.run_requires(self)

        return reqs

    def requires(self):
        reqs = {
            "lfns": GetDatasetLFNs.req(self),
            "calib": [CalibrateEvents.req(self, calibrator=c) for c in self.calibrators],
        }

        # add selector dependent requirements
        reqs["selector"] = self.selector_func.run_requires(self)

        return reqs

    def output(self):
        return {
            "res": self.local_target(f"results_{self.branch}.parquet"),
            "stat": self.local_target(f"stats_{self.branch}.json"),
        }

    @law.decorator.safe_output
    @law.decorator.localize
    @ensure_proxy
    def run(self):
        from ap.columnar_util import (
            Route, ChunkedReader, mandatory_coffea_columns, update_ak_array, add_ak_aliases,
            sorted_ak_to_parquet,
        )

        # prepare inputs and outputs
        inputs = self.input()
        lfn_task = self.requires()["lfns"]
        outputs = self.output()
        output_chunks = {}
        stats = defaultdict(float)

        # run the selector setup
        self.selector_func.run_setup(self, inputs["selector"])

        # create a temp dir for saving intermediate files
        tmp_dir = law.LocalDirectoryTarget(is_tmp=True)
        tmp_dir.touch()

        # get shift dependent aliases
        aliases = self.shift_inst.x("column_aliases", {})

        # define nano columns that need to be loaded
        load_columns = mandatory_coffea_columns | self.selector_func.used_columns
        load_columns_nano = [Route.check(column).nano_column for column in load_columns]

        # let the lfn_task prepare the nano file (basically determine a good pfn)
        [(lfn_index, input_file)] = lfn_task.iter_nano_files(self)

        # open the input file with uproot
        with self.publish_step("load and open ..."):
            nano_file = input_file.load(formatter="uproot")

        # iterate over chunks of events and diffs
        n_calib = len(inputs["calib"])
        with ChunkedReader(
            [nano_file] + [inp.path for inp in inputs["calib"]],
            source_type=["coffea_root"] + n_calib * ["awkward_parquet"],
            read_options=[{"iteritems_options": {"filter_name": load_columns_nano}}] + n_calib * [None],
        ) as reader:
            msg = f"iterate through {reader.n_entries} events ..."
            for (events, *diffs), pos in self.iter_progress(reader, reader.n_chunks, msg=msg):
                # here, we would start evaluating object and event selection criteria, store
                # new columns related to the selection (e.g. categories or regions), and
                # book-keeping of stats

                # apply the calibrated diffs
                events = update_ak_array(events, *diffs)

                # add aliases
                events = add_ak_aliases(events, aliases)

                # invoke the selection function
                results = self.selector_func(events, stats, **self.get_selector_kwargs(self))

                # save as parquet via a thread in the same pool
                chunk = tmp_dir.child(f"file_{lfn_index}_{pos.index}.parquet", type="f")
                output_chunks[(lfn_index, pos.index)] = chunk
                reader.add_task(sorted_ak_to_parquet, (results.to_ak(), chunk.path))

        # merge the mask files
        sorted_chunks = [output_chunks[key] for key in sorted(output_chunks)]
        law.pyarrow.merge_parquet_task(self, sorted_chunks, outputs["res"], local=True)

        # if len(stats) > 0:
        #     # save stats
        #     outputs["stat"].dump(stats, formatter="json")

        #     # print some stats
        #     eff = stats["n_events_selected"] / stats["n_events"]
        #     eff_weighted = stats["sum_mc_weight_selected"] / stats["sum_mc_weight"]
        #     self.publish_message(f"all events         : {int(stats['n_events'])}")
        #     self.publish_message(f"sel. events        : {int(stats['n_events_selected'])}")
        #     self.publish_message(f"efficiency         : {eff:.4f}")
        #     self.publish_message(f"sum mc weights     : {stats['sum_mc_weight']}")
        #     self.publish_message(f"sum sel. mc weights: {stats['sum_mc_weight_selected']}")
        #     self.publish_message(f"efficiency         : {eff_weighted:.4f}")


SelectEventsWrapper = wrapper_factory(
    base_cls=AnalysisTask,
    require_cls=SelectEvents,
    enable=["configs", "skip_configs", "datasets", "skip_datasets", "shifts", "skip_shifts"],
)


class MergeSelectionStats(DatasetTask, CalibratorsSelectorMixin, law.tasks.ForestMerge):

    shifts = {SelectEvents}

    # recursively merge 20 files into one
    merge_factor = 20

    def create_branch_map(self):
        # DatasetTask implements a custom branch map, but we want to use the one in ForestMerge
        return law.tasks.ForestMerge.create_branch_map(self)

    def merge_workflow_requires(self):
        return SelectEvents.req(self, _exclude={"branches"})

    def merge_requires(self, start_branch, end_branch):
        return [SelectEvents.req(self, branch=b) for b in range(start_branch, end_branch)]

    def merge_output(self):
        return self.local_target("stats.json")

    def merge(self, inputs, output):
        # merge input stats
        merged_stats = defaultdict(float)
        for inp in inputs:
            stats = inp["stat"].load(formatter="json")
            self.merge_counts(merged_stats, stats)

        # write the output
        output.dump(merged_stats, indent=4, formatter="json")

    @classmethod
    def merge_counts(cls, dst: dict, src: dict) -> dict:
        """
        Adds counts (integers or floats) in a *src* dictionary recursively into a *dst* dictionary.
        *dst* is updated in-place and also returned.
        """
        for key, obj in src.items():
            if isinstance(obj, dict):
                cls.merge_counts(dst.setdefault(key, {}), obj)
            else:
                if key not in dst:
                    dst[key] = 0.0
                dst[key] += obj
        return dst


MergeSelectionStatsWrapper = wrapper_factory(
    base_cls=AnalysisTask,
    require_cls=MergeSelectionStats,
    enable=["configs", "skip_configs", "datasets", "skip_datasets", "shifts", "skip_shifts"],
)
