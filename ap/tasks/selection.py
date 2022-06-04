# coding: utf-8

"""
Tasks related to obtaining, preprocessing and selecting events.
"""

from collections import defaultdict

import law

from ap.tasks.framework.base import AnalysisTask, DatasetTask, wrapper_factory
from ap.tasks.framework.mixins import CalibratorMixin, CalibratorsSelectorMixin
from ap.tasks.framework.remote import HTCondorWorkflow
from ap.tasks.external import GetDatasetLFNs
from ap.util import ensure_proxy, dev_sandbox


class CalibrateEvents(DatasetTask, CalibratorMixin, law.LocalWorkflow, HTCondorWorkflow):

    sandbox = dev_sandbox("bash::$AP_BASE/sandboxes/venv_columnar.sh")

    def workflow_requires(self):
        reqs = super().workflow_requires()
        reqs["lfns"] = GetDatasetLFNs.req(self)
        return reqs

    def requires(self):
        return {
            "lfns": GetDatasetLFNs.req(self),
        }

    def output(self):
        return self.local_target(f"calib_{self.branch}.parquet")

    @law.decorator.safe_output
    @ensure_proxy
    def run(self):
        from ap.columnar_util import (
            ChunkedReader, mandatory_coffea_columns, get_ak_routes, remove_ak_column,
            sorted_ak_to_parquet,
        )
        from ap.calibration import Calibrator

        # prepare inputs and outputs
        lfn_task = self.requires()["lfns"]
        output = self.output()
        output_chunks = {}

        # create a temp dir for saving intermediate files
        tmp_dir = law.LocalDirectoryTarget(is_tmp=True)
        tmp_dir.touch()

        # get the calibration function
        calibrate = Calibrator.get(self.calibrator)

        # define nano columns that need to be loaded
        load_columns = mandatory_coffea_columns | calibrate.used_columns

        # define columns that will be saved
        keep_columns = calibrate.produced_columns
        remove_routes = None

        # let the lfn_task prepare the nano file (basically determine a good pfn)
        [(lfn_index, input_file)] = lfn_task.iter_nano_files(self)

        # open with uproot
        with self.publish_step("load and open ..."):
            nano_file = input_file.load(formatter="uproot")

        # iterate over chunks
        with ChunkedReader(
            nano_file,
            source_type="coffea_root",
            read_options={"iteritems_options": {"filter_name": load_columns}},
        ) as reader:
            msg = f"iterate through {reader.n_entries} events ..."
            for events, pos in self.iter_progress(reader, reader.n_chunks, msg=msg):
                # just invoke the calibration function
                events = calibrate(events)

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
                chunk = tmp_dir.child(f"file_{lfn_index}_{pos.index}.parquet", type="f")
                output_chunks[(lfn_index, pos.index)] = chunk
                reader.add_task(sorted_ak_to_parquet, (events, chunk.path))

        # merge output files
        with output.localize("w") as outp:
            sorted_chunks = [output_chunks[key] for key in sorted(output_chunks)]
            law.pyarrow.merge_parquet_task(self, sorted_chunks, outp, local=True)


CalibrateEventsWrapper = wrapper_factory(
    base_cls=AnalysisTask,
    require_cls=CalibrateEvents,
    enable=["configs", "skip_configs", "datasets", "skip_datasets", "shifts", "skip_shifts"],
)


class SelectEvents(DatasetTask, CalibratorsSelectorMixin, law.LocalWorkflow, HTCondorWorkflow):

    sandbox = dev_sandbox("bash::$AP_BASE/sandboxes/venv_columnar.sh")

    shifts = CalibrateEvents.shifts | {"jec_up", "jec_down"}

    def workflow_requires(self):
        # workflow super classes might already define requirements, so extend them
        reqs = super().workflow_requires()
        reqs["lfns"] = GetDatasetLFNs.req(self)
        if not self.pilot:
            reqs["calib"] = CalibrateEvents.req(self)
        return reqs

    def requires(self):
        # workflow branches are normal tasks, so define requirements the normal way
        return {
            "lfns": GetDatasetLFNs.req(self),
            "calib": CalibrateEvents.req(self),
        }

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
            ChunkedReader, mandatory_coffea_columns, update_ak_array, add_ak_aliases,
            sorted_ak_to_parquet,
        )
        from ap.selection import Selector

        # prepare inputs and outputs
        inputs = self.input()
        lfn_task = self.requires()["lfns"]
        outputs = self.output()
        output_chunks = {}
        stats = defaultdict(float)

        # create a temp dir for saving intermediate files
        tmp_dir = law.LocalDirectoryTarget(is_tmp=True)
        tmp_dir.touch()

        # get shift dependent aliases
        aliases = self.shift_inst.x("column_aliases", {})

        # get the selection function
        select = Selector.get(self.selector)

        # define nano columns that need to be loaded
        load_columns = mandatory_coffea_columns | select.used_columns

        # let the lfn_task prepare the nano file (basically determine a good pfn)
        [(lfn_index, input_file)] = lfn_task.iter_nano_files(self)

        # open the input file with uproot
        with self.publish_step("load and open ..."):
            nano_file = input_file.load(formatter="uproot")

        # iterate over chunks of events and diffs
        with ChunkedReader(
            [nano_file, inputs["calib"].path],
            source_type=["coffea_root", "awkward_parquet"],
            read_options=[{"iteritems_options": {"filter_name": load_columns}}, None],
        ) as reader:
            msg = f"iterate through {reader.n_entries} events ..."
            for (events, diff), pos in self.iter_progress(reader, reader.n_chunks, msg=msg):
                # here, we would start evaluating object and event selection criteria, store
                # new columns related to the selection (e.g. categories or regions), and
                # book-keeping of stats

                # apply the calibrated diff
                events = update_ak_array(events, diff)

                # add aliases
                events = add_ak_aliases(events, aliases)

                # invoke the selection function
                results = select(events, stats, self.config_inst)

                # save as parquet via a thread in the same pool
                chunk = tmp_dir.child(f"file_{lfn_index}_{pos.index}.parquet", type="f")
                output_chunks[(lfn_index, pos.index)] = chunk
                reader.add_task(sorted_ak_to_parquet, (results.to_ak(), chunk.path))

        # merge the mask files
        sorted_chunks = [output_chunks[key] for key in sorted(output_chunks)]
        law.pyarrow.merge_parquet_task(self, sorted_chunks, outputs["res"], local=True)

        # save stats
        outputs["stat"].dump(stats, formatter="json")

        # print some stats
        eff = stats["n_events_selected"] / stats["n_events"]
        eff_weighted = stats["sum_mc_weight_selected"] / stats["sum_mc_weight"]
        self.publish_message(f"all events         : {int(stats['n_events'])}")
        self.publish_message(f"sel. events        : {int(stats['n_events_selected'])}")
        self.publish_message(f"efficiency         : {eff:.4f}")
        self.publish_message(f"sum mc weights     : {stats['sum_mc_weight']}")
        self.publish_message(f"sum sel. mc weights: {stats['sum_mc_weight_selected']}")
        self.publish_message(f"efficiency         : {eff_weighted:.4f}")


SelectEventsWrapper = wrapper_factory(
    base_cls=AnalysisTask,
    require_cls=SelectEvents,
    enable=["configs", "skip_configs", "datasets", "skip_datasets", "shifts", "skip_shifts"],
)


class MergeSelectionStats(DatasetTask, CalibratorsSelectorMixin, law.tasks.ForestMerge):

    shifts = set(SelectEvents.shifts)

    # recursively merge 20 files into one
    merge_factor = 20

    @classmethod
    def modify_param_values(cls, params):
        params = cls._call_super_cls_method(DatasetTask.modify_param_values, params)
        params = cls._call_super_cls_method(law.tasks.ForestMerge.modify_param_values, params)
        return params

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
