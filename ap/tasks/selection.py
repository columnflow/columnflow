# coding: utf-8

"""
Tasks related to obtaining, preprocessing and selecting events.
"""

from collections import defaultdict

import law

from ap.tasks.framework import DatasetTask, HTCondorWorkflow
from ap.tasks.external import GetDatasetLFNs
from ap.util import ensure_proxy


class CalibrateObjects(DatasetTask, law.LocalWorkflow, HTCondorWorkflow):

    sandbox = "bash::$AP_BASE/sandboxes/venv_columnar.sh"

    def workflow_requires(self):
        reqs = super(CalibrateObjects, self).workflow_requires()
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
            ChunkedReader, mandatory_coffea_columns, get_ak_routes, remove_nano_column,
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
        # TODO: get this from the config or a parameter?
        calibrate = Calibrator.get("calib_test")

        # define nano columns that need to be loaded
        load_columns = mandatory_coffea_columns | calibrate.used_columns

        # define columns that will be saved
        keep_columns = calibrate.produced_columns
        remove_routes = None

        # loop over all input file indices requested by this branch (most likely just one)
        for (file_index, input_file) in lfn_task.iter_nano_files(self):
            # open with uproot
            with self.publish_step("load and open ..."):
                ufile = input_file.load(formatter="uproot")

            # iterate over chunks
            with ChunkedReader(
                ufile,
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
                        events = remove_nano_column(events, route)

                    # save as parquet via a thread in the same pool
                    chunk = tmp_dir.child(f"file_{file_index}_{pos.index}.parquet", type="f")
                    output_chunks[(file_index, pos.index)] = chunk
                    reader.add_task(sorted_ak_to_parquet, (events, chunk.path))

        # merge output files
        with output.localize("w") as outp:
            sorted_chunks = [output_chunks[key] for key in sorted(output_chunks)]
            law.pyarrow.merge_parquet_task(self, sorted_chunks, outp, local=True)


class SelectEvents(DatasetTask, law.LocalWorkflow, HTCondorWorkflow):

    sandbox = "bash::$AP_BASE/sandboxes/venv_columnar.sh"

    shifts = CalibrateObjects.shifts | {"jec_up", "jec_down"}

    def workflow_requires(self):
        # workflow super classes might already define requirements, so extend them
        reqs = super(SelectEvents, self).workflow_requires()
        reqs["lfns"] = GetDatasetLFNs.req(self)
        if not self.pilot:
            reqs["calib"] = CalibrateObjects.req(self)
        return reqs

    def requires(self):
        # workflow branches are normal tasks, so define requirements the normal way
        return {
            "lfns": GetDatasetLFNs.req(self),
            "calib": CalibrateObjects.req(self),
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
            ChunkedReader, mandatory_coffea_columns, update_ak_array, add_nano_aliases,
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
        # TODO: get this from the config or a parameter?
        select = Selector.get("select_test")

        # define nano columns that need to be loaded
        load_columns = mandatory_coffea_columns | select.used_columns

        # loop over all input file indices requested by this branch (most likely just one)
        for (file_index, input_file) in lfn_task.iter_nano_files(self):
            # open the input file with uproot
            with self.publish_step("load and open ..."):
                ufile = input_file.load(formatter="uproot")

            # iterate over chunks of events and diffs
            with ChunkedReader(
                [ufile, inputs["calib"].path],
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
                    events = add_nano_aliases(events, aliases)

                    # invoke the selection function
                    results = select(events, stats)

                    # save as parquet via a thread in the same pool
                    chunk = tmp_dir.child(f"file_{file_index}_{pos.index}.parquet", type="f")
                    output_chunks[(file_index, pos.index)] = chunk
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


class MergeSelectionStats(DatasetTask, law.tasks.ForestMerge):

    shifts = set(SelectEvents.shifts)

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
