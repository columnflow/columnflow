# coding: utf-8

"""
Tasks related to calibrating events.
"""

import law

from ap.tasks.framework.base import AnalysisTask, DatasetTask, wrapper_factory
from ap.tasks.framework.mixins import CalibratorMixin
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
            Route, ChunkedReader, mandatory_coffea_columns, get_ak_routes, remove_ak_column,
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
        load_columns_nano = [Route.check(column).nano_column for column in load_columns]

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
            read_options={"iteritems_options": {"filter_name": load_columns_nano}},
        ) as reader:
            msg = f"iterate through {reader.n_entries} events ..."
            for events, pos in self.iter_progress(reader, reader.n_chunks, msg=msg):
                # just invoke the calibration function
                arr = calibrate(
                    events,
                    config_inst=self.config_inst,
                    dataset_inst=self.dataset_inst,
                    shift_inst=self.shift_inst,
                )

                # manually remove colums that should not be kept
                if not remove_routes:
                    remove_routes = {
                        route
                        for route in get_ak_routes(arr)
                        if not law.util.multi_match(route.column, keep_columns)
                    }
                for route in remove_routes:
                    arr = remove_ak_column(arr, route)

                # save as parquet via a thread in the same pool
                chunk = tmp_dir.child(f"file_{lfn_index}_{pos.index}.parquet", type="f")
                output_chunks[(lfn_index, pos.index)] = chunk
                reader.add_task(sorted_ak_to_parquet, (arr, chunk.path))

        # merge output files
        with output.localize("w") as outp:
            sorted_chunks = [output_chunks[key] for key in sorted(output_chunks)]
            law.pyarrow.merge_parquet_task(self, sorted_chunks, outp, local=True)


CalibrateEventsWrapper = wrapper_factory(
    base_cls=AnalysisTask,
    require_cls=CalibrateEvents,
    enable=["configs", "skip_configs", "datasets", "skip_datasets", "shifts", "skip_shifts"],
)
