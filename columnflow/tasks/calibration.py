# coding: utf-8

"""
Tasks related to calibrating events.
"""

import law

from columnflow.tasks.framework.base import AnalysisTask, DatasetTask, wrapper_factory
from columnflow.tasks.framework.mixins import CalibratorMixin
from columnflow.tasks.framework.remote import HTCondorWorkflow
from columnflow.tasks.external import GetDatasetLFNs
from columnflow.util import maybe_import, ensure_proxy, dev_sandbox


ak = maybe_import("awkward")


class CalibrateEvents(DatasetTask, CalibratorMixin, law.LocalWorkflow, HTCondorWorkflow):

    sandbox = dev_sandbox("bash::$CF_BASE/sandboxes/venv_columnar.sh")

    shifts = set(GetDatasetLFNs.shifts)

    # default upstream dependency task classes
    dep_GetDatasetLFNs = GetDatasetLFNs

    def workflow_requires(self, only_super: bool = False):
        reqs = super().workflow_requires()
        if only_super:
            return reqs

        reqs["lfns"] = self.dep_GetDatasetLFNs.req(self)

        # add calibrator dependent requirements
        reqs["calibrator"] = self.calibrator_inst.run_requires()

        return reqs

    def requires(self):
        reqs = {"lfns": self.dep_GetDatasetLFNs.req(self)}

        # add calibrator dependent requirements
        reqs["calibrator"] = self.calibrator_inst.run_requires()

        return reqs

    def output(self):
        return self.target(f"calib_{self.branch}.parquet")

    @law.decorator.log
    @ensure_proxy
    @law.decorator.localize(input=False, output=True)
    @law.decorator.safe_output
    def run(self):
        from columnflow.columnar_util import (
            Route, RouteFilter, ChunkedReader, mandatory_coffea_columns, sorted_ak_to_parquet,
        )

        # prepare inputs and outputs
        inputs = self.input()
        lfn_task = self.requires()["lfns"]
        output = self.output()
        output_chunks = {}

        # run the calibrator setup
        self.calibrator_inst.run_setup(inputs["calibrator"])

        # create a temp dir for saving intermediate files
        tmp_dir = law.LocalDirectoryTarget(is_tmp=True)
        tmp_dir.touch()

        # define nano columns that need to be loaded
        load_columns = mandatory_coffea_columns | self.calibrator_inst.used_columns
        load_columns_nano = [Route(column).nano_column for column in load_columns]

        # define columns that will be saved
        keep_columns = self.calibrator_inst.produced_columns
        route_filter = RouteFilter(keep_columns)

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
                # shallow-copy the events chunk first due to some coffea/awkward peculiarity which
                # would result in the coffea behavior being partially lost after new columns are
                # added, which - for some reason - does not happen on copies
                events = ak.copy(events)

                # just invoke the calibration function
                self.calibrator_inst(events)

                # remove columns
                events = route_filter(events)

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
