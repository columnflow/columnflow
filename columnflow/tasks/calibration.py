# coding: utf-8

"""
Tasks related to calibrating events.
"""

import law

from columnflow.tasks.framework.base import AnalysisTask, DatasetTask, wrapper_factory
from columnflow.tasks.framework.mixins import CalibratorMixin, ChunkedIOMixin
from columnflow.tasks.framework.remote import RemoteWorkflow
from columnflow.tasks.external import GetDatasetLFNs
from columnflow.util import maybe_import, ensure_proxy, dev_sandbox


ak = maybe_import("awkward")


class CalibrateEvents(
    DatasetTask,
    CalibratorMixin,
    ChunkedIOMixin,
    law.LocalWorkflow,
    RemoteWorkflow,
):
    # default sandbox, might be overwritten by calibrator function
    sandbox = dev_sandbox("bash::$CF_BASE/sandboxes/venv_columnar.sh")

    # default upstream dependency task classes
    dep_GetDatasetLFNs = GetDatasetLFNs

    @classmethod
    def get_allowed_shifts(cls, config_inst, params):
        shifts = super().get_allowed_shifts(config_inst, params)
        shifts |= cls.dep_GetDatasetLFNs.get_allowed_shifts(config_inst, params)
        return shifts

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
            Route, RouteFilter, mandatory_coffea_columns, sorted_ak_to_parquet,
        )

        # prepare inputs and outputs
        reqs = self.requires()
        lfn_task = reqs["lfns"]
        inputs = self.input()
        output = self.output()
        output_chunks = {}

        # run the calibrator setup
        self.calibrator_inst.run_setup(reqs["calibrator"], inputs["calibrator"])

        # create a temp dir for saving intermediate files
        tmp_dir = law.LocalDirectoryTarget(is_tmp=True)
        tmp_dir.touch()

        # define columns that need to be read
        read_columns = mandatory_coffea_columns | self.calibrator_inst.used_columns
        read_columns = {Route(c) for c in read_columns}

        # define columns that will be written
        write_columns = self.calibrator_inst.produced_columns
        route_filter = RouteFilter(write_columns)

        # let the lfn_task prepare the nano file (basically determine a good pfn)
        [(lfn_index, input_file)] = lfn_task.iter_nano_files(self)

        # open with uproot
        with self.publish_step("load and open ..."):
            nano_file = input_file.load(formatter="uproot")

        # iterate over chunks
        for events, pos in self.iter_chunked_io(
            nano_file,
            source_type="coffea_root",
            read_columns=read_columns,
        ):
            # just invoke the calibration function
            events = self.calibrator_inst(events)

            # remove columns
            events = route_filter(events)

            # optional check for finite values
            if self.check_finite:
                self.raise_if_not_finite(events)

            # save as parquet via a thread in the same pool
            chunk = tmp_dir.child(f"file_{lfn_index}_{pos.index}.parquet", type="f")
            output_chunks[(lfn_index, pos.index)] = chunk
            self.chunked_io.queue(sorted_ak_to_parquet, (events, chunk.path))

        # merge output files
        with output.localize("w") as outp:
            sorted_chunks = [output_chunks[key] for key in sorted(output_chunks)]
            law.pyarrow.merge_parquet_task(self, sorted_chunks, outp, local=True)


# overwrite class defaults
check_finite_tasks = law.config.get_expanded("analysis", "check_finite_output", [], split_csv=True)
CalibrateEvents.check_finite = ChunkedIOMixin.check_finite.copy(
    default=CalibrateEvents.task_family in check_finite_tasks,
    add_default_to_description=True,
)


CalibrateEventsWrapper = wrapper_factory(
    base_cls=AnalysisTask,
    require_cls=CalibrateEvents,
    enable=["configs", "skip_configs", "datasets", "skip_datasets", "shifts", "skip_shifts"],
)
