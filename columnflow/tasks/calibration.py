# coding: utf-8

"""
Tasks related to calibrating events.
"""

import luigi
import law

from columnflow.tasks.framework.base import Requirements, AnalysisTask, DatasetTask, wrapper_factory
from columnflow.tasks.framework.mixins import CalibratorMixin, ChunkedIOMixin
from columnflow.tasks.framework.remote import RemoteWorkflow
from columnflow.tasks.external import GetDatasetLFNs
from columnflow.util import maybe_import, ensure_proxy, dev_sandbox

ak = maybe_import("awkward")


class CalibrateEvents(
    CalibratorMixin,
    ChunkedIOMixin,
    DatasetTask,
    law.LocalWorkflow,
    RemoteWorkflow,
):
    """
    Task to apply calibrations to objects, e.g. leptons and jets.

    The calibrations that are to be applied can be specified on the command line, and are
    implemented as instances of the :py:class:`~columnflow.calibration.Calibrator` class. For
    further information, please consider the documentation there.
    """

    # default sandbox, might be overwritten by calibrator function
    sandbox = dev_sandbox(law.config.get("analysis", "default_columnar_sandbox"))

    # upstream requirements
    reqs = Requirements(
        RemoteWorkflow.reqs,
        GetDatasetLFNs=GetDatasetLFNs,
    )

    # register sandbox and shifts found in the chosen calibrator to this task
    register_calibrator_sandbox = True
    register_calibrator_shifts = True

    def workflow_requires(self) -> dict:
        """
        Configure the requirements for the workflow in general. For more general informations, see
        :external+law:py:meth:`BaseWorkflow.workflow_requires() <law.workflow.base.BaseWorkflow.workflow_requires>`.

        :return: Dictionary containing the requirements for this task.
        """
        reqs = super().workflow_requires()

        reqs["lfns"] = self.reqs.GetDatasetLFNs.req(self)

        # add calibrator dependent requirements
        reqs["calibrator"] = law.util.make_unique(law.util.flatten(self.calibrator_inst.run_requires()))

        return reqs

    def requires(self) -> dict:
        """
        Configure the requirements for the individual branches of the workflow.
        """
        reqs = {"lfns": self.reqs.GetDatasetLFNs.req(self)}

        # add calibrator dependent requirements
        reqs["calibrator"] = law.util.make_unique(law.util.flatten(self.calibrator_inst.run_requires()))

        return reqs

    def output(self):
        """
        Defines the outputs of the current branch within the workflow.
        """
        outputs = {}

        # only declare the output in case the calibrator actually creates columns
        if self.calibrator_inst.produced_columns:
            outputs["columns"] = self.target(f"calib_{self.branch}.parquet")

        return outputs

    @law.decorator.log
    @ensure_proxy
    @law.decorator.localize(input=False)
    @law.decorator.safe_output
    def run(self):
        """
        Run method of this task.
        """
        from columnflow.columnar_util import (
            Route, RouteFilter, mandatory_coffea_columns, sorted_ak_to_parquet, update_ak_array,
        )

        # prepare inputs and outputs
        lfn_task = self.requires()["lfns"]
        output = self.output()
        output_chunks = {}

        # run the calibrator setup
        calibrator_reqs = self.calibrator_inst.run_requires()
        reader_targets = self.calibrator_inst.run_setup(calibrator_reqs, luigi.task.getpaths(calibrator_reqs))

        # create a temp dir for saving intermediate files
        tmp_dir = law.LocalDirectoryTarget(is_tmp=True)
        tmp_dir.touch()

        # define columns that need to be read
        read_columns = set(map(Route, mandatory_coffea_columns))
        read_columns |= self.calibrator_inst.used_columns

        # define columns that will be written
        write_columns = self.calibrator_inst.produced_columns
        route_filter = RouteFilter(write_columns)

        # let the lfn_task prepare the nano file (basically determine a good pfn)
        [(lfn_index, input_file)] = lfn_task.iter_nano_files(self)

        # prepare inputs for localization
        with law.localize_file_targets(
            [input_file, *reader_targets.values()],
            mode="r",
        ) as inps:
            # iterate over chunks
            for (events, *cols), pos in self.iter_chunked_io(
                [inp.abspath for inp in inps],
                source_type=["coffea_root"] + (len(inps) - 1) * [None],
                read_columns=len(inps) * [read_columns],
                chunk_size=self.calibrator_inst.get_min_chunk_size(),
            ):
                # optional check for overlapping inputs
                if self.check_overlapping_inputs:
                    self.raise_if_overlapping([events] + list(cols))

                # insert additional columns
                events = update_ak_array(events, *cols)

                # just invoke the calibration function
                events = self.calibrator_inst(events)

                # remove columns
                events = route_filter(events)

                # optional check for finite values
                if self.check_finite_output:
                    self.raise_if_not_finite(events)

                # save as parquet via a thread in the same pool
                chunk = tmp_dir.child(f"file_{lfn_index}_{pos.index}.parquet", type="f")
                output_chunks[(lfn_index, pos.index)] = chunk
                self.chunked_io.queue(sorted_ak_to_parquet, (events, chunk.abspath))

        # merge output files
        with output["columns"].localize("w") as outp:
            sorted_chunks = [output_chunks[key] for key in sorted(output_chunks)]
            law.pyarrow.merge_parquet_task(
                self, sorted_chunks, outp, local=True, writer_opts=self.get_parquet_writer_opts(),
            )


# overwrite class defaults
check_finite_tasks = law.config.get_expanded("analysis", "check_finite_output", [], split_csv=True)
CalibrateEvents.check_finite_output = ChunkedIOMixin.check_finite_output.copy(
    default=CalibrateEvents.task_family in check_finite_tasks,
    add_default_to_description=True,
)

check_overlap_tasks = law.config.get_expanded("analysis", "check_overlapping_inputs", [], split_csv=True)
CalibrateEvents.check_overlapping_inputs = ChunkedIOMixin.check_overlapping_inputs.copy(
    default=CalibrateEvents.task_family in check_overlap_tasks,
    add_default_to_description=True,
)


CalibrateEventsWrapper = wrapper_factory(
    base_cls=AnalysisTask,
    require_cls=CalibrateEvents,
    enable=["configs", "skip_configs", "datasets", "skip_datasets", "shifts", "skip_shifts"],
    docs="""
Wrapper task to calibrate events for multiple datasets.

:enables: ["configs", "skip_configs", "datasets", "skip_datasets", "shifts", "skip_shifts"]
""",
)
