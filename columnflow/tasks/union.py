# coding: utf-8

"""
Task to unite columns horizontally into a single file for further, possibly external processing.
"""

import luigi
import law

from columnflow.tasks.framework.base import Requirements, AnalysisTask, wrapper_factory
from columnflow.tasks.framework.mixins import ProducersMixin, MLModelsMixin, ChunkedIOMixin
from columnflow.tasks.framework.remote import RemoteWorkflow
from columnflow.tasks.reduction import ReducedEventsUser
from columnflow.tasks.production import ProduceColumns
from columnflow.tasks.ml import MLEvaluation
from columnflow.util import dev_sandbox


class UniteColumns(
    MLModelsMixin,
    ProducersMixin,
    ChunkedIOMixin,
    ReducedEventsUser,
    law.LocalWorkflow,
    RemoteWorkflow,
):
    sandbox = dev_sandbox(law.config.get("analysis", "default_columnar_sandbox"))

    file_type = luigi.ChoiceParameter(
        default="parquet",
        choices=("parquet", "root"),
        description="the file type to create; choices: parquet,root; default: parquet",
    )

    # upstream requirements
    reqs = Requirements(
        ReducedEventsUser.reqs,
        RemoteWorkflow.reqs,
        ProduceColumns=ProduceColumns,
        MLEvaluation=MLEvaluation,
    )

    def workflow_requires(self):
        reqs = super().workflow_requires()

        # require the full merge forest
        reqs["events"] = self.reqs.ProvideReducedEvents.req(self)

        if not self.pilot:
            if self.producer_insts:
                reqs["producers"] = [
                    self.reqs.ProduceColumns.req(self, producer=producer_inst.cls_name)
                    for producer_inst in self.producer_insts
                    if producer_inst.produced_columns
                ]
            if self.ml_model_insts:
                reqs["ml"] = [
                    self.reqs.MLEvaluation.req(self, ml_model=m)
                    for m in self.ml_models
                ]

        return reqs

    def requires(self):
        reqs = {"events": self.reqs.ProvideReducedEvents.req(self)}

        if self.producer_insts:
            reqs["producers"] = [
                self.reqs.ProduceColumns.req(self, producer=producer_inst.cls_name)
                for producer_inst in self.producer_insts
                if producer_inst.produced_columns
            ]
        if self.ml_model_insts:
            reqs["ml"] = [
                self.reqs.MLEvaluation.req(self, ml_model=m)
                for m in self.ml_models
            ]

        return reqs

    workflow_condition = ReducedEventsUser.workflow_condition.copy()

    @workflow_condition.output
    def output(self):
        return {"events": self.target(f"data_{self.branch}.{self.file_type}")}

    @law.decorator.log
    @law.decorator.localize(input=True, output=True)
    @law.decorator.safe_output
    def run(self):
        from columnflow.columnar_util import (
            Route, RouteFilter, mandatory_coffea_columns, update_ak_array, sorted_ak_to_parquet,
            sorted_ak_to_root,
        )

        # prepare inputs and outputs
        inputs = self.input()
        output = self.output()
        output_chunks = {}

        # create a temp dir for saving intermediate files
        tmp_dir = law.LocalDirectoryTarget(is_tmp=True)
        tmp_dir.touch()

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

        # define columns that need to be read
        read_columns = write_columns | set(mandatory_coffea_columns)
        read_columns = {Route(c) for c in read_columns}

        # iterate over chunks of events and diffs
        files = [inputs["events"]["events"].path]
        if self.producer_insts:
            files.extend([inp["columns"].path for inp in inputs["producers"]])
        if self.ml_model_insts:
            files.extend([inp["mlcolumns"].path for inp in inputs["ml"]])
        for (events, *columns), pos in self.iter_chunked_io(
            files,
            source_type=len(files) * ["awkward_parquet"],
            read_columns=len(files) * [read_columns],
        ):
            # optional check for overlapping inputs
            if self.check_overlapping_inputs:
                self.raise_if_overlapping([events] + list(columns))

            # add additional columns
            events = update_ak_array(events, *columns)

            # remove columns
            events = route_filter(events)

            # optional check for finite values
            if self.check_finite_output:
                self.raise_if_not_finite(events)

            # save as parquet or root via a thread in the same pool
            chunk = tmp_dir.child(f"file_{pos.index}.{self.file_type}", type="f")
            output_chunks[pos.index] = chunk
            if self.file_type == "parquet":
                self.chunked_io.queue(sorted_ak_to_parquet, (events, chunk.path))
            else:  # root
                self.chunked_io.queue(sorted_ak_to_root, (events, chunk.path))

        # merge output files
        sorted_chunks = [output_chunks[key] for key in sorted(output_chunks)]
        if self.file_type == "parquet":
            law.pyarrow.merge_parquet_task(
                self, sorted_chunks, output["events"], local=True, writer_opts=self.get_parquet_writer_opts(),
            )
        else:  # root
            law.root.hadd_task(self, sorted_chunks, output["events"], local=True)


# overwrite class defaults
check_finite_tasks = law.config.get_expanded("analysis", "check_finite_output", [], split_csv=True)
UniteColumns.check_finite_output = ChunkedIOMixin.check_finite_output.copy(
    default=UniteColumns.task_family in check_finite_tasks,
    add_default_to_description=True,
)

check_overlap_tasks = law.config.get_expanded("analysis", "check_overlapping_inputs", [], split_csv=True)
UniteColumns.check_overlapping_inputs = ChunkedIOMixin.check_overlapping_inputs.copy(
    default=UniteColumns.task_family in check_overlap_tasks,
    add_default_to_description=True,
)


UniteColumnsWrapper = wrapper_factory(
    base_cls=AnalysisTask,
    require_cls=UniteColumns,
    enable=["configs", "skip_configs", "datasets", "skip_datasets", "shifts", "skip_shifts"],
)
