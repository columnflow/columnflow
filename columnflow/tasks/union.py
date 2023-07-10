# coding: utf-8

"""
Task to unite columns horizontally into a single file for further, possibly external processing.
"""

import law

from columnflow.tasks.framework.base import Requirements, AnalysisTask, wrapper_factory
from columnflow.tasks.framework.mixins import (
    CalibratorsMixin, SelectorStepsMixin, ProducersMixin, MLModelsMixin, ChunkedIOMixin,
)
from columnflow.tasks.framework.remote import RemoteWorkflow
from columnflow.tasks.reduction import MergeReducedEventsUser, MergeReducedEvents
from columnflow.tasks.production import ProduceColumns
from columnflow.tasks.ml import MLEvaluation
from columnflow.util import dev_sandbox


class UniteColumns(
    MLModelsMixin,
    ProducersMixin,
    SelectorStepsMixin,
    CalibratorsMixin,
    ChunkedIOMixin,
    MergeReducedEventsUser,
    law.LocalWorkflow,
    RemoteWorkflow,
):
    sandbox = dev_sandbox(law.config.get("analysis", "default_columnar_sandbox"))

    # upstream requirements
    reqs = Requirements(
        MergeReducedEvents.reqs,
        RemoteWorkflow.reqs,
        MergeReducedEvents=MergeReducedEvents,
        ProduceColumns=ProduceColumns,
        MLEvaluation=MLEvaluation,
    )

    def workflow_requires(self):
        reqs = super().workflow_requires()

        # require the full merge forest
        reqs["events"] = self.reqs.MergeReducedEvents.req(self, tree_index=-1)

        if not self.pilot:
            if self.producers:
                reqs["producers"] = [
                    self.reqs.ProduceColumns.req(self, producer=producer_inst.cls_name)
                    for producer_inst in self.producer_insts
                    if producer_inst.produced_columns
                ]
            if self.ml_models:
                reqs["ml"] = [
                    self.reqs.MLEvaluation.req(self, ml_model=m)
                    for m in self.ml_models
                ]

        return reqs

    def requires(self):
        reqs = {
            "events": self.reqs.MergeReducedEvents.req(self, tree_index=self.branch, _exclude={"branch"}),
        }

        if self.producers:
            reqs["producers"] = [
                self.reqs.ProduceColumns.req(self, producer=producer_inst.cls_name)
                for producer_inst in self.producer_insts
                if producer_inst.produced_columns
            ]
        if self.ml_models:
            reqs["ml"] = [
                self.reqs.MLEvaluation.req(self, ml_model=m)
                for m in self.ml_models
            ]

        return reqs

    @MergeReducedEventsUser.maybe_dummy
    def output(self):
        return {"columns": self.target(f"data_{self.branch}.parquet")}

    @law.decorator.log
    @law.decorator.localize(input=True, output=False)
    @law.decorator.safe_output
    def run(self):
        from columnflow.columnar_util import (
            Route, RouteFilter, mandatory_coffea_columns, update_ak_array, sorted_ak_to_parquet,
        )

        # prepare inputs and outputs
        inputs = self.input()
        output = self.output()
        output_chunks = {}

        # create a temp dir for saving intermediate files
        tmp_dir = law.LocalDirectoryTarget(is_tmp=True)
        tmp_dir.touch()

        # define columns that will be written
        write_columns = set(self.config_inst.x.keep_columns.get(self.task_family, ["*"]))
        route_filter = RouteFilter(write_columns)

        # define columns that need to be read
        read_columns = write_columns | set(mandatory_coffea_columns)
        read_columns = {Route(c) for c in read_columns}

        # iterate over chunks of events and diffs
        files = [inputs["events"]["collection"][0]["events"].path]
        if self.producers:
            files.extend([inp["columns"].path for inp in inputs["producers"]])
        if self.ml_models:
            files.extend([inp["mlcolumns"].path for inp in inputs["ml"]])
        for (events, *columns), pos in self.iter_chunked_io(
            files,
            source_type=len(files) * ["awkward_parquet"],
            read_columns=len(files) * [read_columns],
        ):
            # add additional columns
            events = update_ak_array(events, *columns)

            # remove columns
            events = route_filter(events)

            # optional check for finite values
            if self.check_finite:
                self.raise_if_not_finite(events)

            # save as parquet via a thread in the same pool
            chunk = tmp_dir.child(f"file_{pos.index}.parquet", type="f")
            output_chunks[pos.index] = chunk
            self.chunked_io.queue(sorted_ak_to_parquet, (events, chunk.path))

        # merge output files
        sorted_chunks = [output_chunks[key] for key in sorted(output_chunks)]
        law.pyarrow.merge_parquet_task(self, sorted_chunks, output["columns"], local=True)


# overwrite class defaults
check_finite_tasks = law.config.get_expanded("analysis", "check_finite_output", [], split_csv=True)
UniteColumns.check_finite = ChunkedIOMixin.check_finite.copy(
    default=UniteColumns.task_family in check_finite_tasks,
    add_default_to_description=True,
)


UniteColumnsWrapper = wrapper_factory(
    base_cls=AnalysisTask,
    require_cls=UniteColumns,
    enable=["configs", "skip_configs", "datasets", "skip_datasets", "shifts", "skip_shifts"],
)
