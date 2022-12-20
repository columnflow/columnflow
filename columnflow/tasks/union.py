# coding: utf-8

"""
Task to unite columns horizontally into a single file for further, possibly external processing.
"""

import law

from columnflow.tasks.framework.base import AnalysisTask, wrapper_factory
from columnflow.tasks.framework.mixins import (
    CalibratorsMixin, SelectorStepsMixin, ProducersMixin, MLModelsMixin, ChunkedIOMixin,
)
from columnflow.tasks.framework.remote import RemoteWorkflow
from columnflow.tasks.reduction import MergeReducedEventsUser, MergeReducedEvents
from columnflow.tasks.production import ProduceColumns
from columnflow.tasks.ml import MLEvaluation
from columnflow.util import dev_sandbox


class UniteColumns(
    MergeReducedEventsUser,
    MLModelsMixin,
    ProducersMixin,
    SelectorStepsMixin,
    CalibratorsMixin,
    ChunkedIOMixin,
    law.LocalWorkflow,
    RemoteWorkflow,
):
    sandbox = dev_sandbox("bash::$CF_BASE/sandboxes/venv_columnar.sh")

    # default upstream dependency task classes
    dep_MergeReducedEvents = MergeReducedEvents
    dep_ProduceColumns = ProduceColumns
    dep_MLEvaluation = MLEvaluation

    @classmethod
    def get_allowed_shifts(cls, config_inst, params):
        shifts = super().get_allowed_shifts(config_inst, params)
        shifts |= cls.dep_MergeReducedEvents.get_allowed_shifts(config_inst, params)
        shifts |= cls.dep_ProduceColumns.get_allowed_shifts(config_inst, params)
        return shifts

    def workflow_requires(self, only_super: bool = False):
        reqs = super().workflow_requires()
        if only_super:
            return reqs

        # require the full merge forest
        reqs["events"] = self.dep_MergeReducedEvents.req(self, tree_index=-1)

        if not self.pilot:
            if self.producers:
                reqs["producers"] = [
                    self.dep_ProduceColumns.req(self, producer=p)
                    for p in self.producers
                ]
            if self.ml_models:
                reqs["ml"] = [
                    self.dep_MLEvaluation.req(self, ml_model=m)
                    for m in self.ml_models
                ]

        return reqs

    def requires(self):
        reqs = {
            "events": self.dep_MergeReducedEvents.req(self, tree_index=self.branch, _exclude={"branch"}),
        }

        if self.producers:
            reqs["producers"] = [
                self.dep_ProduceColumns.req(self, producer=p)
                for p in self.producers
            ]
        if self.ml_models:
            reqs["ml"] = [
                self.dep_MLEvaluation.req(self, ml_model=m)
                for m in self.ml_models
            ]

        return reqs

    @MergeReducedEventsUser.maybe_dummy
    def output(self):
        return self.target(f"data_{self.branch}.parquet")

    @law.decorator.log
    @law.decorator.localize(input=True, output=False)
    @law.decorator.safe_output
    def run(self):
        from columnflow.columnar_util import RouteFilter, update_ak_array, sorted_ak_to_parquet

        # prepare inputs and outputs
        inputs = self.input()
        output = self.output()
        output_chunks = {}

        # create a temp dir for saving intermediate files
        tmp_dir = law.LocalDirectoryTarget(is_tmp=True)
        tmp_dir.touch()

        # define nano columns that should be kept, and that need to be loaded
        keep_columns = set(self.config_inst.x.keep_columns.get(self.task_family, ["*"]))
        # load_columns = keep_columns | set(mandatory_coffea_columns)
        route_filter = RouteFilter(keep_columns)

        # iterate over chunks of events and diffs
        files = [inputs["events"]["collection"][0].path]
        if self.producers:
            files.extend([inp.path for inp in inputs["producers"]])
        if self.ml_models:
            files.extend([inp.path for inp in inputs["ml"]])
        for (events, *columns), pos in self.iter_chunked_io(
            files,
            source_type=len(files) * ["awkward_parquet"],
            # TODO: not working yet since parquet columns are nested
            # open_options=[{"columns": load_columns}] + (len(files) - 1) * [None],
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
        law.pyarrow.merge_parquet_task(self, sorted_chunks, output, local=True)


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
