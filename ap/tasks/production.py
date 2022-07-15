# coding: utf-8

"""
Tasks related to producing new columns.
"""

import law

from ap.tasks.framework.base import AnalysisTask, wrapper_factory
from ap.tasks.framework.mixins import CalibratorsMixin, SelectorStepsMixin, ProducerMixin
from ap.tasks.framework.remote import HTCondorWorkflow
from ap.tasks.reduction import MergeReducedEventsUser, MergeReducedEvents
from ap.util import dev_sandbox


class ProduceColumns(
    MergeReducedEventsUser,
    ProducerMixin,
    SelectorStepsMixin,
    CalibratorsMixin,
    law.LocalWorkflow,
    HTCondorWorkflow,
):

    sandbox = dev_sandbox("bash::$AP_BASE/sandboxes/venv_columnar.sh")

    shifts = set(MergeReducedEvents.shifts)

    def workflow_requires(self):
        reqs = super().workflow_requires()

        reqs["events"] = MergeReducedEvents.req(self, _exclude={"branches"})
        reqs["producer"] = self.producer_inst.run_requires()

        return reqs

    def requires(self):
        return {
            "events": MergeReducedEvents.req(self, tree_index=self.branch, _exclude={"branch"}),
            "producer": self.producer_inst.run_requires(),
        }

    @MergeReducedEventsUser.maybe_dummy
    def output(self):
        return self.local_target(f"columns_{self.branch}.parquet")

    @law.decorator.safe_output
    @law.decorator.localize
    def run(self):
        from ap.columnar_util import (
            RouteFilter, ChunkedReader, mandatory_coffea_columns, sorted_ak_to_parquet,
        )

        # prepare inputs and outputs
        inputs = self.input()
        output = self.output()
        output_chunks = {}

        # run the producer setup
        self.producer_inst.run_setup(inputs["producer"])

        # create a temp dir for saving intermediate files
        tmp_dir = law.LocalDirectoryTarget(is_tmp=True)
        tmp_dir.touch()

        # define nano columns that need to be loaded
        load_columns = mandatory_coffea_columns | self.producer_inst.used_columns  # noqa

        # define columns that will be saved
        keep_columns = self.producer_inst.produced_columns
        route_filter = RouteFilter(keep_columns)

        # iterate over chunks of events and diffs
        with ChunkedReader(
            inputs["events"]["collection"][0].path,
            source_type="awkward_parquet",
            # TODO: not working yet since parquet columns are nested
            # open_options={"columns": load_columns},
        ) as reader:
            msg = f"iterate through {reader.n_entries} events ..."
            for events, pos in self.iter_progress(reader, reader.n_chunks, msg=msg):
                # invoke the producer
                self.producer_inst(events)

                # remove columns
                events = route_filter(events)

                # save as parquet via a thread in the same pool
                chunk = tmp_dir.child(f"file_{pos.index}.parquet", type="f")
                output_chunks[pos.index] = chunk
                reader.add_task(sorted_ak_to_parquet, (events, chunk.path))

        # merge output files
        sorted_chunks = [output_chunks[key] for key in sorted(output_chunks)]
        law.pyarrow.merge_parquet_task(self, sorted_chunks, output, local=True)


ProduceColumnsWrapper = wrapper_factory(
    base_cls=AnalysisTask,
    require_cls=ProduceColumns,
    enable=["configs", "skip_configs", "datasets", "skip_datasets", "shifts", "skip_shifts"],
)
