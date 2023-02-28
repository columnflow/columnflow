# coding: utf-8

"""
Tasks related to producing new columns.
"""

import law

from columnflow.tasks.framework.base import Requirements, AnalysisTask, wrapper_factory
from columnflow.tasks.framework.mixins import (
    CalibratorsMixin, SelectorStepsMixin, ProducerMixin, ChunkedIOMixin,
)
from columnflow.tasks.framework.remote import RemoteWorkflow
from columnflow.tasks.reduction import MergeReducedEventsUser, MergeReducedEvents
from columnflow.util import dev_sandbox


class ProduceColumns(
    ProducerMixin,
    SelectorStepsMixin,
    CalibratorsMixin,
    ChunkedIOMixin,
    MergeReducedEventsUser,
    law.LocalWorkflow,
    RemoteWorkflow,
):
    # default sandbox, might be overwritten by producer function
    sandbox = dev_sandbox("bash::$CF_BASE/sandboxes/venv_columnar.sh")

    # upstream requirements
    reqs = Requirements(
        MergeReducedEventsUser.reqs,
        RemoteWorkflow.reqs,
        MergeReducedEvents=MergeReducedEvents,
    )

    register_producer_shifts = True

    def workflow_requires(self):
        reqs = super().workflow_requires()

        # require the full merge forest
        reqs["events"] = self.reqs.MergeReducedEvents.req(self, tree_index=-1)

        # add producer dependent requirements
        reqs["producer"] = self.producer_inst.run_requires()

        return reqs

    def requires(self):
        return {
            "events": self.reqs.MergeReducedEvents.req(self, tree_index=self.branch, _exclude={"branch"}),
            "producer": self.producer_inst.run_requires(),
        }

    @MergeReducedEventsUser.maybe_dummy
    def output(self):
        return self.target(f"columns_{self.branch}.parquet")

    @law.decorator.log
    @law.decorator.localize
    @law.decorator.safe_output
    def run(self):
        from columnflow.columnar_util import (
            Route, RouteFilter, mandatory_coffea_columns, add_ak_aliases, sorted_ak_to_parquet,
        )

        # prepare inputs and outputs
        reqs = self.requires()
        inputs = self.input()
        output = self.output()
        output_chunks = {}

        # run the producer setup
        self.producer_inst.run_setup(reqs["producer"], inputs["producer"])

        # create a temp dir for saving intermediate files
        tmp_dir = law.LocalDirectoryTarget(is_tmp=True)
        tmp_dir.touch()

        # get shift dependent aliases
        aliases = self.local_shift_inst.x("column_aliases", {})

        # define columns that need to be read
        read_columns = mandatory_coffea_columns | self.producer_inst.used_columns | set(aliases.values())
        read_columns = {Route(c) for c in read_columns}

        # define columns that will be written
        write_columns = self.producer_inst.produced_columns
        route_filter = RouteFilter(write_columns)

        # iterate over chunks of events and diffs
        for events, pos in self.iter_chunked_io(
            inputs["events"]["collection"][0].path,
            source_type="awkward_parquet",
            read_columns=read_columns,
        ):
            # add aliases
            events = add_ak_aliases(events, aliases, remove_src=True)

            # invoke the producer
            if len(events):
                events = self.producer_inst(events)

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
ProduceColumns.check_finite = ChunkedIOMixin.check_finite.copy(
    default=ProduceColumns.task_family in check_finite_tasks,
    add_default_to_description=True,
)


ProduceColumnsWrapper = wrapper_factory(
    base_cls=AnalysisTask,
    require_cls=ProduceColumns,
    enable=["configs", "skip_configs", "datasets", "skip_datasets", "shifts", "skip_shifts"],
)
