# coding: utf-8

"""
Tasks related to producing new columns.
"""

import itertools

import luigi
import law

from columnflow.tasks.framework.base import Requirements, AnalysisTask, wrapper_factory
from columnflow.tasks.framework.mixins import ProducerMixin, ChunkedIOMixin
from columnflow.tasks.framework.remote import RemoteWorkflow
from columnflow.tasks.reduction import ReducedEventsUser
from columnflow.util import dev_sandbox


class ProduceColumns(
    ProducerMixin,
    ChunkedIOMixin,
    ReducedEventsUser,
    law.LocalWorkflow,
    RemoteWorkflow,
):
    # default sandbox, might be overwritten by producer function
    sandbox = dev_sandbox(law.config.get("analysis", "default_columnar_sandbox"))

    # upstream requirements
    reqs = Requirements(
        ReducedEventsUser.reqs,
        RemoteWorkflow.reqs,
    )

    # register sandbox and shifts found in the chosen producer to this task
    register_producer_sandbox = True
    register_producer_shifts = True

    # strategy for handling missing source columns when adding aliases on event chunks
    missing_column_alias_strategy = "original"

    def workflow_requires(self):
        reqs = super().workflow_requires()

        # require the full merge forest
        reqs["events"] = self.reqs.ProvideReducedEvents.req(self)

        # add producer dependent requirements
        reqs["producer"] = law.util.make_unique(law.util.flatten(self.producer_inst.run_requires()))

        return reqs

    def requires(self):
        return {
            "events": self.reqs.ProvideReducedEvents.req(self),
            "producer": law.util.make_unique(law.util.flatten(self.producer_inst.run_requires())),
        }

    workflow_condition = ReducedEventsUser.workflow_condition.copy()

    @workflow_condition.output
    def output(self):
        outputs = {}

        # only declare the output in case the producer actually creates columns
        if self.producer_inst.produced_columns:
            outputs["columns"] = self.target(f"columns_{self.branch}.parquet")

        return outputs

    @law.decorator.log
    @law.decorator.localize(input=False)
    @law.decorator.safe_output
    def run(self):
        from columnflow.columnar_util import (
            Route, RouteFilter, mandatory_coffea_columns, update_ak_array, add_ak_aliases,
            sorted_ak_to_parquet,
        )

        # prepare inputs and outputs
        inputs = self.input()
        output = self.output()
        output_chunks = {}

        # run the producer setup
        producer_reqs = self.producer_inst.run_requires()
        reader_targets = self.producer_inst.run_setup(producer_reqs, luigi.task.getpaths(producer_reqs))
        n_ext = len(reader_targets)

        # create a temp dir for saving intermediate files
        tmp_dir = law.LocalDirectoryTarget(is_tmp=True)
        tmp_dir.touch()

        # get shift dependent aliases
        aliases = self.local_shift_inst.x("column_aliases", {})

        # define columns that need to be read
        read_columns = set(map(Route, mandatory_coffea_columns))
        read_columns |= self.producer_inst.used_columns
        read_columns |= set(map(Route, set(aliases.values())))

        # define columns that will be written
        write_columns = self.producer_inst.produced_columns
        route_filter = RouteFilter(write_columns)

        # prepare inputs for localization
        with law.localize_file_targets(
            [inputs["events"]["events"], *reader_targets.values()],
            mode="r",
        ) as inps:
            # iterate over chunks of events and diffs
            for (events, *cols), pos in self.iter_chunked_io(
                [inp.path for inp in inps],
                source_type=["awkward_parquet"] + [None] * n_ext,
                read_columns=[read_columns] * (1 + n_ext),
                chunk_size=self.producer_inst.get_min_chunk_size(),
            ):
                # optional check for overlapping inputs
                if self.check_overlapping_inputs:
                    self.raise_if_overlapping([events] + list(cols))

                # apply the optional columns from custom requirements
                events = update_ak_array(events, *cols)

                # add aliases
                events = add_ak_aliases(
                    events,
                    aliases,
                    remove_src=True,
                    missing_strategy=self.missing_column_alias_strategy,
                )

                # invoke the producer
                if len(events):
                    events = self.producer_inst(events)

                # remove columns
                events = route_filter(events)

                # optional check for finite values
                if self.check_finite_output:
                    self.raise_if_not_finite(events)

                # save as parquet via a thread in the same pool
                chunk = tmp_dir.child(f"file_{pos.index}.parquet", type="f")
                output_chunks[pos.index] = chunk
                self.chunked_io.queue(sorted_ak_to_parquet, (events, chunk.path))

        # merge output files
        sorted_chunks = [output_chunks[key] for key in sorted(output_chunks)]
        law.pyarrow.merge_parquet_task(
            self, sorted_chunks, output["columns"], local=True, writer_opts=self.get_parquet_writer_opts(),
        )


# overwrite class defaults
check_finite_tasks = law.config.get_expanded("analysis", "check_finite_output", [], split_csv=True)
ProduceColumns.check_finite_output = ChunkedIOMixin.check_finite_output.copy(
    default=ProduceColumns.task_family in check_finite_tasks,
    add_default_to_description=True,
)

check_overlap_tasks = law.config.get_expanded("analysis", "check_overlapping_inputs", [], split_csv=True)
ProduceColumns.check_overlapping_inputs = ChunkedIOMixin.check_overlapping_inputs.copy(
    default=ProduceColumns.task_family in check_overlap_tasks,
    add_default_to_description=True,
)


_ProduceColumnsWrapperBase = wrapper_factory(
    base_cls=AnalysisTask,
    require_cls=ProduceColumns,
    enable=["configs", "skip_configs", "datasets", "skip_datasets", "shifts", "skip_shifts"],
)
_ProduceColumnsWrapperBase.exclude_index = True


class ProduceColumnsWrapper(_ProduceColumnsWrapperBase):

    producers = law.CSVParameter(
        default=(),
        description="names of producers to use; if empty, the default producer is used",
    )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        if self.producers:
            # add the producers parameter
            self.wrapper_fields.append("producer")

            # extend the parameter combinations with producers
            self.wrapper_parameters = [
                params + (producer,)
                for params, producer in itertools.product(self.wrapper_parameters, self.producers)
            ]
