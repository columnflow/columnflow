# coding: utf-8

"""
Tasks related to producing new columns.
"""

import law

from ap.tasks.framework.base import AnalysisTask, DatasetTask, wrapper_factory
from ap.tasks.framework.mixins import CalibratorsSelectorMixin, ProducerMixin
from ap.tasks.framework.remote import HTCondorWorkflow
from ap.tasks.reduction import ReduceEvents
from ap.util import ensure_proxy, dev_sandbox


class ProduceColumns(DatasetTask, ProducerMixin, CalibratorsSelectorMixin, law.LocalWorkflow, HTCondorWorkflow):

    sandbox = dev_sandbox("bash::$AP_BASE/sandboxes/venv_columnar.sh")

    shifts = {ReduceEvents}

    update_producer = True

    def workflow_requires(self):
        reqs = super().workflow_requires()

        if not self.pilot:
            reqs["events"] = ReduceEvents.req(self)

        # add producer dependent requirements
        reqs["producer"] = self.producer_func.run_requires(self)

        return reqs

    def requires(self):
        reqs = {"events": ReduceEvents.req(self)}

        # add producer dependent requirements
        reqs["producer"] = self.producer_func.run_requires(self)

        return reqs

    def output(self):
        return self.local_target(f"columns_{self.branch}.parquet")

    @law.decorator.safe_output
    @law.decorator.localize
    @ensure_proxy
    def run(self):
        from ap.columnar_util import (
            ChunkedReader, mandatory_coffea_columns, get_ak_routes, remove_ak_column,
            sorted_ak_to_parquet,
        )

        # prepare inputs and outputs
        inputs = self.input()
        output = self.output()
        output_chunks = {}

        # run the producer setup
        self.producer_func.run_setup(self, inputs["producer"])

        # create a temp dir for saving intermediate files
        tmp_dir = law.LocalDirectoryTarget(is_tmp=True)
        tmp_dir.touch()

        # define nano columns that need to be loaded
        load_columns = mandatory_coffea_columns | self.producer_func.used_columns  # noqa

        # define columns that will be saved
        keep_columns = self.producer_func.produced_columns
        remove_routes = None

        # iterate over chunks of events and diffs
        with ChunkedReader(
            inputs["events"].path,
            source_type="awkward_parquet",
            # TODO: not working yet since parquet columns are nested
            # open_options={"columns": load_columns},
        ) as reader:
            msg = f"iterate through {reader.n_entries} events ..."
            for events, pos in self.iter_progress(reader, reader.n_chunks, msg=msg):
                # invoke the producer
                events = self.producer_func(events, **self.get_producer_kwargs(self))

                # manually remove colums that should not be kept
                if not remove_routes:
                    remove_routes = {
                        route
                        for route in get_ak_routes(events)
                        if not law.util.multi_match(route.column, keep_columns)
                    }
                for route in remove_routes:
                    events = remove_ak_column(events, route)

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
