# coding: utf-8

"""
Column production methods related detecting truth processes.
"""

from ap.production import Producer, producer
from ap.util import maybe_import
from ap.columnar_util import set_ak_column

ak = maybe_import("awkward")


@producer(
    produces={"process_id"},
)
def process_ids(self: Producer, events: ak.Array, **kwargs) -> ak.Array:
    """
    Assigns each event a single process id, based on the first process that is registered for the
    internal py:attr:`dataset_inst`. This is rather a dummy method and should be further implemented
    depending on future needs (e.g. for sample stitching).
    """
    # trivial case
    if len(self.dataset_inst.processes) != 1:
        raise NotImplementedError(
            f"dataset {self.dataset_inst.name} has {len(self.dataset_inst.processes)} processes "
            "assigned, which is not yet implemented",
        )
    process_id = self.dataset_inst.processes.get_first().id

    # store the column
    set_ak_column(events, "process_id", len(events) * [process_id])

    return events
