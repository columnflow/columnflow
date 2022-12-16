# coding: utf-8

"""
Column production methods related detecting truth processes.
"""

from columnflow.production import Producer, producer
from columnflow.util import maybe_import
from columnflow.columnar_util import set_ak_column

np = maybe_import("numpy")
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
    events = set_ak_column(events, "process_id", len(events) * [process_id], value_type=np.int32)

    return events
