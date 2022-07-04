# coding: utf-8

"""
Column production methods related detecting truth processes.
"""

import order as od

from ap.production import producer
from ap.util import maybe_import
from ap.columnar_util import set_ak_column

ak = maybe_import("awkward")


@producer(
    uses={"event"},
    produces={"process_id"},
)
def process_ids(events: ak.Array, dataset_inst: od.Dataset, **kwargs) -> ak.Array:
    """
    Assigns each event a single process id, based on the first process that is registered for the
    *dataset_inst*. This is rather a dummy method and should be further implemented depending on
    future needs (e.g. for sample stitching).
    """
    # trivial case
    if len(dataset_inst.processes) != 1:
        raise NotImplementedError(
            f"dataset {dataset_inst.name} has {len(dataset_inst.processes)} processes assigned, "
            "which is not yet implemented",
        )
    process_id = dataset_inst.processes.get_first().id

    # store the column
    set_ak_column(events, "process_id", ak.ones_like(events.event) * process_id)

    return events
