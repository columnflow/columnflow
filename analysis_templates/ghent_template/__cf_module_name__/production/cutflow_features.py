# coding: utf-8

"""
Column production methods for cutflow features.
"""

from columnflow.selection import SelectionResult
from columnflow.production import Producer, producer
from columnflow.production.categories import category_ids
from columnflow.production.cms.mc_weight import mc_weight
from columnflow.selection.util import create_collections_from_masks
from columnflow.util import maybe_import
from columnflow.columnar_util import EMPTY_FLOAT, Route, set_ak_column

np = maybe_import("numpy")
ak = maybe_import("awkward")


@producer(
    uses={
        mc_weight, category_ids,
        # nano columns
        "Jet.pt",
    },
    produces={
        mc_weight, category_ids,
        # new columns
        "cutflow.jet1_pt",
    },
)
def cutflow_features(
    self: Producer,
    events: ak.Array,
    results: SelectionResult,
    **kwargs,
) -> ak.Array:
    if self.dataset_inst.is_mc:
        events = self[mc_weight](events, **kwargs)

    # apply object masks and create new collections
    reduced_events = create_collections_from_masks(events, results.objects)

    # create category ids per event and add categories back to the
    events = self[category_ids](reduced_events, target_events=events, **kwargs)

    # add cutflow columns
    events = set_ak_column(
        events,
        "cutflow.jet1_pt",
        Route("Jet.pt[:,0]").apply(events, EMPTY_FLOAT),
    )

    return events
