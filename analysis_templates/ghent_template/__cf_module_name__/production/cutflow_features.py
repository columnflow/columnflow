# coding: utf-8

"""
Column production methods for cutflow features.
"""

from columnflow.selection import SelectionResult
from columnflow.production import Producer, producer
from columnflow.production.cms.mc_weight import mc_weight
from columnflow.util import maybe_import, four_vec
from columnflow.columnar_util import EMPTY_FLOAT, Route, set_ak_column

np = maybe_import("numpy")
ak = maybe_import("awkward")


@producer(
    uses={
        mc_weight,
        # nano columns
        "Jet.pt",
    },
    produces={
        mc_weight,
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

    # add cutflow columns
    events = set_ak_column(
        events,
        "cutflow.jet1_pt",
        Route("Jet.pt[:,0]").apply(events, EMPTY_FLOAT),
    )

    if self.dataset_inst.is_mc:
        events = set_ak_column(
            events,
            "genTop",
            events.GenPart[(np.abs(events.GenPart.pdgId) == 6) & (events.GenPart.status == 62)]
        )

    return events


@cutflow_features.init
def cutflow_features_init(self: Producer) -> None:
    if hasattr(self, "dataset_inst"):
        if self.dataset_inst.is_mc:
            self.uses |= four_vec({"GenPart"}, {"pdgId", "status"})
            self.produces |= four_vec({"genTop"})
    return
