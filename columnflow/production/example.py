# coding: utf-8

"""
Example production methods
"""

from columnflow.production import Producer, producer
from columnflow.production.normalization import normalization_weights
from columnflow.production.pileup import pu_weight
from columnflow.util import maybe_import
from columnflow.columnar_util import EMPTY_FLOAT, Route, set_ak_column

ak = maybe_import("awkward")


@producer(
    uses={
        normalization_weights, pu_weight,
        "nJet", "Jet.pt",
    },
    produces={
        normalization_weights, pu_weight,
        "ht", "n_jet",
    },
    shifts={
        "minbias_xs_up", "minbias_xs_down",
    },
)
def example(self: Producer, events: ak.Array, **kwargs) -> ak.Array:
    events = self[normalization_weights](events, **kwargs)
    events = self[pu_weight](events, **kwargs)

    events = set_ak_column(events, "ht", ak.sum(events.Jet.pt, axis=1))
    events = set_ak_column(events, "n_jet", ak.num(events.Jet.pt, axis=1))

    return events


@producer(
    uses={"Jet.pt"},
    produces={"cutflow.jet1_pt"},
)
def cutflow_features(self: Producer, events: ak.Array, **kwargs) -> ak.Array:
    events = set_ak_column(events, "cutflow.jet1_pt", Route("Jet.pt[:,0]").apply(events, EMPTY_FLOAT))

    return events
