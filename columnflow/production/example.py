# coding: utf-8

"""
Example production methods
"""

from columnflow.production import Producer, producer
from columnflow.util import maybe_import
from columnflow.columnar_util import EMPTY_FLOAT, Route, set_ak_column

ak = maybe_import("awkward")


@producer(
    uses={
        "nJet", "Jet.pt",
    },
    produces={
        "ht", "n_jet",
    },
)
def example(self: Producer, events: ak.Array, **kwargs) -> ak.Array:
    set_ak_column(events, "ht", ak.sum(events.Jet.pt, axis=1))
    set_ak_column(events, "n_jet", ak.num(events.Jet.pt, axis=1))

    return events


@producer(
    uses={"Jet.pt"},
    produces={"cutflow.jet1_pt"},
    exposed=True,
)
def cutflow_features(self: Producer, events: ak.Array, **kwargs) -> None:
    set_ak_column(events, "cutflow.jet1_pt", Route("Jet.pt[:,0]").apply(events, EMPTY_FLOAT))
