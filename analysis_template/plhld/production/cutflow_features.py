# coding: utf-8

"""
Selectors to set ak columns for cutflow features
"""

from columnflow.util import maybe_import
from columnflow.columnar_util import set_ak_column, Route, EMPTY_FLOAT
from columnflow.selection import Selector, SelectionResult, selector

ak = maybe_import("awkward")


@selector(
    uses={"Jet.pt"},
    produces={
        "cutflow.jet1_pt", "cutflow.jet2_pt", "cutflow.jet3_pt", "cutflow.jet4_pt",
        "cutflow.n_jet", "cutflow.n_electron", "cutflow.n_muon",
    },
)
def cutflow_features(self: Selector, events: ak.Array, results: SelectionResult, **kwargs) -> ak.Array:

    # determine jet pt before applying jet pt cut (and ideally after applying eta cut?)
    jet_indices = ak.argsort(events.Jet.pt, ascending=False)
    jets = events.Jet[jet_indices]
    for i in range(4):
        events = set_ak_column(events, f"cutflow.jet{i+1}_pt", Route(f"pt[:, {i}]").apply(jets, EMPTY_FLOAT))

    # Number of objects should be counted after appyling
    events = set_ak_column(events, "cutflow.n_jet", ak.num(results.objects.Jet.Jet, axis=1))
    events = set_ak_column(events, "cutflow.n_electron", ak.num(results.objects.Electron.Electron, axis=1))
    events = set_ak_column(events, "cutflow.n_muon", ak.num(results.objects.Muon.Muon, axis=1))

    return events
