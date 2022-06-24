# coding: utf-8

"""
Column production methods related to higher-level features.
"""

from ap.production import producer
from ap.production.weights import event_weights
from ap.util import maybe_import
from ap.columnar_util import EMPTY, set_ak_column

ak = maybe_import("awkward")


def extract(ak_array: ak.Array, idx: int) -> ak.Array:
    """
    Takes an jagged *ak_array* and an index and returns padded array from given index.
    """
    ak_array = ak.pad_none(ak_array, idx + 1)
    ak_array = ak.fill_none(ak_array[:, idx], EMPTY)
    return ak_array


@producer(
    uses={
        event_weights,
        "Electron.pt", "Electron.eta", "Muon.pt", "Muon.eta", "Jet.pt", "Jet.eta",
        "Jet.btagDeepFlavB",
    },
    produces={
        event_weights,
        "HT", "nJet", "nElectron", "nMuon", "nDeepjet", "Electron1_pt", "Muon1_pt",
    } | {
        f"Jet{i}_{attr}"
        for i in range(1, 4 + 1)
        for attr in ["pt", "eta"]
    },
)
def variables(events: ak.Array, **kwargs) -> ak.Array:
    set_ak_column(events, "HT", ak.sum(events.Jet.pt, axis=1))
    set_ak_column(events, "nJet", ak.num(events.Jet.pt, axis=1))
    set_ak_column(events, "nElectron", ak.num(events.Electron.pt, axis=1))
    set_ak_column(events, "nMuon", ak.num(events.Muon.pt, axis=1))
    set_ak_column(events, "nDeepjet", ak.num(events.Jet.pt[events.Jet.btagDeepFlavB > 0.3], axis=1))
    set_ak_column(events, "Electron1_pt", extract(events.Electron.pt, 0))
    set_ak_column(events, "Muon1_pt", extract(events.Muon.pt, 0))

    for i in range(1, 4 + 1):
        set_ak_column(events, f"Jet{i}_pt", extract(events.Jet.pt, i - 1))
        set_ak_column(events, f"Jet{i}_eta", extract(events.Jet.eta, i - 1))

    # add event weights
    event_weights(events, **kwargs)

    return events
