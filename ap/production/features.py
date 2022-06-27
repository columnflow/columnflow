# coding: utf-8

"""
Column production methods related to higher-level features.
"""

from ap.production import producer
from ap.production.weights import event_weights
from ap.util import maybe_import
from ap.columnar_util import set_ak_column

from ap.selection.test import jet_energy_shifts

ak = maybe_import("awkward")


@producer(
    uses={
        event_weights,
        "Electron.pt", "Electron.eta", "Muon.pt", "Muon.eta", "Jet.pt", "Jet.eta",
        "Jet.btagDeepFlavB",
    },
    produces={
        event_weights,
        "ht", "n_jet", "n_electron", "n_muon", "n_deepjet",
    },
    shifts={jet_energy_shifts},
)
def variables(events: ak.Array, **kwargs) -> ak.Array:
    set_ak_column(events, "ht", ak.sum(events.Jet.pt, axis=1))
    set_ak_column(events, "n_jet", ak.num(events.Jet.pt, axis=1))
    set_ak_column(events, "n_electron", ak.num(events.Electron.pt, axis=1))
    set_ak_column(events, "n_muon", ak.num(events.Muon.pt, axis=1))
    set_ak_column(events, "n_deepjet", ak.num(events.Jet.pt[events.Jet.btagDeepFlavB > 0.3], axis=1))

    # add event weights
    event_weights(events, **kwargs)

    return events
