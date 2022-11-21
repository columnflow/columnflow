# coding: utf-8

"""
Column production methods related to higher-level features.
"""

from columnflow.production import Producer, producer
from columnflow.util import maybe_import
from columnflow.columnar_util import set_ak_column
from columnflow.production.util import attach_coffea_behavior

from plhld.production.weights import event_weights
from plhld.selection.general import jet_energy_shifts

ak = maybe_import("awkward")
coffea = maybe_import("coffea")
maybe_import("coffea.nanoevents.methods.nanoaod")
# from coffea.nanoevents.methods.nanoaod import behavior


@producer(
    uses={"Jet.pt", "Jet.eta", "Jet.phi", "Jet.mass"},
    produces={"m_jj", "deltaR_jj"},
)
def jj_features(self: Producer, events: ak.Array, **kwargs) -> ak.Array:
    events = ak.Array(events, behavior=coffea.nanoevents.methods.nanoaod.behavior)
    events["Jet"] = ak.with_name(events.Jet, "PtEtaPhiMLorentzVector")

    if ak.any(ak.num(events.Jet, axis=-1) <= 2):
        raise Exception("In features.py: there should be at least 2 jets in each event")

    m_jj = (events.Jet[:, 0] + events.Jet[:, 1]).mass
    events = set_ak_column(events, "m_jj", m_jj)

    deltaR_jj = events.Jet[:, 0].delta_r(events.Jet[:, 1])
    events = set_ak_column(events, "deltaR_jj", deltaR_jj)

    return events


@producer(
    uses={
        attach_coffea_behavior,
        event_weights, jj_features,
        "Electron.pt", "Electron.eta", "Muon.pt", "Muon.eta",
        "Jet.pt", "Jet.eta", "Jet.btagDeepFlavB",
    },
    produces={
        attach_coffea_behavior,
        event_weights, jj_features,
        "ht", "n_jet", "n_electron", "n_muon", "n_deepjet",
    },
    shifts={
        jet_energy_shifts,
    },
)
def features(self: Producer, events: ak.Array, **kwargs) -> ak.Array:
    # TODO: make work
    """
    # ensure coffea behavior
    custom_collections = {"Bjet": {
        "type_name": "Jet",
        "check_attr": "metric_table",
        "skip_fields": "*Idx*G",
    }}
    events = self[attach_coffea_behavior](
        events, collections=custom_collections, behavior=coffea.nanoevents.methods.nanoaod.behavior, **kwargs
    )
    """
    events = set_ak_column(events, "ht", ak.sum(events.Jet.pt, axis=1))
    events = set_ak_column(events, "n_jet", ak.num(events.Jet.pt, axis=1))
    events = set_ak_column(events, "n_electron", ak.num(events.Electron.pt, axis=1))
    events = set_ak_column(events, "n_muon", ak.num(events.Muon.pt, axis=1))
    events = set_ak_column(events, "n_deepjet", ak.num(events.Jet.pt[events.Jet.btagDeepFlavB > 0.3], axis=1))

    events = self[jj_features](events, **kwargs)

    # add event weights
    events = self[event_weights](events, **kwargs)

    return events
