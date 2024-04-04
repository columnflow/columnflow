# coding: utf-8

"""
Selection modules for object selection of Muon, Electron, and Jet.
"""

from collections import defaultdict
from typing import Tuple

import law

from columnflow.util import maybe_import
from columnflow.columnar_util import set_ak_column
from columnflow.production.util import attach_coffea_behavior
from columnflow.selection import Selector, SelectionResult, selector
from columnflow.selection.util import masked_sorted_indices


def masked_sorted_indices(mask: ak.Array, sort_var: ak.Array, ascending: bool = False) -> ak.Array:
    """
    Helper function to obtain the correct indices of an object mask
    """
    indices = ak.argsort(sort_var, axis=-1, ascending=ascending)
    return indices[mask[indices]]


@selector(
    uses=four_vec(
        ("Muon"),
        ("sip3d", "dxy", "dz", "miniPFRelIso_all", "tightId")
    ),
    produces={"Muon.tight"},
    triggers=None
)
def muon_object(
        self: Selector,
        events: ak.Array,
        stats: defaultdict,
        **kwargs,
) -> Tuple[ak.Array, SelectionResult]:

    muon = (events.Muon)

    # loose object electron mask
    mu_mask_loose = (
        (abs(muon.eta) < 2.4) &
        (muon.pt > 10.) &
        (muon.miniPFRelIso_all < 0.4) &
        (muon.sip3d < 8) &
        (abs(muon.dxy) < 0.05) &
        (abs(muon.dz) < 0.1)
    )

    # tight object muon mask (tight cutbased ID)
    mu_mask_tight = (
        (mu_mask) &
        (muon.tightId)
    )

    events = set_ak_column(events, "Muon.tight", mu_mask_tight, value_type=bool)

    return events, SelectionResult(
        steps={},
        objects={
            "Muon": {
                "Muon": masked_sorted_indices(mu_mask, muon.pt)
            }
        },
    )


@selector(
    uses=four_vec(
        ("Electron"),
        ("sip3d", "charge", "isPFcand", "dxy", "dz", "miniPFRelIso_all", "mvaFall17V2Iso_WP90", "tightCharge",
        "lostHits", "convVeto")
    ) | four_vec(
        ("Muon"),
    ),
    produces={"Electron.tight"},
    triggers=None
)
def electron_object(
        self: Selector,
        events: ak.Array,
        results: SelectionResult,
        stats: defaultdict,
        **kwargs,
) -> Tuple[ak.Array, SelectionResult]:

    electron = (events.Electron)
    # add muon loose selection to veto electrons that coincide with muons
    muon = (events.Muon[results.objects.Muon.Muon])

    # loose object electron mask
    e_mask = (
        (abs(electron.eta) < 2.5) &
        (electron.pt > 15) &
        (electron.miniPFRelIso_all < 0.4) &
        (electron.sip3d < 8) &
        (abs(electron.dxy) < 0.05) &
        (abs(electron.dz) < 0.1) &
        (electron.losthist < 2) &
        (electron.isPFcand) &
        (electron.convVeto) &
        (electron.tightCharge > 1) &
        # remove electrons that have tight muon close to it
        (ak.is_none(electron.nearest(muon, threshold=0.05), axis=-1))
    )
    # tight object electron mask (mvaFall17 WP90)
    e_mask_tight = (
        (e_mask) &
        (electron.mvaFall17V2Iso_WP90)
    )

    events = set_ak_column(events, "Electron.tight", e_mask_tight, value_type=bool)

    return events, SelectionResult(
        steps={},
        objects={
            "Electron": {
                "Electron": masked_sorted_indices(e_mask, electron.pt)
            }
        },
    )


@selector(
    uses=(four_vec({"Electron", "Muon"}) | four_vec("Jet", ("jetId", "btagDeepFlavB"))),
    exposed=False,
)
def jet_object(
    self: Selector,
    events: ak.Array,
    results: SelectionResult,
    stats: defaultdict,
    **kwargs,
) -> Tuple[ak.Array, SelectionResult]:

    jet = (events.Jet)
    muon = (events.Muon)[results.objects.Muon.Muon]
    electron = (events.Electron)[results.objects.Electron.Electron]

    dR_mask = (
        (ak.is_none(jet.nearest(muon, threshold=0.4), axis=-1)) &
        (ak.is_none(jet.nearest(electron, threshold=0.4), axis=-1))
    )

    jet_mask = (
        (jet.pt > 30) &
        (abs(jet.eta) < 2.5) &
        (jet.jetId >= 2) &
        (dR_mask)
    )

    jet_indices = masked_sorted_indices(jet_mask, events.Jet.pt)

    return events, SelectionResult(
        steps={},
        objects={
            "Jet": {
                "Jet": jet_indices,
            },
        },
    )
