# coding: utf-8

"""
Selection modules for __cf_short_name_lc__.
"""

from collections import defaultdict
from typing import Tuple

import law

from columnflow.util import maybe_import, four_vec
from columnflow.columnar_util import optional_column, has_ak_column
from columnflow.production.util import attach_coffea_behavior

from columnflow.selection import Selector, SelectionResult, selector

from columnflow.production.cms.mc_weight import mc_weight
from columnflow.production.categories import category_ids
from columnflow.production.processes import process_ids

from columnflow.production.cmsGhent.btag_weights import jet_btag, btag_efficiency_hists

from __cf_short_name_lc__.production.weights import event_weights_to_normalize
from __cf_short_name_lc__.production.cutflow_features import cutflow_features

from __cf_short_name_lc__.selection.objects import object_selection
from __cf_short_name_lc__.selection.stats import __cf_short_name_lc___increment_stats
from __cf_short_name_lc__.selection.trigger import trigger_selection

np = maybe_import("numpy")
ak = maybe_import("awkward")
coffea = maybe_import("coffea")
maybe_import("coffea.nanoevents.methods.nanoaod")

logger = law.logger.get_logger(__name__)


def TetraVec(arr: ak.Array) -> ak.Array:
    TetraVec = ak.zip({"pt": arr.pt, "eta": arr.eta, "phi": arr.phi, "mass": arr.mass},
    with_name="PtEtaPhiMLorentzVector",
    behavior=coffea.nanoevents.methods.vector.behavior)
    return TetraVec


@selector(
    uses={
        process_ids, attach_coffea_behavior, mc_weight, optional_column("veto"),
    },
    produces={
        process_ids, attach_coffea_behavior, mc_weight
    },
    exposed=False,
)
def pre_selection(
    self: Selector,
    events: ak.Array,
    stats: defaultdict,
    **kwargs,
) -> Tuple[ak.Array, SelectionResult]:

    if self.dataset_inst.is_mc:
        events = self[mc_weight](events, **kwargs)

    # create process ids
    events = self[process_ids](events, **kwargs)
    # ensure coffea behavior
    events = self[attach_coffea_behavior](events, **kwargs)

    results = SelectionResult()
    results.event = ~events.veto if has_ak_column(events, "veto") else ak.full_like(events.mc_weight, True, dtype=bool)
    return events, results


@selector(
    uses=four_vec(
        ("Electron", "Muon"), ("charge", "pdgId", "tight"),
    ),
    triggers=None
)
def lepton_selection(
        self: Selector,
        events: ak.Array,
        results: SelectionResult,
        stats: defaultdict,
        **kwargs,
) -> Tuple[ak.Array, SelectionResult]:

    # apply the object selection from results
    electron = (events.Electron[results.objects.Electron.Electron])
    muon = (events.Muon[results.objects.Muon.Muon])

    # create new object: leptons
    lepton = ak.concatenate([muon, electron], axis=-1)
    lepton = lepton[ak.argsort(lepton.pt, axis=-1, ascending=False)]

    # required for pt cuts and Z-cuts on masks
    fill_with = {
        "pt": -999, "eta": -999, "phi": -999, "charge": -999,
        "pdgId": -999, "mass": -999, "sip3d": -999, 'tight': False,
    }
    lepton = ak.fill_none(ak.pad_none(lepton, 2, axis=-1), fill_with)

    # construct the Z-boson candidate mask
    mll = (TetraVec(lepton[:, 0]) + TetraVec(lepton[:, 1])).mass
    z_mask = (
        (lepton[:, 0].charge != lepton[:, 1].charge) &
        (abs(lepton[:, 0].pdgId) == abs(lepton[:, 1].pdgId)) &
        (abs(mll - 91) < 15)
    )

    lepton_mask = (
        (lepton.pt[:, 0] > 30) &
        (lepton.pt[:, 1] > 20) &
        (~z_mask) &                     # no Z-boson peak leptons
        (ak.all(lepton.tight, axis=-1))  # all loose leptons in the event must be tight
    )

    return events, SelectionResult(
        steps={
            "Lepton": lepton_mask,
        },
        objects={},
        aux={
            # save the selected lepton for the duration of the selection
            # multiplication of a coffea particle with 1 yields the lorentz vector
            "lepton": lepton,
        },
    )


@selector(
    uses=four_vec("Jet", ("btagDeepFlavB")) | {jet_btag},
    exposed=False,
)
def jet_selection(
    self: Selector,
    events: ak.Array,
    results: SelectionResult,
    stats: defaultdict,
    **kwargs,
) -> Tuple[ak.Array, SelectionResult]:

    events = self[jet_btag](events, working_points=["M",], jet_mask=(abs(events.Jet.eta) < 2.4))

    jet = (events.Jet[results.objects.Jet.Jet])

    jet_event_mask = (ak.sum(jet.btag_M, axis=-1) >= 1)
    jet_nobtag_event_mask = (ak.sum(jet.pt > 0, axis=-1) >= 1)
    return events, SelectionResult(
        steps={
            "Jet": jet_event_mask,
            "Jet_nobtag": jet_nobtag_event_mask,
        },
    )


@selector(
    uses={
        category_ids, __cf_short_name_lc___increment_stats, cutflow_features
    },
    produces={
        category_ids, __cf_short_name_lc___increment_stats, cutflow_features
    },
    exposed=False,
)
def post_selection(
    self: Selector,
    events: ak.Array,
    results: SelectionResult,
    stats: defaultdict,
    **kwargs,
) -> Tuple[ak.Array, SelectionResult]:
    # build categories
    events = self[category_ids](events, results=results, **kwargs)
    # add cutflow features
    events = self[cutflow_features](events, results=results, **kwargs)

    # produce event weights
    if self.dataset_inst.is_mc:
        events = self[event_weights_to_normalize](events, results=results, **kwargs)

    # increment stats
    self[__cf_short_name_lc___increment_stats](events, results, stats, **kwargs)

    return events, results


@post_selection.init
def post_selection_init(self: Selector) -> None:

    if not getattr(self, "dataset_inst", None) or self.dataset_inst.is_data:
        return

    self.uses.add(event_weights_to_normalize)
    self.produces.add(event_weights_to_normalize)


@selector(
    uses={
        pre_selection, post_selection, btag_efficiency_hists,
        object_selection, trigger_selection, lepton_selection, jet_selection,
    },
    produces={
        pre_selection, post_selection, btag_efficiency_hists,
        object_selection, trigger_selection, lepton_selection, jet_selection,
    },
    exposed=True,
)
def default(
    self: Selector,
    events: ak.Array,
    stats: defaultdict,
    hists: dict,
    **kwargs,
) -> tuple[ak.Array, SelectionResult]:
    # prepare the selection results that are updated at every step

    # lepton selection
    events, results = self[pre_selection](events, stats, **kwargs)

    # apply trigger selection (with double counting removal for data)
    events, trigger_results = self[trigger_selection](events, **kwargs)
    results += trigger_results

    # apply object selection
    events, object_results = self[object_selection](events, stats, **kwargs)
    results += object_results

    # apply lepton event selection
    events, lepton_selection_results = self[lepton_selection](events, results, stats, **kwargs)
    results += lepton_selection_results

    # apply jet event selection
    events, jet_selection_results = self[jet_selection](events, results, stats, **kwargs)
    results += jet_selection_results

    results.x.event_no_btag = (
        results.event &
        results.steps.Trigger &
        results.steps.Lepton &
        results.steps.Jet_nobtag)

    if self.dataset_inst.is_mc:
        self[btag_efficiency_hists](events, results, hists=hists)

    # combine event selection after all steps
    results.event = (results.event &
                     results.steps.Trigger &
                     results.steps.Lepton &
                     results.steps.Jet)

    # add cutflow features, passing per-object masks
    events, results = self[post_selection](events, results, stats, **kwargs)

    return events, results
