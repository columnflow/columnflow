# coding: utf-8

"""
Selection modules for __cf_short_name_lc__.
"""

from collections import defaultdict
from typing import Tuple

import law

from columnflow.util import maybe_import, four_vec
from columnflow.columnar_util import set_ak_column
from columnflow.production.util import attach_coffea_behavior

from columnflow.selection import Selector, SelectionResult, selector
from columnflow.selection.util import masked_sorted_indices

from columnflow.production.cms.mc_weight import mc_weight
from columnflow.production.categories import category_ids
from columnflow.production.processes import process_ids

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
        process_ids, attach_coffea_behavior,
        mc_weight
    },
    produces={
        process_ids, attach_coffea_behavior,
        mc_weight
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

    # create process ids together with custom ttz definition (and future wz 0b, 1b, >2b definition)
    events = self[process_ids](events, **kwargs)
    # ensure coffea behavior
    events = self[attach_coffea_behavior](events, **kwargs)

    results = SelectionResult()
    return events, results


@selector(
    uses=four_vec(
        ("Electron", "Muon"),
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
    leptons = ak.concatenate([muon, electron], axis=-1)
    leptons = leptons[ak.argsort(leptons.pt, axis=-1, ascending=False)]

    # required for pt cuts and Z-cuts on masks
    fill_with = {
        "pt": -999, "eta": -999, "phi": -999, "charge": -999,
        "pdgId": -999, "mass": -999, "e_idx": -999, "mu_idx": -999,
        "sip3d": -999
    }
    leptons = ak.fill_none(ak.pad_none(leptons, 2, axis=-1), fill_with)

    # construct the Z-boson candidate mask
    mll = (TetraVec(leptons[:, 0]) + TetraVec(leptons[:, 1])).mass
    z_mask = (
        (leptons[:, 0].charge != leptons[:, 1].charge) &
        (abs(leptons[:, 0].pdgId) == abs(leptons[:, 1].pdgId)) &
        (abs(mll - 91) < 15)
    )

    lepton_mask = (
        (leptons.pt[:, 0] > 30) &
        (leptons.pt[:, 1] > 20) &
        (~z_mask) &                     # no Z-boson peak leptons
        (ak.all(lepton.tight, axis=-1))  # all loose leptons in the event must be tight
    )

    # Electron and Muon indices corresponding to lepton selection
    empty_events = ak.zeros_like(1 * events.event, dtype=np.uint16)
    empty_indices = empty_events[..., None][..., :0]
    e_indices = ak.where(lepton_mask, leptons.e_idx, empty_indices)
    mu_indices = ak.where(lepton_mask, leptons.mu_idx, empty_indices)
    e_indices_l = ak.drop_none(e_indices)
    mu_indices_l = ak.drop_none(mu_indices)

    # loose indices on electron and muon
    e_indices = masked_sorted_indices(e_mask_tight, electron.pt)
    mu_indices = masked_sorted_indices(mu_mask_tight, muon.pt)

    return events, SelectionResult(
        steps={
            "Lepton": lepton_mask,
        },
        objects={},
        aux={
            # save the selected lepton for the duration of the selection
            # multiplication of a coffea particle with 1 yields the lorentz vector
            "lepton": leptons,
        },
    )


@selector(
    uses=(four_vec("Jet", ("btagDeepFlavB"))),
    exposed=False,
)
def jet_selection(
    self: Selector,
    events: ak.Array,
    results: SelectionResult,
    stats: defaultdict,
    **kwargs,
) -> Tuple[ak.Array, SelectionResult]:

    jet = (events.Jet[results.objects.Jet.Jet])
    lepton = results.aux.lepton

    bjet_mask_medium = -(jet.btagDeepFlavB >= self.config_inst.x.btag_working_points.deepjet.medium)

    jet_mask = (
        (ak.sum(bjet_mask_medium) >= 1)
    )

    return events, SelectionResult(
        steps={
            "Jet": jet_mask,
        },
        objects={},
    )


@selector(
    uses={
        category_ids, __cf_short_name_lc___increment_stats
    },
    produces={
        category_ids, __cf_short_name_lc___increment_stats
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
    if self.config_inst.x("do_cutflow_features", False):
        events = self[cutflow_features](events, results=results, **kwargs)

    # produce event weights
    if self.dataset_inst.is_mc:
        events = self[event_weights_to_normalize](events, results=results, **kwargs)

    # increment stats
    self[__cf_short_name_lc___increment_stats](events, results, stats, **kwargs)

    return events, results


@post_selection.init
def post_selection_init(self: Selector) -> None:
    if self.config_inst.x("do_cutflow_features", False):
        self.uses.add(cutflow_features)
        self.produces.add(cutflow_features)

    if not getattr(self, "dataset_inst", None) or self.dataset_inst.is_data:
        return

    self.uses.add(event_weights_to_normalize)
    self.produces.add(event_weights_to_normalize)


@selector(
    uses={
        pre_selection, post_selection,
        object_selection, trigger_selection, lepton_selection, jet_selection,
    },
    produces={
        pre_selection, post_selection,
        object_selection, trigger_selection, lepton_selection, jet_selection,
    },
    exposed=True,
)
def default(
    self: Selector,
    events: ak.Array,
    stats: defaultdict,
    **kwargs,
) -> tuple[ak.Array, SelectionResult]:
    # prepare the selection results that are updated at every step

    # lepton selection
    events, results = self[pre_selection](events, stats, **kwargs)

    # apply trigger selection (with double counting removal for data)
    events, trigger_results = self[trigger_selection](events, **kwargs)
    results += trigger_results

    # apply muon object selection
    events, object_results = self[object_selection](events, stats, **kwargs)
    results += object_results

    # apply lepton event selection
    events, lepton_selection_results = self[lepton_selection](events, results, stats, **kwargs)
    results += lepton_selection_results

    # apply jet event selection
    events, jet_selection_results = self[jet_selection](events, results, stats, **kwargs)
    results += jet_selection_results

    # combine event selection after all steps
    results.event = results.steps.Trigger & results.steps.Lepton & results.steps.Jet

    # add cutflow features, passing per-object masks
    events, results = self[post_selection](events, results, stats, **kwargs)

    return events, results
