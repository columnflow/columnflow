# coding: utf-8

"""
Selection methods for plc2hldr.
"""

from collections import defaultdict
from typing import Tuple

from columnflow.util import maybe_import
from columnflow.columnar_util import set_ak_column
from columnflow.production.util import attach_coffea_behavior

from columnflow.selection import Selector, SelectionResult, selector
from columnflow.production.categories import category_ids
from columnflow.production.processes import process_ids
from plhld.selection.general import increment_stats, jet_energy_shifts
from plhld.production.cutflow_features import cutflow_features

np = maybe_import("numpy")
ak = maybe_import("awkward")


@selector(uses={"event"})
def sel_incl(self: Selector, events: ak.Array, **kwargs) -> ak.Array:
    # select all
    return ak.ones_like(events.event)


def masked_sorted_indices(mask: ak.Array, sort_var: ak.Array, ascending: bool = False) -> ak.Array:
    indices = ak.argsort(sort_var, axis=-1, ascending=ascending)
    return indices[mask[indices]]


@selector(
    uses={"Jet.pt", "Jet.eta", "Jet.btagDeepFlavB"},
    produces={"jet_high_multiplicity"},
    shifts={jet_energy_shifts},
    exposed=True,
)
def jet_selection(
    self: Selector,
    events: ak.Array,
    stats: defaultdict,
    **kwargs,
) -> Tuple[ak.Array, SelectionResult]:
    # plc2hldr jet selection
    # - require at least 3 jets with pt>30, eta<2.4
    # - require at least 1 jet with pt>30, eta<2.4, b-score>0.3040 (Medium WP)

    # jets
    jet_mask = (events.Jet.pt > 30) & (abs(events.Jet.eta) < 2.4)
    jet_sel = ak.sum(jet_mask, axis=1) >= 3
    jet_indices = masked_sorted_indices(jet_mask, events.Jet.pt)

    # b-tagged jets, medium working point
    wp_med = self.config_inst.x.btag_working_points.deepcsv.medium
    bjet_mask = (jet_mask) & (events.Jet.btagDeepFlavB >= wp_med)
    bjet_sel = ak.sum(bjet_mask, axis=1) >= 1

    # example column: high jet multiplicity region (>=6 jets)
    events = set_ak_column(events, "jet_high_multiplicity", ak.sum(jet_mask, axis=1) >= 6)

    # build and return selection results plus new columns
    return events, SelectionResult(
        steps={"Jet": jet_sel, "Bjet": bjet_sel},
        objects={"Jet": {"Jet": jet_indices}},
    )


@selector(uses={
    "Electron.pt", "Electron.eta", "Electron.cutBased",
    "Muon.pt", "Muon.eta", "Muon.tightId", "Muon.looseId", "Muon.pfRelIso04_all",
})
def lepton_selection(
        self: Selector,
        events: ak.Array,
        stats: defaultdict,
        **kwargs,
) -> Tuple[ak.Array, SelectionResult]:
    # plc2hldr lepton selection
    # - require exactly 1 lepton (e or mu) with pt>30, eta<2.4 and tight ID

    # Lepton definition for this analysis
    e_mask = (events.Electron.pt > 30) & (abs(events.Electron.eta) < 2.4) & (events.Electron.cutBased == 4)
    mu_mask = (
        (events.Muon.pt > 30) &
        (abs(events.Muon.eta) < 2.4) &
        (events.Muon.tightId) &
        (events.Muon.pfRelIso04_all < 0.15)
    )

    lep_sel = ak.sum(e_mask, axis=-1) + ak.sum(mu_mask, axis=-1) == 1

    # determine the masked lepton indices
    e_indices = masked_sorted_indices(e_mask, events.Electron.pt)
    mu_indices = masked_sorted_indices(mu_mask, events.Muon.pt)

    # build and return selection results plus new columns
    return events, SelectionResult(
        steps={
            "Lepton": lep_sel,
        },
        objects={"Electron": {"Electron": e_indices}, "Muon": {"Muon": mu_indices}},
    )


@selector(
    uses={
        jet_selection, lepton_selection, cutflow_features,
        category_ids, process_ids, increment_stats, attach_coffea_behavior,
        "mc_weight",  # not opened per default but always required in Cutflow tasks
    },
    produces={
        jet_selection, lepton_selection, cutflow_features,
        category_ids, process_ids, increment_stats, attach_coffea_behavior,
        "mc_weight",  # not opened per default but always required in Cutflow tasks
    },
    shifts={
        jet_energy_shifts,
    },
    exposed=True,
)
def default(
    self: Selector,
    events: ak.Array,
    stats: defaultdict,
    **kwargs,
) -> Tuple[ak.Array, SelectionResult]:
    # ensure coffea behavior
    events = self[attach_coffea_behavior](events, **kwargs)

    # prepare the selection results that are updated at every step
    results = SelectionResult()

    # jet selection
    events, jet_results = self[jet_selection](events, stats, **kwargs)
    results += jet_results

    # lepton selection
    events, lepton_results = self[lepton_selection](events, stats, **kwargs)
    results += lepton_results

    # combined event selection after all steps
    event_sel = (
        jet_results.steps.Jet &
        jet_results.steps.Bjet &
        lepton_results.steps.Lepton
    )
    results.main["event"] = event_sel

    # build categories
    events = self[category_ids](events, results=results, **kwargs)

    # create process ids
    events = self[process_ids](events, **kwargs)

    # add cutflow features
    events = self[cutflow_features](events, results=results, **kwargs)

    # increment stats
    self[increment_stats](events, event_sel, stats, **kwargs)

    return events, results
