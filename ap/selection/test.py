# coding: utf-8

"""
Selection methods for testing purposes.
"""

from ap.util import maybe_import
from ap.selection import selector, SelectionResult

ak = maybe_import("awkward")


@selector(uses={"nJet", "Jet_pt"})
def jet_selection_test(events, stats):
    # example cuts:
    # - require at least 4 jets with pt>30
    # example columns:
    # - high jet multiplicity region (>=6 selected jets)

    jet_mask = events.Jet.pt > 30
    jet_indices = ak.argsort(events.Jet.pt, axis=-1, ascending=False)[jet_mask]
    jet_sel = ak.sum(jet_mask, axis=1) >= 4
    jet_high_multiplicity = ak.sum(jet_mask, axis=1) >= 6

    # build and return selection results
    return SelectionResult(
        steps={"jet": jet_sel},
        objects={"jet": jet_indices},
        columns={"jet_high_multiplicity": jet_high_multiplicity},
    )


@selector(uses={"nMuon", "Muon_pt"})
def muon_selection_test(events, stats):
    # example cuts:
    # - require exactly one muon with pt>25

    muon_mask = events.Muon.pt > 25
    muon_indices = ak.argsort(events.Muon.pt, axis=-1, ascending=False)[muon_mask]
    muon_sel = ak.sum(muon_mask, axis=1) == 1

    # build and return selection results
    return SelectionResult(
        steps={"muon": muon_sel},
        objects={"muon": muon_indices},
    )


@selector(uses={jet_selection_test, muon_selection_test, "LHEWeight_originalXWGTUP"})
def select_test(events, stats):
    # example cuts:
    # - jet_selection_test
    # - muon_selection_test
    # example stats:
    # - number of events before and after selection
    # - sum of mc weights before and after selection

    jet_results = jet_selection_test(events, stats)
    muon_results = muon_selection_test(events, stats)

    # combined event selection after all steps
    event_sel = jet_results.steps.jet & muon_results.steps.muon

    # build and merge selection results
    results = SelectionResult(main={"event": event_sel})
    results += jet_results
    results += muon_results

    # increment stats
    events_sel = events[event_sel]
    stats["n_events"] += len(events)
    stats["n_events_selected"] += ak.sum(event_sel, axis=0)
    stats["sum_mc_weight"] += ak.sum(events.LHEWeight.originalXWGTUP)
    stats["sum_mc_weight_selected"] += ak.sum(events_sel.LHEWeight.originalXWGTUP)

    return results
