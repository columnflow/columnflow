# coding: utf-8

"""
Exemplary selection methods.
"""

from collections import defaultdict

from columnflow.selection import Selector, SelectionResult, selector
from columnflow.production.categories import category_ids
from columnflow.production.example import cutflow_features
from columnflow.util import maybe_import
from columnflow.columnar_util import set_ak_column
from columnflow.production.processes import process_ids

np = maybe_import("numpy")
ak = maybe_import("awkward")


#
# generic parametrized selectors
# (not exposed)
#

# example
@selector(uses={"Jet.pt", "Jet.eta"})
def req_jet(self: Selector, events: ak.Array, **kwargs) -> ak.Array:
    mask = (events.Jet.pt > 30) & (abs(events.Jet.eta) < 2.4)
    return mask


#
# selector used by categories definitions
# (not exposed)
#

@selector(uses={"event"})
def sel_incl(self: Selector, events: ak.Array, **kwargs) -> ak.Array:
    # select all
    return ak.ones_like(events.event)


@selector(uses={"event", "nElectron", "Electron.pt", "nMuon", "Muon.pt"})
def sel_1e(self: Selector, events: ak.Array, **kwargs) -> ak.Array:
    # exactly one electron, no muon
    return (ak.num(events.Electron, axis=1) == 1) & (ak.num(events.Muon, axis=1) == 0)


#
# actual selectors
#

@selector(
    uses={req_jet},
    produces={"jet_high_multiplicity"},
    exposed=True,
)
def jet_selection_test(
    self: Selector,
    events: ak.Array,
    stats: defaultdict,
    **kwargs,
) -> SelectionResult:
    # example cuts:
    # - require at least 4 jets with pt>30, eta<2.4
    # example columns:
    # - high jet multiplicity region (>=6 selected jets)

    jet_mask = self[req_jet](events)
    jet_sel = ak.num(jet_mask, axis=1) >= 4

    jet_high_multiplicity = ak.num(jet_mask, axis=1) >= 6
    set_ak_column(events, "jet_high_multiplicity", jet_high_multiplicity)

    jet_indices = ak.argsort(events.Jet.pt, axis=-1, ascending=False)
    masked_jet_indices = jet_indices[jet_mask[jet_indices]]

    # build and return selection results plus new columns
    # "objects" maps source columns to new columns and selections to be applied on the old columns
    # to create them, e.g. {"Jet": {"MyCustomJetCollection": indices_applied_to_Jet}}
    return SelectionResult(
        steps={"Jet": jet_sel},
        objects={"Jet": {"Jet1": masked_jet_indices, "Jet2": masked_jet_indices}},
    )


#
# combined selectors
#

@selector(uses={"LHEWeight.originalXWGTUP"})
def increment_stats(
    self: Selector,
    events: ak.Array,
    mask: ak.Array,
    stats: dict,
    **kwargs,
) -> None:
    """
    Unexposed selector that does not actually select objects but instead increments selection
    *stats* in-place based on all input *events* and the final selection *mask*.
    """
    # apply the mask to obtain selected events
    events_sel = events[mask]

    # increment plain counts
    stats["n_events"] += len(events)
    stats["n_events_selected"] += ak.sum(mask, axis=0)

    # store sum of event weights for mc events
    if self.dataset_inst.is_mc:
        weights = events.LHEWeight.originalXWGTUP

        # sum for all processes
        stats["sum_mc_weight"] += ak.sum(weights)
        stats["sum_mc_weight_selected"] += ak.sum(weights[mask])

        # sums per process id
        stats.setdefault("sum_mc_weight_per_process", defaultdict(float))
        stats.setdefault("sum_mc_weight_selected_per_process", defaultdict(float))
        for p in np.unique(events.process_id):
            stats["sum_mc_weight_per_process"][int(p)] += ak.sum(
                weights[events.process_id == p],
            )
            stats["sum_mc_weight_selected_per_process"][int(p)] += ak.sum(
                weights[mask][events_sel.process_id == p],
            )


@selector(
    uses={
        category_ids, jet_selection_test, process_ids, increment_stats, cutflow_features,
        "LHEWeight.originalXWGTUP",
    },
    produces={
        category_ids, jet_selection_test, process_ids, increment_stats, cutflow_features,
        "LHEWeight.originalXWGTUP",
    },
    exposed=True,
)
def example(
    self: Selector,
    events: ak.Array,
    stats: defaultdict,
    **kwargs,
) -> SelectionResult:
    # example cuts:
    # - jet_selection_test
    # - lepton_selection_test
    # example stats:
    # - number of events before and after selection
    # - sum of mc weights before and after selection

    # prepare the selection results that are updated at every step
    results = SelectionResult()

    # jet selection
    jet_results = self[jet_selection_test](events, stats, **kwargs)
    results += jet_results

    # combined event selection after all steps
    event_sel = (
        jet_results.steps.Jet
        # can be AND-combined with additional steps
    )
    results.main["event"] = event_sel

    # build categories
    self[category_ids](events, **kwargs)

    # create process ids
    self[process_ids](events, **kwargs)

    # include cutflow variables
    self[cutflow_features](events, **kwargs)

    # increment stats
    self[increment_stats](events, event_sel, stats, **kwargs)

    return results
