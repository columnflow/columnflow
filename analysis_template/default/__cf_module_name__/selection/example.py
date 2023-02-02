# coding: utf-8

"""
Exemplary selection methods.
"""

from collections import defaultdict, OrderedDict

from columnflow.selection import Selector, SelectionResult, selector
from columnflow.production.mc_weight import mc_weight
from columnflow.production.seeds import deterministic_seeds
from columnflow.production.categories import category_ids
from columnflow.production.example import cutflow_features
from columnflow.util import maybe_import
from columnflow.columnar_util import set_ak_column
from columnflow.production.processes import process_ids


np = maybe_import("numpy")
ak = maybe_import("awkward")


#
# selector used by categories definitions
# (not "exposed" to be used from the command line)
#

@selector(uses={"event"})
def sel_incl(self: Selector, events: ak.Array, **kwargs) -> ak.Array:
    # return a mask with
    return ak.ones_like(events.event) == 1


@selector(uses={"nJet", "Jet.pt"})
def sel_2j(self: Selector, events: ak.Array, **kwargs) -> ak.Array:
    # two or more jets
    return ak.sum(events.Jet.pt, axis=1) >= 2


#
# actual selectors
#

@selector(
    uses={"nMuon", "Muon.pt", "Muon.eta", "nJet", "Jet.pt", "Jet.eta"},
    exposed=True,
)
def example(
    self: Selector,
    events: ak.Array,
    stats: defaultdict,
    **kwargs,
) -> SelectionResult:
    # example muon selection: exactly one muon
    muon_mask = (events.Muon.pt >= 20.0) & (abs(events.Muon.eta) < 2.1)
    muon_sel = ak.sum(muon_mask, axis=1) == 1

    # example jet selection: at least one jet
    jet_mask = (events.Jet.pt >= 25.0) & (abs(events.Jet.eta) < 2.4)
    jet_sel = ak.sum(jet_mask, axis=1) >= 1

    # created pt sorted jet indices
    jet_indices = ak.argsort(events.Jet.pt, axis=-1, ascending=False)
    jet_indices = jet_indices[jet_mask[jet_indices]]

    # build and return selection results
    # "objects" maps source columns to new columns and selections to be applied on the old columns
    # to create them, e.g. {"Jet": {"MyCustomJetCollection": indices_applied_to_Jet}}
    return events, SelectionResult(
        steps={
            "muon": muon_sel,
            "jet": jet_sel,
        },
        objects={
            "Jet": {
                "Jet": jet_indices,
            },
        },
    )


#
# combined selectors
#

@selector(
    uses={btag_weights, pu_weight},
)
def increment_stats(
    self: Selector,
    events: ak.Array,
    results: SelectionResult,
    stats: dict,
    **kwargs,
) -> ak.Array:
    """
    Unexposed selector that does not actually select objects but instead increments selection
    *stats* in-place based on all input *events* and the final selection *mask*.
    """
    # get the event mask
    event_mask = results.main.event

    # increment plain counts
    stats["n_events"] += len(events)
    stats["n_events_selected"] += ak.sum(event_mask, axis=0)

    # get a list of unique process ids and jet multiplicities present in the chunk
    unique_process_ids = np.unique(events.process_id)
    unique_n_jets = []
    if results.has_aux("n_central_jets"):
        unique_n_jets = np.unique(results.x.n_central_jets)

    # create a map of entry names to (weight, mask) pairs that will be written to stats
    weight_map = OrderedDict()
    if self.dataset_inst.is_mc:
        # mc weight for all events
        weight_map["mc_weight"] = (events.mc_weight, Ellipsis)

        # mc weight for selected events
        weight_map["mc_weight_selected"] = (events.mc_weight, event_mask)

    # get and store the weights
    for name, (weights, mask) in weight_map.items():
        joinable_mask = True if mask is Ellipsis else mask

        # sum for all processes
        stats[f"sum_{name}"] += ak.sum(weights[mask])

        # sums per process id and again per jet multiplicity
        stats.setdefault(f"sum_{name}_per_process", defaultdict(float))
        stats.setdefault(f"sum_{name}_per_process_and_njet", defaultdict(lambda: defaultdict(float)))
        for p in unique_process_ids:
            stats[f"sum_{name}_per_process"][int(p)] += ak.sum(
                weights[(events.process_id == p) & joinable_mask],
            )
            for n in unique_n_jets:
                stats[f"sum_{name}_per_process_and_njet"][int(p)][int(n)] += ak.sum(
                    weights[
                        (events.process_id == p) &
                        (results.x.n_central_jets == n) &
                        joinable_mask
                    ],
                )

    return events


@selector(uses={"mc_weight"})
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
        weights = events.mc_weight

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
        jet_selection_test, deterministic_seeds, mc_weight, category_ids, process_ids,
        increment_stats, cutflow_features,
    },
    produces={
        jet_selection_test, deterministic_seeds, mc_weight, category_ids, process_ids,
        increment_stats, cutflow_features,
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
    events, jet_results = self[jet_selection_test](events, stats, **kwargs)
    results += jet_results

    # combined event selection after all steps
    event_sel = (
        jet_results.steps.Jet
        # can be AND-combined with additional steps
    )
    results.main["event"] = event_sel

    # add the mc weight
    events = self[mc_weight](events, **kwargs)

    # add deterministic seeds
    events = self[deterministic_seeds](events, **kwargs)

    # build categories
    events = self[category_ids](events, **kwargs)

    # create process ids
    events = self[process_ids](events, **kwargs)

    # include cutflow variables
    events = self[cutflow_features](events, **kwargs)

    # increment stats
    self[increment_stats](events, event_sel, stats, **kwargs)

    return events, results
