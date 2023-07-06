# coding: utf-8

"""
Exemplary selection methods.
"""

from collections import defaultdict, OrderedDict

from columnflow.selection import Selector, SelectionResult, selector
from columnflow.selection.util import sorted_indices_from_mask
from columnflow.production.processes import process_ids
from columnflow.production.cms.mc_weight import mc_weight
from columnflow.util import maybe_import

from __cf_module_name__.production.example import cutflow_features


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
    uses={"nMuon", "Muon.pt", "Muon.eta"},
    exposed=True,
)
def muon_selection(
    self: Selector,
    events: ak.Array,
    **kwargs,
) -> SelectionResult:
    # example muon selection: exactly one muon
    muon_mask = (events.Muon.pt >= 20.0) & (abs(events.Muon.eta) < 2.1)
    muon_sel = ak.sum(muon_mask, axis=1) == 1

    # build and return selection results
    # "objects" maps source columns to new columns and selections to be applied on the old columns
    # to create them, e.g. {"Muon": {"MySelectedMuon": indices_applied_to_Muon}}
    return events, SelectionResult(
        steps={
            "muon": muon_sel,
        },
        objects={
            "Muon": {
                "Muon": muon_mask,
            },
        },
    )


@selector(
    uses={"nJet", "Jet.pt", "Jet.eta"},
    exposed=True,
)
def jet_selection(
    self: Selector,
    events: ak.Array,
    **kwargs,
) -> SelectionResult:
    # example jet selection: at least one jet
    jet_mask = (events.Jet.pt >= 25.0) & (abs(events.Jet.eta) < 2.4)
    jet_sel = ak.sum(jet_mask, axis=1) >= 1

    # build and return selection results
    # "objects" maps source columns to new columns and selections to be applied on the old columns
    # to create them, e.g. {"Jet": {"MyCustomJetCollection": indices_applied_to_Jet}}
    return events, SelectionResult(
        steps={
            "jet": jet_sel,
        },
        objects={
            "Jet": {
                "Jet": sorted_indices_from_mask(jet_mask, events.Jet.pt, ascending=False),
            },
        },
    )


#
# combined selectors
#

@selector(
    uses={"mc_weight", "process_id"},
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

        # add more entries here
        # ...

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


@selector(
    uses={
        # selectors / producers called within _this_ selector
        mc_weight, cutflow_features, process_ids, muon_selection, jet_selection, increment_stats,
    },
    produces={
        # selectors / producers whose newly created columns should be kept
        mc_weight, cutflow_features, process_ids,
    },
    exposed=True,
)
def example(
    self: Selector,
    events: ak.Array,
    stats: defaultdict,
    **kwargs,
) -> SelectionResult:
    # prepare the selection results that are updated at every step
    results = SelectionResult()

    # muon selection
    events, muon_results = self[muon_selection](events, **kwargs)
    results += muon_results

    # jet selection
    events, jet_results = self[jet_selection](events, **kwargs)
    results += jet_results

    # combined event selection after all steps
    results.main["event"] = results.steps.muon & results.steps.jet

    # create process ids
    events = self[process_ids](events, **kwargs)

    # add the mc weight
    if self.dataset_inst.is_mc:
        events = self[mc_weight](events, **kwargs)

    # add cutflow features, passing per-object masks
    events = self[cutflow_features](events, results.objects, **kwargs)

    # increment stats
    self[increment_stats](events, results, stats, **kwargs)

    return events, results
