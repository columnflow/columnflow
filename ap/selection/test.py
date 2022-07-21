# coding: utf-8

"""
Selection methods for testing purposes.
"""

from collections import defaultdict
from typing import Callable, Dict, List, Optional, Union

from ap.selection import Selector, SelectionResult, selector
from ap.production.categories import category_ids
from ap.util import maybe_import
from ap.columnar_util import set_ak_column, Route
from ap.production.processes import process_ids

from ap.columnar_util import EMPTY_FLOAT

np = maybe_import("numpy")
ak = maybe_import("awkward")


# pseudo-selectors for declaring dependence on shifts

@selector
def jet_energy_shifts(self: Selector, events: ak.Array, **kwargs) -> ak.Array:
    return events


@jet_energy_shifts.init
def jet_energy_shifts_init(self: Selector) -> None:
    """Declare dependence on JEC/JER uncertainty shifts."""
    self.shifts |= {
        f"jec_{junc_name}_{junc_dir}"
        for junc_name in self.config_inst.x.jec.uncertainty_sources
        for junc_dir in ("up", "down")
    } | {"jer_up", "jer_down"}


# object definitions
# TODO: return masks instead of indices and build indices as late as possible
@selector(uses={"Jet.pt", "Jet.eta"})
def req_jet(self: Selector, events: ak.Array, **kwargs) -> ak.Array:
    mask = (events.Jet.pt > 30) & (abs(events.Jet.eta) < 2.4)
    return ak.argsort(events.Jet.pt, axis=-1, ascending=False)[mask]


@selector(uses={"Electron.pt", "Electron.eta", "Electron.cutBased"})
def req_electron(self: Selector, events: ak.Array, **kwargs) -> ak.Array:
    mask = (events.Electron.pt > 25) & (abs(events.Electron.eta) < 2.4) & (events.Electron.cutBased == 4)
    return ak.argsort(events.Electron.pt, axis=-1, ascending=False)[mask]


@selector(uses={"Muon.pt", "Muon.eta", "Muon.tightId"})
def req_muon(self: Selector, events: ak.Array, **kwargs) -> ak.Array:
    mask = (events.Muon.pt > 25) & (abs(events.Muon.eta) < 2.4) & (events.Muon.tightId)
    return ak.argsort(events.Muon.pt, axis=-1, ascending=False)[mask]


@selector(uses={"Jet.pt", "Jet.eta"})
def req_forwardJet(self: Selector, events: ak.Array, **kwargs) -> ak.Array:
    mask = (events.Jet.pt > 30) & (abs(events.Jet.eta) > 2.4) & (abs(events.Jet.eta) < 5.0)
    return ak.argsort(events.Jet.pt, axis=-1, ascending=False)[mask]


@selector(uses={"Jet.pt", "Jet.eta", "Jet.btagDeepFlavB"})
def req_deepjet(self: Selector, events: ak.Array, **kwargs) -> ak.Array:
    mask = (events.Jet.pt > 30) & (abs(events.Jet.eta) < 2.4) & (events.Jet.btagDeepFlavB > 0.3)
    return ak.argsort(events.Jet.pt, axis=-1, ascending=False)[mask]


@selector(uses={req_jet})
def var_nJet(self: Selector, events: ak.Array, **kwargs) -> ak.Array:
    return ak.num(self[req_jet](events), axis=1)


@selector(uses={req_deepjet})
def var_nDeepjet(self: Selector, events: ak.Array, **kwargs) -> ak.Array:
    return ak.num(self[req_deepjet](events), axis=1)


@selector(uses={req_electron})
def var_nElectron(self: Selector, events: ak.Array, **kwargs) -> ak.Array:
    return ak.num(self[req_electron](events), axis=1)


@selector(uses={req_muon})
def var_nMuon(self: Selector, events: ak.Array, **kwargs) -> ak.Array:
    return ak.num(self[req_muon](events), axis=1)


@selector(uses={req_jet, "Jet.pt"})
def var_HT(self: Selector, events: ak.Array, **kwargs) -> ak.Array:
    jet_pt_sorted = events.Jet.pt[self[req_jet](events)]
    return ak.sum(jet_pt_sorted, axis=1)


@selector(
    uses={var_nJet, var_HT, "Jet.pt"},
    produces={"cutflow.n_jet", "cutflow.ht", "cutflow.jet1_pt"},
    exposed=True,
)
def cutflow_features(self: Selector, events: ak.Array, **kwargs) -> None:
    set_ak_column(events, "cutflow.n_jet", self[var_nJet](events))
    set_ak_column(events, "cutflow.ht", self[var_HT](events))
    set_ak_column(events, "cutflow.jet1_pt", Route("Jet.pt[:,0]").apply(events, EMPTY_FLOAT))


# selection for the main categories
@selector(uses={"event"})
def sel_incl(self: Selector, events: ak.Array, **kwargs) -> ak.Array:
    return ak.ones_like(events.event)


@selector(uses={var_nMuon, var_nElectron})
def sel_1e(self: Selector, events: ak.Array, **kwargs) -> ak.Array:
    return (self[var_nMuon](events) == 0) & (self[var_nElectron](events) == 1)


@selector(uses={var_nMuon, var_nElectron})
def sel_1mu(self: Selector, events: ak.Array, **kwargs) -> ak.Array:
    return (self[var_nMuon](events) == 1) & (self[var_nElectron](events) == 0)


# selection for the sub-categories
@selector(uses={sel_1e, var_nDeepjet})
def sel_1e_eq1b(self: Selector, events: ak.Array, **kwargs) -> ak.Array:
    return (self[sel_1e](events)) & (self[var_nDeepjet](events) == 1)


@selector(uses={sel_1e, var_nDeepjet})
def sel_1e_ge2b(self: Selector, events: ak.Array, **kwargs) -> ak.Array:
    return (self[sel_1e](events)) & (self[var_nDeepjet](events) >= 2)


@selector(uses={sel_1mu, var_nDeepjet})
def sel_1mu_eq1b(self: Selector, events: ak.Array, **kwargs) -> ak.Array:
    return (self[sel_1mu](events)) & (self[var_nDeepjet](events) == 1)


@selector(uses={sel_1mu, var_nDeepjet})
def sel_1mu_ge2b(self: Selector, events: ak.Array, **kwargs) -> ak.Array:
    return (self[sel_1mu](events)) & (self[var_nDeepjet](events) >= 2)


@selector(uses={sel_1mu_ge2b, var_HT})
def sel_1mu_ge2b_lowHT(self: Selector, events: ak.Array, **kwargs) -> ak.Array:
    return (self[sel_1mu_ge2b](events)) & (self[var_HT](events) <= 300)


@selector(uses={sel_1mu_ge2b, var_HT})
def sel_1mu_ge2b_highHT(self: Selector, events: ak.Array, **kwargs) -> ak.Array:
    return (self[sel_1mu_ge2b](events)) & (self[var_HT](events) > 300)


@selector(uses={req_jet}, produces={"jet_high_multiplicity"}, exposed=True)
def jet_selection_test(self: Selector, events: ak.Array, stats: defaultdict, **kwargs) -> SelectionResult:
    # example cuts:
    # - require at least 4 jets with pt>30, eta<2.4
    # example columns:
    # - high jet multiplicity region (>=6 selected jets)

    jet_indices = self[req_jet](events)
    jet_sel = ak.num(jet_indices, axis=1) >= 4

    jet_high_multiplicity = ak.num(jet_indices, axis=1) >= 6
    set_ak_column(events, "jet_high_multiplicity", jet_high_multiplicity)

    # build and return selection results plus new columns
    return SelectionResult(
        steps={"Jet": jet_sel},
        objects={"Jet": {"Jet": jet_indices}},
    )


@selector(uses={req_deepjet}, exposed=True)
def deepjet_selection_test(self: Selector, events: ak.Array, stats: defaultdict, **kwargs) -> SelectionResult:
    deepjet_indices = self[req_deepjet](events)
    deepjet_sel = ak.num(deepjet_indices, axis=1) >= 1

    return SelectionResult(
        steps={"Deepjet": deepjet_sel},
        objects={"Jet": {"Deepjet": deepjet_indices}},
    )


@selector(uses={req_muon}, exposed=True)
def muon_selection_test(self: Selector, events: ak.Array, stats: defaultdict, **kwargs) -> ak.Array:
    # example cuts:
    # - require exactly one muon with pt>25, eta<2.4 and tight Id

    muon_indices = self[req_muon](events)
    muon_sel = ak.num(muon_indices, axis=1) == 1

    # build and return selection results
    return SelectionResult(
        steps={"Muon": muon_sel},
        objects={"Muon": {"Muon": muon_indices}},
    )


@selector(uses={req_electron}, exposed=True)
def electron_selection_test(self: Selector, events: ak.Array, stats: defaultdict, **kwargs) -> SelectionResult:
    # example cuts:
    # - require exactly one muon with pt>25, eta<2.4 and tight Id

    electron_indices = self[req_electron](events)
    electron_sel = ak.num(electron_indices, axis=1) == 1

    # build and return selection results
    return SelectionResult(
        steps={"Electron": electron_sel},
        objects={"Electron": {"Electron": electron_indices}},
    )


@selector(uses={req_muon, req_electron}, exposed=True)
def lepton_selection_test(self: Selector, events: ak.Array, stats: defaultdict, **kwargs) -> SelectionResult:
    # example cuts:
    # - require exactly one lepton with pt>25, eta<2.4 and tight Id

    muon_indices = self[req_muon](events)
    electron_indices = self[req_electron](events)
    lepton_sel = ak.num(muon_indices, axis=1) + ak.num(electron_indices, axis=1) == 1

    # build and return selection results
    return SelectionResult(
        steps={"Lepton": lepton_sel},
        objects={"Muon": {"Muon": muon_indices}, "Electron": {"Electron": electron_indices}},
    )


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
        category_ids, jet_selection_test, lepton_selection_test, deepjet_selection_test,
        process_ids, increment_stats, "LHEWeight.originalXWGTUP", cutflow_features,
    },
    produces={
        category_ids, jet_selection_test, lepton_selection_test, deepjet_selection_test,
        process_ids, increment_stats, "LHEWeight.originalXWGTUP", cutflow_features,
    },
    shifts={
        jet_energy_shifts,
    },
    exposed=True,
)
def test(
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

    # lepton selection
    lepton_results = self[lepton_selection_test](events, stats, **kwargs)
    results += lepton_results

    # deep jet selection
    deepjet_results = self[deepjet_selection_test](events, stats, **kwargs)
    results += deepjet_results

    # combined event selection after all steps
    event_sel = (
        jet_results.steps.Jet &
        lepton_results.steps.Lepton &
        deepjet_results.steps.Deepjet
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


# deltaR cleaning


def cleaning_factory(
    selector_name: str,
    to_clean: str,
    clean_against: List[str],
    metric: Optional[Callable] = None,
) -> Callable:
    """
    factory to generate a function with name *selector_name* that cleans the
    field *to_clean* in the NanoEventArrays agains the field(s) *clean_agains*.
    First, the necessary column names to construct four-momenta for the
    different object fields are constructed, i.e. pt, eta, phi and e for the
    different objects.
    Finally, the actual selector function is generated, which uses these
    columns.
    """
    # default of the metric function is the delta_r function
    # of the coffea LorentzVectors
    if metric is None:
        metric = lambda a, b: a.delta_r(b)

    # compile the list of variables that are necessary for the four momenta
    # this list is always the same
    variables_for_lorentzvec = ["pt", "eta", "phi", "e"]

    # sum up all fields aht are to be considered, i.e. the field *to_clean*
    # and all fields in *clean_against*
    all_fields = clean_against + [to_clean]

    # construct the set of columns that is necessary for the four momenta in
    # the different fields (and thus also for the current implementation of
    # the cleaning itself) by looping through the fields and variables.

    uses = {
        f"{x}.{var}" for x in all_fields for var in variables_for_lorentzvec
    }

    # additionally, also load the lengths of the different fields
    uses |= {f"n{x}" for x in all_fields}

    # finally, construct selector function itself
    @selector(uses=uses, name=selector_name)
    def func(
        self: Selector,
        events: ak.Array,
        to_clean: str,
        clean_against: List[str],
        metric: Optional[Callable] = metric,
        threshold: float = 0.4,
    ) -> List[int]:
        """
        abstract function to perform a cleaning of field *to_clean* against
        a (list of) field(s) *clean_against* based on an abitrary metric
        *metric* (e.g. deltaR).
        First concatenate all fields in *clean_against*, which thus includes
        all fields that are to be used for the comparison of the metric.
        Then construct the metric for all permutations of the different objects
        using the coffea nearest implementation.
        All objects in field *to_clean* are removed if the metric is below the
        *threshold*.
        """

        # concatenate the fields that are to be used in the construction
        # of the metric table
        summed_clean_against = ak.concatenate(
            [events[x] for x in clean_against],
            axis=1,
        )

        # load actual NanoEventArray that is to be cleaned
        to_clean_field = events[to_clean]

        # construct metric table for these objects. The metric table contains
        # the minimal value of the metric *metric* for each object in field
        # *to_clean* w.r.t. all objects in *summed_clean_against*. Thus,
        # it has the dimensions nevents x nto_clean, where *nevents* is
        # the number of events in the current chunk of data and *nto_clean*
        # is the length of the field *to_clean*. Note that the argument
        # *threshold* in the *nearest* function must be set to None since
        # the function will perform a selection itself to extract the nearest
        # objects (i.e. applies the selection we want here in reverse)
        _, metric = to_clean_field.nearest(
            summed_clean_against,
            metric=metric,
            return_metric=True,
            threshold=None,
        )
        # build a binary mask based on the selection threshold provided by the
        # user
        mask = metric > threshold

        # construct final result. Currently, this is the list of indices for
        # clean jets, sorted for pt
        # WARNING: this still contains the bug with the application of the mask
        #           which will be adressed in a PR in the very near future
        # TODO: return the mask itself instead of the list of indices
        sorted_list = ak.argsort(to_clean_field.pt, axis=-1, ascending=False)[mask]
        return sorted_list

    return func


delta_r_jet_lepton = cleaning_factory(
    selector_name="delta_r_jet_lepton",
    to_clean="Jet",
    clean_against=["Muon", "Electron"],
    metric=lambda a, b: a.delta_r(b),
)


@selector(uses={delta_r_jet_lepton})
def jet_lepton_delta_r_cleaning(
    self: Selector,
    events: ak.Array,
    stats: Dict[str, Union[int, float]],
    threshold: float = 0.4,
    **kwargs,
) -> SelectionResult:
    """
    function to apply the selection requirements necessary for a
    cleaning of jets against leptons.
    The function calls the requirements to clean the field *Jet* against
    the concatination of the fields *[Muon, Electron]*, i.e. all leptons
    and passes the desired threshold for the selection
    """
    clean_jet_indices = self[delta_r_jet_lepton](events, "Jet", ["Muon", "Electron"], threshold=threshold)

    # TODO: should not return a new object collection but an array with masks
    return SelectionResult(objects={"Jet": clean_jet_indices})
