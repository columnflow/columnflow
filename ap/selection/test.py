# coding: utf-8

"""
Selection methods for testing purposes.
"""

from typing import Callable, Dict, List, Optional, Union

from ap.selection import selector, SelectionResult
from ap.util import maybe_import
from ap.columnar_util import set_ak_column

ak = maybe_import("awkward")
np = maybe_import("numpy")


# object definitions
# TODO: return masks instead of indices and build indices as late as possible
@selector(uses={"Jet.pt", "Jet.eta"})
def req_jet(events):
    mask = (events.Jet.pt > 30) & (abs(events.Jet.eta) < 2.4)
    return ak.argsort(events.Jet.pt, axis=-1, ascending=False)[mask]


@selector(uses={"Electron.pt", "Electron.eta", "Electron.cutBased"})
def req_electron(events):
    mask = (events.Electron.pt > 25) & (abs(events.Electron.eta) < 2.4) & (events.Electron.cutBased == 4)
    return ak.argsort(events.Electron.pt, axis=-1, ascending=False)[mask]


@selector(uses={"Muon.pt", "Muon.eta", "Muon.tightId"})
def req_muon(events):
    mask = (events.Muon.pt > 25) & (abs(events.Muon.eta) < 2.4) & (events.Muon.tightId)
    return ak.argsort(events.Muon.pt, axis=-1, ascending=False)[mask]


@selector(uses={"Jet.pt", "Jet.eta"})
def req_forwardJet(events):
    mask = (events.Jet.pt > 30) & (abs(events.Jet.eta) > 2.4) & (abs(events.Jet.eta) < 5.0)
    return ak.argsort(events.Jet.pt, axis=-1, ascending=False)[mask]


@selector(uses={"Jet.pt", "Jet.eta", "Jet.btagDeepFlavB"})
def req_deepjet(events):
    mask = (events.Jet.pt > 30) & (abs(events.Jet.eta) < 2.4) & (events.Jet.btagDeepFlavB > 0.3)
    return ak.argsort(events.Jet.pt, axis=-1, ascending=False)[mask]


@selector(uses={req_jet})
def var_nJet(events):
    return ak.num(req_jet(events), axis=1)


@selector(uses={req_deepjet})
def var_nDeepjet(events):
    return ak.num(req_deepjet(events), axis=1)


@selector(uses={req_electron})
def var_nElectron(events):
    return ak.num(req_electron(events), axis=1)


@selector(uses={req_muon})
def var_nMuon(events):
    return ak.num(req_muon(events), axis=1)


@selector(uses={req_jet, "Jet.pt"})
def var_HT(events):
    jet_pt_sorted = events.Jet.pt[req_jet(events)]
    return ak.sum(jet_pt_sorted, axis=1)


# selection for the main categories
@selector(uses={var_nMuon, var_nElectron})
def sel_1e(events):
    return (var_nMuon(events) == 0) & (var_nElectron(events) == 1)


@selector(uses={var_nMuon, var_nElectron})
def sel_1mu(events):
    return (var_nMuon(events) == 1) & (var_nElectron(events) == 0)


# selection for the sub-categories
@selector(uses={sel_1e, var_nDeepjet})
def sel_1e_eq1b(events):
    return (sel_1e(events)) & (var_nDeepjet(events) == 1)


@selector(uses={sel_1e, var_nDeepjet})
def sel_1e_ge2b(events):
    return (sel_1e(events)) & (var_nDeepjet(events) >= 2)


@selector(uses={sel_1mu, var_nDeepjet})
def sel_1mu_eq1b(events):
    return (sel_1mu(events)) & (var_nDeepjet(events) == 1)


@selector(uses={sel_1mu, var_nDeepjet})
def sel_1mu_ge2b(events):
    return (sel_1mu(events)) & (var_nDeepjet(events) >= 2)


@selector(uses={sel_1mu_ge2b, var_HT})
def sel_1mu_ge2b_lowHT(events):
    return (sel_1mu_ge2b(events)) & (var_HT(events) <= 300)


@selector(uses={sel_1mu_ge2b, var_HT})
def sel_1mu_ge2b_highHT(events):
    return (sel_1mu_ge2b(events)) & (var_HT(events) > 300)


@selector(uses={req_jet}, produces={"jet_high_multiplicity"})
def jet_selection_test(events, stats, **kwargs):
    # example cuts:
    # - require at least 4 jets with pt>30, eta<2.4
    # example columns:
    # - high jet multiplicity region (>=6 selected jets)

    jet_indices = req_jet(events)
    jet_sel = ak.num(jet_indices, axis=1) >= 4
    jet_high_multiplicity = ak.num(jet_indices, axis=1) >= 6

    set_ak_column(events, "jet_high_multiplicity", jet_high_multiplicity)

    # build and return selection results plus new columns
    return (
        SelectionResult(steps={"Jet": jet_sel}, objects={"Jet": jet_indices}),
        events,
    )


@selector(uses={req_deepjet})
def deepjet_selection_test(events, stats):
    deepjet_indices = req_deepjet(events)
    deepjet_sel = ak.num(deepjet_indices, axis=1) >= 1

    return (
        SelectionResult(steps={"Deepjet": deepjet_sel}, objects={"Deepjet": deepjet_indices}),
        events,
    )


@selector(uses={req_muon})
def muon_selection_test(events, stats):
    # example cuts:
    # - require exactly one muon with pt>25, eta<2.4 and tight Id

    muon_indices = req_muon(events)
    muon_sel = ak.num(muon_indices, axis=1) == 1

    # build and return selection results
    return (
        SelectionResult(steps={"Muon": muon_sel}, objects={"Muon": muon_indices}),
        events,
    )


@selector(uses={req_electron})
def electron_selection_test(events, stats):
    # example cuts:
    # - require exactly one muon with pt>25, eta<2.4 and tight Id

    electron_indices = req_electron(events)
    electron_sel = ak.num(electron_indices, axis=1) == 1

    # build and return selection results
    return (
        SelectionResult(steps={"Electron": electron_sel}, objects={"Electron": electron_indices}),
        events,
    )


@selector(uses={req_muon, req_electron})
def lepton_selection_test(events, stats):
    # example cuts:
    # - require exactly one lepton with pt>25, eta<2.4 and tight Id

    muon_indices = req_muon(events)
    electron_indices = req_electron(events)
    lepton_sel = ak.num(muon_indices, axis=1) + ak.num(electron_indices, axis=1) == 1

    # build and return selection results
    return (
        SelectionResult(
            steps={"Lepton": lepton_sel},
            objects={"Muon": muon_indices, "Electron": electron_indices},
        ),
        events,
    )


@selector(
    uses={
        jet_selection_test, lepton_selection_test, deepjet_selection_test,
        "LHEWeight.originalXWGTUP",
    },
    produces={
        jet_selection_test, lepton_selection_test, deepjet_selection_test, "cat_array",
    },
)
def test(events, stats, config_inst, **kwargs):
    # example cuts:
    # - jet_selection_test
    # - lepton_selection_test
    # example stats:
    # - number of events before and after selection
    # - sum of mc weights before and after selection

    # prepare the selection results that are updated at every step
    results = SelectionResult()

    # jet selection
    jet_results, events = jet_selection_test(events, stats)
    results += jet_results

    # lepton selection
    lepton_results, events = lepton_selection_test(events, stats)
    results += lepton_results

    # deep jet selection
    deepjet_results, events = deepjet_selection_test(events, stats)
    results += deepjet_results

    # combined event selection after all steps
    event_sel = jet_results.steps.Jet & lepton_results.steps.Lepton & deepjet_results.steps.Deepjet
    results.main["event"] = event_sel

    # build categories
    cat_array = 0
    mask_int = 0
    for cat in config_inst.get_leaf_categories():
        cat_sel = cat.selection
        mask = globals()[cat_sel](events)
        cat_array = np.where(mask, cat.id, cat_array)
        mask_int = mask_int + np.where(mask, 1, 0)  # to check orthogonality of categories
    if not ak.all(mask_int == 1):
        if ak.any(mask_int >= 2):
            print("Leaf categories are not fully orthogonal")
        else:
            print("Some events are without leaf category")
    set_ak_column(events, "cat_array", cat_array)

    # increment stats
    events_sel = events[event_sel]
    stats["n_events"] += len(events)
    stats["n_events_selected"] += ak.sum(event_sel, axis=0)
    stats["sum_mc_weight"] += ak.sum(events.LHEWeight.originalXWGTUP)
    stats["sum_mc_weight_selected"] += ak.sum(events_sel.LHEWeight.originalXWGTUP)

    return results, events


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


def req_clean_delta_r_match_jet_lepton(
    events: ak.Array,
    to_clean: str,
    clean_against: List[str],
    threshold: float = 0.4,
) -> List[int]:
    """
    do the cleaning of jets with respect to leptons.
    returns indices of good (clean) jets, sorted by pt
    *TODO*: this should probably changed to return the boolean masks
    """
    return delta_r_jet_lepton(events, to_clean, clean_against, threshold=threshold)


@selector(uses={delta_r_jet_lepton})
def jet_lepton_delta_r_cleaning_selection(
    events: ak.Array,
    stats: Dict[str, Union[int, float]],
    threshold: float = 0.4,
) -> SelectionResult:
    """
    function to apply the selection requirements necessary for a
    cleaning of jets against leptons.
    The function calls the requirements to clean the field *Jet* against
    the concatination of the fields *[Muon, Electron]*, i.e. all leptons
    and passes the desired threshold for the selection
    """
    clean_jet_indices = req_clean_delta_r_match_jet_lepton(events, "Jet", ["Muon", "Electron"], threshold=threshold)

    return SelectionResult(
        objects={"Jet": clean_jet_indices},
    )


@selector(uses={jet_lepton_delta_r_cleaning_selection})
def jet_lepton_delta_r_cleaning(
    events: ak.Array,
    stats: Dict[str, Union[int, float]],
    threshold: float = 0.4,
    **kwargs,
) -> SelectionResult:
    """
    Selector function that performs cleaning of jets against leptons
    based on a delta_r requirement
    """
    results = jet_lepton_delta_r_cleaning_selection(events, stats, threshold=threshold)

    return results
