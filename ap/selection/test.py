# coding: utf-8

"""
Selection methods for testing purposes.
"""

from ap.selection import selector, SelectionResult
from ap.util import maybe_import
from ap.columnar_util import set_ak_column

ak = maybe_import("awkward")
np = maybe_import("numpy")


def extract(array, idx):
    """
    inputs jagged array and index and returns padded array from given index
    """
    array = ak.pad_none(array, idx + 1)
    array = ak.fill_none(array[:, idx], -999)
    return array


# object definitions
# TODO: Producer instead of Selector
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


# variables (after reducing events)
# TODO: this should maybe be a producer
@selector(
    uses={
        "Electron.pt", "Electron.eta", "Muon.pt", "Muon.eta", "Jet.pt", "Jet.eta",
        "Jet.btagDeepFlavB",
    },
    produces={"HT", "nJet", "nElectron", "nMuon", "nDeepjet", "Electron1_pt", "Muon1_pt"} | {
        f"Jet{i}_{attr}"
        for i in range(1, 4 + 1)
        for attr in ["pt", "eta"]
    },
)
def variables(events):
    set_ak_column(events, "HT", ak.sum(events.Jet.pt, axis=1))
    for i in range(1, 4 + 1):
        set_ak_column[events, f"Jet{i}_pt"] = extract(events.Jet.pt, i - 1)
        set_ak_column[events, f"Jet{i}_eta"] = extract(events.Jet.eta, i - 1)
    set_ak_column(events, "nJet", ak.num(events.Jet.pt, axis=1))
    set_ak_column(events, "nElectron", ak.num(events.Electron.pt, axis=1))
    set_ak_column(events, "nMuon", ak.num(events.Muon.pt, axis=1))
    set_ak_column(events, "nDeepjet", ak.num(events.Jet.pt[events.Jet.btagDeepFlavB > 0.3], axis=1))
    set_ak_column(events, "Electron1_pt", extract(events.Electron.pt, 0))
    set_ak_column(events, "Muon1_pt", extract(events.Muon.pt, 0))

    return None, events


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


"""
# combination of all categories, taking categories from each level into account, not only leaf categories
@selector(uses={sel_1e_eq1b, sel_1e_ge2b, sel_1mu_eq1b, sel_1mu_ge2b, sel_1mu_ge2b_highHT, sel_1mu_ge2b_lowHT})
def categories(events, config):
    cat_titles = []# ["no_cat"]
    cat_array = "no_cat"
    mask_int = 0

    def write_cat_array(events, categories, cat_titles, cat_array):
        for cat in categories:
            cat_titles.append(cat.name)
            mask = globals()[cat.selection](events)
            cat_array = np.where(mask, cat.name, cat_array)
            if not cat.is_leaf_category:
                cat_titles, cat_array = write_cat_array(events, cat.categories, cat_titles, cat_array)
        return cat_titles, cat_array

    cat_titles, cat_array = write_cat_array(events, config.categories, cat_titles, cat_array)
    # TODO checks that categories are set up properly
    return SelectionResult(
        #columns={"cat_titles": cat_titles, "cat_array": cat_array}
        columns={"cat_array": cat_array}
    )
"""


# combination of all leaf categories
@selector(
    uses={sel_1e_eq1b, sel_1e_ge2b, sel_1mu_eq1b, sel_1mu_ge2b},
    produces={"cat_array"},
)
def categories(events, config_inst):
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

    return None, events


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
        categories,
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

    # include categories into results
    category_results, events = categories(events, config_inst)
    results += category_results

    # increment stats
    events_sel = events[event_sel]
    stats["n_events"] += len(events)
    stats["n_events_selected"] += ak.sum(event_sel, axis=0)
    stats["sum_mc_weight"] += ak.sum(events.LHEWeight.originalXWGTUP)
    stats["sum_mc_weight_selected"] += ak.sum(events_sel.LHEWeight.originalXWGTUP)

    return results, events
