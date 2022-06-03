# coding: utf-8

"""
Selection methods for testing purposes.
"""

from ap.util import maybe_import
from ap.selection import selector, SelectionResult

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
# TODO: ArrayFunction instead of Selector
@selector(uses={"Jet_pt", "Jet_eta"})
def req_jet(events):
    mask = (events.Jet.pt > 30) & (abs(events.Jet.eta) < 2.4)
    return ak.argsort(events.Jet.pt, axis=-1, ascending=False)[mask]


@selector(uses={"Electron_pt", "Electron_eta", "Electron_cutBased"})
def req_electron(events):
    mask = (events.Electron.pt > 25) & (abs(events.Electron.eta) < 2.4) & (events.Electron.cutBased == 4)
    return ak.argsort(events.Electron.pt, axis=-1, ascending=False)[mask]


@selector(uses={"Muon_pt", "Muon_eta", "Muon_tightId"})
def req_muon(events):
    mask = (events.Muon.pt > 25) & (abs(events.Muon.eta) < 2.4) & (events.Muon.tightId)
    return ak.argsort(events.Muon.pt, axis=-1, ascending=False)[mask]

@selector(uses={"Jet_pt", "nJet", "Jet_eta", "Jet_phi", "Jet_e", 
                "Muon_pt", "nMuon", "Muon_eta", "Muon_phi", "Muon_e" })
def req_delta_r_match(events, threshold=0.4):
    from IPython import embed; embed()
    return 1


@selector(uses={"Jet_pt", "Jet_eta"})
def req_forwardJet(events):
    mask = (events.Jet.pt > 30) & (abs(events.Jet.eta) > 2.4) & (abs(events.Jet.eta) < 5.0)
    return ak.argsort(events.Jet.pt, axis=-1, ascending=False)[mask]


@selector(uses={"Jet_pt", "Jet_eta", "Jet_btagDeepFlavB"})
def req_deepjet(events):
    mask = (events.Jet.pt > 30) & (abs(events.Jet.eta) < 2.4) & (events.Jet.btagDeepFlavB > 0.3)
    return ak.argsort(events.Jet.pt, axis=-1, ascending=False)[mask]


# variables (after reducing events)
@selector(uses={"Electron_pt", "Electron_eta", "Muon_pt", "Muon_eta", "Jet_pt", "Jet_eta", "Jet_btagDeepFlavB"})
def variables(events):
    columns = {}
    columns["HT"] = ak.sum(events.Jet.pt, axis=1)
    for i in range(1, 5):
        columns["Jet" + str(i) + "_pt"] = extract(events.Jet.pt, i - 1)
        columns["Jet" + str(i) + "_eta"] = extract(events.Jet.eta, i - 1)

    columns["nJet"] = ak.num(events.Jet.pt, axis=1)
    columns["nElectron"] = ak.num(events.Electron.pt, axis=1)
    columns["nMuon"] = ak.num(events.Muon.pt, axis=1)
    # columns["nDeepjet"] = ak.num(events.Deepjet.pt, axis=1)
    columns["nDeepjet"] = ak.num(events.Jet.pt[events.Jet.btagDeepFlavB > 0.3], axis=1)
    columns["Electron1_pt"] = extract(events.Electron.pt, 0)
    columns["Muon1_pt"] = extract(events.Muon.pt, 0)

    return SelectionResult(columns=columns)


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


@selector(uses={req_jet, "Jet_pt"})
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


'''
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

'''


# combination of all leaf categories
@selector(uses={sel_1e_eq1b, sel_1e_ge2b, sel_1mu_eq1b, sel_1mu_ge2b})
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
    return SelectionResult(columns={"cat_array": cat_array})


@selector(uses={req_jet})
def jet_selection_test(events, stats):
    # example cuts:
    # - require at least 4 jets with pt>30, eta<2.4
    # example columns:
    # - high jet multiplicity region (>=6 selected jets)

    jet_indices = req_jet(events)
    jet_sel = ak.num(jet_indices, axis=1) >= 4
    jet_high_multiplicity = ak.num(jet_indices, axis=1) >= 6

    # build and return selection results
    return SelectionResult(
        steps={"Jet": jet_sel},
        objects={"Jet": jet_indices},
        columns={"jet_high_multiplicity": jet_high_multiplicity},
    )


@selector(uses={req_deepjet})
def deepjet_selection_test(events, stats):
    deepjet_indices = req_deepjet(events)
    deepjet_sel = ak.num(deepjet_indices, axis=1) >= 1

    return SelectionResult(
        steps={"Deepjet": deepjet_sel},
        objects={"Deepjet": deepjet_indices},
    )

@selector(uses={req_delta_r_match})
def delta_r_selection_test(events, stats):
    clean_jets = req_delta_r_match(events)
    deepjet_sel = ak.num(clean_jets.Jets, axis=1) >= 1

    return SelectionResult(
        steps={"Deepjet": deepjet_sel},
        objects={"Deepjet": clean_jets},
    )

@selector(uses={req_muon})
def muon_selection_test(events, stats):
    # example cuts:
    # - require exactly one muon with pt>25, eta<2.4 and tight Id

    muon_indices = req_muon(events)
    muon_sel = ak.num(muon_indices, axis=1) == 1

    # build and return selection results
    return SelectionResult(
        steps={"Muon": muon_sel},
        objects={"Muon": muon_indices},
    )


@selector(uses={req_electron})
def electron_selection_test(events, stats):
    # example cuts:
    # - require exactly one muon with pt>25, eta<2.4 and tight Id

    electron_indices = req_electron(events)
    electron_sel = ak.num(electron_indices, axis=1) == 1

    # build and return selection results
    return SelectionResult(
        steps={"Electron": electron_sel},
        objects={"Electron": electron_indices},
    )


@selector(uses={req_muon, req_electron})
def lepton_selection_test(events, stats):
    # example cuts:
    # - require exactly one lepton with pt>25, eta<2.4 and tight Id

    muon_indices = req_muon(events)
    electron_indices = req_electron(events)
    lepton_sel = ak.num(muon_indices, axis=1) + ak.num(electron_indices, axis=1) == 1

    # build and return selection results
    return SelectionResult(
        steps={"Lepton": lepton_sel},
        objects={"Muon": muon_indices, "Electron": electron_indices},
    )


@selector(uses={jet_selection_test, lepton_selection_test, deepjet_selection_test, "LHEWeight_originalXWGTUP"})
def test(events, stats, config_inst):
    # example cuts:
    # - jet_selection_test
    # - lepton_selection_test
    # example stats:
    # - number of events before and after selection
    # - sum of mc weights before and after selection

    jet_results = jet_selection_test(events, stats)
    lepton_results = lepton_selection_test(events, stats)
    deepjet_results = deepjet_selection_test(events, stats)

    # combined event selection after all steps
    event_sel = jet_results.steps.Jet & lepton_results.steps.Lepton & deepjet_results.steps.Deepjet

    # build and merge selection results
    results = SelectionResult(main={"event": event_sel})
    results += jet_results
    results += lepton_results
    results += deepjet_results

    # include categories into results
    category_results = categories(events, config_inst)
    results += category_results

    # increment stats
    events_sel = events[event_sel]
    stats["n_events"] += len(events)
    stats["n_events_selected"] += ak.sum(event_sel, axis=0)
    stats["sum_mc_weight"] += ak.sum(events.LHEWeight.originalXWGTUP)
    stats["sum_mc_weight_selected"] += ak.sum(events_sel.LHEWeight.originalXWGTUP)

    return results

@selector(uses={delta_r_selection_test})
def delta_r_test(events, stats, config_inst):
    # example cuts:
    # - jet_selection_test
    # - lepton_selection_test
    # example stats:
    # - number of events before and after selection
    # - sum of mc weights before and after selection

    results = delta_r_selection_test(events, stats)

    return results