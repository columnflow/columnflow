# coding: utf-8

"""
Selection methods for testing purposes.
"""

from ap.util import maybe_import
from ap.selection import selector, SelectionResult

ak = maybe_import("awkward")
np = maybe_import("numpy")

""" inputs jagged array and index and returns padded array from given index """
def extract(array, idx):
    import awkward as ak
    array = ak.pad_none(array, idx+1)
    array = ak.fill_none(array[:,idx], -999)
    return array

# object definitions
@selector(uses={"Jet_pt", "Jet_eta"})
def req_jet(events): 
    mask = (events.Jet.pt > 30) & (abs(events.Jet.eta) < 2.4)
    return ak.argsort(events.Jet.pt, axis=-1, ascending=False)[mask]
@selector(uses={"Electron_pt", "Electron_eta", "Electron_cutBased"})
def req_electron(events): 
    mask = (events.Electron.pt > 25) & (abs(events.Electron.eta) < 2.4) & (events.Electron.cutBased==4)
    return ak.argsort(events.Electron.pt, axis=-1, ascending=False)[mask]
@selector(uses={"Muon_pt", "Muon_eta", "Muon_tightId"})
def req_muon(events):
    mask = (events.Muon.pt > 25) & (abs(events.Muon.eta) < 2.4) & (events.Muon.tightId)
    return ak.argsort(events.Muon.pt, axis=-1, ascending=False)[mask]
@selector(uses={"Jet_pt", "Jet_eta"})
def req_forwardJet(events):
    mask = (events.Jet.pt > 30) & (abs(events.Jet.eta) > 2.4) & (abs(events.Jet.eta) < 5.0)
    return ak.argsort(events.Jet.pt, axis=-1, ascending=False)[mask]
@selector(uses={"Jet_pt", "Jet_eta", "Jet_btagDeepFlavB"})
def req_deepjet(events):
    mask = (events.Jet.pt > 30) & (abs(events.Jet.eta) < 2.4) & (events.Jet.btagDeepFlavB > 0.3)
    return ak.argsort(events.Jet.pt, axis=-1, ascending=False)[mask]


# variables
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
    import awkward as ak
    return ak.num(req_muon(events), axis=1)

@selector(uses={req_jet, "Jet_pt"})
def var_HT(events):
    jet_pt_sorted = events.Jet.pt[req_jet(events)]
    return ak.sum(jet_pt_sorted, axis=1)

@selector(uses={req_electron, "Electron_pt"})
def var_Electron1_pt(events):
    electron_pt_sorted = events.Electron.pt[req_electron(events)]
    return extract(electron_pt_sorted, 0)
@selector(uses={req_muon, "Muon_pt"})
def var_Muon1_pt(events):
    muon_pt_sorted = events.Muon.pt[req_muon(events)]
    return extract(muon_pt_sorted, 0)

@selector(uses={req_jet, "Jet_pt"})
def var_Jet1_pt(events):
    jet_pt_sorted = events.Jet.pt[req_jet(events)]
    return extract(jet_pt_sorted, 0)
@selector(uses={req_jet, "Jet_pt"})
def var_Jet2_pt(events):
    jet_pt_sorted = events.Jet.pt[req_jet(events)]
    return extract(jet_pt_sorted, 1)
@selector(uses={req_jet, "Jet_pt"})
def var_Jet3_pt(events):
    jet_pt_sorted = events.Jet.pt[req_jet(events)]
    return extract(jet_pt_sorted, 2)
@selector(uses={req_jet, "Jet_eta"})
def var_Jet1_eta(events):
    jet_eta_sorted = events.Jet.eta[req_jet(events)]
    return extract(jet_eta_sorted, 0)
@selector(uses={req_jet, "Jet_eta"})
def var_Jet2_eta(events):
    jet_eta_sorted = events.Jet.eta[req_jet(events)]
    return extract(jet_eta_sorted, 1)
@selector(uses={req_jet, "Jet_eta"})
def var_Jet3_eta(events):
    jet_eta_sorted = events.Jet.eta[req_jet(events)]
    return extract(jet_eta_sorted, 2)


# selection for the main categories
@selector(uses={var_nMuon, var_nElectron})
def sel_1e(events):
    return (var_nMuon(events)==0) & (var_nElectron(events)==1)
@selector(uses={var_nMuon, var_nElectron})
def sel_1mu(events):
    return (var_nMuon(events)==1) & (var_nElectron(events)==0)

# selection for the sub-categories
@selector(uses={sel_1e, var_nDeepjet})
def sel_1e_eq1b(events):
    return (sel_1e(events)) & (var_nDeepjet(events)==1)
@selector(uses={sel_1e, var_nDeepjet})
def sel_1e_ge2b(events):
    return (sel_1e(events)) & (var_nDeepjet(events)>=2)

@selector(uses={sel_1e, var_nDeepjet})
def sel_1mu_eq1b(events):
    return (sel_1mu(events)) & (var_nDeepjet(events)==1)
@selector(uses={sel_1e, var_nDeepjet})
def sel_1mu_ge2b(events):
    return (sel_1mu(events)) & (var_nDeepjet(events)>=2)
# combination of all leaf categories just to get required fields
@selector(uses={sel_1e_eq1b, sel_1e_ge2b, sel_1mu_eq1b, sel_1mu_ge2b})
def categories(events, config):
    cat_titles = ["no_cat"]
    cat_array = "no_cat"
    mask_int = 0
    for cat in config.get_leaf_categories():
        #print(cat.name)
        cat_sel = cat.selection
        cat_titles.append(cat.name)
        mask = globals()[cat_sel](events)
        cat_array = np.where(mask, cat.name, cat_array)
        mask_int = mask_int + np.where(mask,1,0) # to check orthogonality of categories
    if not ak.all(mask_int==1):
        if ak.any(mask_int>=2):
            raise ValueError('Leaf categories are supposed to be fully orthogonal')
        else:
            raise ValueError('Some events are without leaf category')
            #print('Some events are without leaf category')
    return SelectionResult(
        columns={"cat_titles": cat_titles, "cat_array": cat_array}
    )


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
        steps={"jet": jet_sel},
        objects={"jet": jet_indices},
        columns={"jet_high_multiplicity": jet_high_multiplicity},
    )


@selector(uses={req_deepjet})
def deepjet_selection_test(events, stats):
    deepjet_indices = req_deepjet(events)
    deepjet_sel = ak.num(deepjet_indices, axis=1) >= 1

    return SelectionResult(
        steps={"deepjet": deepjet_sel},
        objects={"deepjet": deepjet_indices},
    )

@selector(uses={req_muon})
def muon_selection_test(events, stats):
    # example cuts:
    # - require exactly one muon with pt>25, eta<2.4 and tight Id

    muon_indices = req_muon(events)
    muon_sel = ak.num(muon_indices, axis=1) == 1

    # build and return selection results
    return SelectionResult(
        steps={"muon": muon_sel},
        objects={"muon": muon_indices},
    )

@selector(uses={req_electron})
def electron_selection_test(events, stats):
    # example cuts:
    # - require exactly one muon with pt>25, eta<2.4 and tight Id

    electron_indices = req_electron(events)
    electron_sel = ak.num(electron_indices, axis=1) == 1

    # build and return selection results
    return SelectionResult(
        steps={"electron": electron_sel},
        objects={"electron": electron_indices},
    )

@selector(uses={req_muon, req_electron})
def lepton_selection_test(events, stats):
    # example cuts:
    # - require exactly one lepton with pt>25, eta<2.4 and tight Id

    muon_indices = req_muon(events)
    electron_indices = req_electron(events)
    lepton_sel = ak.num(muon_indices, axis=1)+ak.num(electron_indices, axis=1) == 1

    # build and return selection results
    return SelectionResult(
        steps={"lepton": lepton_sel},
        objects={"muon": muon_indices, "electron": electron_indices},
    )



@selector(uses={jet_selection_test, lepton_selection_test, deepjet_selection_test, "LHEWeight_originalXWGTUP"})
def select_test(events, stats):
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
    event_sel = jet_results.steps.jet & lepton_results.steps.lepton & deepjet_results.steps.deepjet

    # build and merge selection results
    results = SelectionResult(main={"event": event_sel})
    results += jet_results
    results += lepton_results
    results += deepjet_results

    # increment stats
    events_sel = events[event_sel]
    stats["n_events"] += len(events)
    stats["n_events_selected"] += ak.sum(event_sel, axis=0)
    stats["sum_mc_weight"] += ak.sum(events.LHEWeight.originalXWGTUP)
    stats["sum_mc_weight_selected"] += ak.sum(events_sel.LHEWeight.originalXWGTUP)

    return results
