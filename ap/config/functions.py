# coding: utf-8

"""
Functions to define the expressions for categories and variables
"""


""" inputs jagged array and index and returns padded array from given index """
def extract(array, idx):
    import awkward as ak
    array = ak.pad_none(array, idx+1)
    array = ak.fill_none(array[:,idx], -999)
    return array

""" inputs two jagged arrays and returns one combined and sorted jagged array """
def combineAndSort(a1, a2):
    import awkward as ak
    array = ak.concatenate([a1, a2], axis=-1)
    array = ak.sort(array, ascending=False)
    return array

# object definitions: return indices of objects that fulfill requirements
def req_jet(data): 
    import awkward as ak
    mask = (data.Jet_pt > 30) & (abs(data.Jet_eta) < 2.4)
    return ak.argsort(data.Jet_pt, axis=-1, ascending=False)[mask]
def req_electron(data): 
    import awkward as ak
    mask = (data.Electron_pt > 30) & (abs(data.Electron_eta) < 2.4) & (data.Electron_cutBased==4)
    return ak.argsort(data.Electron_pt, axis=-1, ascending=False)[mask]
def req_muon(data):
    import awkward as ak
    mask = (data.Muon_pt > 30) & (abs(data.Muon_eta) < 2.4) & (data.Muon_tightId)
    return ak.argsort(data.Muon_pt, axis=-1, ascending=False)[mask]

def req_forwardJet(data):
    import awkward as ak
    mask = (data.Jet_pt > 30) & (abs(data.Jet_eta) > 5.0)
    return ak.argsort(data.Jet_pt, axis=-1, ascending=False)[mask]
def req_deepjet(data):
    import awkward as ak
    mask = (data.Jet_btagDeepFlavB > 0.3)
    return ak.argsort(data.Jet_pt, axis=-1, ascending=False)[mask]

# variable functions need to change when not simply cleaning object fields...
def var_sum_of_weights(data):
    import awkward as ak
    return ak.zeros_like(data.LHEWeight_originalXWGTUP) # random field with one entry per event

def var_nJet(data):
    import awkward as ak
    return ak.num(req_jet(data), axis=1)
def var_nDeepjet(data):
    import awkward as ak
    return ak.num(req_deepjet(data), axis=1)
def var_nElectron(data):
    import awkward as ak
    return ak.num(req_electron(data), axis=1)
def var_nMuon(data):
    import awkward as ak
    return ak.num(req_muon(data), axis=1)
def var_nLepton(data):
    import awkward as ak
    return ak.num(req_muon(data), axis=1)+ak.num(req_electron(data), axis=1)

def var_HT(data):
    jet_pt_sorted = data.Jet_pt[req_jet(data)]
    return ak.sum(jet_pt_sorted, axis=1)
    
def var_Electron1_pt(data):
    electron_pt_sorted = data.Electron_pt[req_electron(data)]
    return extract(electron_pt_sorted, 0)
def var_Muon1_pt(data):
    muon_pt_sorted = data.Muon_pt[req_muon(data)]
    return extract(muon_pt_sorted, 0)
def var_Lepton1_pt(data):
    lepton_pt_sorted = combineAndSort(data.Electron_pt[req_electron(data)],data.Muon_pt[req_muon(data)])
    return extract(lepton_pt_sorted, 0)
def var_Electron1_eta(data):
    electron_eta_sorted = data.Electron_eta[req_electron(data)]
    return extract(electron_eta_sorted, 0)
def var_Muon1_eta(data):
    muon_eta_sorted = data.Muon_eta[req_muon(data)]
    return extract(muon_eta_sorted, 0)
def var_Lepton1_eta(data):
    lepton_eta_sorted = combineAndSort(data.Electron_eta[req_electron(data)],data.Muon_eta[req_muon(data)])
    return extract(lepton_eta_sorted, 0)


def var_Jet1_pt(data):
    jet_pt_sorted = data.Jet_pt[req_jet(data)]
    return extract(jet_pt_sorted, 0)
def var_Jet2_pt(data):
    jet_pt_sorted = data.Jet_pt[req_jet(data)]
    return extract(jet_pt_sorted, 1)
def var_Jet3_pt(data):
    jet_pt_sorted = data.Jet_pt[req_jet(data)]
    return extract(jet_pt_sorted, 2)
def var_Jet1_eta(data):
    jet_eta_sorted = data.Jet_eta[req_jet(data)]
    return extract(jet_eta_sorted, 0)
def var_Jet2_eta(data):
    jet_eta_sorted = data.Jet_eta[req_jet(data)]
    return extract(jet_eta_sorted, 1)
def var_Jet3_eta(data):
    jet_eta_sorted = data.Jet_eta[req_jet(data)]
    return extract(jet_eta_sorted, 2)



# selections
def sel_trigger(data):
    return (data.HLT_IsoMu27) | (data.HLT_Ele27_WPTight_Gsf)
def sel_1lepton(data):
    import awkward as ak
    return (ak.num(req_electron(data), axis=1) + ak.num(req_muon(data), axis=1) == 1)
def sel_ge3jets(data):
    import awkward as ak
    return (ak.num(req_jet(data), axis=1) >= 3)
def sel_ge1bjets(data):
    import awkward as ak
    return (ak.num(req_bjet(data), axis=1) >= 1)

'''
def sel_geNjets(N):
    import awkward as ak
    return lambda d: (ak.sum(req_jet(d), axis=1), axis=1) >= N
'''



# selection for the main categories
def sel_1e(data):
    return (ak.num(req_electron(data), axis=1)==1) & (ak.num(req_muon(data), axis=1)==0)
def sel_1mu(data):
    return (ak.num(req_electron(data), axis=1)==0) & (ak.num(req_muon(data), axis=1)==1)

# selection for the sub-categories
def sel_1e_eq1b(data):
    return (sel_1e(data)) & (ak.num(req_bjet(data), axis=1)==1)
def sel_1e_ge2b(data):
    return (sel_1e(data)) & (ak.num(req_bjet(data), axis=1)>=2)

def sel_1mu_eq1b(data):
    return (sel_1mu(data)) & (ak.num(req_bjet(data), axis=1)==1)
def sel_1mu_ge2b(data):
    return (sel_1mu(data)) & (ak.num(req_bjet(data), axis=1)>=2)

