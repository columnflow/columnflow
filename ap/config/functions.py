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



# selection for the main categories
def sel_1e(data):
    return (data.nElectron==1)
def sel_1mu(data):
    return (data.nMuon==1)

# selection for the sub-categories
def sel_1e_eq1b(data):
    return (sel_1e(data)) & (data.nDeepjet==1)
def sel_1e_ge2b(data):
    return (sel_1e(data)) & (data.nDeepjet>=2)

def sel_1mu_eq1b(data):
    return (sel_1mu(data)) & (data.nDeepjet==1)
def sel_1mu_ge2b(data):
    return (sel_1mu(data)) & (data.nDeepjet>=2)







def var_HT(data):
    import awkward as ak
    return ak.sum(data.Jet_pt, axis=1)

def var_nElectron(data):
    return data.nElectron
def var_nMuon(data):
    return data.nElectron
def var_nLepton(data):
    return data.nElectron+data.nMuon

def var_Electron1_pt(data):
    return extract(data.Electron_pt, 0)
def var_Muon1_pt(data):
    return extract(data.Muon_pt, 0)
def var_Lepton1_pt(data):
    return extract(combineAndSort(data.Electron_pt, data.Muon_pt), 0)
def var_Electron1_eta(data):
    return extract(data.Electron_eta, 0)
def var_Muon1_eta(data):
    return extract(data.Muon_eta, 0)
def var_Lepton1_eta(data):
    return extract(combineAndSort(data.Electron_eta, data.Muon_eta), 0)

def var_nJet(data):
    return data.nJet
def var_nDeepjet(data):
    return data.nDeepjet

def var_Jet1_pt(data):
    return extract(data.Jet_pt, 0)
def var_Jet2_pt(data):
    return extract(data.Jet_pt, 1)
def var_Jet3_pt(data):
    return extract(data.Jet_pt, 2)

def var_Jet1_eta(data):
    return extract(data.Jet_eta, 0)
def var_Jet2_eta(data):
    return extract(data.Jet_eta, 1)
def var_Jet3_eta(data):
    return extract(data.Jet_eta, 2)
