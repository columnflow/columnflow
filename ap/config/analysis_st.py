# coding: utf-8

"""
Configuration of the single top analysis.
"""

from order import Analysis

import ap.config.processes as procs
from ap.config.campaign_2018 import campaign_2018
import ap.config.functions as fcts


#
# the main analysis object
#

analysis_st = Analysis(
    name="analysis_st",
    id=1,
)

# analysis-global versions
analysis_st.set_aux("versions", {
})

# cmssw sandboxes that should be bundled for remote jobs in case they are needed
analysis_st.set_aux("cmssw_sandboxes", [
    "cmssw_default.sh",
])


#
# 2018 standard config
#

# create a config by passing the campaign, so id and name will be identical
config_2018 = analysis_st.add_config(campaign_2018)

config_2018.set_aux("luminosity", 59740)
'''
config_2018.set_aux("objects", {
    "jet": fcts.req_jet(pt=30, eta=2.4),
    "muon": fcts.req_muon(pt=30, eta=2.4, ID="tight", iso=0.15),
    "electron": fcts.req_electron(pt=30, eta=2.4, ID="tight"),
})
'''
#config_2018.set_aux("objects", ["req_jet", "req_electron", "req_muon", "req_bjet"])
config_2018.set_aux("objects", {
    "Jet": "req_jet",
    "Electron": "req_electron",
    "Muon": "req_muon",
    "Deepjet": "req_deepjet",
    "ForwardJet": "req_forwardJet",
})

config_2018.set_aux("selections", ["sel_trigger", "sel_1lepton", "sel_ge3jets", "sel_ge1bjets"])

# add processes we are interested in
config_2018.add_process(procs.process_st)
config_2018.add_process(procs.process_tt)

# add datasets we need to study
dataset_names = [
    "st_tchannel_t",
    "st_tchannel_tbar",
    "st_twchannel_t",
    "st_twchannel_tbar",
    "tt_sl",
    "tt_dl",
    "tt_fh",
]
for dataset_name in dataset_names:
    config_2018.add_dataset(campaign_2018.get_dataset(dataset_name))

# register shifts
config_2018.add_shift(name="nominal", id=0)
config_2018.add_shift(name="tune_up", id=1, type="shape")
config_2018.add_shift(name="tune_down", id=2, type="shape")
config_2018.add_shift(name="hdamp_up", id=3, type="shape")
config_2018.add_shift(name="hdamp_down", id=4, type="shape")
config_2018.add_shift(name="jec_up", id=5, type="shape")
config_2018.add_shift(name="jec_down", id=6, type="shape")

# define channels
#st_channel = config_2018.add_channel("st", 1)

# define categories
cat_e = config_2018.add_category("1e",
                                 label="1 electron",
                                 selection = "sel_1e"
                                 #selection = "lambda d: d.nElectron==1"
                                 #selection="nElectron==1"
)
cat_mu = config_2018.add_category("1mu",
                                  label="1 muon",
                                  selection = "sel_1mu"
                                  #selection= "lambda d: d.nMuon==1"
                                  #selection="nMuon==1"
)

cat_e_b = cat_e.add_category("1e_eq1b",
                             label = "1e, 1 b-tag",
                             selection = "sel_1e_eq1b",
                             #selection = "lambda d: (d.nElectron==1) & (d.nDeepjet==1)",
                             #selection = "(nElectron==1) & (nDeepjet==1)"
)
cat_e_bb = cat_e.add_category("1e_ge2b",
                              label = "1e, $\geq$ 2 b-tags",
                              selection = "sel_1e_ge2b",
                              #selection = "lambda d: (d.nElectron==1) & (d.nDeepjet>=2)",
                              #selection = "(nElectron==1) & (nDeepjet>=2)"
)
cat_mu_b = cat_mu.add_category("1mu_eq1b",
                               label = "1mu, 1 b-tag",
                               selection = "sel_1mu_eq1b",
                               #selection = "lambda d: (d.nMuon==1) & (d.nDeepjet==1)",
                          #selection = "(nElectron==1) & (nDeepjet==1)"
)
cat_mu_bb = cat_mu.add_category("1mu_ge2b",
                                label = "1mu, $\geq$ 2 b-tags",
                                selection = "sel_1mu_ge2b",
                                #selection = "lambda d: (d.nMuon==1) & (d.nDeepjet>=2)",
                                #selection = "(nElectron==1) & (nDeepjet>=2)"
)


# define objects
objects = {
    "Electron": lambda d: (d.Electron_pt>30) & (abs(d.Electron_eta<2.4)) & (d.Electron_cutBased==4), 
    "Muon": lambda d: (d.Muon_pt>30) & (abs(d.Muon_eta<2.4)) & (d.Muon_tightId),
    "Jet": lambda d: (d.Jet_pt>30) & (abs(d.Jet_eta<2.4)),
}

# just an idea
extraObjects = {
    "ForwardJet": lambda d: (d.Jet_pt>30) & (abs(d.Jet_eta>5.0)), # should be defined before Jet Fields are cleaned
    "BJet": lambda d: (d.Jet_btagDeepFlavB > 0.3), # should be defined after Jet Fields are cleaned
    "Lightjet": lambda d: (d.Jet_btagDeepFlavB <= 0.3),
}

# define selections
selections = {
    "trigger_sel": lambda d: (d.HLT_IsoMu27) | (d.HLT_Ele27_WPTight_Gsf),
    "lep_sel": lambda d: (d.nMuon + d.nElectron == 1),
    "jet_sel": lambda d: (d.nJet >= 2),
    "bjet_sel": lambda d: (d.nDeepjet >= 1),
}


# define variables
config_2018.add_variable("sum_of_weights",
                         expression = "var_sum_of_weights",
                         binning = (1,-1.,1.),
                         x_title = "sum of weights",
)

config_2018.add_variable("HT",
                         expression = "var_HT",
                         binning = (40, 0., 800.),
                         unit = "GeV",
                         x_title = "HT"
)
config_2018.add_variable("nElectron",
                         expression = "var_nElectron",
                         binning = (6, -0.5, 5.5),
                         x_title = "Number of electrons",
)
config_2018.add_variable("nMuon",
                         expression = "var_nMuon",
                         binning = (6, -0.5, 5.5),
                         x_title = "Number of muons",
)
config_2018.add_variable("nLepton",
                         expression = "var_nLepton",
                         binning = (6, -0.5, 5.5),
                         x_title = "Number of leptons",
)
config_2018.add_variable("Electron1_pt",
                         expression = "var_Electron1_pt",
                         binning = (40, 0., 400.),
                         unit = "GeV",
                         x_title = "Leading electron $p_{T}$",
)
config_2018.add_variable("Muon1_pt",
                         expression = "var_Muon1_pt",
                         binning = (40, 0., 400.),
                         unit = "GeV",
                         x_title = "Leading muon $p_{T}$",
)
config_2018.add_variable("Lepton1_pt",
                         expression = "var_Lepton1_pt",
                         binning = (40, 0., 400.),
                         unit = "GeV",
                         x_title = "Leading lepton $p_{T}$",
)
config_2018.add_variable("Electron1_eta",
                         expression = "var_Electron1_eta",
                         binning = (50, -2.5, 2.5),
                         x_title = "Leading electron $\eta$",
)
config_2018.add_variable("Muon1_eta",
                         expression = "var_Muon1_eta",
                         binning = (50, -2.5, 2.5),
                         x_title = "Leading muon $\eta$",
)
config_2018.add_variable("Lepton1_eta",
                         expression = "var_Lepton1_eta",
                         binning = (50, -2.5, 2.5),
                         x_title = "Leading lepton $\eta$",
)

config_2018.add_variable("nJet",
                         expression = "var_nJet",
                         binning = (11, -0.5, 10.5),
                         x_title = "Number of jets",
)
config_2018.add_variable("nDeepjet",
                         expression = "var_nDeepjet",
                         binning = (8, -0.5, 7.5),
                         x_title = "Number of deepjets",
)
config_2018.add_variable("Jet1_pt",
                         expression = "var_Jet1_pt",
                         binning = (40, 0., 400.),
                         unit = "GeV",
                         log_y = True,
                         x_title = "Leading jet $p_{T}$",
)
config_2018.add_variable("Jet2_pt",
                         expression = "var_Jet2_pt",
                         binning = (40, 0., 400.),
                         unit = "GeV",
                         log_y = True,
                         x_title = "Jet 2 $p_{T}$",
)
config_2018.add_variable("Jet3_pt",
                         expression = "var_Jet3_pt",
                         binning = (40, 0., 400.),
                         unit = "GeV",
                         x_title = "Jet 3 $p_{T}$",
)
config_2018.add_variable("Jet1_eta",
                         expression = "var_Jet1_eta",
                         binning = (50, -2.5, 2.5),
                         log_y = True,
                         x_title = "Leading jet $\eta$",
)
config_2018.add_variable("Jet2_eta",
                         expression = "var_Jet2_eta",
                         binning = (50, -2.5, 2.5),
                         x_title = "Jet 2 $\eta$",
)
config_2018.add_variable("Jet3_eta",
                         expression = "var_Jet3_eta",
                         binning = (50, -2.5, 2.5),
                         x_title = "Jet 3 $\eta$",
)
""" # does not work, would need an args parameter to call mask = getattr(functions, selection)(*args)(data)
config_2018.add_variable("Jet4_eta",
                         expression = "var_JetN_eta(4)",
                         binning = (40, -2.5, 2.5),
                         x_title = "Jet 4 $\eta$",
)
"""



# file merging values
# key -> dataset -> files per branch (-1 or not present = all)
config_2018.set_aux("file_merging", {
})

# versions per task and potentially other keys
config_2018.set_aux("versions", {
})
