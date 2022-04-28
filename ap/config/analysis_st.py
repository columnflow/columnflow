# coding: utf-8

"""
Configuration of the single top analysis.
"""

from order import Analysis

import ap.config.processes as procs
from ap.config.campaign_2018 import campaign_2018


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
st_channel = config_2018.add_channel("st", 1)

# 1b or 2b category (ch_e)

cat_e = config_2018.add_category("1e",
                                 label="1 electron",
                                 selection = "lambda d: d.nElectron==1"
                                 #selection="nElectron==1"
)
cat_mu = config_2018.add_category("1mu",
                                  label="1 muon",
                                  selection= "lambda d: d.nMuon==1"
                                  #selection="nMuon==1"
)

cat_e_b = cat_e.add_category("1e_eq1b",
                             label = "1e, 1 b-tag",
                             selection = "lambda d: (d.nElectron==1) & (d.nDeepjet==1)",
                             #selection = "(nElectron==1) & (nDeepjet==1)"
)
cat_e_bb = cat_e.add_category("1e_ge2b",
                              label = "1e, $\geq$ 2 b-tags",
                              selection = "lambda d: (d.nElectron==1) & (d.nDeepjet>=2)",
                              #selection = "(nElectron==1) & (nDeepjet>=2)"
)
cat_mu_b = cat_mu.add_category("1mu_eq1b",
                               label = "1mu, 1 b-tag",
                               selection = "lambda d: (d.nMuon==1) & (d.nDeepjet==1)",
                          #selection = "(nElectron==1) & (nDeepjet==1)"
)
cat_mu_bb = cat_mu.add_category("1mu_ge2b",
                                label = "1mu, $\geq$ 2 b-tags",
                                selection = "lambda d: (d.nMuon==1) & (d.nDeepjet>=2)",
                                #selection = "(nElectron==1) & (nDeepjet>=2)"
)


# define objects
objects = {
    "Electron": lambda d: (d.Electron_pt>30) & (d.Electron_eta<2.4) & (d.Electron_cutBased==4), 
    "Muon": lambda d: (d.Muon_pt>30) & (d.Muon_eta<2.4) & (d.Muon_tightId),
    "Jet": lambda d: (d.Jet_pt>30) & (d.Jet_eta<2.4),
}

# just an idea
extraObjects = {
    "ForwardJet": lambda d: (d.Jet_pt>30) & (d.Jet_eta>5.0), # should be defined before Jet Fields are cleaned
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

# define variables of interest in config? Implement a task that loads them into the array?
eventVars = {
    "HT": lambda d: ak.sum(d.Jet_pt, axis=1),
    "nDeepjet": lambda d: ak.num(d.Jet_btagDeepFlavB > 0.3),
}



# define variables

# do I need to save HT in my awkward array to use it?
#config_2018.add_variable("HT",
#                         expression = "ak.sum(events.Jet_pt, axis=1)",
#                         binning = (40, 0., 800.),
#                         unit = "GeV"
#                         x_title = "HT"
#)

config_2018.add_variable("N_jets",
                         expression = "nJet", # events.nJet ??
                         binning = (11, -0.5, 10.5),
                         unit = "",
                         x_title = "Number of jets",

)

config_2018.add_variable("jet1_pt",
                         expression = "Jet_pt[:,0]",
                         binning = (40, 0., 500.),
                         unit = "GeV",
                         x_title = "Leading jet $p_{T}$",
)
config_2018.add_variable("jet1_eta",
                         expression = "Jet_eta[:,0]",
                         binning = (40, 0., 500.),
                         unit = "GeV",
                         x_title = "Leading jet $/eta$",
)




# file merging values
# key -> dataset -> files per branch (-1 or not present = all)
config_2018.set_aux("file_merging", {
})

# versions per task and potentially other keys
config_2018.set_aux("versions", {
})
