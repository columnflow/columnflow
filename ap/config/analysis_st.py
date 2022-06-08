# coding: utf-8

"""
Configuration of the single top analysis.
"""

from scinum import Number, REL

import re

from order import Analysis, Shift

import ap.config.processes as procs
from ap.config.campaign_2018 import campaign_2018
from ap.util import DotDict

from ap.util import expand_path

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
    # "cmssw_default.sh",
])

# config groups for conveniently looping over certain configs
# (used in wrapper_factory)
analysis_st.set_aux("config_groups", {})


#
# 2018 standard config
#

# create a config by passing the campaign, so id and name will be identical
config_2018 = analysis_st.add_config(campaign_2018)

# location of JEC txt files
config_2018.set_aux("jec", {
    "levels": ("L1FastJet", "L2Relative", "L2L3Residual", "L3Absolute"),
    "version": "Summer19UL18_V5",
    "jet_type": "AK4PFchs",
    "txt_file_path": expand_path("$AP_BASE/static/jec/"),  # TODO: use external_files
    "uncertainty_sources": ("Total",),  # TODO: use to construct custom calibrator
})

# 2018 luminosity with values in inverse pb and uncertainties taken from
# https://twiki.cern.ch/twiki/bin/view/CMS/TWikiLUM?rev=171#LumiComb
config_2018.set_aux("luminosity", Number(59740, {
    "lumi_13TeV_correlated": (REL, 0.02),
    "lumi_13TeV_2018": (REL, 0.015),
    "lumi_13TeV_1718": (REL, 0.002),
}))

# 2018 minimum bias cross section in mb (milli) for creating PU weights, values from
# https://twiki.cern.ch/twiki/bin/viewauth/CMS/PileupJSONFileforData?rev=44#Pileup_JSON_Files_For_Run_II
config_2018.set_aux("minbiasxs", Number(69.2, (REL, 0.046)))

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


# process groups for conveniently looping over certain processs
# (used in wrapper_factory)
analysis_st.set_aux("process_groups", {})

# dataset groups for conveniently looping over certain datasets
# (used in wrapper_factory)
analysis_st.set_aux("dataset_groups", {})


# helper to add column aliases for both shifts of a source
def add_aliases(shift_source, aliases):
    for direction in ["up", "down"]:
        shift = config_2018.get_shift(Shift.join_name(shift_source, direction))
        # format keys and values
        inject_shift = lambda s: re.sub(r"\{([^_])", r"{_\1", s).format(**shift.__dict__)
        _aliases = {inject_shift(key): inject_shift(value) for key, value in aliases.items()}
        # extend existing or register new column aliases
        shift.set_aux("column_aliases", shift.get_aux("column_aliases", {})).update(_aliases)


# register shifts
config_2018.add_shift(name="nominal", id=0)
config_2018.add_shift(name="tune_up", id=1, type="shape")
config_2018.add_shift(name="tune_down", id=2, type="shape")
config_2018.add_shift(name="hdamp_up", id=3, type="shape")
config_2018.add_shift(name="hdamp_down", id=4, type="shape")

# FIXME: ensure JEC shifts get the same id every time
for i, jec_source in enumerate(config_2018.get_aux("jec")["uncertainty_sources"]):
    config_2018.add_shift(name=f"jec_{jec_source}_up", id=500 + 2 * i, type="shape")
    config_2018.add_shift(name=f"jec_{jec_source}_down", id=501 + 2 * i, type="shape")
    add_aliases(f"jec_{jec_source}", {"Jet.pt": "Jet.pt_{name}", "Jet.mass": "Jet.mass_{name}"})

config_2018.add_shift(name="minbias_xs_up", id=7, type="shape")
config_2018.add_shift(name="minbias_xs_down", id=8, type="shape")

# external files
config_2018.set_aux("external_files", DotDict.wrap({
    # files from TODO
    "lumi": {
        "golden": ("/afs/cern.ch/cms/CAF/CMSCOMM/COMM_DQM/certification/Collisions18/13TeV/Legacy_2018/Cert_314472-325175_13TeV_Legacy2018_Collisions18_JSON.txt", "v1"),  # noqa
        "normtag": ("/afs/cern.ch/user/l/lumipro/public/Normtags/normtag_PHYSICS.json", "v1"),
    },

    # files from https://twiki.cern.ch/twiki/bin/viewauth/CMS/PileupJSONFileforData?rev=44#Pileup_JSON_Files_For_Run_II
    "pu": {
        "json": ("/afs/cern.ch/cms/CAF/CMSCOMM/COMM_DQM/certification/Collisions18/13TeV/PileUp/UltraLegacy/pileup_latest.txt", "v1"),  # noqa
        "mc_profile": ("https://raw.githubusercontent.com/cms-sw/cmssw/435f0b04c0e318c1036a6b95eb169181bbbe8344/SimGeneral/MixingModule/python/mix_2018_25ns_UltraLegacy_PoissonOOTPU_cfi.py", "v1"),  # noqa
        "data_profile": {
            "nominal": ("/afs/cern.ch/cms/CAF/CMSCOMM/COMM_DQM/certification/Collisions18/13TeV/PileUp/UltraLegacy/PileupHistogram-goldenJSON-13tev-2018-69200ub-99bins.root", "v1"),  # noqa
            "minbias_xs_up": ("/afs/cern.ch/cms/CAF/CMSCOMM/COMM_DQM/certification/Collisions18/13TeV/PileUp/UltraLegacy/PileupHistogram-goldenJSON-13tev-2018-72400ub-99bins.root", "v1"),  # noqa
            "minbias_xs_down": ("/afs/cern.ch/cms/CAF/CMSCOMM/COMM_DQM/certification/Collisions18/13TeV/PileUp/UltraLegacy/PileupHistogram-goldenJSON-13tev-2018-66000ub-99bins.root", "v1"),  # noqa
        },
    },
}))

# columns to keep after certain steps
config_2018.set_aux("keep_columns", DotDict.wrap({
    "ReduceEvents": {
        "run", "luminosityBlock", "event",
        "nJet", "Jet.pt", "Jet.eta", "Jet.btagDeepFlavB",
        "nMuon", "Muon.pt", "Muon.eta",
        "nElectron", "Electron.pt", "Electron.eta",
        "LHEWeight.originalXWGTUP",
        "PV.npvs",
        "jet_high_multiplicity", "cat_array",
    },
    "CreateHistograms": {
        "LHEWeight.originalXWGTUP",
    },
}))

# default calibrator, selector and producer
config_2018.set_aux("default_calibrator", "test")
config_2018.set_aux("default_selector", "test")
config_2018.set_aux("default_producer", None)

# file merging values
# key -> dataset -> files per branch (-1 or not present = all)
# TODO: maybe add selector name as additional layer
config_2018.set_aux("file_merging", {
})

# versions per task family and optionally also dataset and shift
# None can be used as a key to define a default value
config_2018.set_aux("versions", {
})

# define categories
# TODO: move their definition to dedicated file
cat_e = config_2018.add_category(
    name="1e",
    id=1,
    label="1 electron",
    selection="sel_1e",
)
cat_mu = config_2018.add_category(
    name="1mu",
    id=2,
    label="1 muon",
    selection="sel_1mu",
)

cat_e_b = cat_e.add_category(
    name="1e_eq1b",
    id=3,
    label="1e, 1 b-tag",
    selection="sel_1e_eq1b",
)
cat_e_bb = cat_e.add_category(
    name="1e_ge2b",
    id=4,
    label=r"1e, $\geq$ 2 b-tags",
    selection="sel_1e_ge2b",
)
cat_mu_b = cat_mu.add_category(
    name="1mu_eq1b",
    id=5,
    label="1mu, 1 b-tag",
    selection="sel_1mu_eq1b",
)
cat_mu_bb = cat_mu.add_category(
    name="1mu_ge2b",
    id=6,
    label=r"1mu, $\geq$ 2 b-tags",
    selection="sel_1mu_ge2b",
)
cat_mu_bb_lowHT = cat_mu_bb.add_category(
    name="1mu_ge2b_lowHT",
    id=7,
    label=r"1mu, $\geq$ 2 b-tags, HT<=300 GeV",
    selection="sel_1mu_ge2b_lowHT",
)
cat_mu_bb_highHT = cat_mu_bb.add_category(
    name="1mu_ge2b_highHT",
    id=8,
    label=r"1mu, $\geq$ 2 b-tags, HT>300 GeV",
    selection="sel_1mu_ge2b_highHT",
)

# define variables
# TODO: move their definition to dedicated file
config_2018.add_variable(
    name="HT",
    expression="var_HT",
    binning=[0, 80, 120, 160, 200, 240, 280, 320, 400, 500, 600, 800],
    unit="GeV",
    x_title="HT",
)
config_2018.add_variable(
    name="nElectron",
    expression="var_nElectron",
    binning=(6, -0.5, 5.5),
    x_title="Number of electrons",
)
config_2018.add_variable(
    name="nMuon",
    expression="var_nMuon",
    binning=(6, -0.5, 5.5),
    x_title="Number of muons",
)
config_2018.add_variable(
    name="Electron1_pt",
    expression="var_Electron1_pt",
    binning=(40, 0., 400.),
    unit="GeV",
    x_title="Leading electron $p_{T}$",
)
config_2018.add_variable(
    name="Muon1_pt",
    expression="var_Muon1_pt",
    binning=(40, 0., 400.),
    unit="GeV",
    x_title="Leading muon $p_{T}$",
)
config_2018.add_variable(
    name="nJet",
    expression="var_nJet",
    binning=(11, -0.5, 10.5),
    x_title="Number of jets",
)
config_2018.add_variable(
    name="nDeepjet",
    expression="var_nDeepjet",
    binning=(8, -0.5, 7.5),
    x_title="Number of deepjets",
)
config_2018.add_variable(
    name="Jet1_pt",
    expression="var_Jet1_pt",
    binning=(40, 0., 400.),
    unit="GeV",
    x_title="Leading jet $p_{T}$",
)
config_2018.add_variable(
    name="Jet2_pt",
    expression="var_Jet2_pt",
    binning=(40, 0., 400.),
    unit="GeV",
    x_title="Jet 2 $p_{T}$",
)
config_2018.add_variable(
    name="Jet3_pt",
    expression="var_Jet3_pt",
    binning=(40, 0., 400.),
    unit="GeV",
    x_title="Jet 3 $p_{T}$",
)
config_2018.add_variable(
    name="Jet1_eta",
    expression="var_Jet1_eta",
    binning=(50, -2.5, 2.5),
    x_title=r"Leading jet $\eta$",
)
config_2018.add_variable(
    name="Jet2_eta",
    expression="var_Jet2_eta",
    binning=(50, -2.5, 2.5),
    x_title=r"Jet 2 $\eta$",
)
config_2018.add_variable(
    name="Jet3_eta",
    expression="var_Jet3_eta",
    binning=(50, -2.5, 2.5),
    x_title=r"Jet 3 $\eta$",
)
