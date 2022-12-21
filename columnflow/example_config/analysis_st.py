# coding: utf-8

"""
Configuration of the single top analysis.
"""

import re

from scinum import Number
from order import Analysis, Shift

from columnflow.example_config.campaign_2018 import (
    campaign_2018, process_data, process_st_tchannel_t, process_tt_sl,
)
from columnflow.util import DotDict


#
# the main analysis object
#

analysis_st = Analysis(
    name="analysis_st",
    id=1,
)

# analysis-global versions
analysis_st.x.versions = {}

# files of bash sandboxes that might be required by remote tasks
# (used in cf.HTCondorWorkflow)
analysis_st.x.job_sandboxes = [
    "$CF_BASE/sandboxes/venv_columnar.sh",
]

# files of cmssw sandboxes that should be bundled for remote jobs in case they are needed
# (used in cf.HTCondorWorkflow)
analysis_st.x.cmssw_sandboxes = [
    # "$CF_BASE/sandboxes/cmssw_default.sh",
]

# config groups for conveniently looping over certain configs
# (used in wrapper_factory)
analysis_st.x.config_groups = {}


#
# 2018 standard config
#

# create a config by passing the campaign, so id and name will be identical
config_2018 = analysis_st.add_config(campaign_2018)

# add processes we are interested in
config_2018.add_process(process_data)
config_2018.add_process(process_st_tchannel_t)
config_2018.add_process(process_tt_sl)

# add datasets we need to study
config_2018.add_dataset(campaign_2018.get_dataset("data_mu_a"))
config_2018.add_dataset(campaign_2018.get_dataset("st_tchannel_t"))
config_2018.add_dataset(campaign_2018.get_dataset("tt_sl"))


# default calibrator, selector, producer, ml model and inference model
config_2018.x.default_calibrator = "example"
config_2018.x.default_selector = "example"
config_2018.x.default_producer = "example"
config_2018.x.default_ml_model = None
config_2018.x.default_inference_model = "example"
config_2018.x.default_categories = ("incl",)
config_2018.x.default_variables = ("ht", "jet1_pt")

# process groups for conveniently looping over certain processs
# (used in wrapper_factory and during plotting)
config_2018.x.process_groups = {}

# dataset groups for conveniently looping over certain datasets
# (used in wrapper_factory and during plotting)
config_2018.x.dataset_groups = {}

# category groups for conveniently looping over certain categories
# (used during plotting)
config_2018.x.category_groups = {}

# variable groups for conveniently looping over certain variables
# (used during plotting)
config_2018.x.variable_groups = {}

# shift groups for conveniently looping over certain shifts
# (used during plotting)
config_2018.x.shift_groups = {}

# selector step groups for conveniently looping over certain steps
# (used in cutflow tasks)
config_2018.x.selector_step_groups = {
    "example": ["Jet"],
}

# custom method for determining dataset lfns
config_2018.x.determine_dataset_lfns = None

# 2018 luminosity with values in inverse pb and uncertainties taken from
# https://twiki.cern.ch/twiki/bin/view/CMS/TWikiLUM?rev=171#LumiComb
config_2018.x.luminosity = Number(59740, {
    "lumi_13TeV_correlated": 0.02j,
    "lumi_13TeV_2018": 0.015j,
    "lumi_13TeV_1718": 0.002j,
})

# 2018 minimum bias cross section in mb (milli) for creating PU weights, values from
# https://twiki.cern.ch/twiki/bin/viewauth/CMS/PileupJSONFileforData?rev=44#Pileup_JSON_Files_For_Run_II
config_2018.x.minbiasxs = Number(69.2, 0.046j)


# helper to add column aliases for both shifts of a source
def add_aliases(shift_source: str, aliases: set[str], selection_dependent: bool):
    for direction in ["up", "down"]:
        shift = config_2018.get_shift(Shift.join_name(shift_source, direction))
        # format keys and values
        inject_shift = lambda s: re.sub(r"\{([^_])", r"{_\1", s).format(**shift.__dict__)
        _aliases = {inject_shift(key): inject_shift(value) for key, value in aliases.items()}
        alias_type = "column_aliases_selection_dependent" if selection_dependent else "column_aliases"
        # extend existing or register new column aliases
        shift.set_aux(alias_type, shift.get_aux(alias_type, {})).update(_aliases)


# register shifts
config_2018.add_shift(name="nominal", id=0)
config_2018.add_shift(name="tune_up", id=1, type="shape", tags={"disjoint_from_nominal"})
config_2018.add_shift(name="tune_down", id=2, type="shape", tags={"disjoint_from_nominal"})
config_2018.add_shift(name="hdamp_up", id=3, type="shape", tags={"disjoint_from_nominal"})
config_2018.add_shift(name="hdamp_down", id=4, type="shape", tags={"disjoint_from_nominal"})
config_2018.add_shift(name="minbias_xs_up", id=7, type="shape")
config_2018.add_shift(name="minbias_xs_down", id=8, type="shape")
add_aliases("minbias_xs", {"pu_weight": "pu_weight_{name}"}, selection_dependent=False)
config_2018.add_shift(name="jecdummy_up", id=9, type="shape")
config_2018.add_shift(name="jecdummy_down", id=10, type="shape")
add_aliases(
    "jecdummy",
    {"Jet.pt": "Jet.pt_jec_{direction}", "Jet.mass": "Jet.mass_jec_{direction}"},
    selection_dependent=True,
)

# external files
config_2018.x.external_files = DotDict.wrap({
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
})

# columns to keep after certain steps
config_2018.x.keep_columns = DotDict.wrap({
    "cf.ReduceEvents": {
        "run", "luminosityBlock", "event",
        "nJet", "Jet.pt", "Jet.eta", "Jet.btagDeepFlavB",
        "mc_weight", "PV.npvs", "category_ids", "deterministic_seed",
    },
    "cf.MergeSelectionMasks": {
        "mc_weight", "normalization_weight", "process_id", "category_ids", "cutflow.*",
    },
})


# event weight columns as keys in an OrderedDict, mapped to shift instances they depend on
def get_shifts(*names):
    return sum((
        [
            config_2018.get_shift(f"{name}_up"),
            config_2018.get_shift(f"{name}_down"),
        ]
        for name in names
    ), [])


config_2018.x.event_weights = DotDict()
config_2018.x.event_weights["normalization_weight"] = []
config_2018.x.event_weights["pu_weight"] = get_shifts("minbias_xs")

# versions per task family and optionally also dataset and shift
# None can be used as a key to define a default value
config_2018.x.versions = {}

# add categories
config_2018.add_category(
    name="incl",
    id=1,
    selection="sel_incl",
    label="inclusive",
)
cat_e = config_2018.add_category(
    name="1e",
    id=2,
    selection="sel_1e",
    label="1 electron",
)

# add variables
config_2018.add_variable(
    name="mc_weight",
    expression="mc_weight",
    binning=(200, -10, 10),
    x_title="MC weight",
)
config_2018.add_variable(
    name="ht",
    binning=[0, 80, 120, 160, 200, 240, 280, 320, 400, 500, 600, 800],
    unit="GeV",
    x_title="HT",
)
config_2018.add_variable(
    name="jet1_pt",
    expression="Jet.pt[:,0]",
    null_value=-1e5,
    binning=(40, 0., 400.),
    unit="GeV",
    x_title=r"Jet 1 $p_{T}$",
)

# dedicated cutflow variables
# (added explicitly in e.g. a producer in order to drop everything that is not needed,
#  which could otherwise be very expensive to store)
config_2018.add_variable(
    name="cf_jet1_pt",
    expression="cutflow.jet1_pt",
    binning=(40, 0., 400.),
    unit="GeV",
    x_title=r"Jet 1 $p_{T}$",
)
