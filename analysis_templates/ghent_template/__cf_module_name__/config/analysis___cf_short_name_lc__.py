# coding: utf-8

"""
Configuration of the __cf_analysis_name__ analysis.
"""

import functools

import law
import order as od
from scinum import Number

from columnflow.util import DotDict, maybe_import
from columnflow.columnar_util import EMPTY_FLOAT, ColumnCollection
from columnflow.config_util import (
    get_root_processes_from_campaign, add_shift_aliases, get_shifts_from_sources, add_category,
    verify_config_processes,
)

ak = maybe_import("awkward")


#
# the main analysis object
#

analysis___cf_short_name_lc__ = ana = od.Analysis(
    name="analysis___cf_short_name_lc__",
    id=1,
)

# analysis-global versions
# (see cfg.x.versions below for more info)
ana.x.versions = {}

# files of bash sandboxes that might be required by remote tasks
# (used in cf.HTCondorWorkflow)
ana.x.bash_sandboxes = ["$CF_BASE/sandboxes/cf.sh"]
default_sandbox = law.Sandbox.new(law.config.get("analysis", "default_columnar_sandbox"))
if default_sandbox.sandbox_type == "bash" and default_sandbox.name not in ana.x.bash_sandboxes:
    ana.x.bash_sandboxes.append(default_sandbox.name)

# files of cmssw sandboxes that might be required by remote tasks
# (used in cf.HTCondorWorkflow)
ana.x.cmssw_sandboxes = [
    "$CF_BASE/sandboxes/cmssw_default.sh",
]

# config groups for conveniently looping over certain configs
# (used in wrapper_factory)
ana.x.config_groups = {}


#
# setup configs
#

# an example config is setup below, based on cms NanoAOD v9 for Run2 2017, focussing on
# ttbar and single top MCs, plus single muon data
# update this config or add additional ones to accomodate the needs of your analysis

from cmsdb.campaigns.run2_2018_nano_v9 import campaign_run2_2018_nano_v9

# copy the campaign
# (creates copies of all linked datasets, processes, etc. to allow for encapsulated customization)
campaign = campaign_run2_2018_nano_v9.copy()

# get all root processes
procs = get_root_processes_from_campaign(campaign)

# create a config by passing the campaign, so id and name will be identical
cfg = ana.add_config(campaign)

# gather campaign data
year = campaign.x.year
ecm = campaign.ecm
year2 = year % 100
corr_postfix = f"{campaign.x.vfp}VFP" if year == 2016 else ""

# add processes we are interested in
process_names = [
    "data",
    "tt",
    "st",
]
for process_name in process_names:
    # add the process
    proc = cfg.add_process(procs.get(process_name))

    # configuration of colors, labels, etc. can happen here
    if proc.is_mc:
        proc.color1 = (244, 182, 66) if proc.name == "tt" else (244, 93, 66)

# add datasets we need to study
dataset_names = [
    # data
    "data_mu_b",
    # backgrounds
    "tt_sl_powheg",
    # signals
    "st_tchannel_t_powheg",
]
for dataset_name in dataset_names:
    # add the dataset
    dataset = cfg.add_dataset(campaign.get_dataset(dataset_name))

    # for testing purposes, limit the number of files to 2
    for info in dataset.info.values():
        info.n_files = min(info.n_files, 2)

# verify that the root process of all datasets is part of any of the registered processes
verify_config_processes(cfg, warn=True)

# default objects, such as calibrator, selector, producer, ml model, inference model, etc
cfg.x.default_calibrator = "example"
cfg.x.default_selector = "example"
cfg.x.default_producer = "example"
cfg.x.default_ml_model = None
cfg.x.default_inference_model = "example"
cfg.x.default_categories = ("incl",)
cfg.x.default_variables = ("n_jet", "jet1_pt")


# process groups for conveniently looping over certain processs
# (used in wrapper_factory and during plotting)
cfg.x.process_groups = {}

# dataset groups for conveniently looping over certain datasets
# (used in wrapper_factory and during plotting)
cfg.x.dataset_groups = {}

# category groups for conveniently looping over certain categories
# (used during plotting)
cfg.x.category_groups = {}

# variable groups for conveniently looping over certain variables
# (used during plotting)
cfg.x.variable_groups = {}

# shift groups for conveniently looping over certain shifts
# (used during plotting)
cfg.x.shift_groups = {}

# general_settings groups for conveniently looping over different values for the general-settings parameter
# (used during plotting)
cfg.x.general_settings_groups = {}

# process_settings groups for conveniently looping over different values for the process-settings parameter
# (used during plotting)
cfg.x.process_settings_groups = {}

# variable_settings groups for conveniently looping over different values for the variable-settings parameter
# (used during plotting)
cfg.x.variable_settings_groups = {}

# custom_style_config groups for conveniently looping over certain style configs
# (used during plotting)
cfg.x.custom_style_config_groups = {}

# selector step groups for conveniently looping over certain steps
# (used in cutflow tasks)
cfg.x.selector_step_groups = {
    "default": ["muon", "jet"],
}

# calibrator groups for conveniently looping over certain calibrators
# (used during calibration)
cfg.x.calibrator_groups = {}

# producer groups for conveniently looping over certain producers
# (used during the ProduceColumns task)
cfg.x.producer_groups = {}

# ml_model groups for conveniently looping over certain ml_models
# (used during the machine learning tasks)
cfg.x.ml_model_groups = {}


# custom method and sandbox for determining dataset lfns
cfg.x.get_dataset_lfns = None
cfg.x.get_dataset_lfns_sandbox = None

# whether to validate the number of obtained LFNs in GetDatasetLFNs
# (currently set to false because the number of files per dataset is truncated to 2)
cfg.x.validate_dataset_lfns = False

# lumi values in inverse pb
# https://twiki.cern.ch/twiki/bin/view/CMS/LumiRecommendationsRun2?rev=2#Combination_and_correlations
cfg.x.luminosity = Number(41480, {
    "lumi_13TeV_2017": 0.02j,
    "lumi_13TeV_1718": 0.006j,
    "lumi_13TeV_correlated": 0.009j,
})

# names of muon correction sets and working points
# (used in the muon producer)
cfg.x.muon_sf_names = ("NUM_TightRelIso_DEN_TightIDandIPCut", f"{year}_UL")

# register shifts
cfg.add_shift(name="nominal", id=0)

# tune shifts are covered by dedicated, varied datasets, so tag the shift as "disjoint_from_nominal"
# (this is currently used to decide whether ML evaluations are done on the full shifted dataset)
cfg.add_shift(name="tune_up", id=1, type="shape", tags={"disjoint_from_nominal"})
cfg.add_shift(name="tune_down", id=2, type="shape", tags={"disjoint_from_nominal"})

# fake jet energy correction shift, with aliases flaged as "selection_dependent", i.e. the aliases
# affect columns that might change the output of the event selection
cfg.add_shift(name="jec_up", id=20, type="shape")
cfg.add_shift(name="jec_down", id=21, type="shape")
add_shift_aliases(
    cfg,
    "jec",
    {
        "Jet.pt": "Jet.pt_{name}",
        "Jet.mass": "Jet.mass_{name}",
        "MET.pt": "MET.pt_{name}",
        "MET.phi": "MET.phi_{name}",
    },
)

# event weights due to muon scale factors
cfg.add_shift(name="mu_up", id=10, type="shape")
cfg.add_shift(name="mu_down", id=11, type="shape")
add_shift_aliases(cfg, "mu", {"muon_weight": "muon_weight_{direction}"})

# external files
json_mirror = "modules/jsonpog-integration"
lumi_cert_site = f"https://cms-service-dqmdc.web.cern.ch/CAF/certification/Collisions{year2}/{ecm:g}TeV/"
pu_reweighting_site = f"{lumi_cert_site}/PileUp/UltraLegacy"
goldenjsons = {
    2016: f"Cert_271036-284044_13TeV_Legacy2016_Collisions16_JSON.txt",
    2017: f"Cert_294927-306462_13TeV_UL2017_Collisions17_GoldenJSON.txt",
    2018: f"Cert_314472-325175_13TeV_Legacy2018_Collisions18_JSON.txt",
}
cfg.x.external_files = DotDict.wrap({
    # lumi files (golden run 2 only!!)
    "lumi": {
        "golden": (f"{lumi_cert_site}/Legacy_{year}/{goldenjsons[year]}", "v1"),
        "normtag": ("modules/Normtags/normtag_PHYSICS.json", "v1"),
    },

    # jet energy correction
    "jet_jerc": (f"{json_mirror}/POG/JME/{year}{corr_postfix}_UL/jet_jerc.json.gz", "v1"),

    # electron scale factors
    "electron_sf": (f"{json_mirror}/POG/EGM/{year}{corr_postfix}_UL/electron.json.gz", "v1"),

    # muon scale factors
    "muon_sf": (f"{json_mirror}/POG/MUO/{year}{corr_postfix}_UL/muon_Z.json.gz", "v1"),

    # btag scale factor
    "btag_sf_corr": (f"{json_mirror}/POG/BTV/{year}{corr_postfix}_UL/btagging.json.gz", "v1"),

    # run 2 only!!
    # files from https://twiki.cern.ch/twiki/bin/viewauth/CMS/PileupJSONFileforData?rev=44#Pileup_JSON_Files_For_Run_II # noqa
    "pu": {
        "json": (f"{pu_reweighting_site}/pileup_latest.txt", "v1"),  # noqa
        "mc_profile": (
        "https://raw.githubusercontent.com/cms-sw/cmssw/435f0b04c0e318c1036a6b95eb169181bbbe8344/SimGeneral/MixingModule/python/mix_2018_25ns_UltraLegacy_PoissonOOTPU_cfi.py", # noqa
        "v1"),  # noqa
        "data_profile": {
            "nominal": (
            f"{pu_reweighting_site}/PileupHistogram-goldenJSON-{ecm:g}tev-{year}-69200ub-99bins.root", "v1"),
            # noqa
            "minbias_xs_up": (
            f"{pu_reweighting_site}/PileupHistogram-goldenJSON-{ecm:g}tev-{year}-72400ub-99bins.root", "v1"),
            # noqa
            "minbias_xs_down": (
            f"{pu_reweighting_site}/PileupHistogram-goldenJSON-{ecm:g}tev-{year}-66000ub-99bins.root", "v1"),
            # noqa
        },
    },
})

# target file size after MergeReducedEvents in MB
cfg.x.reduced_file_size = 512.0

# columns to keep after certain steps
cfg.x.keep_columns = DotDict.wrap({
    "cf.ReduceEvents": {
        # general event info, mandatory for reading files with coffea
        ColumnCollection.MANDATORY_COFFEA,  # additional columns can be added as strings, similar to object info
        # object info
        "Jet.pt", "Jet.eta", "Jet.phi", "Jet.mass", "Jet.btagDeepFlavB", "Jet.hadronFlavour",
        "Muon.pt", "Muon.eta", "Muon.phi", "Muon.mass", "Muon.pfRelIso04_all",
        "MET.pt", "MET.phi", "MET.significance", "MET.covXX", "MET.covXY", "MET.covYY",
        "PV.npvs",
        # all columns added during selection using a ColumnCollection flag
        ColumnCollection.ALL_FROM_SELECTOR,
    },
    "cf.MergeSelectionMasks": {
        "cutflow.*",
    },
    "cf.UniteColumns": {
        "*",
    },
})

# event weight columns as keys in an OrderedDict, mapped to shift instances they depend on
get_shifts = functools.partial(get_shifts_from_sources, cfg)
cfg.x.event_weights = DotDict({
    "normalization_weight": [],
    "muon_weight": get_shifts("mu"),
})

# versions per task family, either referring to strings or to callables receving the invoking
# task instance and parameters to be passed to the task family
cfg.x.versions = {
    # "cf.CalibrateEvents": "prod1",
    # "cf.SelectEvents": (lambda cls, inst, params: "prod1" if params.get("selector") == "default" else "dev1"),
    # ...
}

# channels
# (just one for now)
cfg.add_channel(name="mutau", id=1)

# add categories using the "add_category" tool which adds auto-generated ids
# the "selection" entries refer to names of categorizers, e.g. in categorization/example.py
# note: it is recommended to always add an inclusive category with id=1 or name="incl" which is used
#       in various places, e.g. for the inclusive cutflow plots and the "empty" selector
add_category(
    cfg,
    id=1,
    name="incl",
    selection="cat_incl",
    label="inclusive",
)
add_category(
    cfg,
    name="2j",
    selection="cat_2j",
    label="2 jets",
)

# add variables
# (the "event", "run" and "lumi" variables are required for some cutflow plotting task,
# and also correspond to the minimal set of columns that coffea's nano scheme requires)
cfg.add_variable(
    name="event",
    expression="event",
    binning=(1, 0.0, 1.0e9),
    x_title="Event number",
    discrete_x=True,
)
cfg.add_variable(
    name="run",
    expression="run",
    binning=(1, 100000.0, 500000.0),
    x_title="Run number",
    discrete_x=True,
)
cfg.add_variable(
    name="lumi",
    expression="luminosityBlock",
    binning=(1, 0.0, 5000.0),
    x_title="Luminosity block",
    discrete_x=True,
)
cfg.add_variable(
    name="n_jet",
    expression="n_jet",
    binning=(11, -0.5, 10.5),
    x_title="Number of jets",
    discrete_x=True,
)
# pt of all jets in every event
cfg.add_variable(
    name="jets_pt",
    expression="Jet.pt",
    binning=(40, 0.0, 400.0),
    unit="GeV",
    x_title=r"$p_{T} of all jets$",
)
# pt of the first jet in every event
cfg.add_variable(
    name="jet1_pt",  # variable name, to be given to the "--variables" argument for the plotting task
    expression="Jet.pt[:,0]",  # content of the variable
    null_value=EMPTY_FLOAT,  # value to be given if content not available for event
    binning=(40, 0.0, 400.0),  # (bins, lower edge, upper edge)
    unit="GeV",  # unit of the variable, if any
    x_title=r"Jet 1 $p_{T}$",  # x title of histogram when plotted
)
# eta of the first jet in every event
cfg.add_variable(
    name="jet1_eta",
    expression="Jet.eta[:,0]",
    null_value=EMPTY_FLOAT,
    binning=(30, -3.0, 3.0),
    x_title=r"Jet 1 $\eta$",
)
cfg.add_variable(
    name="ht",
    expression=lambda events: ak.sum(events.Jet.pt, axis=1),
    binning=(40, 0.0, 800.0),
    unit="GeV",
    x_title="HT",
)
# weights
cfg.add_variable(
    name="mc_weight",
    expression="mc_weight",
    binning=(200, -10, 10),
    x_title="MC weight",
)
# cutflow variables
cfg.add_variable(
    name="cf_jet1_pt",
    expression="cutflow.jet1_pt",
    binning=(40, 0.0, 400.0),
    unit="GeV",
    x_title=r"Jet 1 $p_{T}$",
)
