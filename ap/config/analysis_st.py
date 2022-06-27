# coding: utf-8

"""
Configuration of the single top analysis.
"""

from scinum import Number, REL

import re

from order import Analysis, Shift

import ap.config.processes as procs
from ap.config.campaign_2018 import campaign_2018
from ap.config.categories import add_categories
from ap.config.variables import add_variables
from ap.util import DotDict


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

# add processes we are interested in
config_2018.add_process(procs.process_data)
config_2018.add_process(procs.process_st)
config_2018.add_process(procs.process_tt)

# add datasets we need to study
dataset_names = [
    "data_mu_a",
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
# (used in wrapper_factory and during plotting)
config_2018.set_aux("process_groups", {})

# dataset groups for conveniently looping over certain datasets
# (used in wrapper_factory and during plotting)
config_2018.set_aux("dataset_groups", {})

# category groups for conveniently looping over certain categories
# (used during plotting)
config_2018.set_aux("category_groups", {})

# variable groups for conveniently looping over certain variables
# (used during plotting)
config_2018.set_aux("variable_groups", {})

# shift groups for conveniently looping over certain shifts
# (used during plotting)
config_2018.set_aux("shift_groups", {})

# default calibrator, selector and producer
config_2018.set_aux("default_calibrator", "test")
config_2018.set_aux("default_selector", "test")
config_2018.set_aux("default_producer", "variables")

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

# location of JEC txt files
config_2018.set_aux("jec", DotDict.wrap({
    "source": "https://raw.githubusercontent.com/cms-jet/JECDatabase/master/textFiles",
    "campaign": "Summer19UL18",
    "version": "V5",
    "jet_type": "AK4PFchs",
    "levels": ["L1FastJet", "L2Relative", "L2L3Residual", "L3Absolute"],
    "data_eras": ["RunA", "RunB", "RunC", "RunD"],
    "uncertainty_sources": [
        # comment out most for now to prevent large file sizes
        # "AbsoluteStat",
        # "AbsoluteScale",
        # "AbsoluteSample",
        # "AbsoluteFlavMap",
        # "AbsoluteMPFBias",
        # "Fragmentation",
        # "SinglePionECAL",
        # "SinglePionHCAL",
        # "FlavorQCD",
        # "TimePtEta",
        # "RelativeJEREC1",
        # "RelativeJEREC2",
        # "RelativeJERHF",
        # "RelativePtBB",
        # "RelativePtEC1",
        # "RelativePtEC2",
        # "RelativePtHF",
        # "RelativeBal",
        # "RelativeSample",
        # "RelativeFSR",
        # "RelativeStatFSR",
        # "RelativeStatEC",
        # "RelativeStatHF",
        # "PileUpDataMC",
        # "PileUpPtRef",
        # "PileUpPtBB",
        # "PileUpPtEC1",
        # "PileUpPtEC2",
        # "PileUpPtHF",
        # "PileUpMuZero",
        # "PileUpEnvelope",
        # "SubTotalPileUp",
        # "SubTotalRelative",
        # "SubTotalPt",
        # "SubTotalScale",
        # "SubTotalAbsolute",
        # "SubTotalMC",
        "Total",
        # "TotalNoFlavor",
        # "TotalNoTime",
        # "TotalNoFlavorNoTime",
        # "FlavorZJet",
        # "FlavorPhotonJet",
        # "FlavorPureGluon",
        # "FlavorPureQuark",
        # "FlavorPureCharm",
        # "FlavorPureBottom",
        # "TimeRunA",
        # "TimeRunB",
        # "TimeRunC",
        # "TimeRunD",
        "CorrelationGroupMPFInSitu",
        "CorrelationGroupIntercalibration",
        "CorrelationGroupbJES",
        "CorrelationGroupFlavor",
        "CorrelationGroupUncorrelated",
    ],
}))

config_2018.set_aux("jer", DotDict.wrap({
    "source": "https://raw.githubusercontent.com/cms-jet/JRDatabase/master/textFiles",
    "campaign": "Summer19UL18",
    "version": "JRV2",
    "jet_type": "AK4PFchs",
}))


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
for i, jec_source in enumerate(config_2018.x.jec["uncertainty_sources"]):
    config_2018.add_shift(name=f"jec_{jec_source}_up", id=5000 + 2 * i, type="shape")
    config_2018.add_shift(name=f"jec_{jec_source}_down", id=5001 + 2 * i, type="shape")
    add_aliases(f"jec_{jec_source}", {"Jet.pt": "Jet.pt_{name}", "Jet.mass": "Jet.mass_{name}"})

config_2018.add_shift(name="jer_up", id=600, type="shape")
config_2018.add_shift(name="jer_down", id=601, type="shape")
add_aliases("jer", {"Jet.pt": "Jet.pt_{name}", "Jet.mass": "Jet.mass_{name}"})

config_2018.add_shift(name="minbias_xs_up", id=7, type="shape")
config_2018.add_shift(name="minbias_xs_down", id=8, type="shape")
add_aliases("minbias_xs", {"pu_weight": "pu_weight_{name}"})
config_2018.add_shift(name="top_pt_up", id=9, type="shape")
config_2018.add_shift(name="top_pt_down", id=10, type="shape")
add_aliases("top_pt", {"top_pt_weight": "top_pt_weight_{direction}"})


def make_jme_filenames(jme_aux, sample_type, names, era=None):
    """Convenience function to compute paths to JEC files."""

    # normalize and validate sample type
    sample_type = sample_type.upper()
    if sample_type not in ("DATA", "MC"):
        raise ValueError(f"Invalid sample type '{sample_type}'. Expected either 'DATA' or 'MC'.")

    jme_full_version = "_".join(s for s in (jme_aux.campaign, era, jme_aux.version, sample_type) if s)

    return [
        f"{jme_aux.source}/{jme_full_version}/{jme_full_version}_{name}_{jme_aux.jet_type}.txt"
        for name in names
    ]


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

    # jet energy correction
    "jec": {
        "mc": [
            (fname, "v1")
            for fname in make_jme_filenames(config_2018.x.jec, "mc", names=config_2018.x.jec.levels)
        ],
        "data": {
            era: [
                (fname, "v1")
                for fname in make_jme_filenames(config_2018.x.jec, "data", names=config_2018.x.jec.levels, era=era)
            ]
            for era in config_2018.x.jec.data_eras
        },
    },

    # jec energy correction uncertainties
    "junc": {
        "mc": [(make_jme_filenames(config_2018.x.jec, "mc", names=["UncertaintySources"])[0], "v1")],
        "data": {
            era: [(make_jme_filenames(config_2018.x.jec, "data", names=["UncertaintySources"], era=era)[0], "v1")]
            for era in config_2018.x.jec.data_eras
        },
    },

    # jet energy resolution (pt resolution)
    "jer": {
        "mc": [(make_jme_filenames(config_2018.x.jer, "mc", names=["PtResolution"])[0], "v1")],
    },

    # jet energy resolution (data/mc scale factors)
    "jersf": {
        "mc": [(make_jme_filenames(config_2018.x.jer, "mc", names=["SF"])[0], "v1")],
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

# event weight columns
config_2018.set_aux("event_weights", ["normalization_weight", "pu_weight"])

# file merging values
# key -> dataset -> files per branch (-1 or not present = all)
# TODO: maybe add selector name as additional layer
config_2018.set_aux("file_merging", {
})

# versions per task family and optionally also dataset and shift
# None can be used as a key to define a default value
config_2018.set_aux("versions", {
})

# add categories
add_categories(config_2018)

# add variables
add_variables(config_2018)
