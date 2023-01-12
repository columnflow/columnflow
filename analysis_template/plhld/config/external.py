# coding: utf-8

from collections import OrderedDict

import order as od

from columnflow.util import DotDict


def make_jme_filename(jme_aux, sample_type, name, era=None):
    """
    Convenience function to compute paths to JEC files.
    """
    # normalize and validate sample type
    sample_type = sample_type.upper()
    if sample_type not in ("DATA", "MC"):
        raise ValueError(f"invalid sample type '{sample_type}', expected either 'DATA' or 'MC'")

    jme_full_version = "_".join(s for s in (jme_aux.campaign, era, jme_aux.version, sample_type) if s)

    return f"{jme_aux.source}/{jme_full_version}/{jme_full_version}_{name}_{jme_aux.jet_type}.txt"


def add_external_files(config: od.Config) -> None:
    # external files
    config.x.external_files = DotDict.wrap({
        # files from TODO
        "lumi": {
            "golden": ("/afs/cern.ch/cms/CAF/CMSCOMM/COMM_DQM/certification/Collisions17/13TeV/Legacy_2017/Cert_294927-306462_13TeV_UL2017_Collisions17_GoldenJSON.txt", "v1"),  # noqa
            "normtag": ("/afs/cern.ch/user/l/lumipro/public/Normtags/normtag_PHYSICS.json", "v1"),
        },

        # files from
        # https://twiki.cern.ch/twiki/bin/viewauth/CMS/PileupJSONFileforData?rev=44#Pileup_JSON_Files_For_Run_II
        "pu": {
            "json": ("/afs/cern.ch/cms/CAF/CMSCOMM/COMM_DQM/certification/Collisions17/13TeV/PileUp/UltraLegacy/pileup_latest.txt", "v1"),  # noqa
            "mc_profile": ("https://raw.githubusercontent.com/cms-sw/cmssw/435f0b04c0e318c1036a6b95eb169181bbbe8344/SimGeneral/MixingModule/python/mix_2017_25ns_UltraLegacy_PoissonOOTPU_cfi.py", "v1"),  # noqa
            "data_profile": {
                "nominal": ("/afs/cern.ch/cms/CAF/CMSCOMM/COMM_DQM/certification/Collisions17/13TeV/PileUp/UltraLegacy/PileupHistogram-goldenJSON-13tev-2017-69200ub-99bins.root", "v1"),  # noqa
                "minbias_xs_up": ("/afs/cern.ch/cms/CAF/CMSCOMM/COMM_DQM/certification/Collisions17/13TeV/PileUp/UltraLegacy/PileupHistogram-goldenJSON-13tev-2017-72400ub-99bins.root", "v1"),  # noqa
                "minbias_xs_down": ("/afs/cern.ch/cms/CAF/CMSCOMM/COMM_DQM/certification/Collisions17/13TeV/PileUp/UltraLegacy/PileupHistogram-goldenJSON-13tev-2017-66000ub-99bins.root", "v1"),  # noqa
            },
        },

        # jet energy correction
        "jet_jerc": ("/afs/cern.ch/user/m/mrieger/public/mirrors/jsonpog-integration-f018adfb/POG/JME/2017_UL/jet_jerc.json.gz", "v1"),  # noqa

        # met phi corrector
        "met_phi_corr": ("/afs/cern.ch/user/m/mrieger/public/mirrors/jsonpog-integration-f018adfb/POG/JME/2017_UL/met.json.gz", "v1"),  # noqa

        # btag scale factor
        "btag_sf_corr": ("/afs/cern.ch/user/m/mrieger/public/mirrors/jsonpog-integration-f018adfb/POG/BTV/2017_UL/btagging.json.gz", "v1"),  # noqa

        # electron scale factors
        "electron_sf": ("/afs/cern.ch/user/m/mrieger/public/mirrors/jsonpog-integration-f018adfb/POG/EGM/2017_UL/electron.json.gz", "v1"),  # noqa

        # muon scale factors
        "muon_sf": ("/afs/cern.ch/user/m/mrieger/public/mirrors/jsonpog-integration-f018adfb/POG/MUO/2017_UL/muon_Z.json.gz", "v1"),  # noqa

    })
