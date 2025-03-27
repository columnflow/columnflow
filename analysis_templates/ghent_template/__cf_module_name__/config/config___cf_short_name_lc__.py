# coding: utf-8

"""
Configuration of the __cf_short_name_lc__ analysis.
"""
from __future__ import annotations

import order as od
from scinum import Number

from columnflow.util import DotDict, maybe_import, four_vec
from columnflow.config_util import (
    verify_config_processes,
)

from __cf_short_name_lc__.config.styling import stylize_processes
from __cf_short_name_lc__.config.datasets import add_datasets, configure_datasets
from __cf_short_name_lc__.config.processes import add_processes
from __cf_short_name_lc__.config.settings.settings import add_settings
from __cf_short_name_lc__.config.veto import add_vetoes
from __cf_short_name_lc__.config.categories import add_categories_selection
from __cf_short_name_lc__.config.variables import add_variables
from __cf_short_name_lc__.config.shifts import add_shifts
from __cf_short_name_lc__.config.groups import add_groups

from __cf_short_name_lc__.selection.trigger import add_triggers

ak = maybe_import("awkward")


def add_config(
    analysis: od.Analysis,
    campaign: od.Campaign,
    config_name: str | None = None,
    config_id: int | None = None,
    limit_dataset_files: int | None = None,
) -> od.Config:
    # validations
    year = campaign.x.year
    assert year in [2016, 2017, 2018]  # only run 2 implemented
    if year == 2016:
        assert campaign.x.vfp in ["pre", "post"]

    # only 2018 fully implemented
    if year != 2018:
        raise NotImplementedError("For now, only 2018 campaign is fully implemented")

    cfg = analysis.add_config(campaign, name=config_name, id=config_id, tags=analysis.tags)

    year2 = year % 100
    corr_postfix = f"{campaign.x.vfp}VFP" if year == 2016 else ""
    ecm = campaign.ecm

    cfg.x.year = year
    cfg.x.year2 = year2
    cfg.x.corr_postfix = corr_postfix
    cfg.x.ecm = ecm

    add_processes(cfg, campaign)

    add_triggers(cfg, campaign)
    add_datasets(cfg, campaign)
    add_settings(cfg)
    add_vetoes(cfg)
    configure_datasets(cfg, limit_dataset_files)

    # verify that the root process of all datasets is part of any of the registered processes
    verify_config_processes(cfg, warn=True)

    # lumi values in inverse pb
    # https://twiki.cern.ch/twiki/bin/view/CMS/LumiRecommendationsRun2?rev=2#Combination_and_correlations
    if year == 2016:
        cfg.x.luminosity = Number(36310, {
            "lumi_13TeV_2016": 0.01j,
            "lumi_13TeV_correlated": 0.006j,
        })
    elif year == 2017:
        cfg.x.luminosity = Number(41480, {
            "lumi_13TeV_2017": 0.02j,
            "lumi_13TeV_1718": 0.006j,
            "lumi_13TeV_correlated": 0.009j,
        })
    elif year == 2018:  # 2018
        cfg.x.luminosity = Number(59830, {
            "lumi_13TeV_2018": 0.015j,
            "lumi_13TeV_1718": 0.002j,
            "lumi_13TeV_correlated": 0.02j,
        })

    cfg.x.minbias_xs = Number(69.2, 0.046j)

    # external files
    json_mirror = "/cvmfs/cms.cern.ch/rsync/cms-nanoAOD/jsonpog-integration"
    year_short = str(year)[2:]  # 20XX > XX
    lumi_cert_site = f"https://cms-service-dqmdc.web.cern.ch/CAF/certification/Collisions{year_short}/{ecm:g}TeV"

    goldenjsons = {
        2016: "Cert_271036-284044_13TeV_Legacy2016_Collisions16_JSON.txt",
        2017: "Cert_294927-306462_13TeV_UL2017_Collisions17_GoldenJSON.txt",
        2018: "Cert_314472-325175_13TeV_Legacy2018_Collisions18_JSON.txt",
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

        # Pile up scale factor
        "pu_sf": (f"{json_mirror}/POG/LUM/{year}{corr_postfix}_UL/puWeights.json.gz", "v1")
    })

    # shift groups for conveniently looping over certain shifts
    # (used during plotting)
    cfg.x.event_weights = DotDict()
    cfg.x.event_weights["normalization_weight"] = []
    add_shifts(cfg)
    add_groups(cfg)

    # selector step groups for conveniently looping over certain steps
    # (used in cutflow tasks)
    cfg.x.selector_step_groups = {}

    # custom method and sandbox for determining dataset lfns
    cfg.x.get_dataset_lfns = None
    cfg.x.get_dataset_lfns_sandbox = None

    # whether to validate the number of obtained LFNs in GetDatasetLFNs
    # (currently set to false because the number of files per dataset is truncated to 2)
    cfg.x.validate_dataset_lfns = False

    # columns to keep after certain steps
    cfg.x.keep_columns = DotDict.wrap({
        "cf.MergeSelectionMasks": {
            "mc_weight", "normalization_weight", "process_id", "category_ids", "cutflow.*",
        },
    })

    cfg.x.keep_columns["cf.ReduceEvents"] = (
        {
            # general event information
            "run", "luminosityBlock", "event",
            # columns added during selection, required in general
            "mc_weight", "PV.npvs", "process_id", "category_ids", "deterministic_seed",
            # weight-related columns
            "pu_weight*", "pdf_weight*", "isr_weight*", "fsr_weight*",
            "murf_envelope_weight*", "mur_weight*", "muf_weight*",
            "btag_weight*",
            # extra columns
        } | four_vec(  # Jets
            {"Jet"},
            {"btagDeepFlavB", "btagDeepFlavCvB"},
        ) | four_vec(  # Leptons
            {"Electron", "Muon", },
            {"deltaEtaSC"},
        )
    )

    cfg.x.default_calibrator = "skip_jecunc"  # skip jet energy correction up and down variation to save time in running
    cfg.x.default_selector = "default"
    cfg.x.default_producer = "default"
    cfg.x.default_ml_model = None
    cfg.x.default_inference_model = "example"
    cfg.x.default_variables = ("n_jet",)
    cfg.x.default_categories = ("incl",)

    add_categories_selection(cfg)
    add_variables(cfg)
    stylize_processes(cfg)

    return cfg
