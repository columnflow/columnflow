# coding: utf-8

"""
Dataset configuration for the Ghent Columnflow analysis template (based on tt).
"""

from __future__ import annotations

import law
import order as od
import cmsdb.processes as procs
from columnflow.tasks.external import GetDatasetLFNs
# import cmsdb.processes as procs


logger = law.logger.get_logger(__name__)


def add_datasets(config: od.Config, campaign: od.Campaign):

    # add datasets we need to study
    dataset_names = {
        "2018": [
            # data
            "data_mumu_a",
            "data_mumu_b",
            "data_mumu_c",
            "data_mumu_d",

            "data_egamma_a",
            "data_egamma_b",
            "data_egamma_c",
            "data_egamma_d",

            "data_muoneg_a",
            "data_muoneg_b",
            "data_muoneg_c",
            "data_muoneg_d",

            "data_mu_a",
            "data_mu_b",
            "data_mu_c",
            "data_mu_d",

            # backgrounds

            # ewk
            "dy_lept_m50_ht-100to200_madgraph",
            "dy_lept_m50_ht-200to400_madgraph",
            "dy_lept_m50_ht-400to600_madgraph",
            "dy_lept_m50_ht-600to800_madgraph",
            "dy_lept_m50_ht-800to1200_madgraph",
            "dy_lept_m50_ht-1200to2500_madgraph",

            # ttbar

            "tt_dl_powheg",
            "tt_sl_powheg"
        ]}[f"{config.x.year}{config.x.corr_postfix}"]

    # loop over all dataset names and add them to the config
    for dataset_name in dataset_names:
        config.add_dataset(campaign.get_dataset(dataset_name))


def configure_datasets(config: od.Config, limit_dataset_files: int | None = None):

    for dataset in config.datasets:
        if limit_dataset_files:
            # apply optional limit on the max. number of files per dataset
            for info in dataset.info.values():
                if info.n_files > limit_dataset_files:
                    info.n_files = limit_dataset_files

        # adding tag info to datasets for data double counting removal
        if dataset.name.startswith("data_egamma"):
            dataset.add_tag("EGamma")
        elif dataset.name.startswith("data_mumu"):
            dataset.add_tag("DoubleMuon")
        elif dataset.name.startswith("data_mu_"):
            dataset.add_tag("SingleMuon")
        elif dataset.name.startswith("data_muoneg"):
            dataset.add_tag("MuonEG")

        # for each dataset, select which triggers to require
        # (and which to veto to avoid double counting events
        # in recorded data)
        if dataset.is_data:
            prev_triggers = set()
            for tag, triggers in config.x.trigger_matrix:
                if dataset.has_tag(tag):
                    dataset.x.require_triggers = triggers
                    dataset.x.veto_triggers = prev_triggers
                    break
                prev_triggers = prev_triggers | triggers

        elif dataset.is_mc:
            dataset.x.require_triggers = config.x.all_triggers

        # add more tag info to datasets
        if dataset.name.startswith(("t")):
            dataset.x.has_top = True
            dataset.add_tag("has_top")

        # example of removing scale, pdf variations for a specific dataset
        if dataset.name.startswith(("GluGLuToContinToZZ")):
            dataset.add_tag("skip_scale")
            dataset.add_tag("skip_pdf")
