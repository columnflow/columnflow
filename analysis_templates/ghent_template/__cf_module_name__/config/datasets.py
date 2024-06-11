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
            *[f"data_mumu_{era}" for era in ['a', 'b', 'c', 'd']],
            *[f"data_egamma_{era}" for era in ['a', 'b', 'c', 'd']],
            *[f"data_muoneg_{era}" for era in ['a', 'b', 'c', 'd']],
            *[f"data_mu_{era}" for era in ['a', 'b', 'c', 'd']],

            # backgrounds

            # ewk
            *[f"dy_lept_m50_ht-{htr}_madgraph" for htr in ['100to200', '200to400', '400to600',
                                                           '600to800', '800to1200', '1200to2500']],

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
