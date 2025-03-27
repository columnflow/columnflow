# coding: utf-8

"""
Definition of b-tagging settings.
"""

from columnflow.columnar_util import EMPTY_FLOAT

import order as od

from columnflow.production.cms.btag import BTagSFConfig


def add_btagging_settings(config: od.Config) -> None:

    # b-tagging uncertainties
    btagging_uncertainty_sources = [
        "correlated",
        "uncorrelated",
    ]

    # b-tagging configuration
    config.x.btag_sf = BTagSFConfig(
        correction_set="deepJet",
        sources=btagging_uncertainty_sources,
        jec_sources=config.x.btag_sf_jec_sources,
    )

    # config.x.external_files.btag_sf_corr is configered in ... TODO

    # define the variables for the efficiency binning
    config.x.default_btag_variables = ("btag_jet_pt", "btag_jet_eta")
    config.add_variable(
        name="btag_jet_pt",
        expression="Jet.pt",
        null_value=EMPTY_FLOAT,
        # recommendation by BTV (https://btv-wiki.docs.cern.ch/PerformanceCalibration/fixedWPSFRecommendations/)
        binning=[20, 50, 70, 100, 140, 200, 400, 1000],
        unit="GeV",
        log_x=True,
        x_title=r"Jet $p_{T}$",
    )
    config.add_variable(
        name="btag_jet_eta",
        expression=lambda events: abs(events.Jet.eta),
        null_value=EMPTY_FLOAT,
        binning=[0, 1.5, 2.5],  # TODO
        x_title=r"Jet $\eta$",
        aux=dict(inputs=["Jet.eta"]),
    )

    # Datasets to combine for efficiency calculation
    config.x.btag_dataset_groups = {
        "tt": ["tt_dl_powheg", "tt_sl_powheg"],
        "dy": [dataset.name for dataset in config.datasets if dataset.name.startswith("dy")],
    }

    return
