# coding: utf-8

"""
Definition of variables.
"""

import order as od

from ap.columnar_util import EMPTY_FLOAT


def add_variables(config: od.Config) -> None:
    """
    Adds all variables to a *config*.
    """
    config.add_variable(
        name="lhe_weight",
        expression="LHEWeight.originalXWGTUP",
        binning=(200, -10, 10),
        x_title="LHE weight",
    )
    config.add_variable(
        name="ht",
        binning=[0, 80, 120, 160, 200, 240, 280, 320, 400, 500, 600, 800],
        unit="GeV",
        x_title="HT",
    )
    config.add_variable(
        name="n_electron",
        binning=(6, -0.5, 5.5),
        x_title="Number of electrons",
    )
    config.add_variable(
        name="n_muon",
        binning=(6, -0.5, 5.5),
        x_title="Number of muons",
    )
    config.add_variable(
        name="electron1_pt",
        expression="Electron.pt[:,0]",
        null_value=EMPTY_FLOAT,
        binning=(40, 0., 400.),
        unit="GeV",
        x_title=r"Electron 1 $p_{T}$",
    )
    config.add_variable(
        name="muon1_pt",
        expression="Muon.pt[:,0]",
        null_value=EMPTY_FLOAT,
        binning=(40, 0., 400.),
        unit="GeV",
        x_title=r"Muon 1 $p_{T}$",
    )
    config.add_variable(
        name="n_jet",
        binning=(11, -0.5, 10.5),
        x_title="Number of jets",
    )
    config.add_variable(
        name="n_deepjet",
        binning=(8, -0.5, 7.5),
        x_title="Number of deepjets",
    )
    config.add_variable(
        name="jet1_pt",
        expression="Jet.pt[:,0]",
        null_value=EMPTY_FLOAT,
        binning=(40, 0., 400.),
        unit="GeV",
        x_title=r"Jet 1 $p_{T}$",
    )
    config.add_variable(
        name="jet2_pt",
        expression="Jet.pt[:,1]",
        null_value=EMPTY_FLOAT,
        binning=(40, 0., 400.),
        unit="GeV",
        x_title=r"Jet 2 $p_{T}$",
        log_y=True,
    )
    config.add_variable(
        name="jet3_pt",
        expression="Jet.pt[:,2]",
        null_value=EMPTY_FLOAT,
        binning=(40, 0., 400.),
        unit="GeV",
        x_title=r"Jet 3 $p_{T}$",
    )
    config.add_variable(
        name="jet1_eta",
        expression="Jet.eta[:,0]",
        null_value=EMPTY_FLOAT,
        binning=(50, -2.5, 2.5),
        x_title=r"Jet 1 $\eta$",
    )
    config.add_variable(
        name="jet2_eta",
        expression="Jet.eta[:,1]",
        null_value=EMPTY_FLOAT,
        binning=(50, -2.5, 2.5),
        x_title=r"Jet 2 $\eta$",
    )
    config.add_variable(
        name="jet3_eta",
        expression="Jet.eta[:,2]",
        null_value=EMPTY_FLOAT,
        binning=(50, -2.5, 2.5),
        x_title=r"Jet 3 $\eta$",
    )

    # cutflow variables
    config.add_variable(
        name="cf_ht",
        expression="cutflow.ht",
        binning=[0, 80, 120, 160, 200, 240, 280, 320, 400, 500, 600, 800],
        unit="GeV",
        x_title="HT",
    )
    config.add_variable(
        name="cf_n_jet",
        expression="cutflow.n_jet",
        binning=(11, -0.5, 10.5),
        x_title="Number of jets",
    )
    config.add_variable(
        name="cf_jet1_pt",
        expression="cutflow.jet1_pt",
        binning=(40, 0., 400.),
        unit="GeV",
        x_title=r"Jet 1 $p_{T}$",
    )
