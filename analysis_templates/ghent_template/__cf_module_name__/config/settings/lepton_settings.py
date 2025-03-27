# coding: utf-8

"""
Definition of Lepton settings.
"""

import order as od

from columnflow.util import maybe_import
from columnflow.production.cmsGhent.lepton import ElectronBaseWeightConfig, MuonBaseWeightConfig

ak = maybe_import("awkward")


def add_lepton_settings(config: od.Config) -> None:

    #
    # config for electron
    #

    ElectronRecoBelow20WeightConfig = ElectronBaseWeightConfig.copy(
        year=f"{config.x.year}{config.x.corr_postfix}",
        correction_set="UL-Electron-ID-SF",
        weight_name="electron_weight_recobelow20",
        input_pars=dict(WorkingPoint="RecoBelow20"),
    )

    @ElectronRecoBelow20WeightConfig.mask(uses={"Electron.pt"})
    def electron_recobelow20_mask(events):
        return events.Electron.pt < 20

    ElectronRecoAbove20WeightConfig = ElectronRecoBelow20WeightConfig.copy(
        weight_name="electron_weight_recoabove20",
        input_pars=dict(WorkingPoint="RecoAbove20"),
    )

    @ElectronRecoAbove20WeightConfig.mask(uses={"Electron.pt"})
    def electron_recoabove20_mask(events):
        return (events.Electron.pt > 20)

    ElectronWP90IsoWeightConfig = ElectronRecoBelow20WeightConfig.copy(
        weight_name="electron_weight_wp90iso",
        input_pars=dict(WorkingPoint="wp90iso"),
    )

    #
    # config for muon
    #

    MuonMediumIDWeightConfig = MuonBaseWeightConfig.copy(
        year=f"{config.x.year}{config.x.corr_postfix}",
        correction_set="NUM_MediumID_DEN_TrackerMuons",
        weight_name="muon_weight_mediumid",
    )

    @MuonMediumIDWeightConfig.mask(uses={"Muon.pt"})
    def muon_mask(events):
        return (events.Muon.pt > 15)

    MuonLooseRelIsoWeightConfig = MuonMediumIDWeightConfig.copy(
        correction_set="NUM_LooseRelIso_DEN_MediumID",
        weight_name="muon_weight_loosereliso",
    )

    config.x.lepton_weight_configs = [
        ElectronRecoBelow20WeightConfig,
        ElectronRecoAbove20WeightConfig,
        ElectronWP90IsoWeightConfig,
        MuonMediumIDWeightConfig,
        MuonLooseRelIsoWeightConfig,
    ]

    return
