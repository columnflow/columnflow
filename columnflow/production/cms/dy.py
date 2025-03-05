# coding: utf-8

"""
Column production methods related to Drell-Yan reweighting.
"""

from __future__ import annotations

import law

from dataclasses import dataclass

from columnflow.production import Producer, producer
from columnflow.util import maybe_import, InsertableDict
from columnflow.columnar_util import set_ak_column

np = maybe_import("numpy")
ak = maybe_import("awkward")
vector = maybe_import("vector")

logger = law.logger.get_logger(__name__)


@dataclass
class DrellYanConfig:
    era: str
    order: str
    correction: str
    unc_correction: str

    def __post_init__(self) -> None:
        if not self.era or not self.order or not self.correction or not self.unc_correction:
            raise ValueError("Incomplete dy_weight_config: missing era, order, correction or unc_correction.")  # noqa

@producer(
    uses={"GenPart.*"},
    produces={
        "gen_dilepton_pt",
        "gen_dilepton_vis.{pt,phi}",
        "gen_dilepton_all.{pt,phi}"
    },
)
def gen_dilepton(self, events: ak.Array, **kwargs) -> ak.Array:
    """
    Reconstruct the di-lepton pair from generator level info. This considers only visible final-state particles. In addition it provides the four-momenta of all leptons (including neutrinos) from the hard process.
    """

    # get the absolute pdg id (to account for anti-particles) and status of the particles
    pdg_id = abs(events.GenPart.pdgId)
    status = events.GenPart.status

    # lepton masks for DY ptll reweighting corrections
    # -> https://indico.cern.ch/event/1495537/contributions/6359516/attachments/3014424/5315938/HLepRare_25.02.14.pdf
    ele_mu_mask = (
        ((pdg_id == 11) | (pdg_id == 13)) &
        (status == 1) &
        events.GenPart.hasFlags("fromHardProcess")
    )
    # taus need to have status == 2,
    tau_mask = (
        (pdg_id == 15) &
        (status == 2) &
        events.GenPart.hasFlags("fromHardProcess")
    )

    # lepton masks for recoil corrections
    # -> https://indico.cern.ch/event/1495537/contributions/6359516/attachments/3014424/5315938/HLepRare_25.02.14.pdf
    lepton_all_mask = (
        # e, mu, taus, neutrinos
        ((pdg_id >= 11) & (pdg_id <= 16) & (status == 1) & events.GenPart.hasFlags("fromHardProcess")) |
        # tau decay products
        events.GenPart.hasFlags("isDirectHardProcessTauDecayProduct")
    )
    lepton_vis_mask = lepton_all_mask & (
        # no e neutrinos, mu neutrinos, or taus neutrinos
        (pdg_id != 12) & (pdg_id != 14)| (pdg_id != 16)
    )

    # combine the masks
    lepton_mask = ele_mu_mask | tau_mask
    lepton_pairs = events.GenPart[lepton_mask]
    lepton_pairs_vis = events.GenPart[lepton_vis_mask]
    lepton_pairs_all = events.GenPart[lepton_all_mask]

    # some up the four momenta of the leptons
    lepton_pair_momenta = lepton_pairs.sum(axis=-1)
    lepton_pair_momenta_vis = lepton_pairs_vis.sum(axis=-1)
    lepton_pair_momenta_all = lepton_pairs_all.sum(axis=-1)

    # finally, save the pt and phi of the lepton pair on generator level
    events = set_ak_column(events, "gen_dilepton_pt", lepton_pair_momenta.pt)
    events = set_ak_column(events, "gen_dilepton_vis.pt", lepton_pair_momenta_vis.pt)
    events = set_ak_column(events, "gen_dilepton_vis.phi", lepton_pair_momenta_vis.phi)
    events = set_ak_column(events, "gen_dilepton_all.pt", lepton_pair_momenta_all.pt)
    events = set_ak_column(events, "gen_dilepton_all.phi", lepton_pair_momenta_all.phi)

    return events

@producer(
    uses={"gen_dilepton_pt"},
    # weight variations are defined in init
    produces={"dy_weight"},
    # only run on mc
    mc_only=True,
    # function to determine the correction file
    get_dy_weight_file=(lambda self, external_files: external_files.dy_weight_sf),
    # function to load the config
    get_dy_weight_config=(lambda self: self.config_inst.x.dy_weight_config),
)
def dy_weights(self: Producer, events: ak.Array, **kwargs) -> ak.Array:
    """
    Creates Drell-Yan weights using the correctionlib.
    https://cms-higgs-leprare.docs.cern.ch/htt-common/DY_reweight/#correctionlib-file

    Requires an external file in the config under ``dy_weight_sf``:

    .. code-block:: python

        cfg.x.external_files = DotDict.wrap({
            "dy_weight_sf": "/afs/cern.ch/work/m/mrieger/public/mirrors/external_files/DY_pTll_weights_v1.json.gz",  # noqa
        })

    *get_dy_weight_file* can be adapted in a subclass in case it is stored differently in the external files.

    The campaign era and name of the correction set (see link above) should be given as an auxiliary entry in the config:

    .. code-block:: python

        cfg.x.dy_weight_config = DrellYanConfig(
            era="2022preEE_NLO",
            order="NLO",
            correction="DY_pTll_reweighting",
            unc_correction="DY_pTll_reweighting_N_uncertainty",
        )

    *get_dy_weight_config* can be adapted in a subclass in case it is stored differently in the config.
    """

    # map the input variable names from the corrector to our columns
    variable_map = {
        "era": self.dy_config.era,
        "order": self.dy_config.order,
        "ptll": events.gen_dilepton_pt,
    }

    # initializing the list of weight variations
    weights_list = [("dy_weight", "nom")]

    # appending the respective number of uncertainties to the weight list
    for i in range(self.n_unc):
        for shift in ("up", "down"):
            tmp_tuple = (f"dy_weight{i + 1}_{shift}", f"{shift}{i + 1}")
            weights_list.append(tmp_tuple)

    # preparing the input variables for the corrector
    for column_name, syst in weights_list:
        variable_map_syst = {**variable_map, "syst": syst}

        # evaluating dy weights given a certain era, ptll array and sytematic shift
        inputs = [variable_map_syst[inp.name] for inp in self.dy_corrector.inputs]
        dy_weight = self.dy_corrector.evaluate(*inputs)

        # save the weights in a new column
        events = set_ak_column(events, column_name, dy_weight, value_type=np.float32)

    return events


@dy_weights.init
def dy_weights_init(self: Producer) -> None:
    # the number of weights in partial run 3 is always 10
    if self.config_inst.campaign.x.year not in (2022, 2023):
        raise NotImplementedError(
            f"campaign year {self.config_inst.campaign.x.year} is not yet supported by {self.cls_name}",
        )
    self.n_unc = 10

    # register dynamically produced weight columns
    for i in range(self.n_unc):
        self.produces.add(f"dy_weight{i + 1}_{{up,down}}")


@dy_weights.requires
def dy_weights_requires(self: Producer, reqs: dict) -> None:
    """
    Adds the requirements needed the underlying task to derive the Drell-Yan weights into *reqs*.
    """
    if "external_files" in reqs:
        return

    from columnflow.tasks.external import BundleExternalFiles
    reqs["external_files"] = BundleExternalFiles.req(self.task)


@dy_weights.setup
def dy_weights_setup(
    self: Producer,
    reqs: dict,
    inputs: dict,
    reader_targets: InsertableDict,
) -> None:
    """
    Loads the Drell-Yan weight calculator from the external files bundle and saves them in the
    py:attr:`dy_corrector` attribute for simpler access in the actual callable. The number of uncertainties
    is calculated, per era, by another correcter in the external file and is saved in the
    py:attr:`dy_unc_corrector` attribute.
    """
    bundle = reqs["external_files"]

    # import all correctors from the external file
    import correctionlib
    correctionlib.highlevel.Correction.__call__ = correctionlib.highlevel.Correction.evaluate
    correction_set = correctionlib.CorrectionSet.from_string(
        self.get_dy_weight_file(bundle.files).load(formatter="gzip").decode("utf-8"),
    )

    # check number of fetched correctors
    if len(correction_set.keys()) != 2:
        raise Exception("Expected exactly two types of Drell-Yan correction")

    # create the weight and uncertainty correctors
    self.dy_config: DrellYanConfig = self.get_dy_weight_config()
    self.dy_corrector = correction_set[self.dy_config.correction]
    self.dy_unc_corrector = correction_set[self.dy_config.unc_correction]

    dy_n_unc = int(self.dy_unc_corrector.evaluate(self.dy_config.order))

    if dy_n_unc != self.n_unc:
        raise ValueError(
            f"Expected {self.n_unc} uncertainties, got {dy_n_unc}",
        )

#
# Helper functions for kinematics.
#
def GetXYfromPTPHI(pt, phi):
    """Convert (pt, phi) to (x, y)."""
    return pt * np.cos(phi), pt * np.sin(phi)

def GetPTPHIfromXY(x, y):
    """Convert (x, y) to (pt, phi)."""
    pt = np.sqrt(x * x + y * y)
    phi = np.arctan2(y, x)
    return pt, phi

def GetU(met_pt, met_phi, full_pt, full_phi, vis_pt, vis_phi):
    """
    Compute the recoil vector U:
      U = MET + (visible boson) – (full boson)
    and then project U into components parallel (Upara) and perpendicular (Uperp)
    to the full boson direction.
    """
    met_x, met_y = GetXYfromPTPHI(met_pt, met_phi)
    full_x, full_y = GetXYfromPTPHI(full_pt, full_phi)
    vis_x, vis_y = GetXYfromPTPHI(vis_pt, vis_phi)
    Ux = met_x + vis_x - full_x
    Uy = met_y + vis_y - full_y
    U_pt, U_phi = GetPTPHIfromXY(Ux, Uy)
    # Projection along and perpendicular to full boson phi:
    Upara = U_pt * np.cos(U_phi - full_phi)
    Uperp = U_pt * np.sin(U_phi - full_phi)
    return Upara, Uperp

def GetMETfromU(upara, uperp, full_pt, full_phi, vis_pt, vis_phi):
    """
    Reconstruct MET from the corrected U components.
    """
    U_pt = np.sqrt(upara * upara + uperp * uperp)
    # Recover U_phi (note the rotation by full_phi)
    U_phi = np.arctan2(uperp, upara) + full_phi
    Ux, Uy = GetXYfromPTPHI(U_pt, U_phi)
    full_x, full_y = GetXYfromPTPHI(full_pt, full_phi)
    vis_x, vis_y = GetXYfromPTPHI(vis_pt, vis_phi)
    met_x = Ux - vis_x + full_x
    met_y = Uy - vis_y + full_y
    met_pt, met_phi = GetPTPHIfromXY(met_x, met_y)
    return met_pt, met_phi

def GetH(METpt, METphi, FullVPt, FullVPhi, VisVPt, VisVPhi):
    METx,METy = GetXYfromPTPHI(METpt,METphi)
    VisVx,VisVy = GetXYfromPTPHI(VisVPt,VisVPhi)
    Hx = -METx - VisVx
    Hy = -METy - VisVy
    Hpt,Hphi = GetPTPHIfromXY(Hx,Hy)
    Hpara,Hperp = GetXYfromPTPHI(Hpt, Hphi-FullVPhi)
    return Hpara, Hperp

def GetMETfromH(Hpara, Hperp, FullVPt, FullVPhi, VisVPt, VisVPhi):
    VisVx,VisVy = GetXYfromPTPHI(VisVPt,VisVPhi)
    Hpt,HphiMinusFullVPhi = GetPTPHIfromXY(Hpara,Hperp)
    Hphi = HphiMinusFullVPhi + FullVPhi
    Hx,Hy = GetXYfromPTPHI(Hpt,Hphi)
    METx = -Hx - VisVx
    METy = -Hy - VisVy
    METpt,METphi = GetPTPHIfromXY(METx,METy)
    return METpt, METphi


@producer(
    uses={
        # MET information
        # -> only Run 3 (PuppiMET) is supported
        "PuppiMET.{pt,phi}",
        # Number of jets (as a per-event scalar)
        "Jet.{pt,phi,eta,mass}",
        # Gen-level boson information (full boson momentum)
        # -> gen_dilepton_vis.pt, gen_dilepton_vis.phi, gen_dilepton_all.pt, gen_dilepton_all.phi
        gen_dilepton.PRODUCES,
    },
    produces={
        "RecoilCorrMET.{pt,phi}",
    },
    mc_only=True,
    # function to determine the recoil correction file from external files
    get_dy_recoil_file=(lambda self, external_files: external_files.dy_recoil_sf),
    # function to load the config
    get_dy_recoil_config=(lambda self: self.config_inst.x.dy_recoil_config),
)
def recoil_corrections(self: Producer, events: ak.Array, **kwargs) -> ak.Array:
    """
    Producer for bosonic recoil corrections.

    Steps:
      1) Compute the recoil vector U (Upara, Uperp) from the PuppiMET and the boson information.
      2) Apply the nominal recoil correction to recoil vector U.
      3) Recompute the corrected MET from the corrected U components.
      4) Compute systematic variations (Recoil uncertainties) by recalculating MET variations using the
         Recoil_correction_Uncertainty correction.
    """
    # Retrieve inputs as numpy arrays.
    met_pt    = np.asarray(events.PuppiMET.pt)
    met_phi   = np.asarray(events.PuppiMET.phi)

    full_pt   = np.asarray(events.gen_dilepton_all.pt)
    full_phi  = np.asarray(events.gen_dilepton_all.phi)

    vis_pt    = np.asarray(events.gen_dilepton_vis.pt)
    vis_phi   = np.asarray(events.gen_dilepton_vis.phi)

    # nJet is expected to be a per-event scalar; convert to float for the correction functions.
    # number of of jets with pT>30 GeV at |eta|<2.5, plus jets with pT>50 GeV outside of tracker region
    jet_selection = (
        ((events.Jet.pt > 30) & (np.abs(events.Jet.eta) < 2.5)) |
        ((events.Jet.pt > 50) & (np.abs(events.Jet.eta) >= 2.5))
    )
    selected_jets = events.Jet[jet_selection]
    njet = np.asarray(ak.num(selected_jets, axis=1), dtype=np.float32)

    #-------------------------------------------------------------------------
    # Nominal recoil correction:
    # (see here: https://cms-higgs-leprare.docs.cern.ch/htt-common/V_recoil/#example-snippet)
    # 1) Compute Upara and Uperp from the original MET and boson information.
    upara, uperp = GetU(met_pt, met_phi, full_pt, full_phi, vis_pt, vis_phi)

    # 2) Apply the nominal recoil correction using the QuantileMapHist method.
    #    Correction function signature:
    #      (era: str, njet: float, ptll: float, var: "Upara"/"Uperp", value: real)
    upara_corr = self.recoil_corrector.evaluate(self.dy_recoil_config.era, self.dy_recoil_config.order, njet, events.gen_dilepton_all.pt, "Upara", upara)
    uperp_corr = self.recoil_corrector.evaluate(self.dy_recoil_config.era, self.dy_recoil_config.order, njet, events.gen_dilepton_all.pt, "Uperp", uperp)

    # 3) Recompute the corrected MET from the corrected U components.
    met_pt_corr, met_phi_corr = GetMETfromU(upara_corr, uperp_corr, full_pt, full_phi, vis_pt, vis_phi)
    events = set_ak_column(events, "RecoilCorrMET.pt", met_pt_corr, value_type=np.float32)
    events = set_ak_column(events, "RecoilCorrMET.phi", met_phi_corr, value_type=np.float32)

    #-------------------------------------------------------------------------
    # Recoil uncertainty variations:
    # First, derive H components from the nominal corrected MET.
    hpara, hperp = GetH(met_pt_corr, met_phi_corr, full_pt, full_phi, vis_pt, vis_phi)

    # Loop over systematic variations.
    for syst in self.systematics:
        # The recoil uncertainty correction for H components expects:
        #   (era: str, njet: float, ptll: float, var: "Hpara"/"Hperp", value: real, syst: string)
        hpara_var = self.recoil_unc_corrector.evaluate(self.dy_recoil_config.era, self.dy_recoil_config.order, njet, events.gen_dilepton_all.pt, "Hpara", hpara, syst)
        hperp_var = self.recoil_unc_corrector.evaluate(self.dy_recoil_config.era, self.dy_recoil_config.order, njet, events.gen_dilepton_all.pt, "Hperp", hperp, syst)
        met_pt_var, met_phi_var = GetMETfromH(hpara_var, hperp_var, full_pt, full_phi, vis_pt, vis_phi)
        events = set_ak_column(events, f"RecoilCorrMET.pt_{syst}", met_pt_var, value_type=np.float32)
        events = set_ak_column(events, f"RecoilCorrMET.phi_{syst}", met_phi_var, value_type=np.float32)

    return events

@recoil_corrections.init
def recoil_corrections_init(self: Producer) -> None:
    # the number of weights in partial run 3 is always 10
    if self.config_inst.campaign.x.year not in (2022, 2023):
        raise NotImplementedError(
            f"campaign year {self.config_inst.campaign.x.year} is not yet supported by {self.cls_name}",
        )

    self.systematics = ["RespUp", "RespDown", "ResolUp", "ResolDown"]
    # register dynamically systematics
    for syst in self.systematics:
        self.produces.add(f"RecoilCorrMET.pt_{syst}")
        self.produces.add(f"RecoilCorrMET.phi_{syst}")

@recoil_corrections.requires
def recoil_corrections_requires(self: Producer, reqs: dict) -> None:
    # Ensure that external files are bundled.
    if "external_files" in reqs:
        return
    from columnflow.tasks.external import BundleExternalFiles
    reqs["external_files"] = BundleExternalFiles.req(self.task)


@recoil_corrections.setup
def recoil_corrections_setup(self: Producer, reqs: dict, inputs: dict, reader_targets: InsertableDict) -> None:
    """
    Setup the recoil corrections by loading the CorrectionSet via correctionlib.
    The external recoil correction file should be provided as external_files.recoil.
    """
    bundle = reqs["external_files"]

    import correctionlib
    correctionlib.highlevel.Correction.__call__ = correctionlib.highlevel.Correction.evaluate

    # Load the correction set from the external file.
    correction_set = correctionlib.CorrectionSet.from_string(
        self.get_dy_recoil_file(bundle.files).load(formatter="gzip").decode("utf-8")
    )

    # check number of fetched correctors
    if len(correction_set.keys()) != 4:
        raise Exception("Expected exactly four types of bosonic recoil corrections")

    # Retrieve the corrections used for the nominal correction and for uncertainties.
    self.dy_recoil_config: DrellYanConfig = self.get_dy_recoil_config()
    self.recoil_corrector = correction_set[self.dy_recoil_config.correction]
    self.recoil_unc_corrector  = correction_set[self.dy_recoil_config.unc_correction]
