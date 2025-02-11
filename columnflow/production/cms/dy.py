# coding: utf-8

"""
Column production methods related to Drell-Yan reweighting and corrections of bosonic recoil.
"""

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
    correction: str
    unc_correction: str

    def __post_init__(self) -> None:
        if not self.era or not self.correction or not self.unc_correction:
            raise ValueError("Campaign era, correction  and unc_correction must be set")


@producer(
    uses={"GenPart.*"},
    produces={"gen_dilepton_vis.{pt,phi}", "gen_dilepton_all.{pt,phi}"},
)
def get_gen_dilepton(self, events: ak.Array, **kwargs) -> ak.Array:
    """
    Reconstruct the di-lepton pair from generator level info.
    """

    # get the absolute pdg id (to account for anti-particles) and status of the particles
    pdg_id = abs(events.GenPart.pdgId)
    status = events.GenPart.status

    # electrons and muons need to have status == 1, taus need to have status == 2,
    lepton_vis_mask = (
        # e, mu
        (((pdg_id == 11) | (pdg_id == 13)) & (status == 1)) |
        # tau
        ((pdg_id == 15) & (status == 2))
    )
    # also include neutrinos
    lepton_all_mask = (
        # e, mu
        (((pdg_id == 11) | (pdg_id == 13)) & (status == 1)) |
        # tau
        ((pdg_id == 15) & (status == 2)) |
        # neutrinos
        ((pdg_id == 12) | (pdg_id == 14) | (pdg_id == 16)) & (status == 1)
    )
    # only consider leptons from hard process (i.e. from the matrix element)
    mask_vis = ak.mask(events.GenPart, lepton_vis_mask).hasFlags("fromHardProcess")
    mask_all = ak.mask(events.GenPart, lepton_all_mask).hasFlags("fromHardProcess")

    # fill the mask with False if it is None and extract the gen leptons
    mask_vis = ak.fill_none(mask_vis, False)
    mask_all = ak.fill_none(mask_all, False)
    lepton_pairs_vis = events.GenPart[mask_vis]
    lepton_pairs_all = events.GenPart[mask_all]

    # some up the four momenta of the leptons
    lepton_pair_momenta_vis = lepton_pairs_vis.sum(axis=-1)
    lepton_pair_momenta_all = lepton_pairs_all.sum(axis=-1)

    # finally, save the pt and phi of the lepton pair on generator level
    events = set_ak_column(events, "gen_dilepton_vis.pt", lepton_pair_momenta_vis.pt)
    events = set_ak_column(events, "gen_dilepton_vis.phi", lepton_pair_momenta_vis.phi)
    events = set_ak_column(events, "gen_dilepton_all.pt", lepton_pair_momenta_all.pt)
    events = set_ak_column(events, "gen_dilepton_all.phi", lepton_pair_momenta_all.phi)
    return events


@producer(
    uses={get_gen_dilepton.PRODUCES},
    produces={"dy_weight"},
    # only run on mc
    mc_only=True,
    # function to determine the correction file
    get_dy_file=(lambda self, external_files: external_files.dy_sf),
    # function to load the config
    get_dy_config=(lambda self: self.config_inst.x.dy_config),
)
def dy_weights(self: Producer, events: ak.Array, **kwargs) -> ak.Array:
    """
    Creates Drell-Yan weights using the correctionlib. Requires an external file in the config under
    ``dy_sf``:

    .. code-block:: python

        cfg.x.external_files = DotDict.wrap({
            "dy_sf": "/afs/cern.ch/work/m/mrieger/public/mirrors/external_files/DY_pTll_weights_v1.json.gz",  # noqa
        })

    *get_dy_file* can be adapted in a subclass in case it is stored differently in the external files.

    The campaign era and name of the correction set should be given as an auxiliary entry in the config:

    .. code-block:: python

        cfg.x.dy_config = DrellYanConfig(
            era="2022preEE_NLO",
            correction="DY_pTll_reweighting",
            unc_correction="DY_pTll_reweighting_N_uncertainty",
        )

    *get_dy_config* can be adapted in a subclass in case it is stored differently in the config.
    """

    # map the input variable names from the corrector to our columns
    variable_map = {
        "era": self.dy_config.era,
        "ptll": events.gen_dilepton_vis.pt,
    }

    # initializing the list of weight variations
    weights_list = [("dy_weight", "nom")]

    # determining the number of uncertainties (dependent on the era)
    inputs_unc = [variable_map[inp.name] for inp in self.dy_unc_corrector.inputs]
    dy_n_unc = int(self.dy_unc_corrector.evaluate(*inputs_unc))

    # appending the respective number of uncertainties to the weight list
    for i in range(1, dy_n_unc + 1):
        for shift in ("up", "down"):
            tmp_tuple = (f"dy_weight_{shift}{i}", f"{shift}{i}")
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
        self.get_dy_file(bundle.files).load(formatter="gzip").decode("utf-8"),
    )

    # check number of fetched correctors
    if len(correction_set.keys()) != 2:
        raise Exception("Expected exactly two types of Drell-Yan correction")

    # create the weight and uncertainty correctors
    self.dy_config: DrellYanConfig = self.get_dy_config()
    self.dy_corrector = correction_set[self.dy_config.correction]
    self.dy_unc_corrector = correction_set[self.dy_config.unc_correction]

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
  return Hpara,Hperp

def GetMETfromH(Hpara, Hperp, FullVPt, FullVPhi, VisVPt, VisVPhi):
  VisVx,VisVy = GetXYfromPTPHI(VisVPt,VisVPhi)
  Hpt,HphiMinusFullVPhi = GetPTPHIfromXY(Hpara,Hperp)
  Hphi = HphiMinusFullVPhi + FullVPhi
  Hx,Hy = GetXYfromPTPHI(Hpt,Hphi)
  METx = -Hx - VisVx
  METy = -Hy - VisVy
  METpt,METphi = GetPTPHIfromXY(METx,METy)
  return METpt,METphi


#
# The recoil corrections producer.
#
@producer(
    uses={
        # PuppiMET information
        "PuppiMET.{pt,phi}",
        # Number of jets (as a per-event scalar)
        "Jet.{pt,phi,eta,mass}",
        # Gen-level boson information (full boson momentum)
        # -> gen_dilepton_vis.pt, gen_dilepton_vis.phi, gen_dilepton_all.pt, gen_dilepton_all.phi
        get_gen_dilepton.PRODUCES,
    },
    produces={
        "PuppiMET.pt_recoil", "PuppiMET.phi_recoil",
        # TODO: figure out how to better provide outputs in style of columnflow
        "PuppiMET.pt_recoil_RespUp", "PuppiMET.phi_recoil_RespUp",
        "PuppiMET.pt_recoil_RespDown", "PuppiMET.phi_recoil_RespDown",
        "PuppiMET.pt_recoil_ResolUp", "PuppiMET.phi_recoil_ResolUp",
        "PuppiMET.pt_recoil_ResolDown", "PuppiMET.phi_recoil_ResolDown",
    },
    mc_only=True,
    # function to determine the recoil correction file from external files
    get_dy_recoil_file=(lambda self, external_files: external_files.dy_recoil),
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
    upara_corr = self.recoil_corrector.evaluate(self.dy_recoil_config.era, njet, events.gen_dilepton_all.pt, "Upara", upara)
    uperp_corr = self.recoil_corrector.evaluate(self.dy_recoil_config.era, njet, events.gen_dilepton_all.pt, "Uperp", uperp)

    # 3) Recompute the corrected MET from the corrected U components.
    met_pt_corr, met_phi_corr = GetMETfromU(upara_corr, uperp_corr, full_pt, full_phi, vis_pt, vis_phi)
    events = set_ak_column(events, "PuppiMET.pt_recoil", met_pt_corr, value_type=np.float32)
    events = set_ak_column(events, "PuppiMET.phi_recoil", met_phi_corr, value_type=np.float32)

    #-------------------------------------------------------------------------
    # Recoil uncertainty variations:
    # First, derive H components from the nominal corrected MET.
    hpara, hperp = GetH(met_pt_corr, met_phi_corr, full_pt, full_phi, vis_pt, vis_phi)

    # TODO: figure out how to store variations only for nominal event or more in line
    #       with columnflow philosophy
    # Loop over systematic variations.
    for syst in ["RespUp", "RespDown", "ResolUp", "ResolDown"]:
        # The recoil uncertainty correction for H components expects:
        #   (era: str, njet: float, ptll: float, var: "Hpara"/"Hperp", value: real, syst: string)
        hpara_var = self.recoil_unc_corrector.evaluate(self.dy_recoil_config.era, njet, events.gen_dilepton_all.pt, "Hpara", hpara, syst)
        hperp_var = self.recoil_unc_corrector.evaluate(self.dy_recoil_config.era, njet, events.gen_dilepton_all.pt, "Hperp", hperp, syst)
        met_pt_var, met_phi_var = GetMETfromH(hpara_var, hperp_var, full_pt, full_phi, vis_pt, vis_phi)
        events = set_ak_column(events, f"PuppiMET.pt_recoil_{syst}", met_pt_var, value_type=np.float32)
        events = set_ak_column(events, f"PuppiMET.phi_recoil_{syst}", met_phi_var, value_type=np.float32)

    return events


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
