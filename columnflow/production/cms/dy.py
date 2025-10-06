# coding: utf-8

"""
Column production methods related to Drell-Yan reweighting.
"""

from __future__ import annotations

import law

from dataclasses import dataclass

from columnflow.production import Producer, producer
from columnflow.util import maybe_import, load_correction_set
from columnflow.columnar_util import set_ak_column

np = maybe_import("numpy")
ak = maybe_import("awkward")


logger = law.logger.get_logger(__name__)


@dataclass
class DrellYanConfig:
    era: str
    correction: str
    unc_correction: str | None = None
    order: str | None = None
    njets: bool = False
    systs: list[str] | None = None

    def __post_init__(self) -> None:
        if not self.era or not self.correction:
            raise ValueError(f"{self.__class__.__name__}: missing era or correction")
        if self.unc_correction and not self.order:
            raise ValueError(f"{self.__class__.__name__}: when unc_correction is defined, order must be set")


@producer(
    uses={"GenPart.*"},
    produces={"gen_dilepton_{pdgid,pt}", "gen_dilepton_{vis,all}.{pt,eta,phi,mass}"},
)
def gen_dilepton(self, events: ak.Array, **kwargs) -> ak.Array:
    """
    Reconstruct the di-lepton pair from generator level info. This considers only visible final-state particles.
    In addition it provides the four-momenta of all leptons (including neutrinos) from the hard process.
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
    # taus need to have status == 2
    tau_mask = (
        (pdg_id == 15) & (status == 2) & events.GenPart.hasFlags("fromHardProcess")
    )

    # lepton masks for recoil corrections
    # -> https://indico.cern.ch/event/1495537/contributions/6359516/attachments/3014424/5315938/HLepRare_25.02.14.pdf
    lepton_all_mask = (
        # e, mu, taus, neutrinos
        (
            (pdg_id >= 11) &
            (pdg_id <= 16) &
            (status == 1) &
            events.GenPart.hasFlags("fromHardProcess")
        ) |
        # tau decay products
        events.GenPart.hasFlags("isDirectHardProcessTauDecayProduct")
    )
    lepton_vis_mask = lepton_all_mask & (
        # no e neutrinos, mu neutrinos, or taus neutrinos
        (pdg_id != 12) & (pdg_id != 14) | (pdg_id != 16)
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

    # absolute pdg id of one parrticle of the lepton pair
    lepton_pair_pdgid = ak.without_parameters(abs(events.GenPart[lepton_mask].pdgId[:, 0]))

    # finally, save generator-level lepton pair variables
    events = set_ak_column(events, "gen_dilepton_pdgid", lepton_pair_pdgid)
    events = set_ak_column(events, "gen_dilepton_pt", lepton_pair_momenta.pt)
    events = set_ak_column(events, "gen_dilepton_vis.pt", lepton_pair_momenta_vis.pt)
    events = set_ak_column(events, "gen_dilepton_vis.eta", lepton_pair_momenta_vis.eta)
    events = set_ak_column(events, "gen_dilepton_vis.phi", lepton_pair_momenta_vis.phi)
    events = set_ak_column(events, "gen_dilepton_vis.mass", lepton_pair_momenta_vis.mass)
    events = set_ak_column(events, "gen_dilepton_all.pt", lepton_pair_momenta_all.pt)
    events = set_ak_column(events, "gen_dilepton_all.eta", lepton_pair_momenta_all.eta)
    events = set_ak_column(events, "gen_dilepton_all.phi", lepton_pair_momenta_all.phi)
    events = set_ak_column(events, "gen_dilepton_all.mass", lepton_pair_momenta_all.mass)

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
            "dy_weight_sf": "/afs/cern.ch/work/m/mrieger/public/mirrors/external_files/DY_pTll_weights_v2.json.gz",  # noqa
        })

    *get_dy_weight_file* can be adapted in a subclass in case it is stored differently in the external files.

    The campaign era and name of the correction set (see link above) should be given as an auxiliary entry in the config:

    .. code-block:: python

        cfg.x.dy_weight_config = DrellYanConfig(
            era="2022preEE",
            order="NLO",
            correction="DY_pTll_reweighting",
            unc_correction="DY_pTll_reweighting_N_uncertainty",
        )

    *get_dy_weight_config* can be adapted in a subclass in case it is stored differently in the config.
    """
    # map the input variable names from the corrector to our columns
    variable_map = {
        "era": self.dy_config.era,
        "ptll": events.gen_dilepton_pt,
    }

    # optionals
    if self.dy_config.order:
        variable_map["order"] = self.dy_config.order
    if self.dy_config.njets:
        variable_map["njets"] = ak.num(events.Jet, axis=1)

    # initializing the list of weight variations (called syst in the dy files)
    systs = [("nom", "")]

    # add specific uncertainties or additional systs
    if self.dy_config.unc_correction:
        for i in range(self.n_unc):
            for direction in ["up", "down"]:
                systs.append((f"{direction}{i + 1}", f"_{direction}{i + 1}"))
    elif self.dy_config.systs:
        for syst in self.dy_config.systs:
            systs.append((syst, f"_{syst}"))

    # preparing the input variables for the corrector
    for syst, postfix in systs:
        _variable_map = {**variable_map, "syst": syst}

        # evaluating dy weights given a certain era, ptll array and sytematic shift
        inputs = [_variable_map[inp.name] for inp in self.dy_corrector.inputs]
        dy_weight = self.dy_corrector.evaluate(*inputs)

        # save the weights in a new column
        events = set_ak_column(events, f"dy_weight{postfix}", dy_weight, value_type=np.float32)

    return events


@dy_weights.init
def dy_weights_init(self: Producer) -> None:
    if self.config_inst.campaign.x.year not in {2022, 2023}:
        raise NotImplementedError(
            f"campaign year {self.config_inst.campaign.x.year} is not yet supported by {self.cls_name}",
        )

    # declare additional used columns
    self.dy_config: DrellYanConfig = self.get_dy_weight_config()
    if self.dy_config.njets:
        self.uses.add("Jet.pt")

    # declare additional produced columns
    if self.dy_config.unc_correction:
        # the number should always be 10
        self.n_unc = 10
        for i in range(self.n_unc):
            self.produces.add(f"dy_weight{i + 1}_{{up,down}}")
    elif self.dy_config.systs:
        for syst in self.dy_config.systs:
            self.produces.add(f"dy_weight_{syst}")


@dy_weights.requires
def dy_weights_requires(self: Producer, task: law.Task, reqs: dict) -> None:
    """
    Adds the requirements needed the underlying task to derive the Drell-Yan weights into *reqs*.
    """
    if "external_files" in reqs:
        return

    from columnflow.tasks.external import BundleExternalFiles
    reqs["external_files"] = BundleExternalFiles.req(task)


@dy_weights.setup
def dy_weights_setup(
    self: Producer,
    task: law.Task,
    reqs: dict,
    inputs: dict,
    reader_targets: law.util.InsertableDict,
) -> None:
    """
    Loads the Drell-Yan weight calculator from the external files bundle and saves them in the py:attr:`dy_corrector`
    attribute for simpler access in the actual callable. The number of uncertainties is calculated, per era, by another
    correcter in the external file and is saved in the py:attr:`dy_unc_corrector` attribute.
    """
    bundle = reqs["external_files"]

    # import all correctors from the external file
    correction_set = load_correction_set(self.get_dy_weight_file(bundle.files))

    # create the weight corrector
    self.dy_corrector = correction_set[self.dy_config.correction]

    # create the uncertainty corrector
    if self.dy_config.unc_correction:
        self.dy_unc_corrector = correction_set[self.dy_config.unc_correction]
        dy_n_unc = int(self.dy_unc_corrector.evaluate(self.dy_config.order))
        if dy_n_unc != self.n_unc:
            raise ValueError(
                f"Expected {self.n_unc} uncertainties, got {dy_n_unc}",
            )


@producer(
    uses={
        # MET information
        # -> only Run 3 (PuppiMET) is supported
        "PuppiMET.{pt,phi}",
        # Gen-level boson information (full boson momentum)
        # -> gen_dilepton_vis.pt, gen_dilepton_vis.phi, gen_dilepton_all.pt, gen_dilepton_all.phi
        gen_dilepton.PRODUCES,
    },
    produces={
        "RecoilCorrMET.{pt,phi}",
        "RecoilCorrMET.{pt,phi}_{recoilresp,recoilres}_{up,down}",
    },
    # custom njet column to be used to derive corrections
    njet_column=None,
    mc_only=True,
    # function to determine the recoil correction file from external files
    get_dy_recoil_file=(lambda self, external_files: external_files.dy_recoil_sf),
    # function to load the config
    get_dy_recoil_config=(lambda self: self.config_inst.x.dy_recoil_config),
)
def recoil_corrected_met(self: Producer, events: ak.Array, **kwargs) -> ak.Array:
    """
    Producer for bosonic recoil corrections which are applied to PuppiMET to create a new ``RecoilCorrMET`` collection.
    See https://cms-higgs-leprare.docs.cern.ch/htt-common/V_recoil for more info.

    Requires an external file in the config under ``dy_recoil_sf``:

    .. code-block:: python

        cfg.x.external_files = DotDict.wrap({
            "dy_recoil_sf": "/afs/cern.ch/work/m/mrieger/public/mirrors/external_files/Recoil_corrections_v2.json.gz",
        })

    *get_dy_recoil_file* can be adapted in a subclass in case it is stored differently in the external files.

    The campaign era and name of the correction set (see link above) should be given as an auxiliary entry in the config:

    .. code-block:: python

        cfg.x.dy_recoil_config = DrellYanConfig(
            era="2022preEE",
            order="NLO",
            correction="Recoil_correction_Rescaling",
            unc_correction="Recoil_correction_Uncertainty",
        )

    *get_dy_recoil_config* can be adapted in a subclass in case it is stored differently in the config.
    """
    import vector

    # steps:
    # 1) Build transverse vectors for MET and the generator-level boson (full and visible).
    # 2) Compute the recoil vector U = MET + vis - full in the transverse plane.
    # 3) Project U along and perpendicular to the full boson direction.
    # 4) Apply the nominal recoil correction and reassemble the corrected MET.
    # 5) For each systematic variation, apply the uncertainty correction on H components and reconstruct MET.
    # Build MET vector (using dummy eta and mass, since only x and y matter)
    met = vector.array({
        "pt": events.PuppiMET.pt,
        "phi": events.PuppiMET.phi,
        "eta": np.zeros_like(events.PuppiMET.pt),
        "mass": np.zeros_like(events.PuppiMET.pt),
    })

    # Build full and visible boson vectors from generator-level information
    full = vector.array({
        "pt": events.gen_dilepton_all.pt,
        "phi": events.gen_dilepton_all.phi,
        "eta": np.zeros_like(events.gen_dilepton_all.pt),
        "mass": np.zeros_like(events.gen_dilepton_all.pt),
    })
    vis = vector.array({
        "pt": events.gen_dilepton_vis.pt,
        "phi": events.gen_dilepton_vis.phi,
        "eta": np.zeros_like(events.gen_dilepton_vis.pt),
        "mass": np.zeros_like(events.gen_dilepton_vis.pt),
    })

    # Compute the recoil vector U = MET + vis - full
    u_x = met.x + vis.x - full.x
    u_y = met.y + vis.y - full.y

    # Project U onto the full boson direction
    full_pt = full.pt
    full_unit_x = full.x / full_pt
    full_unit_y = full.y / full_pt
    upara = u_x * full_unit_x + u_y * full_unit_y
    uperp = -u_x * full_unit_y + u_y * full_unit_x

    # Determine jet multiplicity for the event (jet selection as in original)
    if self.njet_column:
        njet = np.asarray(events[self.njet_column], dtype=np.float32)
    else:
        jet_selection = (
            ((events.Jet.pt > 30) & (np.abs(events.Jet.eta) < 2.5)) |
            ((events.Jet.pt > 50) & (np.abs(events.Jet.eta) >= 2.5))
        )
        selected_jets = events.Jet[jet_selection]
        njet = np.asarray(ak.num(selected_jets, axis=1), dtype=np.float32)

    # Apply nominal recoil correction on U components
    # (see here: https://cms-higgs-leprare.docs.cern.ch/htt-common/V_recoil/#example-snippet)
    upara_corr = self.recoil_corrector.evaluate(
        self.dy_recoil_config.era,
        self.dy_recoil_config.order,
        njet,
        events.gen_dilepton_all.pt,
        "Upara",
        upara,
    )
    uperp_corr = self.recoil_corrector.evaluate(
        self.dy_recoil_config.era,
        self.dy_recoil_config.order,
        njet,
        events.gen_dilepton_all.pt,
        "Uperp",
        uperp,
    )

    # Reassemble the corrected U vector
    ucorr_x = upara_corr * full_unit_x - uperp_corr * full_unit_y
    ucorr_y = upara_corr * full_unit_y + uperp_corr * full_unit_x

    # Recompute corrected MET: MET_corr = U_corr - vis + full
    met_corr_x = ucorr_x - vis.x + full.x
    met_corr_y = ucorr_y - vis.y + full.y
    met_corr_pt = np.sqrt(met_corr_x**2 + met_corr_y**2)
    met_corr_phi = np.arctan2(met_corr_y, met_corr_x)

    events = set_ak_column(events, "RecoilCorrMET.pt", met_corr_pt, value_type=np.float32)
    events = set_ak_column(events, "RecoilCorrMET.phi", met_corr_phi, value_type=np.float32)

    # --- Systematic variations ---
    # Derive H from the nominal corrected MET: H = - (MET_corr + vis)
    h_x = -met_corr_x - vis.x
    h_y = -met_corr_y - vis.y
    h_pt = np.sqrt(h_x**2 + h_y**2)
    h_phi = np.arctan2(h_y, h_x)
    # Project H into the full boson coordinate system
    hpara = h_pt * np.cos(h_phi - full.phi)
    hperp = h_pt * np.sin(h_phi - full.phi)

    for syst, postfix in [
        ("RespUp", "recoilresp_up"),
        ("RespDown", "recoilresp_down"),
        ("ResolUp", "recoilres_up"),
        ("ResolDown", "recoilres_down"),
    ]:
        hpara_var = self.recoil_unc_corrector.evaluate(
            self.dy_recoil_config.era,
            self.dy_recoil_config.order,
            njet,
            events.gen_dilepton_all.pt,
            "Hpara",
            hpara,
            syst,
        )
        hperp_var = self.recoil_unc_corrector.evaluate(
            self.dy_recoil_config.era,
            self.dy_recoil_config.order,
            njet,
            events.gen_dilepton_all.pt,
            "Hperp",
            hperp,
            syst,
        )
        # Reconstruct the corrected H vector in the full boson frame
        hcorr_x = hpara_var * np.cos(full.phi) - hperp_var * np.sin(full.phi)
        hcorr_y = hpara_var * np.sin(full.phi) + hperp_var * np.cos(full.phi)
        # Reconstruct the MET variation: MET_var = -H_corr - vis
        met_var_x = -hcorr_x - vis.x
        met_var_y = -hcorr_y - vis.y
        met_var_pt = np.sqrt(met_var_x**2 + met_var_y**2)
        met_var_phi = np.arctan2(met_var_y, met_var_x)
        events = set_ak_column(events, f"RecoilCorrMET.pt_{postfix}", met_var_pt, value_type=np.float32)
        events = set_ak_column(events, f"RecoilCorrMET.phi_{postfix}", met_var_phi, value_type=np.float32)

    return events


@recoil_corrected_met.init
def recoil_corrected_met_init(self: Producer) -> None:
    if self.njet_column:
        self.uses.add(f"{self.njet_column}")
    else:
        self.uses.add("Jet.{pt,eta,phi,mass}")


@recoil_corrected_met.requires
def recoil_corrected_met_requires(self: Producer, task: law.Task, reqs: dict) -> None:
    # Ensure that external files are bundled.
    if "external_files" in reqs:
        return

    from columnflow.tasks.external import BundleExternalFiles
    reqs["external_files"] = BundleExternalFiles.req(task)


@recoil_corrected_met.setup
def recoil_corrected_met_setup(
    self: Producer,
    task: law.Task,
    reqs: dict,
    inputs: dict,
    reader_targets: law.util.InsertableDict,
) -> None:
    # load the correction set
    bundle = reqs["external_files"]
    correction_set = load_correction_set(self.get_dy_recoil_file(bundle.files))

    # Retrieve the corrections used for the nominal correction and for uncertainties.
    self.dy_recoil_config: DrellYanConfig = self.get_dy_recoil_config()
    self.recoil_corrector = correction_set[self.dy_recoil_config.correction]
    self.recoil_unc_corrector = correction_set[self.dy_recoil_config.unc_correction]
