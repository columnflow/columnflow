# coding: utf-8

"""
Column production methods related to Drell-Yan reweighting.
"""

from __future__ import annotations
import functools

import law

from dataclasses import dataclass, field

from columnflow.production import Producer, producer
from columnflow.util import maybe_import, InsertableDict
from columnflow.columnar_util import set_ak_column

np = maybe_import("numpy")
ak = maybe_import("awkward")
vector = maybe_import("vector")


# helper
set_ak_column_f32 = functools.partial(set_ak_column, value_type=np.float32)


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
    produces={"gen_dilepton.pt"},
)
def get_gen_dilepton(self, events: ak.Array, **kwargs) -> ak.Array:
    """
    Reconstruct the di-lepton pair from generator level info.
    """

    # get the absolute pdg id (to account for anti-particles) and status of the particles
    pdg_id = abs(events.GenPart.pdgId)
    status = events.GenPart.status

    # electrons and muons need to have status == 1, so find them
    ele_mu_mask = (pdg_id == 11) | (pdg_id == 13)
    final_ele_mu_mask = ele_mu_mask & (status == 1)

    # taus need to have status == 2,
    tau_mask = pdg_id == 15
    final_tau_mask = tau_mask & (status == 2)

    # combine the masks
    lepton_mask = final_ele_mu_mask | final_tau_mask
    # only consider leptons from hard process (i.e. from the matrix element)
    mask_gen = ak.mask(events.GenPart, lepton_mask).hasFlags("fromHardProcess")

    # fill the mask with False if it is None and extract the gen leptons
    mask_gen = ak.fill_none(mask_gen, False)
    lepton_pairs = events.GenPart[mask_gen]

    # some up the four momenta of the leptons
    lepton_pair_momenta = lepton_pairs.sum(axis=-1)

    # finally, save the pt of the lepton pair on generator level
    events = set_ak_column(events, "gen_dilepton.pt", lepton_pair_momenta.pt)
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

    # map the variable names from the corrector to our columns
    variable_map = {
        "era": self.dy_config.era,
        "ptll": events.gen_dilepton.pt,
    }

    for column_name, syst in (
        ("dy_weight", "nom"),

        # skip systematics for now TODO
        # possible values : nom, down1, down2, down3, down4, down5, down6, down7, down8, down9, down10, up1, up2, up3, up4, up5, up6, up7, up8, up9, up10
        # ("dy_weight_up", "up1"),
        # ("dy_weight_down", "down1"),
    ):
        # get the inputs for this type of variation
        variable_map_syst = {**variable_map, "syst": syst}
        inputs = [variable_map_syst[inp.name] for inp in self.dy_corrector.inputs]

        # evaluate and store the produced column
        dy_weight = self.dy_corrector.evaluate(*inputs)
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
    Loads the Drell-Yan calculator from the external files bundle and saves them in the
    py:attr:`dy_corrector` attribute for simpler access in the actual callable.
    """
    bundle = reqs["external_files"]

    # create the corrector
    import correctionlib
    correctionlib.highlevel.Correction.__call__ = correctionlib.highlevel.Correction.evaluate
    correction_set = correctionlib.CorrectionSet.from_string(
        self.get_dy_file(bundle.files).load(formatter="gzip").decode("utf-8"),
    )

    # check
    if len(correction_set.keys()) != 2:
        raise Exception("Expected exactly two types of Drell-Yan correction")

    self.dy_config: DrellYanConfig = self.get_dy_config()
    self.dy_corrector = correction_set[self.dy_config.correction]
    self.dy_unc_corrector = correction_set[self.dy_config.unc_correction]
