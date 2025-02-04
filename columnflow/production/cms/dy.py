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
from columnflow.columnar_util import (
    set_ak_column, remove_ak_column, attach_behavior, EMPTY_FLOAT, get_ak_routes, remove_ak_column,
    optional_column as optional,
)
from columnflow.types import Sequence

np = maybe_import("numpy")
ak = maybe_import("awkward")
vector = maybe_import("vector")


# helper
set_ak_column_f32 = functools.partial(set_ak_column, value_type=np.float32)


logger = law.logger.get_logger(__name__)


@dataclass
class DrellYanConfig:
    """
    Configuration class for Drell-Yan reweighting.
    """

    # campaign config
    era: str = field(default="2022preEE_NLO")
    correction_set: str = "DY_pTll_reweighting"


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
    Drell-Yan reweighting using correctionlib.
    """

    # get campaign era
    era = self.dy_config.era

    # get the gen dilepton
    gen_dilepton = events.gen_dilepton.pt

    # map the variable names from the corrector to our columns
    variable_map = {
        "era": era,
        "ptll": gen_dilepton.pt,
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

    self.dy_config: DrellYanConfig = self.get_dy_config()
    # check
    if len(correction_set.keys()) != 1:
        raise Exception("Expected exactly one type of Drell-Yan correction")

    # index of available correctors:
    # [0] DY_pTll_reweighting
    # [1] DY_pTll_reweighting_N_uncertainty
    corrector_name = self.dy_config.correction_set
    self.dy_corrector = correction_set[corrector_name]


# @producer(
#     uses={"Pileup.nTrueInt"},
#     produces={"dy_weights"},
#     # only run on mc
#     mc_only=True,
# )
# def dy_weights_from_columnflow(self: Producer, events: ak.Array, **kwargs) -> ak.Array:
#     # compute the indices for looking up weights
#     indices = # events.Pileup.nTrueInt.to_numpy().astype("int32") - 1
#     max_bin = len(self.dy_weights) - 1
#     indices[indices > max_bin] = max_bin

#     # save the weights
#     events = set_ak_column_f32(events, "dy_weight", self.dy_weights.nominal[indices])
#     # events = set_ak_column_f32(events, "dy_weight_up", self.dy_weight.up[indices])
#     # events = set_ak_column_f32(events, "dy_weight_down", self.dy_weight.down[indices])

#     return events


# @dy_weights_from_columnflow.requires
# def dy_weights_from_columnflow_requires(self: Producer, reqs: dict) -> None:
#     """
#     Adds the requirements needed the underlying task to derive the Drell-Yan weights into *reqs*.
#     """
#     if "dy_weights" in reqs:
#         return

#     from columnflow.tasks.cms.external import CreatePileupWeights
#     reqs["dy_weights"] = CreatePileupWeights.req(self.task)


# @dy_weights_from_columnflow.setup
# def dy_weights_from_columnflow_setup(
#     self: Producer,
#     reqs: dict,
#     inputs: dict,
#     reader_targets: InsertableDict,
# ) -> None:
#     """
#     Loads the Drell-Yan weights added through the requirements and saves them in the
#     py:attr:`dy_weights` attribute for simpler access in the actual callable.
#     """
#     self.dy_weights = ak.zip(inputs["dy_weights"].load(formatter="json"))
