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
    correction: str
    unc_correction: str

    def __post_init__(self) -> None:
        if not self.era or not self.correction or not self.unc_correction:
            raise ValueError("Campaign era, correction  and unc_correction must be set")


@producer(
    uses={"GenPart.*"},
    produces={"gen_dilepton.pt"},
)
def gen_dilepton(self, events: ak.Array, **kwargs) -> ak.Array:
    """
    Reconstruct the di-lepton pair from generator level info.
    """

    # get the absolute pdg id (to account for anti-particles) and status of the particles
    pdg_id = abs(events.GenPart.pdgId)
    status = events.GenPart.status

    # electrons and muons need to have status == 1, so find them
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

    # combine the masks
    lepton_mask = ele_mu_mask | tau_mask
    lepton_pairs = events.GenPart[lepton_mask]

    # some up the four momenta of the leptons
    lepton_pair_momenta = lepton_pairs.sum(axis=-1)

    # finally, save the pt of the lepton pair on generator level
    events = set_ak_column(events, "gen_dilepton.pt", lepton_pair_momenta.pt)
    return events


@producer(
    uses={"gen_dilepton.pt"},
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
            correction="DY_pTll_reweighting",
            unc_correction="DY_pTll_reweighting_N_uncertainty",
        )

    *get_dy_weight_config* can be adapted in a subclass in case it is stored differently in the config.
    """

    # map the input variable names from the corrector to our columns
    variable_map = {
        "era": self.dy_config.era,
        "ptll": events.gen_dilepton.pt,
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

    dy_n_unc = int(self.dy_unc_corrector.evaluate(self.dy_config.era))

    if dy_n_unc != self.n_unc:
        raise ValueError(
            f"Expected {self.n_unc} uncertainties, got {dy_n_unc}",
        )
