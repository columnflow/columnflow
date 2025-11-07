# coding: utf-8

"""
Muon related event weights.
"""

from __future__ import annotations

import law

import dataclasses

from columnflow.production import Producer, producer
from columnflow.util import maybe_import, load_correction_set, DotDict
from columnflow.columnar_util import set_ak_column
from columnflow.types import Any

np = maybe_import("numpy")
ak = maybe_import("awkward")


@dataclasses.dataclass
class MuonSFConfig:
    correction: str
    campaign: str = ""
    min_pt: float = 0.0

    @classmethod
    def new(cls, obj: MuonSFConfig | tuple[str, str]) -> MuonSFConfig:
        # purely for backwards compatibility with the old tuple format
        if isinstance(obj, cls):
            return obj
        if isinstance(obj, str):
            return cls(obj)
        if isinstance(obj, (list, tuple)):
            return cls(*obj)
        if isinstance(obj, dict):
            return cls(**obj)
        raise ValueError(f"cannot convert {obj} to MuonSFConfig")


@producer(
    uses={"Muon.{pt,eta}"},
    # produces defined in init
    # only run on mc
    mc_only=True,
    # function to determine the correction file
    get_muon_file=(lambda self, external_files: external_files.muon_sf),
    # function to determine the muon weight config
    get_muon_config=(lambda self: MuonSFConfig.new(self.config_inst.x("muon_sf", self.config_inst.x("muon_sf_names", None)))),  # noqa: E501
    # name of the saved weight column
    weight_name="muon_weight",
    supported_versions={1, 2},
)
def muon_weights(
    self: Producer,
    events: ak.Array,
    muon_mask: ak.Array | type(Ellipsis) = Ellipsis,
    **kwargs,
) -> ak.Array:
    """
    Creates muon weights using the correctionlib. Requires an external file in the config under ``muon_sf``:

    .. code-block:: python

        cfg.x.external_files = DotDict.wrap({
            "muon_sf": "/afs/cern.ch/work/m/mrieger/public/mirrors/jsonpog-integration-9ea86c4c/POG/MUO/2017_UL/muon_z.json.gz",  # noqa
        })

    *get_muon_file* can be adapted in a subclass in case it is stored differently in the external files.

    The name of the correction set and the year string for the weight evaluation should be given as an auxiliary entry
    in the config:

    .. code-block:: python

        cfg.x.muon_sf = MuonSFConfig(
            correction="NUM_TightRelIso_DEN_TightIDandIPCut",
            campaign="2017_UL",
        )

    *get_muon_config* can be adapted in a subclass in case it is stored differently in the config.

    Optionally, a *muon_mask* can be supplied to compute the scale factor weight based only on a subset of muons.
    """
    # fold muon mask with minimum pt cut if given
    if self.muon_config.min_pt > 0.0:
        pt_mask = events.Muon.pt >= self.muon_config.min_pt
        muon_mask = pt_mask if muon_mask is Ellipsis else (pt_mask & muon_mask)

    # prepare input variables
    muons = events.Muon[muon_mask]
    variable_map = {
        "year": self.muon_config.campaign,
        "eta": muons.eta,
        "abseta": abs(muons.eta),
        "pt": muons.pt,
    }

    # loop over systematics
    for syst, postfix in [
        ("sf", ""),
        ("systup", "_up"),
        ("systdown", "_down"),
    ]:
        # get the inputs for this type of variation
        variable_map_syst = {
            **variable_map,
            "scale_factors": "nominal" if syst == "sf" else syst,  # syst key in 2022
            "ValType": syst,  # syst key in 2017
        }
        inputs = [variable_map_syst[inp.name] for inp in self.muon_sf_corrector.inputs]
        sf = self.muon_sf_corrector.evaluate(*inputs)

        # create the product over all muons in one event
        weight = ak.prod(sf, axis=1, mask_identity=False)

        # store it
        events = set_ak_column(events, f"{self.weight_name}{postfix}", weight, value_type=np.float32)

    return events


@muon_weights.init
def muon_weights_init(self: Producer, **kwargs) -> None:
    # add the product of nominal and up/down variations to produced columns
    self.produces.add(f"{self.weight_name}{{,_up,_down}}")


@muon_weights.requires
def muon_weights_requires(
    self: Producer,
    task: law.Task,
    reqs: dict[str, DotDict[str, Any]],
    **kwargs,
) -> None:
    if "external_files" in reqs:
        return

    from columnflow.tasks.external import BundleExternalFiles
    reqs["external_files"] = BundleExternalFiles.req(task)


@muon_weights.setup
def muon_weights_setup(
    self: Producer,
    task: law.Task,
    reqs: dict[str, DotDict[str, Any]],
    inputs: dict[str, Any],
    reader_targets: law.util.InsertableDict,
    **kwargs,
) -> None:
    bundle = reqs["external_files"]

    # load the corrector
    correction_set = load_correction_set(self.get_muon_file(bundle.files))

    self.muon_config = self.get_muon_config()
    self.muon_sf_corrector = correction_set[self.muon_config.correction]

    # check versions
    if self.supported_versions and self.muon_sf_corrector.version not in self.supported_versions:
        raise Exception(f"unsupported muon sf corrector version {self.muon_sf_corrector.version}")


# custom muon weight that runs trigger SFs
muon_trigger_weights = muon_weights.derive(
    "muon_trigger_weights",
    cls_dict={
        "get_muon_file": (lambda self, external_files: external_files.muon_trigger_sf),
        "get_muon_config": (lambda self: self.config_inst.x.muon_trigger_sf_names),
        "weight_name": "muon_trigger_weight",
    },
)
