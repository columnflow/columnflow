# coding: utf-8

"""
Electron related event weights.
"""

from __future__ import annotations

import dataclasses

import law

from columnflow.production import Producer, producer
from columnflow.util import maybe_import, load_correction_set, DotDict
from columnflow.columnar_util import set_ak_column, full_like, flat_np_view, layout_ak_array
from columnflow.types import Any, Callable

np = maybe_import("numpy")
ak = maybe_import("awkward")


@dataclasses.dataclass
class ElectronSFConfig:
    correction: str
    campaign: str
    working_point: str | dict[str, Callable] = ""
    hlt_path: str = ""
    min_pt: float = 0.0
    max_pt: float = 0.0

    def __post_init__(self) -> None:
        if not self.working_point and not self.hlt_path:
            raise ValueError("either working_point or hlt_path must be set")
        if self.working_point and self.hlt_path:
            raise ValueError("only one of working_point or hlt_path must be set")
        if 0.0 < self.max_pt <= self.min_pt:
            raise ValueError(f"{self.__class__.__name__}: max_pt must be larger than min_pt")

    @classmethod
    def new(cls, obj: ElectronSFConfig | tuple[str, str, str]) -> ElectronSFConfig:
        # purely for backwards compatibility with the old tuple format
        if isinstance(obj, cls):
            return obj
        if isinstance(obj, list) or isinstance(obj, tuple) or isinstance(obj, set):
            return cls(*obj)
        if isinstance(obj, dict):
            return cls(**obj)
        raise ValueError(f"cannot convert {obj} to ElectronSFConfig")


@producer(
    uses={"Electron.{pt,eta,phi,deltaEtaSC}"},
    # produces in the init
    # only run on mc
    mc_only=True,
    # function to determine the correction file
    get_electron_file=(lambda self, external_files: external_files.electron_sf),
    # function to determine the electron weight config
    get_electron_config=(lambda self: ElectronSFConfig.new(self.config_inst.x("electron_sf", self.config_inst.x("electron_sf_names", None)))),  # noqa: E501
    # choose if the eta variable should be the electron eta or the super cluster eta
    use_supercluster_eta=True,
    # name of the saved weight column
    weight_name="electron_weight",
    supported_versions={1, 2, 3, 4},
)
def electron_weights(
    self: Producer,
    events: ak.Array,
    electron_mask: ak.Array | type(Ellipsis) = Ellipsis,
    **kwargs,
) -> ak.Array:
    """
    Creates electron weights using the correctionlib. Requires an external file in the config under ``electron_sf``:

    .. code-block:: python

        cfg.x.external_files = DotDict.wrap({
            "electron_sf": "/afs/cern.ch/work/m/mrieger/public/mirrors/jsonpog-integration-9ea86c4c/POG/EGM/2017_UL/electron.json.gz",  # noqa
        })

    *get_electron_file* can be adapted in a subclass in case it is stored differently in the external files.

    The name of the correction set, the year string for the weight evaluation, and the name of the working point should
    be given as an auxiliary entry in the config:

    .. code-block:: python

        cfg.x.electron_sf = ElectronSFConfig(
            correction="UL-Electron-ID-SF",
            campaign="2017",
            working_point="wp80iso",  # for trigger weights use hlt_path instead
        )

    The *working_point* can also be a dictionary mapping working point names to functions that return a boolean mask for
    the electrons. This is useful to compute scale factors for multiple working points at once, e.g. for the electron
    reconstruction scale factors:

    .. code-block:: python

        cfg.x.electron_sf = ElectronSFConfig(
            correction="Electron-ID-SF",
            campaign="2022Re-recoE+PromptFG",
            working_point={
                "RecoBelow20": lambda variable_map: variable_map["pt"] < 20.0,
                "Reco20to75": lambda variable_map: (variable_map["pt"] >= 20.0) & (variable_map["pt"] < 75.0),
                "RecoAbove75": lambda variable_map: variable_map["pt"] >= 75.0,
            },
        )

    *get_electron_config* can be adapted in a subclass in case it is stored differently in the config.

    Optionally, an *electron_mask* can be supplied to compute the scale factor weight based only on a subset of
    electrons.
    """
    # fold electron mask with pt cuts if given
    if self.electron_config.min_pt > 0.0:
        pt_mask = events.Electron.pt >= self.electron_config.min_pt
        electron_mask = pt_mask if electron_mask is Ellipsis else (pt_mask & electron_mask)
    if self.electron_config.max_pt > 0.0:
        pt_mask = events.Electron.pt <= self.electron_config.max_pt
        electron_mask = pt_mask if electron_mask is Ellipsis else (pt_mask & electron_mask)

    # prepare input variables
    electrons = events.Electron[electron_mask]
    eta = electrons.eta
    if self.use_supercluster_eta:
        eta = (
            electrons.superclusterEta
            if "superclusterEta" in electrons.fields
            else electrons.eta + electrons.deltaEtaSC
        )
    variable_map = {
        "year": self.electron_config.campaign,
        "Path": self.electron_config.hlt_path,
        "pt": electrons.pt,
        "phi": electrons.phi,
        "eta": eta,
    }

    # loop over systematics
    for syst, postfix in zip(self.sf_variations, ["", "_up", "_down"]):
        # get the inputs for this type of variation
        variable_map_syst = variable_map | {"ValType": syst}

        # add working point
        wp = self.electron_config.working_point
        if isinstance(wp, str):
            # single wp, just evaluate
            variable_map_syst_wp = variable_map_syst | {"WorkingPoint": wp}
            inputs = [variable_map_syst_wp[inp.name] for inp in self.electron_sf_corrector.inputs]
            sf = self.electron_sf_corrector.evaluate(*inputs)
        elif isinstance(wp, dict):
            # mapping of wps to masks, evaluate per wp and combine
            sf_flat = flat_np_view(full_like(eta, 1.0))
            for _wp, mask_fn in wp.items():
                mask = mask_fn(variable_map)
                variable_map_syst_wp = variable_map_syst | {"WorkingPoint": _wp}
                # call the corrector with the masked inputs
                inputs = [
                    (
                        variable_map_syst_wp[inp.name][mask]
                        if isinstance(variable_map_syst_wp[inp.name], (np.ndarray, ak.Array))
                        else variable_map_syst_wp[inp.name]
                    )
                    for inp in self.electron_sf_corrector.inputs
                ]
                sf_flat[flat_np_view(mask)] = flat_np_view(self.electron_sf_corrector.evaluate(*inputs))
            sf = layout_ak_array(sf_flat, eta)
        else:
            raise ValueError(f"unsupported working point type {type(variable_map['WorkingPoint'])}")

        # create the product over all electrons in one event
        weight = ak.prod(sf, axis=1, mask_identity=False)

        # store it
        events = set_ak_column(events, f"{self.weight_name}{postfix}", weight, value_type=np.float32)

    return events


@electron_weights.init
def electron_weights_init(self: Producer, **kwargs) -> None:
    # add the product of nominal and up/down variations to produced columns
    self.produces.add(f"{self.weight_name}{{,_up,_down}}")


@electron_weights.requires
def electron_weights_requires(
    self: Producer,
    task: law.Task,
    reqs: dict[str, DotDict[str, Any]],
    **kwargs,
) -> None:
    if "external_files" in reqs:
        return

    from columnflow.tasks.external import BundleExternalFiles
    reqs["external_files"] = BundleExternalFiles.req(task)


@electron_weights.setup
def electron_weights_setup(
    self: Producer,
    task: law.Task,
    reqs: dict[str, DotDict[str, Any]],
    inputs: dict[str, Any],
    reader_targets: law.util.InsertableDict,
    **kwargs,
) -> None:
    self.electron_config = self.get_electron_config()

    # load the corrector
    e_file = self.get_electron_file(reqs["external_files"].files)
    self.electron_sf_corrector = load_correction_set(e_file)[self.electron_config.correction]

    # the ValType key accepts different arguments for efficiencies and scale factors
    if self.electron_config.correction.endswith("Eff"):
        self.sf_variations = ["nom", "up", "down"]
    else:
        self.sf_variations = ["sf", "sfup", "sfdown"]

    # check versions
    if self.supported_versions and self.electron_sf_corrector.version not in self.supported_versions:
        raise Exception(f"unsupported electron sf corrector version {self.electron_sf_corrector.version}")


# custom electron weight that runs trigger SFs
electron_trigger_weights = electron_weights.derive(
    "electron_trigger_weights",
    cls_dict={
        "get_electron_file": (lambda self, external_files: external_files.electron_trigger_sf),
        "get_electron_config": (lambda self: self.config_inst.x.electron_trigger_sf_names),
        "use_supercluster_eta": False,
        "weight_name": "electron_trigger_weight",
    },
)


@producer(
    uses={"Electron.{pt,phi,eta,deltaEtaSC}"},
    produces={"Electron.superclusterEta"},
)
def electron_sceta(self, events: ak.Array, **kwargs) -> ak.Array:
    """
    Returns the electron super cluster eta.
    """
    sc_eta = events.Electron.eta + events.Electron.deltaEtaSC
    events = set_ak_column(events, "Electron.superclusterEta", sc_eta, value_type=np.float32)
    return events
