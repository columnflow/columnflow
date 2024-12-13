# coding: utf-8

"""
Electron related event weights.
"""

from __future__ import annotations

from dataclasses import dataclass

from columnflow.production import Producer, producer
from columnflow.util import maybe_import, InsertableDict
from columnflow.columnar_util import set_ak_column, flat_np_view, layout_ak_array

np = maybe_import("numpy")
ak = maybe_import("awkward")


@dataclass
class ElectronSFConfig:
    correction: str
    campaign: str
    working_point: str = ""
    hlt_path: str = ""

    def __post_init__(self) -> None:
        if not self.working_point and not self.hlt_path:
            raise ValueError("either working_point or hlt_path must be set")
        if self.working_point and self.hlt_path:
            raise ValueError("only one of working_point or hlt_path must be set")

    @classmethod
    def new(
        cls,
        obj: ElectronSFConfig | tuple[str, str, str],
    ) -> ElectronSFConfig:
        # purely for backwards compatibility with the old tuple format
        if isinstance(obj, cls):
            return obj
        elif isinstance(obj, list) or isinstance(obj, tuple) or isinstance(obj, set):
            return cls(*obj)
        elif isinstance(obj, dict):
            return cls(**obj)
        else:
            raise ValueError(f"cannot convert {obj} to ElectronSFConfig")


@producer(
    uses={"Electron.{pt,eta,phi,deltaEtaSC}"},
    # produces in the init
    # only run on mc
    mc_only=True,
    # function to determine the correction file
    get_electron_file=(lambda self, external_files: external_files.electron_sf),
    # function to determine the electron weight config
    get_electron_config=(lambda self: ElectronSFConfig.new(self.config_inst.x.electron_sf_names)),
    weight_name="electron_weight",
    supported_versions=(1, 2, 3),
)
def electron_weights(
    self: Producer,
    events: ak.Array,
    electron_mask: ak.Array | type(Ellipsis) = Ellipsis,
    **kwargs,
) -> ak.Array:
    """
    Creates electron weights using the correctionlib. Requires an external file in the config under
    ``electron_sf``:

    .. code-block:: python

        cfg.x.external_files = DotDict.wrap({
            "electron_sf": "/afs/cern.ch/work/m/mrieger/public/mirrors/jsonpog-integration-9ea86c4c/POG/EGM/2017_UL/electron.json.gz",  # noqa
        })

    *get_electron_file* can be adapted in a subclass in case it is stored differently in the
    external files.

    The name of the correction set, the year string for the weight evaluation, and the name of the
    working point should be given as an auxiliary entry in the config:

    .. code-block:: python

        cfg.x.electron_sf_names = ElectronSFConfig(
            correction="UL-Electron-ID-SF",
            campaign="2017",
            working_point="wp80iso",  # for trigger weights use hlt_path instead
        )

    *get_electron_config* can be adapted in a subclass in case it is stored differently in the
    config.

    Optionally, an *electron_mask* can be supplied to compute the scale factor weight
    based only on a subset of electrons.
    """
    # flat super cluster eta and pt views
    sc_eta = flat_np_view((
        events.Electron.eta[electron_mask] +
        events.Electron.deltaEtaSC[electron_mask]
    ), axis=1)
    pt = flat_np_view(events.Electron.pt[electron_mask], axis=1)
    phi = flat_np_view(events.Electron.phi[electron_mask], axis=1)

    variable_map = {
        "year": self.electron_config.campaign,
        "WorkingPoint": self.electron_config.working_point,
        "Path": self.electron_config.hlt_path,
        "pt": pt,
        "eta": sc_eta,
        "phi": phi,
    }

    # loop over systematics
    for syst, postfix in [
        ("sf", ""),
        ("sfup", "_up"),
        ("sfdown", "_down"),
    ]:
        # get the inputs for this type of variation
        variable_map_syst = {
            **variable_map,
            "ValType": syst,
        }
        inputs = [variable_map_syst[inp.name] for inp in self.electron_sf_corrector.inputs]
        sf_flat = self.electron_sf_corrector(*inputs)

        # add the correct layout to it
        sf = layout_ak_array(sf_flat, events.Electron.pt[electron_mask])

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
def electron_weights_requires(self: Producer, reqs: dict) -> None:
    if "external_files" in reqs:
        return

    from columnflow.tasks.external import BundleExternalFiles
    reqs["external_files"] = BundleExternalFiles.req(self.task)


@electron_weights.setup
def electron_weights_setup(
    self: Producer,
    reqs: dict,
    inputs: dict,
    reader_targets: InsertableDict,
) -> None:
    bundle = reqs["external_files"]

    # create the corrector
    import correctionlib
    correctionlib.highlevel.Correction.__call__ = correctionlib.highlevel.Correction.evaluate
    correction_set = correctionlib.CorrectionSet.from_string(
        self.get_electron_file(bundle.files).load(formatter="gzip").decode("utf-8"),
    )
    self.electron_config: ElectronSFConfig = self.get_electron_config()
    self.electron_sf_corrector = correction_set[self.electron_config.correction]

    # check versions
    if self.supported_versions and self.electron_sf_corrector.version not in self.supported_versions:
        raise Exception(f"unsupported electron sf corrector version {self.electron_sf_corrector.version}")


# custom electron weight that runs trigger SFs
electron_trigger_weights = electron_weights.derive(
    "electron_trigger_weights",
    cls_dict={
        "get_electron_file": (lambda self, external_files: external_files.electron_trigger_sf),
        "get_electron_config": (lambda self: self.config_inst.x.electron_trigger_sf_names),
        "weight_name": "electron_trigger_weight",
    },
)
