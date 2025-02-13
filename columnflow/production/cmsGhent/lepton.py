from __future__ import annotations

import law
from typing import Sequence, Callable
import dataclasses

from columnflow.production import Producer, producer
from columnflow.calibration.cms.jets import ak_evaluate

from columnflow.util import maybe_import, InsertableDict, DotDict
from columnflow.columnar_util import set_ak_column

ak = maybe_import("awkward")
np = maybe_import("numpy")
hist = maybe_import("hist")
correctionlib = maybe_import("correctionlib")

logger = law.logger.get_logger(__name__)


@dataclasses.dataclass
class LeptonWeightConfig:
    """Config for mva type lepton IDs

    Parameters:
        @param year: integer or string giving the data year or era
        @param weight_name: Name for the produced weight columns
        @param correction_set: name of the corrections in the json
        @param get_sf_file: function mapping external files to the scale factor json
        @param input_pars: dictionary passed to the corrector inputs

        @param aux: dictionary with other useful information
        @param uses: columns used for the weight calculation
        @param input_func: function that calculates a dictionary with input arrays for the weight calculation
        @param mask_func: function that calculates a mask to apply before calculating the weights

    """
    year: str | int
    weight_name: str
    correction_set: Sequence[str] | str
    get_sf_file: Callable

    input_pars: dict = None
    aux: dict = None
    syst_key: str = "ValType"
    systematics: tuple[tuple[str, str] | str] | dict[str, str] = (("nominal", ""), "up", "down")

    # can also be set with LeptonWeightConfig.input and LeptonWeightConfig.mask
    uses: set = None
    input_func: Callable[[ak.Array], dict[ak.Array]] = None
    mask_func: Callable[[ak.Array], ak.Array] = None

    def __post_init__(self):
        if self.aux is None:
            self.aux = dict()
        self.x = DotDict(self.aux)
        if self.uses is None:
            self.uses = set()

        self.input_pars = {"year": str(self.year)} | (self.input_pars or {})

        if not isinstance(self.systematics, dict):
            systs = dict()
            for s in self.systematics:
                if isinstance(s, str):
                    systs[s] = f"_{s}"
                elif isinstance(s, Sequence) and len(s) == 2:
                    systs[s[0]] = f"_{s[1]}" if s[1] else ""
                else:
                    AssertionError(f"provided illegal systematics for {self}")
            self.systematics = systs
        else:
            self.systematics = {
                s: ("_" if ss else "") + ss.lstrip("_")
                for s, ss in self.systematics.items()
            }

    def copy(self, /, **changes):
        return dataclasses.replace(self, **changes)

    def input(self, func: Callable[[ak.Array], dict[ak.Array]] = None, uses: set = None) -> None:
        """
        Decorator to wrap a function *func* that should be registered as :py:meth:`input_func`
        which is used to calculate the input to the correctionlib corrector object.
        One should specify the used columns via the uses

        The function should accept one positional argument:

            - *events*, an awkward array from which the inouts are calculate


        The decorator does not return the wrapped function.
        """
        def decorator(func: Callable[[ak.Array], dict[ak.Array]]):
            self.input_func = func
            if uses:
                self.uses = self.uses | uses

        return decorator(func) if func else decorator

    def mask(self, func: Callable[[ak.Array], ak.Array] = None, uses: set = None) -> None:
        """
        Decorator to wrap a function *func* that should be registered as :py:meth:`mask_func`
        which is used to calculate the mask that should be applied to the lepton

        The function should accept one positional argument:

            - *events*, an awkward array from which the inouts are calculate


        The decorator does not return the wrapped function.
        """

        def decorator(func: Callable[[ak.Array], dict[ak.Array]]):
            self.mask_func = func
            self.uses = self.uses | uses

        return decorator(func) if func else decorator


@producer(
    # only run on mc
    mc_only=True,
    # lepton config, or function to determine the lepton weight config
    lepton_config=None,
)
def lepton_weights(
    self: Producer,
    events: ak.Array,
    mask: ak.Array = None,
    **kwargs,
) -> ak.Array:
    """
    Creates lepton weights using the correctionlib. Requires an external file in the config as specified in
    the *lepton_config*. E.g.

    .. code-block:: python

        cfg.x.external_files = DotDict.wrap({
            "electron_sf": "/afs/cern.ch/work/m/mrieger/public/mirrors/jsonpog-integration-9ea86c4c/POG/EGM/2017_UL/electron.json.gz",  # noqa
        })


    The name of the correction set should be specified in the *lepton_config*, as well as the year string
    ans other parameters passed to the correction evaluation (in the *input_pars* dict)

    *lepton_config* should be a LeptonWeightConfig or a function returning a LeptonWeightConfig. It
    can be adapted in a subclass

    Optionally, an *mask* can be supplied to compute the scale factor weight
    based only on a subset of leptons. This mask is applied on top of the *mask* specified in the *lepton_config*
    """

    variable_map = self.input_func(events)
    if self.mask_func or mask is not None:
        if self.mask_func:
            mask = (mask or True) & self.mask_func(events)
        assert mask.ndim == 2, f"unexpected mask dimension {mask.ndim}"
        variable_map = {
            key: value[mask] if value.ndim == 2 else value
            for key, value in variable_map.items()
        }
    variable_map |= self.input_pars

    # loop over systematics
    for syst, postfix in self.systematics.items():
        # initialize weights (take the weights if already applied once)
        # but is needed to apply multiple corrections from different correctionlib files
        weight = ak.ones_like(events.event)

        # add year, WorkingPoint, and ValType to inputs
        inputs = variable_map | {self.syst_key: syst}

        inputs = [
            inputs[inp.name]
            for inp in self.corrector.inputs
        ]
        sf = ak_evaluate(self.corrector, *inputs)

        # add the correct layout to it
        # sf = layout_ak_array(sf_flat, events.Electron.pt[electron_mask])

        # create the product over all electrons in one event and multiply with the existing weight
        weight = weight * ak.prod(sf, axis=1, mask_identity=False)

        # store it
        events = set_ak_column(events, f"{self.weight_name}{postfix}", weight, value_type=np.float32)
    return events


@lepton_weights.init
def lepton_weights_init(self: Producer) -> None:
    if callable(self.lepton_config):
        self.lepton_config = self.lepton_config()
    for key, value in dataclasses.asdict(self.lepton_config).items():
        if not hasattr(self, key):
            setattr(self, key, value)
    self.uses |= self.lepton_config.uses
    self.produces |= {f"{self.weight_name}{postfix}" for postfix in self.systematics.values()}


@lepton_weights.requires
def lepton_weights_requires(self: Producer, reqs: dict) -> None:
    if "external_files" in reqs or self.lepton_config is None:
        return

    from columnflow.tasks.external import BundleExternalFiles
    reqs["external_files"] = BundleExternalFiles.req(self.task)


@lepton_weights.setup
def lepton_weights_setup(
    self: Producer,
    reqs: dict,
    inputs: dict,
    reader_targets: InsertableDict,
) -> None:
    if self.lepton_config is None:
        return
    bundle = reqs["external_files"]

    # create the corrector
    import correctionlib
    correctionlib.highlevel.Correction.__call__ = correctionlib.highlevel.Correction.evaluate
    file_path = self.get_sf_file(bundle.files)
    if file_path.path.endswith(".gz"):
        correction_set = correctionlib.CorrectionSet.from_string(
            file_path.load(formatter="gzip").decode("utf-8"),
        )
    elif file_path.path.endswith(".json"):
        correction_set = correctionlib.CorrectionSet.from_file(
            file_path.path,
        )
    else:
        raise Exception(
            f"unsupported muon sf corrector file type: {file_path}",
        )

    self.corrector = correction_set[self.correction_set]


# example config for electron
ElectronRecoBelow20WeightConfig = LeptonWeightConfig(
    year="2018",
    weight_name="weight_electron_recobelow20",
    correction_set="UL-Electron-ID-SF",
    get_sf_file=lambda bundle: bundle.electron_sf,
    input_pars=dict(WorkingPoint="RecoBelow20"),
    systematics=dict(sf="", sfup="up", sfdown="down"),
)


@ElectronRecoBelow20WeightConfig.input(uses={"Electron.{pt,eta,eltaEtaSC,phi}"})
def electron_reco_input(events):
    return {
        "pt": events.Electron.pt,
        "eta": events.Electron.eta + events.Electron.deltaEtaSC,
        "phi": events.Electron.phi,
    }


@ElectronRecoBelow20WeightConfig.mask(uses={"Electron.pt"})
def electron_recobelow20_mask(events):
    return events.Electron.pt < 20


electron_recobelow20_weights = lepton_weights.derive(
    "electron_recobelow20_weights",
    cls_dict=dict(lepton_config=ElectronRecoBelow20WeightConfig),
)


# Use .copy to overwrite any property
ElectronRecoAbove20WeightConfig = ElectronRecoBelow20WeightConfig.copy(
    weight_name="weight_electron_recoabove20",
    input_pars=dict(WorkinPoint="RecoAbove20"),
)

@ElectronRecoAbove20WeightConfig.mask(uses={"Electron.pt"})
def electron_recoabove20_mask(events):
    return events.Electron.pt > 20

electron_recoabove20_weights = lepton_weights.derive(
    "electron_recoabove20_weights",
    cls_dict=dict(lepton_config=ElectronRecoAbove20WeightConfig),
)


# example config for top lepton mva
ElectronMVATOPWeightConfig = LeptonWeightConfig(
    year="2018",
    weight_name="weight_electron_mva",
    correction_set="sf_Electron",
    get_sf_file=lambda bundle: bundle.lepton_mva.sf,
    input_pars=dict(working_point="Tight"),
    syst_key="systematic",
    systematics=dict(central="") | {f"{d}_{s}": f"{s}_{d}" for s in ["stat", "syst"] for d in ["up", "down"]},
)


@ElectronMVATOPWeightConfig.input(uses={"Electron.{pt,eta}"})
def electron_mva_input(events):
    return {
        "pt": events.Electron.pt,
        "abseta": events.Electron.eta,
    }


electron_mva_weights = lepton_weights.derive(
    "electron_mva_weights",
    cls_dict=dict(lepton_config=ElectronMVATOPWeightConfig),
)


MuonMVATOPWeightConfig = ElectronMVATOPWeightConfig.copy(
    weight_name="weight_muon_mva",
    correction_set="sf_Muon",
)


@MuonMVATOPWeightConfig.input(uses={"Muon.{pt,eta}"})
def muon_mva_input(events):
    return {
        "pt": events.Muon.pt,
        "abseta": events.Muon.eta,
    }


muon_mva_weights = lepton_weights.derive(
    "muon_mva_weights",
    cls_dict=dict(lepton_config=MuonMVATOPWeightConfig),
)
