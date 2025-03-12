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
        @syst_key: systematic variable key of the correction set
        @param systematics: tuple of tuples (or dict) of systematic variable input of the correction set and the postfix linked to the systematic 
        @param aux: dictionary with other useful information
        @param uses: columns used for the weight calculation
        @param input_func: function that calculates a dictionary with input arrays for the weight calculation
        @param mask_func: function that calculates a mask to apply before calculating the weights

    """  # noqa
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
        self.aux = self.aux or dict()
        self.x = DotDict(self.aux)
        self.uses = self.uses or set()

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
            f"unsupported lepton sf corrector file type: {file_path}",
        )

    self.corrector = correction_set[self.correction_set]


#
#  example config for muon
#

ElectronBaseWeightConfig = LeptonWeightConfig(
    year=None,
    weight_name="weight_electron",
    correction_set="UL-Electron-ID-SF",
    get_sf_file=lambda bundle: bundle.electron_sf,
    input_pars=dict(WorkingPoint=None),
    systematics=dict(sf="", sfup="up", sfdown="down"),
)


@ElectronBaseWeightConfig.input(uses={"Electron.{pt,eta,deltaEtaSC,phi}"})
def electron_input(events):
    return {
        "pt": events.Electron.pt,
        "eta": events.Electron.eta + events.Electron.deltaEtaSC,
        "phi": events.Electron.phi,
    }


ElectronRecoBelow20WeightConfig = ElectronBaseWeightConfig.copy(
    year=2018,
    weight_name="weight_electron_recobelow20",
    input_pars=dict(WorkinPoint="RecoBelow20"),
)


@ElectronRecoBelow20WeightConfig.mask(uses={"Electron.pt"})
def electron_recobelow20_mask(events):
    return events.Electron.pt < 20


MuonBaseWeightConfig = LeptonWeightConfig(
    year=None,
    weight_name="weight_muon",
    correction_set="NUM_MediumID_DEN_TrackerMuons",
    get_sf_file=lambda bundle: bundle.muon_sf,
    syst_key="scale_factors",
    systematics=dict(nominal="", systup="_up", systdown="_down"),
)


@MuonBaseWeightConfig.input(uses={"Muon.{pt,eta}"})
def muon_mva_input(events):
    return {
        "pt": events.Muon.pt,
        "eta": events.Muon.eta,
        "phi": events.Muon.phi,
    }


#
# bundle of all lepton weight producers
#

@producer(
    # only run on mc
    mc_only=True,
    # lepton config bundle, function to determine the location of a list of LeptonWeightConfig's
    lepton_configs=lambda self: self.config_inst.x.lepton_weight_configs,
    config_naming=lambda self, cfg: cfg.weight_name,
)
def bundle_lepton_weights(
    self: Producer,
    events: ak.Array,
    **kwargs,
) -> ak.Array:

    for lepton_weight_producer in self.uses:
        events = self[lepton_weight_producer](events, **kwargs)

    return events


@bundle_lepton_weights.init
def bundle_lepton_weights_init(self: Producer) -> None:

    lepton_configs = self.lepton_configs()
    for config in lepton_configs:
        self.uses.add(lepton_weights.derive(
            self.config_naming(config),
            cls_dict=dict(lepton_config=config),
        ))

    self.produces |= self.uses
