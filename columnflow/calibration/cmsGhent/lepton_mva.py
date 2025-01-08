"""
Code to add lepton MVA to NanoAOD
"""

# from collections import OrderedDict

from columnflow.calibration import Calibrator, calibrator
from columnflow.production import producer
from columnflow.util import maybe_import
from columnflow.columnar_util import set_ak_column, InsertableDict
# from columnflow.columnar_util_Ghent import TetraVec
from columnflow.tasks.external import BundleExternalFiles

np = maybe_import("numpy")
ak = maybe_import("awkward")
coffea = maybe_import("coffea")
maybe_import("coffea.nanoevents.methods.nanoaod")


@producer(
    uses={
        f"{lep}.{p}"
        for lep in ["Muon", "Electron"]
        for p in ["pt", "eta", "miniPFRelIso_all", "miniPFRelIso_chg", "jetRelIso", "dxy", "dz", "jetIdx",
                  "jetNDauCharged", "jetPtRelv2", "pfRelIso03_all", "sip3d"]
    } | {"Jet.btagDeepFlavB", "Electron.mvaFall17V2noIso", "Muon.segmentComp"},
    produces={
        f"{lep}.{p}"
        for lep in ["Muon", "Electron"]
        for p in ["abseta", "miniPFRelIso_neutral", "jetPtRatio", "jetBTagDeepFlavor", "log_absdxy", "log_absdz"]
    },
)
def lepton_mva_inputs_producer(self: Calibrator, events: ak.Array, **kwargs) -> ak.Array:
    """
    collects all inputs to the TOP lepton MVA (v1) and makes the necessary transformations
    """
    for lepton_name in ["Muon", "Electron"]:
        lepton = events[lepton_name]
        matched_jet = lepton.jetIdx
        is_matched = matched_jet != -1

        # replace jetRelIso by the equivalent used in the MVA
        # if no matched jet, jetRelIso == pfRelIso04_all in NanoAOD, but MVA assumes then zero
        events = set_ak_column(events, f"{lepton_name}.jetPtRatio", 1. / (lepton.jetRelIso + 1.))

        # matched deepJet score of closest jet if any (zero otherwise)
        btag_values = ak.pad_none(events.Jet.btagDeepFlavB, target=1)[matched_jet]
        events = set_ak_column(events, f"{lepton_name}.jetBTagDeepFlavor", ak.where(is_matched, btag_values, 0.))

        # impact parameters in log
        for impact in ["dxy", "dz"]:
            events = set_ak_column(events, f"{lepton_name}.log_abs" + impact, np.log(np.abs(lepton[impact])))

        # Relative mini-isolation with neutral PF objects
        events = set_ak_column(events, f"{lepton_name}.miniPFRelIso_neutral",
                    lepton.miniPFRelIso_all - lepton.miniPFRelIso_chg)

        # absolute eta
        events = set_ak_column(events, f"{lepton_name}.abseta", np.abs(lepton.eta))

    return events


_shared_mva_inputs = [
    "pt",
    "eta",
    "jetNDauCharged",
    "miniPFRelIso_chg",
    "miniPFRelIso_neutral",
    "jetPtRelv2",
    "jetPtRatio",
    "pfRelIso03_all",
    "jetBTagDeepFlavor",
    "sip3d",
    "log_absdxy",
    "log_absdz",
]

lepton_mva_inputs = {
    "Electron": [*_shared_mva_inputs, "mvaFall17V2noIso"],  # add "lost hits" for version 2
    "Muon": [*_shared_mva_inputs, "segmentComp"],
    "Lepton": _shared_mva_inputs,
}


@calibrator(
    uses={lepton_mva_inputs_producer},
    produces={"Electron.mvaTOP", "Muon.mvaTOP"},
    sandbox="bash::$CF_BASE/sandboxes/venv_lepton_mva.sh",
)
def lepton_mva_producer(self: Calibrator, events: ak.Array, **kwargs) -> ak.Array:
    """
    Produces the TOP lepton MVA (v1) scores.
    Requires an external file in the config under ``lepton_mva.weights``:

    .. code-block:: python

        cfg.x.external_files = DotDict.wrap({
            "lepton_mva":
                "weights": {
                    "Muon": f"YOURDIRECTORY/mu_TOPUL18_XGB.weights.bin",
                    "Electron": f"YOURDIRECTORY/weights/el_TOPUL18_XGB.weights.bin",
                },
        })

    Requires adding the environment venv_lepton_mva which included xgboost to the analysis or config. E.g.

    analysis_inst.x.bash_sandboxes = [
        "$CF_BASE/sandboxes/cf.sh",
        "$CF_BASE/sandboxes/venv_lepton_mva.sh",
    ]

    """
    events = self[lepton_mva_inputs_producer](events)
    for lepton in ["Muon", "Electron"]:
        features = [events[lepton][p] for p in lepton_mva_inputs[lepton]]
        # set None values (e.g. when there is no matched jet) to zero
        features = ak.fill_none(features, 0.)
        # flatten into a numpy array of shape (ninstances, nfeatures)
        counts = ak.num(features[0])
        features = np.transpose(np.array(ak.flatten(features, axis=2)))
        # make c-contiguous (rows are stored as contiguous blocks of memory.)
        features = np.ascontiguousarray(features)

        if np.any(features):
            # call xgboost predictor
            scores = self.mva[lepton].inplace_predict(features)
            # unflatten into an awkward array
            scores = ak.unflatten(scores, counts)
            # set the scores as an additional field for muons
        else:
            scores = ak.zeros_like(events[lepton][lepton_mva_inputs[lepton][0]], dtype=np.float32)
        events = set_ak_column(events, f"{lepton}.mvaTOP", scores)

    return events


@lepton_mva_producer.requires
def lepton_mva_producer_requires(self: Calibrator, reqs: dict) -> None:
    if "external_files" in reqs:
        return
    reqs["external_files"] = BundleExternalFiles.req(self.task)


@lepton_mva_producer.setup
def lepton_mva_producer_setup(
        self: Calibrator,
        reqs: dict,
        inputs: dict,
        reader_targets: InsertableDict,
) -> None:
    bundle = reqs["external_files"]

    # create the xgboost predictor
    import xgboost

    self.mva = {}

    for lepton in ["Electron", "Muon"]:
        self.mva[lepton] = xgboost.Booster()
        self.mva[lepton].load_model(bundle.files.lepton_mva["weights"][lepton].path)
