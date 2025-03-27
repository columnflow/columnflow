# coding: utf-8

"""
Electron related event weights.
"""

from __future__ import annotations

from columnflow.production import Producer, producer
from columnflow.calibration.cms.jets import ak_evaluate
from columnflow.util import maybe_import, InsertableDict
from columnflow.columnar_util import set_ak_column, flat_np_view, layout_ak_array

np = maybe_import("numpy")
ak = maybe_import("awkward")


@producer(
    uses={
        "Electron.pt", "Electron.eta", "Electron.deltaEtaSC", "Electron.phi",
    },
    produces={
        "electron_weight", "electron_weight_up", "electron_weight_down",
    },
    # only run on mc
    mc_only=True,
    # function to determine the correction file
    get_electron_file=(lambda self, external_files: external_files.electron_sf),
    # function to determine the electron weight config
    get_electron_config=(lambda self: self.config_inst.x.electron_sf_names),
)
def electron_weights(
    self: Producer,
    events: ak.Array,
    electron_mask: ak.Array | type(Ellipsis) = Ellipsis,
    var_map: dict(ak.Array) = dict(),
    syst_key: str = "ValType",
    postfixes: list(list(str)) = [("sf", ""), ("sfup", "_up"), ("sfdown", "_down")],
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

    The name of the correction sets, the year string for the weight evaluation, and the name of the
    working point should be given as an auxiliary entry in the config:

    .. code-block:: python

        cfg.x.electron_sf_names = {
            "Electron-ID-SF": [
                (2022, "RecoBelow20"),
                (2022, "Reco20to75"),
                (2022, "RecoAbove75")
            ],
        }

    *get_electron_config* can be adapted in a subclass in case it is stored differently in the
    config.

    Optionally, an *electron_mask* can be supplied to compute the scale factor weight
    based only on a subset of electrons.

    Optionally, an *electron_config* can be supplied to overwrite the scale factor year and wp
    which is useful to apply multiple scale factors of a single corrector.
    """

    variable_map = {
        "pt": flat_np_view(events.Electron.pt, axis=1),
        "eta": flat_np_view((
            events.Electron.eta +
            events.Electron.deltaEtaSC
        ), axis=1),
        "abseta": abs(flat_np_view((
            events.Electron.eta +
            events.Electron.deltaEtaSC
        ), axis=1)),
        "phi": flat_np_view(events.Electron.phi, axis=1),
    } | var_map

    # loop over systematics
    for syst, postfix in postfixes:

        # initialize weights (take the weights if already applied once)
        # TODO might be dangerous if the electron_weight is applied multiple times
        # but is needed to apply multiple corrections from different correctionlib files
        weight = getattr(events, f"electron_weight{postfix}", ak.ones_like(events.event))

        # loop over correctors
        for corrector_name, electron_configs in self.get_electron_config().items():
            corrector = self.electron_sf_correctors[corrector_name]

            # loop over scale factors within 1 corrector
            for electron_config in electron_configs:
                year, wp, mask_func = (*electron_config, None)[:3]

                # apply electron mask if defined in
                if mask_func:
                    electron_mask = mask_func(events)
                elif not electron_mask:
                    electron_mask = Ellipsis
                flat_electron_mask = flat_np_view(electron_mask)

                inputs = {
                    key: value[flat_electron_mask]
                    for key, value in variable_map.items()
                }

                # add year, WorkingPoint, and ValType to inputs
                inputs |= {
                    "year": year,
                    "WorkingPoint": wp,
                    syst_key: syst,
                }

                inputs = [
                    inputs[inp.name]
                    for inp in corrector.inputs
                ]
                sf_flat = ak_evaluate(corrector, *inputs)

                # add the correct layout to it
                sf = layout_ak_array(sf_flat, events.Electron.pt[electron_mask])

                # create the product over all electrons in one event and multiply with the existing weight
                weight = weight * ak.prod(sf, axis=1, mask_identity=False)

        # store it
        events = set_ak_column(events, f"electron_weight{postfix}", weight, value_type=np.float32)
    return events


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
    file_path = self.get_electron_file(bundle.files)
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

    corrector_names = self.get_electron_config().keys()
    self.electron_sf_correctors = {corrector_name: correction_set[corrector_name] for corrector_name in corrector_names}
