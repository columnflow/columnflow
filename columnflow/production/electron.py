# coding: utf-8

"""
Electron related event weights.
"""

from columnflow.production import Producer, producer
from columnflow.util import maybe_import
from columnflow.columnar_util import set_ak_column, layout_ak_array

np = maybe_import("numpy")
ak = maybe_import("awkward")


@producer(
    uses={
        "Electron.pt", "Electron.eta", "Electron.deltaEtaSC",
    },
    produces={
        "electron_weight", "electron_weight_up", "electron_weight_down",
    },
)
def electron_weights(self: Producer, events: ak.Array, **kwargs) -> ak.Array:
    """
    Electron scale factor producer. Requires an external file in the config as (e.g.)

    .. code-block:: python

        "electron_sf": ("/afs/cern.ch/user/m/mrieger/public/mirrors/jsonpog-integration-d0a522ea/POG/EGM/2017_UL/electron.json.gz", "v1"),  # noqa

    as well as an auxiliary entry in the config to refer to three values in a tuple, i.e., the name
    of the correction set, a year string to be used as a correctionlib input, and the name of the
    selection working point.

    .. code-block:: python

        cfg.x.electron_sf_names = ("UL-Electron-ID-SF", "2017", "wp80iso")
    """
    if self.dataset_inst.is_data:
        return events

    # get year string and working point name
    year, wp = self.config_inst.x.electron_sf_names[1:]

    # flat super cluster eta and pt views
    sc_eta = ak.flatten(events.Electron.eta + events.Electron.deltaEtaSC, axis=1)
    pt = ak.flatten(events.Electron.pt, axis=1)

    # loop over systematics
    for syst, postfix in [
        ("sf", ""),
        ("sfup", "_up"),
        ("sfdown", "_down"),
    ]:
        sf_flat = self.electron_sf_corrector.evaluate(year, syst, wp, sc_eta, pt).astype(np.float32)

        # add the correct layout to it
        sf = layout_ak_array(sf_flat, events.Electron.pt)

        # create the product over all electrons in one event
        weight = ak.prod(sf, axis=1, mask_identity=False)

        # store it
        events = set_ak_column(events, f"electron_weight{postfix}", weight)

    return events


@electron_weights.requires
def electron_weights_requires(self: Producer, reqs: dict) -> None:
    if self.dataset_inst.is_data or "external_files" in reqs:
        return

    from columnflow.tasks.external import BundleExternalFiles
    reqs["external_files"] = BundleExternalFiles.req(self.task)


@electron_weights.setup
def electron_weights_setup(self: Producer, reqs: dict, inputs: dict) -> None:
    self.electron_sf_corrector = None

    if self.dataset_inst.is_data:
        return

    bundle = reqs["external_files"]

    # create the corrector
    import correctionlib
    correction_set = correctionlib.CorrectionSet.from_string(
        bundle.files.electron_sf.load(formatter="gzip").decode("utf-8"),
    )
    corrector_name = self.config_inst.x.electron_sf_names[0]
    self.electron_sf_corrector = correction_set[corrector_name]
