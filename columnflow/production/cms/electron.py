# coding: utf-8

"""
Electron related event weights.
"""

from __future__ import annotations

from columnflow.production import Producer, producer
from columnflow.util import maybe_import, InsertableDict
from columnflow.columnar_util import set_ak_column, flat_np_view, layout_ak_array

np = maybe_import("numpy")
ak = maybe_import("awkward")


@producer(
    uses={
        "Electron.pt", "Electron.eta", "Electron.deltaEtaSC",
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

        cfg.x.electron_sf_names = ("UL-Electron-ID-SF", "2017", "wp80iso")

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

    # loop over systematics
    for syst, postfix in [
        ("sf", ""),
        ("sfup", "_up"),
        ("sfdown", "_down"),
    ]:
        sf_flat = self.electron_sf_corrector(self.year, syst, self.wp, sc_eta, pt)

        # add the correct layout to it
        sf = layout_ak_array(sf_flat, events.Electron.pt[electron_mask])

        # create the product over all electrons in one event
        weight = ak.prod(sf, axis=1, mask_identity=False)

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
    correction_set = correctionlib.CorrectionSet.from_string(
        self.get_electron_file(bundle.files).load(formatter="gzip").decode("utf-8"),
    )
    corrector_name, self.year, self.wp = self.get_electron_config()
    self.electron_sf_corrector = correction_set[corrector_name]

    # check versions
    if self.electron_sf_corrector.version not in (2,):
        raise Exception(
            f"unsuppprted electron sf corrector version {self.electron_sf_corrector.version}",
        )
