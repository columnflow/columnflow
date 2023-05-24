# coding: utf-8

"""
Muon related event weights.
"""

from __future__ import annotations

from columnflow.production import Producer, producer
from columnflow.util import maybe_import, InsertableDict
from columnflow.columnar_util import set_ak_column, flat_np_view, layout_ak_array


np = maybe_import("numpy")
ak = maybe_import("awkward")


@producer(
    uses={
        "Muon.pt", "Muon.eta",
    },
    produces={
        "muon_weight", "muon_weight_up", "muon_weight_down",
    },
    # only run on mc
    mc_only=True,
    # function to determine the correction file
    get_muon_file=(lambda self, external_files: external_files.muon_sf),
    # function to determine the muon weight config
    get_muon_config=(lambda self: self.config_inst.x.muon_sf_names),
)
def muon_weights(
    self: Producer,
    events: ak.Array,
    muon_mask: ak.Array | type(Ellipsis) = Ellipsis,
    **kwargs,
) -> ak.Array:
    """
    Creates muon weights using the correctionlib. Requires an external file in the config under
    ``muon_sf``:

    .. code-block:: python

        cfg.x.external_files = DotDict.wrap({
            "muon_sf": "/afs/cern.ch/user/m/mrieger/public/mirrors/jsonpog-integration-d0a522ea/POG/MUO/2017_UL/muon_z.json.gz",  # noqa
        })

    *get_muon_file* can be adapted in a subclass in case it is stored differently in the external
    files.

    The name of the correction set and the year string for the weight evaluation should be given as
    an auxiliary entry in the config:

    .. code-block:: python

        cfg.x.muon_sf_names = ("NUM_TightRelIso_DEN_TightIDandIPCut", "2017_UL")

    *get_muon_config* can be adapted in a subclass in case it is stored differently in the config.

    Optionally, a *muon_mask* can be supplied to compute the scale factor weight based only on a
    subset of muons.
    """
    # flat absolute eta and pt views
    abs_eta = flat_np_view(abs(events.Muon.eta[muon_mask]), axis=1)
    pt = flat_np_view(events.Muon.pt[muon_mask], axis=1)

    # loop over systematics
    for syst, postfix in [
        ("sf", ""),
        ("systup", "_up"),
        ("systdown", "_down"),
    ]:
        sf_flat = self.muon_sf_corrector(self.year, abs_eta, pt, syst)

        # add the correct layout to it
        sf = layout_ak_array(sf_flat, events.Muon.pt[muon_mask])

        # create the product over all muons in one event
        weight = ak.prod(sf, axis=1, mask_identity=False)

        # store it
        events = set_ak_column(events, f"muon_weight{postfix}", weight, value_type=np.float32)

    return events


@muon_weights.requires
def muon_weights_requires(self: Producer, reqs: dict) -> None:
    if "external_files" in reqs:
        return

    from columnflow.tasks.external import BundleExternalFiles
    reqs["external_files"] = BundleExternalFiles.req(self.task)


@muon_weights.setup
def muon_weights_setup(self: Producer, reqs: dict, inputs: dict, reader_targets: InsertableDict) -> None:
    bundle = reqs["external_files"]

    # create the corrector
    import correctionlib
    correctionlib.highlevel.Correction.__call__ = correctionlib.highlevel.Correction.evaluate
    correction_set = correctionlib.CorrectionSet.from_string(
        self.get_muon_file(bundle.files).load(formatter="gzip").decode("utf-8"),
    )
    corrector_name, self.year = self.get_muon_config()
    self.muon_sf_corrector = correction_set[corrector_name]

    # check versions
    assert self.muon_sf_corrector.version in [1]
