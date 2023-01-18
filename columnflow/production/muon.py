# coding: utf-8

"""
Muon related event weights.
"""

from __future__ import annotations

from columnflow.production import Producer, producer
from columnflow.util import maybe_import
from columnflow.columnar_util import set_ak_column, flat_np_view, layout_ak_array

np = maybe_import("numpy")
ak = maybe_import("awkward")


@producer(
    uses={
        "nMuon", "Muon.pt", "Muon.eta",
    },
    produces={
        "muon_weight", "muon_weight_up", "muon_weight_down",
    },
)
def muon_weights(
    self: Producer,
    events: ak.Array,
    muon_mask: ak.Array | type(Ellipsis) = Ellipsis,
    **kwargs,
) -> ak.Array:
    """
    Reads the muon scale factor from the external file given in the config,
    using the keys from the corresponding auxiliary entry in the config.
    As of 10.2022, external files originating from a specific commit of
    https://gitlab.cern.ch/cms-nanoAOD/jsonpog-integration/-/tree/master/POG/MUO

    Example of external file in config:

    .. code-block:: python

        "muon_sf": ("/afs/cern.ch/user/m/mrieger/public/mirrors/jsonpog-integration-d0a522ea/POG/MUO/2017_UL/muon_z.json.gz", "v1"),  # noqa

    Example of the corresponding auxiliary entry to read the correct sets from the json file:

    .. code-block:: python

        cfg.x.muon_sf_names = ("NUM_TightRelIso_DEN_TightIDandIPCut", "2017_UL")

    Optionally, a *muon_mask* can be supplied to compute the scale factor weight
    based only on a subset of muons.
    """
    # fail when running on data
    if self.dataset_inst.is_data:
        raise ValueError("attempt to compute muon weights in data")

    # get year string
    sf_year = self.config_inst.x.muon_sf_names[1]

    # flat absolute eta and pt views
    abs_eta = flat_np_view(abs(events.Muon.eta[muon_mask]), axis=1)
    pt = flat_np_view(events.Muon.pt[muon_mask], axis=1)

    # loop over systematics
    for syst, postfix in [
        ("sf", ""),
        ("systup", "_up"),
        ("systdown", "_down"),
    ]:
        sf_flat = self.muon_sf_corrector(sf_year, abs_eta, pt, syst)

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
def muon_weights_setup(self: Producer, reqs: dict, inputs: dict) -> None:
    bundle = reqs["external_files"]

    # create the corrector
    import correctionlib
    correctionlib.highlevel.Correction.__call__ = correctionlib.highlevel.Correction.evaluate
    correction_set = correctionlib.CorrectionSet.from_string(
        bundle.files.muon_sf.load(formatter="gzip").decode("utf-8"),
    )
    corrector_name = self.config_inst.x.muon_sf_names[0]
    self.muon_sf_corrector = correction_set[corrector_name]
