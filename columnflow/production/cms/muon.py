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
    # produces in the init
    # only run on mc
    mc_only=True,
    # function to determine the correction file
    get_muon_file=(lambda self, external_files: external_files.muon_sf),
    # function to determine the muon weight config
    get_muon_config=(lambda self: self.config_inst.x.muon_sf_names),
    weight_name="muon_weight",
    supported_versions=(1, 2),
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
            "muon_sf": "/afs/cern.ch/work/m/mrieger/public/mirrors/jsonpog-integration-9ea86c4c/POG/MUO/2017_UL/muon_z.json.gz",  # noqa
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

    variable_map = {
        "year": self.year,
        "abseta": abs_eta,
        "eta": abs_eta,
        "pt": pt,
    }

    # loop over systematics
    for syst, postfix in [
        ("sf", ""),
        ("systup", "_up"),
        ("systdown", "_down"),
    ]:
        # get the inputs for this type of variation
        variable_map_syst = {
            **variable_map,
            "scale_factors": "nominal" if syst == "sf" else syst,  # syst key in 2022
            "ValType": syst,  # syst key in 2017
        }
        inputs = [variable_map_syst[inp.name] for inp in self.muon_sf_corrector.inputs]
        sf_flat = self.muon_sf_corrector(*inputs)

        # add the correct layout to it
        sf = layout_ak_array(sf_flat, events.Muon.pt[muon_mask])

        # create the product over all muons in one event
        weight = ak.prod(sf, axis=1, mask_identity=False)

        # store it
        events = set_ak_column(events, f"{self.weight_name}{postfix}", weight, value_type=np.float32)

    return events


@muon_weights.requires
def muon_weights_requires(self: Producer, reqs: dict) -> None:
    if "external_files" in reqs:
        return

    from columnflow.tasks.external import BundleExternalFiles
    reqs["external_files"] = BundleExternalFiles.req(self.task)


@muon_weights.setup
def muon_weights_setup(
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
        self.get_muon_file(bundle.files).load(formatter="gzip").decode("utf-8"),
    )
    corrector_name, self.year = self.get_muon_config()
    self.muon_sf_corrector = correction_set[corrector_name]

    # check versions
    if self.supported_versions and self.muon_sf_corrector.version not in self.supported_versions:
        raise Exception(f"unsuppprted muon sf corrector version {self.muon_sf_corrector.version}")


@muon_weights.init
def muon_weights_init(self: Producer, **kwargs) -> None:
    weight_name = self.weight_name
    self.produces |= {weight_name, f"{weight_name}_up", f"{weight_name}_down"}
