# coding: utf-8

"""
Muon related event weights.
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
        "Muon.pt", "Muon.eta", "Muon.phi",
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
            "muon_sf": "/afs/cern.ch/work/m/mrieger/public/mirrors/jsonpog-integration-9ea86c4c/POG/MUO/2022_Summer22/muon_Z.json.gz",  # noqa
        })

    *get_muon_file* can be adapted in a subclass in case it is stored differently in the
    external files.

    The name of the correction sets, the year string for the weight evaluation, and the name of the
    working point should be given as an auxiliary entry in the config:

    .. code-block:: python

        cfg.x.muon_sf_names = {
            "NUM_MediumID_DEN_TrackerMuons": [
                (f"{year}{corr_postfix}_UL", lambda events: events.Muon.pt > 15),
            ],
            "NUM_LoosePFIso_DEN_MediumID": [
                (f"{year}{corr_postfix}_UL", lambda events: events.Muon.pt > 15),
            ]
        }

    *get_muon_config* can be adapted in a subclass in case it is stored differently in the
    config.

    Optionally, an *muon_mask* can be supplied to compute the scale factor weight
    based only on a subset of muons.

    Optionally, an *muon_config* can be supplied to overwrite the scale factor year and wp
    which is useful to apply multiple scale factors of a single corrector.
    """

    variable_map = {
        "pt": flat_np_view(events.Muon.pt, axis=1),
        "eta": flat_np_view(events.Muon.eta, axis=1),
        "phi": flat_np_view(events.Muon.phi, axis=1),
    }

    # loop over systematics
    for syst, postfix in [
        ("nominal", ""),
        ("systup", "_up"),
        ("systdown", "_down"),
    ]:

        # initialize weights (take the weights if already applied once)
        # TODO might be dangerous if the electron_weight is applied multiple times
        # but is needed to apply multiple corrections from different correctionlib files
        weight = getattr(events, f"muon_weight{postfix}", ak.ones_like(events.event))

        # loop over correctors
        for corrector_name, muon_configs in self.get_muon_config().items():
            corrector = self.muon_sf_correctors[corrector_name]

            # loop over scale factors within 1 corrector
            for muon_config in muon_configs:
                year, mask_func = (*muon_config, None)[:2]

                # apply muon mask if defined in
                if mask_func:
                    muon_mask = mask_func(events)
                elif not muon_mask:
                    muon_mask = Ellipsis
                flat_muon_mask = flat_np_view(muon_mask)

                inputs = {
                    key: value[flat_muon_mask]
                    for key, value in variable_map.items()
                }

                # add year, WorkingPoint, and ValType to inputs
                inputs |= {
                    "year": year,
                    "scale_factors": syst,
                }

                inputs = [
                    inputs[inp.name]
                    for inp in corrector.inputs
                ]
                sf_flat = ak_evaluate(corrector, *inputs)

                # add the correct layout to it
                sf = layout_ak_array(sf_flat, events.Muon.pt[muon_mask])

                # create the product over all muons in one event and multiply with the existing weight
                weight = weight * ak.prod(sf, axis=1, mask_identity=False)

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

    corrector_names = self.get_muon_config().keys()
    self.muon_sf_correctors = {corrector_name: correction_set[corrector_name] for corrector_name in corrector_names}

    # check versions
    for corrector_name, muon_sf_corrector in self.muon_sf_correctors.items():
        if muon_sf_corrector.version not in (1, 2):
            raise Exception(
                f"unsuppprted muon sf corrector version {muon_sf_corrector.version} of {corrector_name}",
            )
