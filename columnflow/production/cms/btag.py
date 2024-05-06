# coding: utf-8

"""
Producers for btag scale factor weights.
"""

from __future__ import annotations

import law

from columnflow.production import Producer, producer
from columnflow.util import maybe_import, InsertableDict
from columnflow.columnar_util import set_ak_column, flat_np_view, layout_ak_array

np = maybe_import("numpy")
ak = maybe_import("awkward")

logger = law.logger.get_logger(__name__)


@producer(
    uses={
        "Jet.hadronFlavour", "Jet.eta", "Jet.pt",
    },
    # only run on mc
    mc_only=True,
    # function to determine the correction file
    get_btag_file=(lambda self, external_files: external_files.btag_sf_corr),
    # function to determine the muon weight config
    get_btag_config=(lambda self: self.config_inst.x.btag_sf),
)
def btag_weights(
    self: Producer,
    events: ak.Array,
    jet_mask: ak.Array | type(Ellipsis) = Ellipsis,
    negative_b_score_action: str = "ignore",
    negative_b_score_log_mode: str = "warning",
    **kwargs,
) -> ak.Array:
    """
    B-tag scale factor weight producer. Requires an external file in the config as under
    ``btag_sf_corr``:

    .. code-block:: python

        cfg.x.external_files = DotDict.wrap({
            "btag_sf_corr": "/afs/cern.ch/work/m/mrieger/public/mirrors/jsonpog-integration-9ea86c4c/POG/BTV/2017_UL/btagging.json.gz",  # noqa
        })

    *get_btag_file* can be adapted in a subclass in case it is stored differently in the external
    files.

    The name of the correction set, a list of JEC uncertainty sources which should be
    propagated through the weight calculation, and the column used for b-tagging should
    be given as an auxiliary entry in the config:

    .. code-block:: python

        cfg.x.btag_sf = ("deepJet_shape", ["Absolute", "FlavorQCD", ...], "btagDeepFlavB")

    *get_btag_config* can be adapted in a subclass in case it is stored differently in the config.

    Optionally, a *jet_mask* can be supplied to compute the scale factor weight based only on a
    subset of jets.

    The *negative_b_score_action* defines the procedure of how to handle jets with a negative b-tag.
    Supported modes are:

        - "ignore": the *jet_mask* is extended to exclude jets with b_score < 0
        - "remove": the btag_weight is set to 0 for jets with b_score < 0
        - "raise": an exception is raised

    The verbosity of the handling of jets with negative b-score can be
    set via *negative_b_score_log_mode*, which offers the following options:

        - ``"none"``: no message is given
        - ``"info"``: a `logger.info` message is given
        - ``"debug"``: a `logger.debug` message is given
        - ``"warning"``: a `logger.warning` message is given

    Resources:

       - https://twiki.cern.ch/twiki/bin/view/CMS/BTagShapeCalibration?rev=26
       - https://indico.cern.ch/event/1096988/contributions/4615134/attachments/2346047/4000529/Nov21_btaggingSFjsons.pdf
    """
    known_actions = ("ignore", "remove", "raise")
    if negative_b_score_action not in known_actions:
        raise ValueError(
            f"unknown negative_b_score_action '{negative_b_score_action}', "
            f"known values are {','.join(known_actions)}",
        )

    known_log_modes = ("none", "info", "debug", "warning")
    if negative_b_score_log_mode not in known_log_modes:
        raise ValueError(
            f"unknown negative_b_score_log_mode '{negative_b_score_log_mode}', "
            f"known values are {','.join(known_log_modes)}",
        )

    # get the total number of jets in the chunk
    n_jets_all = ak.sum(ak.num(events.Jet, axis=1))

    # check that the b-tag score is not negative for all jets considered in the SF calculation
    jets_negative_b_score = events.Jet[self.b_score_column][jet_mask] < 0
    if ak.any(jets_negative_b_score):
        msg_func = {
            "none": lambda msg: None,
            "info": logger.info,
            "warning": logger.warning,
            "debug": logger.debug,
        }[negative_b_score_log_mode]
        msg = f"In dataset {self.dataset_inst.name}, {ak.sum(jets_negative_b_score)} jets have a negative b-tag score."

        if negative_b_score_action == "ignore":
            msg_func(
                f"{msg} The *jet_mask* will be adjusted to exclude these jets, resulting in a "
                "*btag_weight* of 1 for these jets.",
            )
        elif negative_b_score_action == "remove":
            msg_func(
                f"{msg} The *btag_weight* will be set to 0 for these jets.",
            )
        elif negative_b_score_action == "raise":
            raise Exception(msg)

        # set jet mask to False when b_score is negative
        if jet_mask is Ellipsis:
            jet_mask = (events.Jet[self.b_score_column] >= 0)
        else:
            jet_mask = jet_mask & (events.Jet[self.b_score_column] >= 0)

    # get flat inputs, evaluated at jet_mask
    flavor = flat_np_view(events.Jet.hadronFlavour[jet_mask], axis=1)
    abs_eta = flat_np_view(abs(events.Jet.eta[jet_mask]), axis=1)
    pt = flat_np_view(events.Jet.pt[jet_mask], axis=1)
    b_discr = flat_np_view(events.Jet[self.b_score_column][jet_mask], axis=1)

    # helper to create and store the weight
    def add_weight(syst_name, syst_direction, column_name):
        # define a mask that selects the correct flavor to assign to, depending on the systematic
        flavor_mask = Ellipsis
        if syst_name in ["cferr1", "cferr2"]:
            # only apply to c flavor
            flavor_mask = flavor == 4
        elif syst_name != "central":
            # apply to all but c flavor
            flavor_mask = flavor != 4

        # get the flat scale factors
        sf_flat = self.btag_sf_corrector(
            syst_name if syst_name == "central" else f"{syst_direction}_{syst_name}",
            flavor[flavor_mask],
            abs_eta[flavor_mask],
            pt[flavor_mask],
            b_discr[flavor_mask],
        )

        # insert them into an array of ones whose length corresponds to the total number of jets
        sf_flat_all = np.ones(n_jets_all, dtype=np.float32)
        if jet_mask is Ellipsis:
            indices = flavor_mask
        else:
            indices = flat_np_view(jet_mask)
            if flavor_mask is not Ellipsis:
                indices = np.where(indices)[0][flavor_mask]
        sf_flat_all[indices] = sf_flat

        # enforce the correct shape and create the product over all jets per event
        sf = layout_ak_array(sf_flat_all, events.Jet.pt)

        if negative_b_score_log_mode == "remove":
            # set the weight to 0 for jets with negative btag score
            sf = ak.where(jets_negative_b_score, 0, sf)

        weight = ak.prod(sf, axis=1, mask_identity=False)

        # save the new column
        return set_ak_column(events, column_name, weight, value_type=np.float32)

    # when the requested uncertainty is a known jec shift, obtain the propagated effect and
    # do not produce additional systematics
    shift_inst = self.local_shift_inst
    if shift_inst.is_nominal:
        # nominal weight and those of all method intrinsic uncertainties
        events = add_weight("central", None, "btag_weight")
        for syst_name, col_name in self.btag_uncs.items():
            for direction in ["up", "down"]:
                name = col_name.format(year=self.config_inst.campaign.x.year)
                events = add_weight(
                    syst_name,
                    direction,
                    f"btag_weight_{name}_{direction}",
                )
                if syst_name in ["cferr1", "cferr2"]:
                    # for c flavor uncertainties, multiply the uncertainty with the nominal btag weight
                    events = set_ak_column(
                        events,
                        f"btag_weight_{name}_{direction}",
                        events.btag_weight * events[f"btag_weight_{name}_{direction}"],
                        value_type=np.float32,
                    )
    elif self.shift_is_known_jec_source:
        # TODO: year dependent jec variations fully covered?
        events = add_weight(
            f"jes{'' if self.jec_source == 'Total' else self.jec_source}",
            shift_inst.direction,
            f"btag_weight_jec_{self.jec_source}_{shift_inst.direction}",
        )
    else:
        # any other shift, just produce the nominal weight
        events = add_weight("central", None, "btag_weight")

    return events


def get_b_score_column(btag_config: tuple[str, list, str] | tuple[str, list]) -> str:
    """
    Helper function to resolve the btag score column from the btag configuration.

    :param btag_config: Entry in auxiliary `config_inst.x.btag_sf`, see example
    :py:meth:`~columflow.production.cms.btag.btag_weights`. If tuple has less
    than 3 entries, the column name is derived from the name of the correction set.
    :returns: Name of column that is required for the calculation of this set of corrections.
    """
    corrector_name = btag_config[0]
    if len(btag_config) >= 3:
        b_score_column = btag_config[2]
    else:
        # resolve the column name from the corrector name
        if "deepjet" in corrector_name.lower():
            b_score_column = "btagDeepFlavB"
        elif "particlenet" in corrector_name.lower():
            b_score_column = "btagPNetB"
        else:
            raise NotImplementedError(f"Cannot automatically determine btag column for Corrector '{corrector_name}'")
        logger.info(f"No btag column specified; defaulting to column '{b_score_column}'")

    # warn about potentially wrong column usage
    if (
        "deepjet" in corrector_name.lower() and "pnet" in b_score_column or
        "particlenet" in corrector_name.lower() and "deepflav" in b_score_column
    ):
        logger.warning(
            f"Using btag column '{b_score_column}' for BTag Corrector '{corrector_name}' is highly discouraged",
        )

    return b_score_column


@btag_weights.init
def btag_weights_init(self: Producer) -> None:
    # depending on the requested shift_inst, there are three cases to handle:
    #   1. when a JEC uncertainty is requested whose propagation to btag weights is known, the
    #      producer should only produce that specific weight column
    #   2. when the nominal shift is requested, the central weight and all variations related to the
    #      method-intrinsic shifts are produced
    #   3. when any other shift is requested, only create the central weight column
    btag_config = self.get_btag_config()
    self.b_score_column = get_b_score_column(btag_config)

    self.uses.add(f"Jet.{self.b_score_column}")

    shift_inst = getattr(self, "local_shift_inst", None)
    if not shift_inst:
        return

    # to handle this efficiently in one spot, store jec information
    self.jec_source = shift_inst.x.jec_source if shift_inst.has_tag("jec") else None
    btag_sf_jec_source = "" if self.jec_source == "Total" else self.jec_source
    self.shift_is_known_jec_source = (
        self.jec_source and
        btag_sf_jec_source in btag_config[1]
    )

    # save names of method-intrinsic uncertainties
    self.btag_uncs = {
        "hf": "hf",
        "lf": "lf",
        "hfstats1": "hfstats1_{year}",
        "hfstats2": "hfstats2_{year}",
        "lfstats1": "lfstats1_{year}",
        "lfstats2": "lfstats2_{year}",
        "cferr1": "cferr1",
        "cferr2": "cferr2",
    }

    # add uncertainty sources of the method itself
    if shift_inst.is_nominal:
        # nominal column
        self.produces.add("btag_weight")
        # all varied columns
        for col_name in self.btag_uncs.values():
            name = col_name.format(year=self.config_inst.campaign.x.year)
            for direction in ["up", "down"]:
                self.produces.add(f"btag_weight_{name}_{direction}")
    elif self.shift_is_known_jec_source:
        # jec varied column
        self.produces.add(f"btag_weight_jec_{self.jec_source}_{shift_inst.direction}")
    else:
        # only the nominal column
        self.produces.add("btag_weight")


@btag_weights.requires
def btag_weights_requires(self: Producer, reqs: dict) -> None:
    if "external_files" in reqs:
        return

    from columnflow.tasks.external import BundleExternalFiles
    reqs["external_files"] = BundleExternalFiles.req(self.task)


@btag_weights.setup
def btag_weights_setup(
    self: Producer,
    reqs: dict,
    inputs: dict,
    reader_targets: InsertableDict,
) -> None:
    bundle = reqs["external_files"]

    # create the btag sf corrector
    import correctionlib
    correctionlib.highlevel.Correction.__call__ = correctionlib.highlevel.Correction.evaluate
    correction_set = correctionlib.CorrectionSet.from_string(
        self.get_btag_file(bundle.files).load(formatter="gzip").decode("utf-8"),
    )
    corrector_name = self.get_btag_config()[0]
    self.btag_sf_corrector = correction_set[corrector_name]
