# coding: utf-8

"""
Producers for btag scale factor weights.
"""

from __future__ import annotations

import dataclasses

import law

from columnflow.production import Producer, producer
from columnflow.util import maybe_import, load_correction_set
from columnflow.columnar_util import set_ak_column, DotDict, TAFConfig, full_like
from columnflow.types import Any

np = maybe_import("numpy")
ak = maybe_import("awkward")


logger = law.logger.get_logger(__name__)


@dataclasses.dataclass
class BTagSFConfig(TAFConfig):
    correction_set: str
    jec_sources: list[str]
    discriminator: str = ""  # when empty, set in post init based on correction set
    corrector_kwargs: dict[str, Any] = dataclasses.field(default_factory=dict)

    def __post_init__(self):
        cs = self.correction_set.lower()
        if not self.discriminator:
            if "deepjet" in cs:
                self.discriminator = "btagDeepFlavB"
            elif "particlenet" in cs:
                self.discriminator = "btagPNetB"
            else:
                raise NotImplementedError(
                    "cannot identify btag discriminator for correction set "
                    f"'{self.correction_set}', please set it manually",
                )

        # warn about potentially wrong column usage
        if (
            ("deepjet" in cs and "pnet" in self.discriminator) or
            ("particlenet" in cs and "deepflav" in self.discriminator)
        ):
            logger.warning(
                f"using btag column '{self.discriminator}' for btag sf corrector "
                f"'{self.correction_set}' is highly discouraged",
            )

    @classmethod
    def new(
        cls,
        obj: BTagSFConfig | tuple[str, list[str]] | tuple[str, list[str], str],
    ) -> BTagSFConfig:
        # purely for backwards compatibility with the old tuple format
        if isinstance(obj, cls):
            return obj
        if isinstance(obj, (list, tuple)) or isinstance(obj, tuple):
            return cls(*obj)
        if isinstance(obj, dict):
            return cls(**obj)
        raise ValueError(f"cannot convert {obj} to BTagSFConfig")


@producer(
    uses={"Jet.{pt,eta,phi,mass,hadronFlavour}"},
    # only run on mc
    mc_only=True,
    # configurable weight name
    weight_name="btag_weight",
    # function to determine the correction file
    get_btag_file=(lambda self, external_files: external_files.btag_sf_corr),
    # function to determine the btag sf config
    get_btag_config=(lambda self: BTagSFConfig.new(self.config_inst.x.btag_sf)),
)
def btag_weights(
    self: Producer,
    events: ak.Array,
    task: law.Task,
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

        cfg.x.btag_sf = BTagSFConfig(
            correction_set="deepJet_shape",
            jec_sources=["Absolute", "FlavorQCD", ...],
            discriminator="btagDeepFlavB",
            corrector_kwargs={...},
        )

    *get_btag_config* can be adapted in a subclass in case it is stored differently in the config.

    Optionally, a *jet_mask* can be supplied to compute the scale factor weight based only on a
    subset of jets.

    The *negative_b_score_action* defines the procedure of how to handle jets with a negative b-tag.
    Supported modes are:

        - "ignore": the *jet_mask* is extended to exclude jets with b_score < 0
        - "remove": the scale factor is set to 0 for jets with b_score < 0, resulting in an overall
            btag weight of 0 for the event
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

    # check that the b-tag score is not negative for all jets considered in the SF calculation
    discr = events.Jet[self.btag_config.discriminator]
    jets_negative_b_score = discr[jet_mask] < 0
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
                "btag weight of 1 for these jets.",
            )
        elif negative_b_score_action == "remove":
            msg_func(
                f"{msg} The btag weight will be set to 0 for these jets.",
            )
        elif negative_b_score_action == "raise":
            raise Exception(msg)

        # set jet mask to False when b_score is negative
        jet_mask = (discr >= 0) & (True if jet_mask is Ellipsis else jet_mask)

    # get inputs, evaluated at jet_mask
    flavor = events.Jet.hadronFlavour[jet_mask]
    abs_eta = abs(events.Jet.eta[jet_mask])
    pt = events.Jet.pt[jet_mask]
    discr = discr[jet_mask]

    # fix edge cases where the discriminator is non-finite
    discr = ak.where(np.isfinite(discr), discr, 0.0)

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

        # prepare arguments
        variable_map = {
            "systematic": syst_name if syst_name == "central" else f"{syst_direction}_{syst_name}",
            "flavor": flavor[flavor_mask],
            "abseta": abs_eta[flavor_mask],
            "pt": pt[flavor_mask],
            "discriminant": discr[flavor_mask],
            **self.btag_config.corrector_kwargs,
        }

        # get the scale factors
        sf = self.btag_sf_corrector(*(
            variable_map[inp.name]
            for inp in self.btag_sf_corrector.inputs
        ))

        if negative_b_score_action == "remove":
            # set the weight to 0 for jets with negative btag score
            sf = ak.where(jets_negative_b_score, 0.0, sf)

        weight = ak.prod(sf, axis=1, mask_identity=False)

        # save the new column
        return set_ak_column(events, column_name, weight, value_type=np.float32)

    # when the requested uncertainty is a known jec shift, obtain the propagated effect and
    # do not produce additional systematics
    shift_inst = task.global_shift_inst
    if shift_inst.is_nominal:
        # nominal weight and those of all method intrinsic uncertainties
        events = add_weight("central", None, self.weight_name)
        for syst_name, col_name in self.btag_uncs.items():
            for direction in ["up", "down"]:
                events = add_weight(
                    syst_name,
                    direction,
                    f"{self.weight_name}_{col_name}_{direction}",
                )
                if syst_name in ["cferr1", "cferr2"]:
                    # for c flavor uncertainties, multiply the uncertainty with the nominal btag weight
                    events = set_ak_column(
                        events,
                        f"{self.weight_name}_{col_name}_{direction}",
                        events[self.weight_name] * events[f"{self.weight_name}_{col_name}_{direction}"],
                        value_type=np.float32,
                    )
    elif self.shift_is_known_jec_source:
        # TODO: year dependent jec variations fully covered?
        events = add_weight(
            f"jes{'' if self.jec_source == 'Total' else self.jec_source}",
            shift_inst.direction,
            f"{self.weight_name}_jec_{self.jec_source}_{shift_inst.direction}",
        )
    else:
        # any other shift, just produce the nominal weight
        events = add_weight("central", None, self.weight_name)

    return events


@btag_weights.post_init
def btag_weights_post_init(self: Producer, task: law.Task, **kwargs) -> None:
    # depending on the requested shift_inst, there are three cases to handle:
    #   1. when a JEC uncertainty is requested whose propagation to btag weights is known, the
    #      producer should only produce that specific weight column
    #   2. when the nominal shift is requested, the central weight and all variations related to the
    #      method-intrinsic shifts are produced
    #   3. when any other shift is requested, only create the central weight column

    # NOTE: we currently setup the produced columns only during the post_init. This means
    # that the `produces` of this Producer will be empty during task initialization, meaning
    # that this Producer would be skipped if one would directly request it on the command line

    # gather info
    self.btag_config = self.get_btag_config()
    shift_inst = task.global_shift_inst

    # use the btag discriminator
    self.uses.add(f"Jet.{self.btag_config.discriminator}")

    # to handle this efficiently in one spot, store jec information
    self.jec_source = shift_inst.x.jec_source if shift_inst.has_tag("jec") else None
    btag_sf_jec_source = "" if self.jec_source == "Total" else self.jec_source
    self.shift_is_known_jec_source = (
        self.jec_source and btag_sf_jec_source in self.btag_config.jec_sources
    )

    # names of method-intrinsic uncertainties, mapped to how they are namend in produced columns
    self.btag_uncs = {
        "hf": "hf",
        "lf": "lf",
        "hfstats1": "hfstats1",
        "hfstats2": "hfstats2",
        "lfstats1": "lfstats1",
        "lfstats2": "lfstats2",
        "cferr1": "cferr1",
        "cferr2": "cferr2",
    }

    # add uncertainty sources of the method itself
    if shift_inst.is_nominal:
        # nominal column
        self.produces.add(self.weight_name)
        # all varied columns
        for col_name in self.btag_uncs.values():
            self.produces.add(f"{self.weight_name}_{col_name}_{{up,down}}")
    elif self.shift_is_known_jec_source:
        # jec varied column
        self.produces.add(f"{self.weight_name}_jec_{self.jec_source}_{shift_inst.direction}")
    else:
        # only the nominal column
        self.produces.add(self.weight_name)


@btag_weights.requires
def btag_weights_requires(
    self: Producer,
    task: law.Task,
    reqs: dict[str, DotDict[str, Any]],
    **kwargs,
) -> None:
    if "external_files" in reqs:
        return

    from columnflow.tasks.external import BundleExternalFiles
    reqs["external_files"] = BundleExternalFiles.req(task)


@btag_weights.setup
def btag_weights_setup(
    self: Producer,
    task: law.Task,
    reqs: dict[str, DotDict[str, Any]],
    inputs: dict[str, Any],
    reader_targets: law.util.InsertableDict,
    **kwargs,
) -> None:
    # load the btag sf corrector
    btag_file = self.get_btag_file(reqs["external_files"].files)
    self.btag_sf_corrector = load_correction_set(btag_file)[self.btag_config.correction_set]


@dataclasses.dataclass
class BTagWPSFConfig(TAFConfig):
    # name of the jet collection
    jet_name: str = "Jet"
    # name of the b-tag score column
    btag_column: str = "btagUParTAK4B"
    # name of the correction set to load
    correction_set: str = "UParTAK4_merged"
    # mapping of working point names and thresholds
    # ! values are merely examples and should be overwritten
    btag_wps: dict[str, float] = dataclasses.field(default_factory=lambda: {
        "loose": 0.0246,
        "medium": 0.1272,
        "tight": 0.4648,
        "xtight": 0.6298,
        "xxtight": 0.9739,
    })
    # edges for histogram re-binning when set
    # ! note that, when given, these edges need to be a valid subset of the original bin edges of the counting hists
    pt_edges: tuple[float, ...] | None = None
    abs_eta_edges: tuple[float, ...] | None = None
    # key of the histogram to save in selector hists
    hist_key: str = "btag_wp_counts"
    # name of the weight column to produce
    weight_name: str = "btag_weight"


@producer(
    # only run on mc
    mc_only=True,
    # function to determine the correction file
    get_btag_wp_file=(lambda self, external_files: external_files.btag_wp_sf_corr),
    # function to configure how to retrieve the BTagWPSFConfig
    get_btag_wp_sf_config=(lambda self: self.config_inst.x.btag_wp_sf_config),
)
def btag_wp_weights(
    self: Producer,
    events: ak.Array,
    task: law.Task,
    jet_mask: ak.Array | type(Ellipsis) = Ellipsis,
    **kwargs,
) -> ak.Array:
    """
    B-tag scale factor weight producer using the fixed working point method. Requires an external file in the config as under
    ``btag_sf_corr``:

    .. code-block:: python

        cfg.x.external_files = DotDict.wrap({
            "btag_wp_sf_corr": "/afs/cern.ch/work/m/mrieger/public/mirrors/jsonpog-integration-9ea86c4c/POG/BTV/2017_UL/btagging.json.gz",  # noqa
        })

    *get_btag_wp_file* can be adapted in a subclass in case it is stored differently in the external
    files.

    The name of the correction set, a list of JEC uncertainty sources which should be
    propagated through the weight calculation, and the column used for b-tagging should
    be given as an auxiliary entry in the config:

    .. code-block:: python

        cfg.x.btag_sf = BTagSFConfig(
            correction_set="btagUParTAK4B_merged",
            jec_sources=["Absolute", "FlavorQCD", ...],
            discriminator="btagUParTAK4B",
            corrector_kwargs={...},
        )

    *get_btag_wp_sf_config* can be adapted in a subclass in case it is stored differently in the config.

    Resources:
        - https://btv-wiki.docs.cern.ch/PerformanceCalibration/fixedWPSFRecommendations/#recommendations-for-fixed-working-point-sfs  # noqa
        - https://cms-analysis-corrections.docs.cern.ch/corrections_era/Run3-24CDEReprocessingFGHIPrompt-Summer24-NanoAODv15/BTV/2026-01-30/#btagging_preliminaryjsongz  # noqa
        - https://indico.cern.ch/event/1583955/contributions/6771046/attachments/3176162/5648591/BTVreportHIGPAG_18112025.pdf  # noqa
    """
    from hist import loc

    # get inputs
    discr = events.Jet[self.cfg.btag_column]
    flavor = events[self.cfg.jet_name].hadronFlavour
    abs_eta = abs(events[self.cfg.jet_name].eta)
    pt = events[self.cfg.jet_name].pt

    # helpers to get the sf and efficiencies
    def get_sf_and_eff(
        wp: str,
        variable_map: dict[str, ak.Array]
    ) -> tuple[ak.Array, ak.Array]:

        variable_map = variable_map.copy()

        # for compatibility with BTV correction set naming
        convert_wp_str = {"loose": "L", "medium": "M", "tight": "T", "xtight": "XT", "xxtight": "XXT"}
        wp_btv = convert_wp_str[wp]
        variable_map["working_point"] = wp_btv

        # get scale factor
        sf = self.btag_wp_sf_corrector(
            *(variable_map[inp.name] for inp in self.btag_wp_sf_corrector.inputs)
        )

        # for compatibility with btag efficiency corrector
        variable_map_eff = variable_map.copy()
        variable_map_eff.pop("working_point", None)
        variable_map_eff.pop("systematic", None)
        variable_map_eff["wp"] = wp
        variable_map_eff["abs_eta"] = variable_map_eff["abseta"]

        # get efficiency
        wp_eff = self.wp_eff_corrector(
            *(variable_map_eff[inp.name] for inp in self.wp_eff_corrector.inputs)
        )

        return sf, wp_eff

    # sort the WPs therehold values and make the WP strings all lowercase
    wps = dict(sorted(((k.lower(), v) for k, v in self.cfg.btag_wps.items()), key=lambda tpl: tpl[1]))

    # helper to create and store the weight
    def add_wp_weight(syst_name, syst_direction, column_name):

        # initialize b-tag event weight with ones
        btag_weight = ak.ones_like(events.event, dtype=np.float32)

        # apply WP masks to each jet in the event falling between the WP thresholds
        for lower_key, upper_key in zip([None] + list(wps.keys()), list(wps.keys()) + [None]):
            jet_mask = (discr >= wps.get(lower_key, -np.inf)) & (discr < wps.get(upper_key, np.inf))

            # prepare arguments with specific WP jet_mask applied
            variable_map = {
                "systematic": syst_name if syst_name == "central" else f"{syst_direction}_{syst_name}",
                "flavor": flavor[jet_mask],
                "abseta": abs_eta[jet_mask],
                "pt": pt[jet_mask],
            }

            # handle three cases for sf calculation
            if lower_key is None:  # jets that fail lowest WP
                sf_fail, wp_eff_fail = get_sf_and_eff(upper_key, variable_map)
                sf_pass, wp_eff_pass = 1.0, 1.0
            elif upper_key is None:  # jets that pass highest WP
                sf_fail, wp_eff_fail = 0.0, 0.0
                sf_pass, wp_eff_pass = get_sf_and_eff(lower_key, variable_map)
            else:  # jets that pass one WP but fail the next one
                sf_fail, wp_eff_fail = get_sf_and_eff(upper_key, variable_map)
                sf_pass, wp_eff_pass = get_sf_and_eff(lower_key, variable_map)

            # if ak.any(wp_eff_pass - wp_eff_fail <= 0):
            #     raise ValueError(
            #         """
            #         Encountered a negative or null value when comparing working point efficiencies.
            #         The efficiency of a tigher WP should be, by definition, lower than that of a looser one.
            #         This error was likely caused by a lack of statistics in a measurement bin.
            #         To ensure enough statistics, consider a more coarse binning in pT/eta, or merging process groups.
            #         """
            #     )

            # calculate scale factor term per jet, depending on WP efficiency
            # sf_term = (sf_pass * wp_eff_pass - sf_fail * wp_eff_fail) / (wp_eff_pass - wp_eff_fail)
            denominator = ak.where(wp_eff_pass - wp_eff_fail <= 0, 1.0, wp_eff_pass - wp_eff_fail)
            sf_term = (sf_pass * wp_eff_pass - sf_fail * wp_eff_fail) / denominator

            # handle edge cases where the SF becomes negative (unphysical) or zero (killing the event)
            sf_term = ak.where(sf_term <= 0, 1.0, sf_term)

            # calculate final b-tag event weight as a product of the individual jet scale factor terms
            btag_weight = btag_weight * ak.prod(sf_term, axis=1, mask_identity=False)

        return set_ak_column(events, column_name, btag_weight, value_type=np.float32)

    shift_inst = task.global_shift_inst
    if shift_inst.is_nominal:
        # nominal weight and those of all method intrinsic uncertainties
        events = add_wp_weight("central", None, self.cfg.weight_name)
        for syst_name, col_name in self.btag_uncs.items():
            for direction in ["up", "down"]:
                events = add_wp_weight(
                    syst_name,
                    direction,
                    f"{self.cfg.weight_name}_{col_name}_{direction}",
                )
    else:
        # any other shift, just produce the nominal weight
        events = add_wp_weight("central", None, self.cfg.weight_name)

    events = set_ak_column(events, self.cfg.weight_name, full_like(events.event, 1.0, dtype=np.float32))

    return events


@btag_wp_weights.post_init
def btag_wp_weights_post_init(self: Producer, task: law.Task, **kwargs) -> None:

    # retrieve and store the config and shift information
    self.cfg = self.get_btag_wp_sf_config()
    shift_inst = task.global_shift_inst

    # add used columns
    self.uses.add(f"{self.cfg.jet_name}.{{pt,eta,phi,mass,hadronFlavour,{self.cfg.btag_column}}}")

    # names of method-intrinsic uncertainties, mapped to how they are namend in produced columns
    self.btag_uncs = {
        "hdamp": "hdamp",
    }

    # add uncertainty sources of the method itself
    if shift_inst.is_nominal:
        # nominal column
        self.produces.add(self.cfg.weight_name)
        # all varied columns
        for col_name in self.btag_uncs.values():
            self.produces.add(f"{self.cfg.weight_name}_{col_name}_{{up,down}}")
    else:
        # only the nominal column
        self.produces.add(self.cfg.weight_name)


@btag_wp_weights.requires
def btag_wp_weights_requires(
    self: Producer,
    task: law.Task,
    reqs: dict[str, DotDict[str, Any]],
    **kwargs,
) -> None:
    if "external_files" not in reqs:
        from columnflow.tasks.external import BundleExternalFiles
        reqs["external_files"] = BundleExternalFiles.req(task)

    if "selection_stats" not in reqs:
        from columnflow.tasks.selection import MergeSelectionStats
        reqs["selection_stats"] = MergeSelectionStats.req_different_branching(
            task,
            branch=-1 if task.is_workflow() else 0,
        )


@btag_wp_weights.setup
def btag_wp_weights_setup(
    self: Producer,
    task: law.Task,
    reqs: dict[str, DotDict[str, Any]],
    inputs: dict[str, Any],
    reader_targets: law.util.InsertableDict,
    **kwargs,
) -> None:
    import hist
    import correctionlib as clib
    import correctionlib.convert

    # load the btag sf corrector
    btag_wp_file = self.get_btag_wp_file(reqs["external_files"].files)
    self.btag_wp_sf_corrector = load_correction_set(btag_wp_file)[self.cfg.correction_set]

    # load the count histograms and compute efficiencies
    hists = inputs["selection_stats"]["hists"].load(formatter="pickle")
    h = hists[self.cfg.hist_key]
    # optionally rebin pt and abs_eta axes
    if self.cfg.pt_edges:
        h = h[{"pt": hist.rebin(edges=self.cfg.pt_edges)}]
    if self.cfg.abs_eta_edges:
        h = h[{"abs_eta": hist.rebin(edges=self.cfg.abs_eta_edges)}]
    # compute efficiencies
    counts_total = h[{"wp": "total"}]
    effs = h[{"wp": [wp for wp in h.axes["wp"] if wp != "total"]}]
    eff_values = effs.view()
    eff_values[...] /= counts_total.view()[..., None]
    eff_values[np.isnan(eff_values)] = 1.0
    self.btag_effs = effs

    # convert to clib corrector
    self.btag_effs.name = "btag_efficiencies"
    self.btag_effs.label = "eff"
    self.wp_eff_corrector = clib.convert.from_histogram(self.btag_effs).to_evaluator()
