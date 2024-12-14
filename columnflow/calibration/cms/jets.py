# coding: utf-8

"""
Jet energy corrections and jet resolution smearing.
"""

import functools

import law

from columnflow.types import Any
from columnflow.calibration import Calibrator, calibrator
from columnflow.calibration.util import ak_random, propagate_met
from columnflow.production.util import attach_coffea_behavior
from columnflow.util import maybe_import, InsertableDict, DotDict
from columnflow.columnar_util import set_ak_column, layout_ak_array, optional_column as optional

np = maybe_import("numpy")
ak = maybe_import("awkward")
correctionlib = maybe_import("correctionlib")

logger = law.logger.get_logger(__name__)


#
# helper functions
#

set_ak_column_f32 = functools.partial(set_ak_column, value_type=np.float32)


def get_evaluators(
    correction_set: correctionlib.highlevel.CorrectionSet,
    names: list[str],
) -> list[Any]:
    """
    Helper function to get a list of correction evaluators from a
    :external+correctionlib:py:class:`correctionlib.highlevel.CorrectionSet` object given
    a list of *names*. The *names* can refer to either simple or compound
    corrections.

    :param correction_set: evaluator provided by :external+correctionlib:doc:`index`
    :param names: List of names of corrections to be applied
    :raises RuntimeError: If a requested correction in *names* is not available
    :return: List of compounded corrections, see
        :external+correctionlib:py:class:`correctionlib.highlevel.CorrectionSet`
    """
    # raise nice error if keys not found
    available_keys = set(correction_set.keys()).union(correction_set.compound.keys())
    missing_keys = set(names) - available_keys
    if missing_keys:
        raise RuntimeError("corrections not found:" + "".join(
            f"\n  - {name}" for name in names if name in missing_keys
        ) + "\navailable:" + "".join(
            f"\n  - {name}" for name in sorted(available_keys)
        ))

    # retrieve the evaluators
    return [
        correction_set.compound[name]
        if name in correction_set.compound
        else correction_set[name]
        for name in names
    ]


def ak_evaluate(evaluator: correctionlib.highlevel.Correction, *args) -> float:
    """
    Evaluate a :external+correctionlib:py:class:`correctionlib.highlevel.Correction`
    using one or more :external+ak:py:class:`awkward arrays <ak.Array>` as inputs.

    :param evaluator: Evaluator instance
    :raises ValueError: If no :external+ak:py:class:`awkward arrays <ak.Array>` are provided
    :return: The correction factor derived from the input arrays
    """
    # fail if no arguments
    if not args:
        raise ValueError("Expected at least one argument.")

    # collect arguments that are awkward arrays
    ak_args = [
        arg for arg in args if isinstance(arg, ak.Array)
    ]

    # broadcast akward arrays together and flatten
    if ak_args:
        bc_args = ak.broadcast_arrays(*ak_args)
        flat_args = (
            np.asarray(ak.flatten(bc_arg, axis=None))
            for bc_arg in bc_args
        )
        output_layout_array = bc_args[0]
    else:
        flat_args = iter(())
        output_layout_array = None

    # multiplex flattened and non-awkward inputs
    all_flat_args = [
        next(flat_args) if isinstance(arg, ak.Array) else arg
        for arg in args
    ]

    # apply evaluator to flattened/multiplexed inputs
    result = evaluator.evaluate(*all_flat_args)

    # apply broadcasted layout to result
    if output_layout_array is not None:
        result = layout_ak_array(result, output_layout_array)

    return result


#
# jet energy corrections
#

# define default functions for jec calibrator
def get_jerc_file_default(self: Calibrator, external_files: DotDict) -> str:
    """
    Function to obtain external correction files for JEC and/or JER.

    By default, this function extracts the location of the jec correction
    files from the current config instance *config_inst*. The key of the
    external file depends on the jet collection. For ``Jet`` (AK4 jets), this
    resolves to ``jet_jerc``, and for ``FatJet`` it is resolved to
    ``fat_jet_jerc``.

    .. code-block:: python

        cfg.x.external_files = DotDict.wrap({
            "jet_jerc": "/afs/cern.ch/work/m/mrieger/public/mirrors/jsonpog-integration-9ea86c4c/POG/JME/2017_UL/jet_jerc.json.gz",
            "fat_jet_jerc": "/afs/cern.ch/work/m/mrieger/public/mirrors/jsonpog-integration-9ea86c4c/POG/JME/2017_UL/fatJet_jerc.json.gz",
        })

    :param external_files: Dictionary containing the information about the file location
    :return: path or url to correction file(s)
    """ # noqa

    # get config
    try_attrs = ("get_jec_config", "get_jer_config")
    jerc_config = None
    for try_attr in try_attrs:
        try:
            jerc_config = getattr(self, try_attr)()
        except AttributeError:
            continue
        else:
            break

    # fail if not found
    if jerc_config is None:
        raise ValueError(
            "could not retrieve jer/jec config, none of the following methods "
            f"were found: {try_attrs}",
        )

    # first check config for user-supplied `external_file_key`
    ext_file_key = jerc_config.get("external_file_key", None)
    if ext_file_key is not None:
        return external_files[ext_file_key]

    # if not found, try to resolve from jet collection name and fail if not standard NanoAOD
    if self.jet_name not in get_jerc_file_default.map_jet_name_file_key:
        available_keys = ", ".join(sorted(get_jerc_file_default.map_jet_name_file_key))
        raise ValueError(
            f"could not determine external file key for jet collection '{self.jet_name}', "
            f"name is not one of standard NanoAOD jet collections: {available_keys}",
        )

    # return external file
    ext_file_key = get_jerc_file_default.map_jet_name_file_key[self.jet_name]
    return external_files[ext_file_key]


# default external file keys for known jet collections
get_jerc_file_default.map_jet_name_file_key = {
    "Jet": "jet_jerc",
    "FatJet": "fat_jet_jerc",
}


def get_jec_config_default(self: Calibrator) -> DotDict:
    """
    Load config relevant to the jet energy corrections (JEC).

    By default, this is extracted from the current *config_inst*,
    assuming the JEC configurations are stored under the 'jec'
    aux key. Separate configurations should be specified for each
    jet collection, using the collection name as a key. For example,
    the configuration for the default jet collection ``Jet`` will
    be retrieved from the following config entry:

    .. code-block:: python

        self.config_inst.x.jec.Jet

    Used in :py:meth:`~.jec.setup_func`.

    :return: Dictionary containing configuration for jet energy calibration
    """
    jec_cfg = self.config_inst.x.jec

    # check for old-style config
    if self.jet_name not in jec_cfg:
        # if jet collection is `Jet`, issue deprecation warning
        if self.jet_name == "Jet":
            logger.warning_once(
                f"{id(self)}_depr_jec_config",
                "config aux 'jec' does not contain key for input jet "
                f"collection '{self.jet_name}'. This may be due to "
                "an outdated config. Continuing under the assumption that "
                "the entire 'jec' entry refers to this jet collection. "
                "This assumption will be removed in future versions of "
                "columnflow, so please adapt the config according to the "
                "documentation to remove this warning and ensure future "
                "compatibility of the code.",
            )
            return jec_cfg

        # otherwise raise exception
        raise ValueError(
            "config aux 'jec' does not contain key for input jet "
            f"collection '{self.jet_name}'.",
        )

    return jec_cfg[self.jet_name]


@calibrator(
    uses={
        optional("fixedGridRhoFastjetAll"),
        optional("Rho.fixedGridRhoFastjetAll"),
        attach_coffea_behavior,
    },
    # name of the jet collection to calibrate
    jet_name="Jet",
    # name of the associated MET collection
    met_name="MET",
    # name of the associated Raw MET collection
    raw_met_name="RawMET",
    # custom uncertainty sources, defaults to config when empty
    uncertainty_sources=None,
    # toggle for propagation to MET
    propagate_met=True,
    # function to determine the correction file
    get_jec_file=get_jerc_file_default,
    # function to determine the jec configuration dict
    get_jec_config=get_jec_config_default,
)
def jec(
    self: Calibrator,
    events: ak.Array,
    min_pt_met_prop: float = 15.0,
    max_eta_met_prop: float = 5.2,
    **kwargs,
) -> ak.Array:
    """Performs the jet energy corrections (JECs) and uncertainty shifts using the
    :external+correctionlib:doc:`index`, optionally
    propagating the changes to the MET.

    The *jet_name* should be set to the name of the NanoAOD jet collection to calibrate
    (default: ``Jet``, i.e. AK4 jets).

    Requires an external file in the config pointing to the JSON files containing the JECs.
    The file key can be specified via an optional ``external_file_key`` in the ``jec`` config entry.
    If not given, the file key will be determined automatically based on the jet collection name:
    ``jet_jerc`` for ``Jet`` (AK4 jets), ``fat_jet_jerc`` for``FatJet`` (AK8 jets). A full set of JSON files
    can be specified as:

    .. code-block:: python

        cfg.x.external_files = DotDict.wrap({
            "jet_jerc": "/afs/cern.ch/work/m/mrieger/public/mirrors/jsonpog-integration-9ea86c4c/POG/JME/2017_UL/jet_jerc.json.gz",
            "fat_jet_jerc": "/afs/cern.ch/work/m/mrieger/public/mirrors/jsonpog-integration-9ea86c4c/POG/JME/2017_UL/fatJet_jerc.json.gz",
        })

    For more file-grained control, the *get_jec_file* can be adapted in a subclass in case it is stored
    differently in the external files

    The JEC configuration should be an auxiliary entry in the config, specifying the correction
    details under "jec". Separate configs should be given for each jet collection to calibrate,
    using the jet collection name as a subkey. An example of a valid configuration for correction
    AK4 jets with JEC is:

    .. code-block:: python

        cfg.x.jec = {
            "Jet": {
                "campaign": "Summer19UL17",
                "version": "V5",
                "jet_type": "AK4PFchs",
                "levels": ["L1L2L3Res"],  # or individual correction levels
                "levels_for_type1_met": ["L1FastJet"],
                "uncertainty_sources": [
                    "Total",
                    "CorrelationGroupMPFInSitu",
                    "CorrelationGroupIntercalibration",
                    "CorrelationGroupbJES",
                    "CorrelationGroupFlavor",
                    "CorrelationGroupUncorrelated",
                ]
            },
        }

    *get_jec_config* can be adapted in a subclass in case it is stored differently in the config.

    If running on data, the datasets must have an auxiliary field *jec_era* defined, e.g. "RunF",
    or an auxiliary field *era*, e.g. "F".

    This instance of :py:class:`~columnflow.calibration.Calibrator` is
    initialized with the following parameters by default:

    :param events: awkward array containing events to process

    :param min_pt_met_prop: If *propagate_met* variable is ``True`` propagate the updated jet values
        to the missing transverse energy (MET) using
        :py:func:`~columnflow.calibration.util.propagate_met` for events where
        ``met.pt > *min_pt_met_prop*``.
    :param max_eta_met_prop: If *propagate_met* variable is ``True`` propagate the updated jet
        values to the missing transverse energy (MET) using
        :py:func:`~columnflow.calibration.util.propagate_met` for events where
        ``met.eta > *min_eta_met_prop*``.
    """ # noqa
    # use local variable for convenience
    jet_name = self.jet_name

    # calculate uncorrected pt, mass
    events = set_ak_column_f32(events, f"{jet_name}.pt_raw", events[jet_name].pt * (1 - events[jet_name].rawFactor))
    events = set_ak_column_f32(events, f"{jet_name}.mass_raw", events[jet_name].mass * (1 - events[jet_name].rawFactor))

    def correct_jets(*, pt, eta, phi, area, rho, evaluator_key="jec"):
        # variable naming convention
        variable_map = {
            "JetA": area,
            "JetEta": eta,
            "JetPt": pt,
            "JetPhi": phi,
            "Rho": ak.values_astype(rho, np.float32),
        }

        # apply all correctors sequentially, updating the pt each time
        full_correction = ak.ones_like(pt, dtype=np.float32)
        for corrector in self.evaluators[evaluator_key]:
            # determine correct inputs (change depending on corrector)
            inputs = [
                variable_map[inp.name]
                for inp in corrector.inputs
            ]
            correction = ak_evaluate(corrector, *inputs)
            # update pt for subsequent correctors
            variable_map["JetPt"] = variable_map["JetPt"] * correction
            full_correction = full_correction * correction

        return full_correction

    # obtain rho, which might be located at different routes, depending on the nano version
    rho = (
        events.fixedGridRhoFastjetAll
        if "fixedGridRhoFastjetAll" in events.fields
        else events.Rho.fixedGridRhoFastjetAll
    )

    # correct jets with only a subset of correction levels
    # (for calculating TypeI MET correction)
    if self.propagate_met:
        # get correction factors
        jec_factors_subset_type1_met = correct_jets(
            pt=events[jet_name].pt_raw,
            eta=events[jet_name].eta,
            phi=events[jet_name].phi,
            area=events[jet_name].area,
            rho=rho,
            evaluator_key="jec_subset_type1_met",
        )

        # temporarily apply the new factors with only subset of corrections
        events = set_ak_column_f32(events, f"{jet_name}.pt", events[jet_name].pt_raw * jec_factors_subset_type1_met)
        events = set_ak_column_f32(events, f"{jet_name}.mass", events[jet_name].mass_raw * jec_factors_subset_type1_met)
        events = self[attach_coffea_behavior](events, collections=[jet_name], **kwargs)

        # store pt and phi of the full jet system for MET propagation, including a selection in raw info
        # see https://twiki.cern.ch/twiki/bin/view/CMS/JECAnalysesRecommendations?rev=19#Minimum_jet_selection_cuts
        met_prop_mask = (events[jet_name].pt_raw > min_pt_met_prop) & (abs(events[jet_name].eta) < max_eta_met_prop)
        jetsum = events[jet_name][met_prop_mask].sum(axis=1)
        jetsum_pt_subset_type1_met = jetsum.pt
        jetsum_phi_subset_type1_met = jetsum.phi

    # factors for full jet correction with all levels
    jec_factors = correct_jets(
        pt=events[jet_name].pt_raw,
        eta=events[jet_name].eta,
        phi=events[jet_name].phi,
        area=events[jet_name].area,
        rho=rho,
        evaluator_key="jec",
    )

    # apply full jet correction
    events = set_ak_column_f32(events, f"{jet_name}.pt", events[jet_name].pt_raw * jec_factors)
    events = set_ak_column_f32(events, f"{jet_name}.mass", events[jet_name].mass_raw * jec_factors)
    rawFactor = ak.nan_to_num(1 - events[jet_name].pt_raw / events[jet_name].pt, nan=0.0)
    events = set_ak_column_f32(events, f"{jet_name}.rawFactor", rawFactor)
    events = self[attach_coffea_behavior](events, collections=[jet_name], **kwargs)

    # nominal met propagation
    if self.propagate_met:
        # get pt and phi of all jets after correcting
        jetsum = events[jet_name][met_prop_mask].sum(axis=1)
        jetsum_pt_all_levels = jetsum.pt
        jetsum_phi_all_levels = jetsum.phi
        # propagate changes to MET, starting from jets corrected with subset of JEC levels
        # (recommendation is to propagate only L2 corrections and onwards)
        met_pt, met_phi = propagate_met(
            jetsum_pt_subset_type1_met,
            jetsum_phi_subset_type1_met,
            jetsum_pt_all_levels,
            jetsum_phi_all_levels,
            events[self.raw_met_name].pt,
            events[self.raw_met_name].phi,
        )
        events = set_ak_column_f32(events, f"{self.met_name}.pt", met_pt)
        events = set_ak_column_f32(events, f"{self.met_name}.phi", met_phi)

    # variable naming conventions
    variable_map = {
        "JetEta": events[jet_name].eta,
        "JetPt": events[jet_name].pt_raw,
    }

    # jet energy uncertainty components
    for name, evaluator in self.evaluators["junc"].items():
        # get uncertainty
        inputs = [variable_map[inp.name] for inp in evaluator.inputs]
        jec_uncertainty = ak_evaluate(evaluator, *inputs)

        # apply jet uncertainty shifts
        events = set_ak_column_f32(
            events, f"{jet_name}.pt_jec_{name}_up", events[jet_name].pt * (1.0 + jec_uncertainty),
        )
        events = set_ak_column_f32(
            events, f"{jet_name}.pt_jec_{name}_down", events[jet_name].pt * (1.0 - jec_uncertainty),
        )
        events = set_ak_column_f32(
            events, f"{jet_name}.mass_jec_{name}_up", events[jet_name].mass * (1.0 + jec_uncertainty),
        )
        events = set_ak_column_f32(
            events, f"{jet_name}.mass_jec_{name}_down", events[jet_name].mass * (1.0 - jec_uncertainty),
        )

        # propagate shifts to MET
        if self.propagate_met:
            jet_pt_up = events[jet_name][met_prop_mask][f"pt_jec_{name}_up"]
            jet_pt_down = events[jet_name][met_prop_mask][f"pt_jec_{name}_down"]
            met_pt_up, met_phi_up = propagate_met(
                jetsum_pt_all_levels,
                jetsum_phi_all_levels,
                jet_pt_up,
                events[jet_name][met_prop_mask].phi,
                met_pt,
                met_phi,
            )
            met_pt_down, met_phi_down = propagate_met(
                jetsum_pt_all_levels,
                jetsum_phi_all_levels,
                jet_pt_down,
                events[jet_name][met_prop_mask].phi,
                met_pt,
                met_phi,
            )
            events = set_ak_column_f32(events, f"{self.met_name}.pt_jec_{name}_up", met_pt_up)
            events = set_ak_column_f32(events, f"{self.met_name}.pt_jec_{name}_down", met_pt_down)
            events = set_ak_column_f32(events, f"{self.met_name}.phi_jec_{name}_up", met_phi_up)
            events = set_ak_column_f32(events, f"{self.met_name}.phi_jec_{name}_down", met_phi_down)

    return events


@jec.init
def jec_init(self: Calibrator) -> None:
    jec_cfg = self.get_jec_config()

    sources = self.uncertainty_sources
    if sources is None:
        sources = jec_cfg.uncertainty_sources

    # register used jet columns
    self.uses.add(f"{self.jet_name}.{{pt,eta,phi,mass,area,rawFactor}}")

    # register produced jet columns
    self.produces.add(f"{self.jet_name}.{{pt,mass,rawFactor}}")

    # add shifted jet variables
    self.produces |= {
        f"{self.jet_name}.{shifted_var}_jec_{junc_name}_{junc_dir}"
        for shifted_var in ("pt", "mass")
        for junc_name in sources
        for junc_dir in ("up", "down")
    }

    # add MET variables
    if self.propagate_met:
        self.uses.add(f"{self.raw_met_name}.{{pt,phi}}")
        self.produces.add(f"{self.met_name}.{{pt,phi}}")

        # add shifted MET variables
        self.produces |= {
            f"{self.met_name}.{shifted_var}_jec_{junc_name}_{junc_dir}"
            for shifted_var in ("pt", "phi")
            for junc_name in sources
            for junc_dir in ("up", "down")
        }


@jec.requires
def jec_requires(self: Calibrator, reqs: dict) -> None:
    if "external_files" in reqs:
        return

    from columnflow.tasks.external import BundleExternalFiles
    reqs["external_files"] = BundleExternalFiles.req(self.task)


@jec.setup
def jec_setup(self: Calibrator, reqs: dict, inputs: dict, reader_targets: InsertableDict) -> None:
    """
    Load the correct jec files using the :py:func:`from_string` method of the
    :external+correctionlib:py:class:`correctionlib.highlevel.CorrectionSet`
    function and apply the corrections as needed.

    The source files for the :external+correctionlib:py:class:`correctionlib.highlevel.CorrectionSet`
    instance are extracted with the :py:meth:`~.jec.get_jec_file`.

    Uses the member function :py:meth:`~.jec.get_jec_config` to construct the
    required keys, which are based on the following information about the JEC:

        - levels
        - campaign
        - version
        - jet_type

    A corresponding example snippet wihtin the *config_inst* could like something
    like this:

    .. code-block:: python

        cfg.x.jec = DotDict.wrap({
            "Jet": {
                # campaign name for this JEC correctiono
                "campaign": f"Summer19UL{year2}{jerc_postfix}",
                # version of the corrections
                "version": "V7",
                # Type of jets that the corrections should be applied on
                "jet_type": "AK4PFchs",
                # relevant levels in the derivation process of the JEC
                "levels": ["L1FastJet", "L2Relative", "L2L3Residual", "L3Absolute"],
                # relevant levels in the derivation process of the Type 1 MET JEC
                "levels_for_type1_met": ["L1FastJet"],
                # names of the uncertainties to be applied
                "uncertainty_sources": [
                    "Total",
                    "CorrelationGroupMPFInSitu",
                    "CorrelationGroupIntercalibration",
                    "CorrelationGroupbJES",
                    "CorrelationGroupFlavor",
                    "CorrelationGroupUncorrelated",
                ],
            },
        })

    :param reqs: Requirement dictionary for this
        :py:class:`~columnflow.calibration.Calibrator` instance
    :param inputs: Additional inputs, currently not used
    :param reader_targets: TODO: add documentation
    """
    bundle = reqs["external_files"]

    # import the correction sets from the external file
    import correctionlib
    correction_set = correctionlib.CorrectionSet.from_string(
        self.get_jec_file(bundle.files).load(formatter="gzip").decode("utf-8"),
    )

    # compute JEC keys from config information
    jec_cfg = self.get_jec_config()

    def make_jme_keys(names, jec=jec_cfg, is_data=self.dataset_inst.is_data):
        if is_data:
            jec_era = self.dataset_inst.get_aux("jec_era", None)
            # if no special JEC era is specified, infer based on 'era'
            if jec_era is None:
                jec_era = "Run" + self.dataset_inst.get_aux("era")

        return [
            f"{jec.campaign}_{jec_era}_{jec.version}_DATA_{name}_{jec.jet_type}"
            if is_data else
            f"{jec.campaign}_{jec.version}_MC_{name}_{jec.jet_type}"
            for name in names
        ]

    # take sources from constructor or config
    sources = self.uncertainty_sources
    if sources is None:
        sources = jec_cfg.uncertainty_sources

    jec_keys = make_jme_keys(jec_cfg.levels)
    jec_keys_subset_type1_met = make_jme_keys(jec_cfg.levels_for_type1_met)
    junc_keys = make_jme_keys(sources, is_data=False)  # uncertainties only stored as MC keys

    # store the evaluators
    self.evaluators = {
        "jec": get_evaluators(correction_set, jec_keys),
        "jec_subset_type1_met": get_evaluators(correction_set, jec_keys_subset_type1_met),
        "junc": dict(zip(sources, get_evaluators(correction_set, junc_keys))),
    }


# custom jec calibrator that only runs nominal correction
jec_nominal = jec.derive("jec_nominal", cls_dict={"uncertainty_sources": []})

# explicit calibrators for standard jet collections
jec_ak4 = jec.derive("jec_ak4", cls_dict={"jet_name": "Jet"})
jec_ak8 = jec.derive("jec_ak8", cls_dict={"jet_name": "FatJet", "propagate_met": False})
jec_ak4_nominal = jec_ak4.derive("jec_ak4", cls_dict={"uncertainty_sources": []})
jec_ak8_nominal = jec_ak8.derive("jec_ak8", cls_dict={"uncertainty_sources": []})


def get_jer_config_default(self: Calibrator) -> DotDict:
    """
    Load config relevant to the jet energy resolution (JER) smearing.

    By default, this is extracted from the current *config_inst*,
    assuming the JER configurations are stored under the 'jer'
    aux key. Separate configurations should be specified for each
    jet collection, using the collection name as a key. For example,
    the configuration for the default jet collection ``Jet`` will
    be retrieved from the following config entry:

    .. code-block:: python

        self.config_inst.x.jer.Jet

    Used in :py:meth:`~.jer.setup_func`.

    :return: Dictionary containing configuration for JER smearing
    """
    jer_cfg = self.config_inst.x.jer

    # check for old-style config
    if self.jet_name not in jer_cfg:
        # if jet collection is `Jet`, issue deprecation warning
        if self.jet_name == "Jet":
            logger.warning_once(
                f"{id(self)}_depr_jer_config",
                "config aux 'jer' does not contain key for input jet "
                f"collection '{self.jet_name}'. This may be due to "
                "an outdated config. Continuing under the assumption that "
                "the entire 'jer' entry refers to this jet collection. "
                "This assumption will be removed in future versions of "
                "columnflow, so please adapt the config according to the "
                "documentation to remove this warning and ensure future "
                "compatibility of the code.",
            )
            return jer_cfg

        # otherwise raise exception
        raise ValueError(
            "config aux 'jer' does not contain key for input jet "
            f"collection '{self.jet_name}'.",
        )

    return jer_cfg[self.jet_name]


#
# jet energy resolution smearing
#

@calibrator(
    uses={
        optional("Rho.fixedGridRhoFastjetAll"),
        optional("fixedGridRhoFastjetAll"),
        attach_coffea_behavior,
    },
    # name of the jet collection to smear
    jet_name="Jet",
    # name of the associated gen jet collection
    gen_jet_name="GenJet",
    # name of the associated MET collection
    met_name="MET",
    # toggle for propagation to MET
    propagate_met=True,
    # only run on mc
    mc_only=True,
    # use deterministic seeds for random smearing and
    # take the "index"-th random number per seed when not -1
    deterministic_seed_index=-1,
    # function to determine the correction file
    get_jer_file=get_jerc_file_default,
    # function to determine the jer configuration dict
    get_jer_config=get_jer_config_default,
)
def jer(self: Calibrator, events: ak.Array, **kwargs) -> ak.Array:
    """
    Applies the jet energy resolution smearing in MC and calculates the associated uncertainty
    shifts using the :external+correctionlib:doc:`index`, following the recommendations given in
    https://twiki.cern.ch/twiki/bin/viewauth/CMS/JetResolution.

    The *jet_name* and *gen_jet_name* should be set to the name of the NanoAOD jet and gen jet
    collections to use as an input for JER smearing (default: ``Jet`` and ``GenJet``, respectively,
    i.e. AK4 jets).

    Requires an external file in the config pointing to the JSON files containing the JER information.
    The file key can be specified via an optional ``external_file_key`` in the ``jer`` config entry.
    If not given, the file key will be determined automatically based on the jet collection name:
    ``jet_jerc`` for ``Jet`` (AK4 jets), ``fat_jet_jerc`` for``FatJet`` (AK8 jets). A full set of JSON files
    can be specified as:

    .. code-block:: python

        cfg.x.external_files = DotDict.wrap({
            "jet_jerc": "/afs/cern.ch/work/m/mrieger/public/mirrors/jsonpog-integration-9ea86c4c/POG/JME/2017_UL/jet_jerc.json.gz",
            "fat_jet_jerc": "/afs/cern.ch/work/m/mrieger/public/mirrors/jsonpog-integration-9ea86c4c/POG/JME/2017_UL/fatJet_jerc.json.gz",
        })

    For more fine-grained control, the *get_jer_file* can be adapted in a subclass in case it is stored
    differently in the external files.

    The JER smearing configuration should be an auxiliary entry in the config, specifying the input
    JER to use under "jer". Separate configs should be given for each jet collection to smear, using
    the jet collection name as a subkey. An example of a valid configuration for smearing
    AK4 jets with JER is:

    .. code-block:: python

        cfg.x.jer = {
            "Jet": {
                "campaign": "Summer19UL17",
                "version": "JRV2",
                "jet_type": "AK4PFchs",
            },
        }

    *get_jer_config* can be adapted in a subclass in case it is stored differently in the config.

    Throws an error if running on data.

    :param events: awkward array containing events to process
    """ # noqa
    # use local variables for convenience
    jet_name = self.jet_name
    gen_jet_name = self.gen_jet_name

    # fail when running on data
    if self.dataset_inst.is_data:
        raise ValueError("attempt to apply jet energy resolution smearing in data")

    # save the unsmeared properties in case they are needed later
    events = set_ak_column_f32(events, f"{jet_name}.pt_unsmeared", events[jet_name].pt)
    events = set_ak_column_f32(events, f"{jet_name}.mass_unsmeared", events[jet_name].mass)

    # obtain rho, which might be located at different routes, depending on the nano version
    rho = (
        events.fixedGridRhoFastjetAll
        if "fixedGridRhoFastjetAll" in events.fields else
        events.Rho.fixedGridRhoFastjetAll
    )

    # variable naming convention
    variable_map = {
        "JetEta": events[jet_name].eta,
        "JetPt": events[jet_name].pt,
        "Rho": rho,
    }

    # pt resolution
    inputs = [variable_map[inp.name] for inp in self.evaluators["jer"].inputs]
    jer = ak_evaluate(self.evaluators["jer"], *inputs)

    # JER scale factors and systematic variations
    jersf = {}
    for syst in ("nom", "up", "down"):
        variable_map_syst = dict(variable_map, systematic=syst)
        inputs = [variable_map_syst[inp.name] for inp in self.evaluators["sf"].inputs]
        jersf[syst] = ak_evaluate(self.evaluators["sf"], *inputs)

    # array with all JER scale factor variations as an additional axis
    # (note: axis needs to be regular for broadcasting to work correctly)
    jersf = ak.concatenate(
        [jersf[syst][..., None] for syst in ("nom", "up", "down")],
        axis=-1,
    )

    # -- stochastic smearing
    # normally distributed random numbers according to JER
    jer_random_normal = (
        ak_random(0, jer, events[jet_name].deterministic_seed, rand_func=self.deterministic_normal)
        if self.deterministic_seed_index >= 0
        else ak_random(0, jer, rand_func=np.random.Generator(
            np.random.SFC64(events.event.to_list())).normal,
        )
    )

    # scale random numbers according to JER SF
    jersf2_m1 = jersf ** 2 - 1
    add_smear = np.sqrt(ak.where(jersf2_m1 < 0, 0, jersf2_m1))

    # broadcast over JER SF variations
    jer_random_normal, jersf_z = ak.broadcast_arrays(jer_random_normal, add_smear)

    # compute smearing factors (stochastic method)
    smear_factors_stochastic = 1.0 + jer_random_normal * add_smear

    # -- scaling method (using gen match)

    # mask negative gen jet indices (= no gen match)
    gen_jet_idx = events[jet_name][self.gen_jet_idx_column]
    valid_gen_jet_idxs = ak.mask(gen_jet_idx, gen_jet_idx >= 0)

    # pad list of gen jets to prevent index error on match lookup
    padded_gen_jets = ak.pad_none(events[gen_jet_name], ak.max(valid_gen_jet_idxs) + 1)

    # gen jets that match the reconstructed jets
    matched_gen_jets = padded_gen_jets[valid_gen_jet_idxs]

    # compute the relative (reco - gen) pt difference
    pt_relative_diff = (events[jet_name].pt - matched_gen_jets.pt) / events[jet_name].pt

    # test if matched gen jets are within 3 * resolution
    is_matched_pt = np.abs(pt_relative_diff) < 3 * jer
    is_matched_pt = ak.fill_none(is_matched_pt, False)  # masked values = no gen match

    # (no check for Delta-R matching criterion; we assume this was done during
    # nanoAOD production to get the `genJetIdx`)

    # broadcast over JER SF variations
    pt_relative_diff, jersf = ak.broadcast_arrays(pt_relative_diff, jersf)

    # compute smearing factors (scaling method)
    smear_factors_scaling = 1.0 + (jersf - 1.0) * pt_relative_diff

    # -- hybrid smearing: take smear factors from scaling if there was a match,
    # otherwise take the stochastic ones
    smear_factors = ak.where(
        is_matched_pt[:, :, None],
        smear_factors_scaling,
        smear_factors_stochastic,
    )

    # ensure array is not nullable (avoid ambiguity on Arrow/Parquet conversion)
    smear_factors = ak.fill_none(smear_factors, 0.0)

    # store pt and phi of the full jet system
    if self.propagate_met:
        jetsum = events[jet_name].sum(axis=1)
        jetsum_pt_before = jetsum.pt
        jetsum_phi_before = jetsum.phi

    # apply the smearing factors to the pt and mass
    # (note: apply variations first since they refer to the original pt)
    events = set_ak_column_f32(events, f"{jet_name}.pt_jer_up", events[jet_name].pt * smear_factors[:, :, 1])
    events = set_ak_column_f32(events, f"{jet_name}.mass_jer_up", events[jet_name].mass * smear_factors[:, :, 1])
    events = set_ak_column_f32(events, f"{jet_name}.pt_jer_down", events[jet_name].pt * smear_factors[:, :, 2])
    events = set_ak_column_f32(events, f"{jet_name}.mass_jer_down", events[jet_name].mass * smear_factors[:, :, 2])
    events = set_ak_column_f32(events, f"{jet_name}.pt", events[jet_name].pt * smear_factors[:, :, 0])
    events = set_ak_column_f32(events, f"{jet_name}.mass", events[jet_name].mass * smear_factors[:, :, 0])

    # recover coffea behavior
    events = self[attach_coffea_behavior](events, collections=[jet_name], **kwargs)

    # met propagation
    if self.propagate_met:

        # save unsmeared quantities
        events = set_ak_column_f32(events, f"{self.met_name}.pt_unsmeared", events[self.met_name].pt)
        events = set_ak_column_f32(events, f"{self.met_name}.phi_unsmeared", events[self.met_name].phi)

        # get pt and phi of all jets after correcting
        jetsum = events[jet_name].sum(axis=1)
        jetsum_pt_after = jetsum.pt
        jetsum_phi_after = jetsum.phi

        # propagate changes to MET
        met_pt, met_phi = propagate_met(
            jetsum_pt_before,
            jetsum_phi_before,
            jetsum_pt_after,
            jetsum_phi_after,
            events[self.met_name].pt,
            events[self.met_name].phi,
        )
        events = set_ak_column_f32(events, f"{self.met_name}.pt", met_pt)
        events = set_ak_column_f32(events, f"{self.met_name}.phi", met_phi)

        # syst variations on top of corrected MET
        met_pt_up, met_phi_up = propagate_met(
            jetsum_pt_after,
            jetsum_phi_after,
            events[jet_name].pt_jer_up,
            events[jet_name].phi,
            met_pt,
            met_phi,
        )
        met_pt_down, met_phi_down = propagate_met(
            jetsum_pt_after,
            jetsum_phi_after,
            events[jet_name].pt_jer_down,
            events[jet_name].phi,
            met_pt,
            met_phi,
        )
        events = set_ak_column_f32(events, f"{self.met_name}.pt_jer_up", met_pt_up)
        events = set_ak_column_f32(events, f"{self.met_name}.pt_jer_down", met_pt_down)
        events = set_ak_column_f32(events, f"{self.met_name}.phi_jer_up", met_phi_up)
        events = set_ak_column_f32(events, f"{self.met_name}.phi_jer_down", met_phi_down)

    return events


@jer.init
def jer_init(self: Calibrator) -> None:
    # determine gen-level jet index column
    lower_first = lambda s: s[0].lower() + s[1:] if s else s
    self.gen_jet_idx_column = lower_first(self.gen_jet_name) + "Idx"

    # register used jet columns
    self.uses.add(f"{self.jet_name}.{{pt,eta,phi,mass,{self.gen_jet_idx_column}}}")

    # register used gen jet columns
    self.uses.add(f"{self.gen_jet_name}.{{pt,eta,phi}}")

    # register produced jet columns
    self.produces.add(f"{self.jet_name}.{{pt,mass}}{{,_unsmeared,_jer_up,_jer_down}}")

    # register produced MET columns
    if self.propagate_met:
        # register used MET columns
        self.uses.add(f"{self.met_name}.{{pt,phi}}")

        # register produced MET columns
        self.produces.add(f"{self.met_name}.{{pt,phi}}{{,_jer_up,_jer_down,_unsmeared}}")


@jer.requires
def jer_requires(self: Calibrator, reqs: dict) -> None:
    if "external_files" in reqs:
        return

    from columnflow.tasks.external import BundleExternalFiles
    reqs["external_files"] = BundleExternalFiles.req(self.task)


@jer.setup
def jer_setup(self: Calibrator, reqs: dict, inputs: dict, reader_targets: InsertableDict) -> None:
    """
    Load the correct jer files using the :py:func:`from_string` method of the
    :external+correctionlib:py:class:`correctionlib.highlevel.CorrectionSet` function and apply the
    corrections as needed.

    The source files for the :external+correctionlib:py:class:`correctionlib.highlevel.CorrectionSet`
    instance are extracted with the :py:meth:`~.jer.get_jer_file`.

    Uses the member function :py:meth:`~.jer.get_jer_config` to construct the required keys, which
    are based on the following information about the JER:

    - campaign
    - version
    - jet_type

    A corresponding example snippet within the *config_inst* could like something like this:

    .. code-block:: python

        cfg.x.jer = DotDict.wrap({
            "Jet": {
                "campaign": f"Summer19UL{year2}{jerc_postfix}",
                "version": "JRV3",
                "jet_type": "AK4PFchs",
            },
        })

    :param reqs: Requirement dictionary for this :py:class:`~columnflow.calibration.Calibrator`
        instance.
    :param inputs: Additional inputs, currently not used.
    :param reader_targets: TODO: add documentation.
    """
    bundle = reqs["external_files"]

    # import the correction sets from the external file
    import correctionlib
    correction_set = correctionlib.CorrectionSet.from_string(
        self.get_jer_file(bundle.files).load(formatter="gzip").decode("utf-8"),
    )

    # compute JER keys from config information
    jer_cfg = self.get_jer_config()
    jer_keys = {
        "jer": f"{jer_cfg.campaign}_{jer_cfg.version}_MC_PtResolution_{jer_cfg.jet_type}",
        "sf": f"{jer_cfg.campaign}_{jer_cfg.version}_MC_ScaleFactor_{jer_cfg.jet_type}",
    }

    # store the evaluators
    self.evaluators = {
        name: get_evaluators(correction_set, [key])[0]
        for name, key in jer_keys.items()
    }

    # use deterministic seeds for random smearing if requested
    if self.deterministic_seed_index >= 0:
        idx = self.deterministic_seed_index
        bit_generator = np.random.SFC64
        def deterministic_normal(loc, scale, seed):
            return np.asarray([
                np.random.Generator(bit_generator(_seed)).normal(_loc, _scale, size=idx + 1)[-1]
                for _loc, _scale, _seed in zip(loc, scale, seed)
            ])
        self.deterministic_normal = deterministic_normal


# explicit calibrators for standard jet collections
jer_ak4 = jer.derive("jer_ak4", cls_dict={"jet_name": "Jet", "gen_jet_name": "GenJet"})
jer_ak8 = jer.derive("jer_ak8", cls_dict={"jet_name": "FatJet", "gen_jet_name": "GenJetAK8", "propagate_met": False})


#
# single calibrator for doing both JEC and JER smearing
#

@calibrator(
    uses={jec, jer},
    produces={jec, jer},
    # name of the jet collection to smear
    jet_name="Jet",
    # name of the associated gen jet collection (for JER smearing)
    gen_jet_name="GenJet",
    # toggle for propagation to MET
    propagate_met=None,
    # functions to determine configs and files
    get_jec_file=None,
    get_jec_config=None,
    get_jer_file=None,
    get_jer_config=None,
)
def jets(self: Calibrator, events: ak.Array, **kwargs) -> ak.Array:
    """
    Instance of :py:class:`~columnflow.calibration.Calibrator` that does all relevant calibrations
    for jets, i.e. JEC and JER. For more information, see :py:func:`~.jec` and :py:func:`~.jer`.

    :param events: awkward array containing events to process
    """
    # apply jet energy corrections
    events = self[jec](events, **kwargs)

    # apply jer smearing on MC only
    if self.dataset_inst.is_mc:
        events = self[jer](events, **kwargs)

    return events


@jets.init
def jets_init(self: Calibrator) -> None:
    # forward argument to the producers
    self.deps_kwargs[jec]["jet_name"] = self.jet_name
    self.deps_kwargs[jer]["jet_name"] = self.jet_name
    self.deps_kwargs[jer]["gen_jet_name"] = self.gen_jet_name
    if self.propagate_met is not None:
        self.deps_kwargs[jec]["propagate_met"] = self.propagate_met
        self.deps_kwargs[jer]["propagate_met"] = self.propagate_met
    if self.get_jec_file is not None:
        self.deps_kwargs[jec]["get_jec_file"] = self.get_jec_file
    if self.get_jec_config is not None:
        self.deps_kwargs[jec]["get_jec_config"] = self.get_jec_config
    if self.get_jer_file is not None:
        self.deps_kwargs[jer]["get_jer_file"] = self.get_jer_file
    if self.get_jer_config is not None:
        self.deps_kwargs[jer]["get_jer_config"] = self.get_jer_config


# explicit calibrators for standard jet collections
jets_ak4 = jets.derive("jets_ak4", cls_dict={"jet_name": "Jet", "gen_jet_name": "GenJet"})
jets_ak8 = jets.derive("jets_ak8", cls_dict={"jet_name": "FatJet", "gen_jet_name": "GenJetAK8"})
