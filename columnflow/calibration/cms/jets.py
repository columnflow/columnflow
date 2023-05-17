# coding: utf-8

"""
Jet energy corrections and jet resolution smearing.
"""

import functools

from columnflow.calibration import Calibrator, calibrator
from columnflow.calibration.util import ak_random, propagate_met
from columnflow.production.util import attach_coffea_behavior
from columnflow.util import maybe_import
from columnflow.columnar_util import set_ak_column, layout_ak_array

from typing import Any

np = maybe_import("numpy")
ak = maybe_import("awkward")
correctionlib = maybe_import("correctionlib")


#
# helper functions
#

set_ak_column_f32 = functools.partial(set_ak_column, value_type=np.float32)


def get_evaluators(
    correction_set: correctionlib.highlevel.CorrectionSet,
    names: list[str],
) -> list[Any]:
    """Helper function to get a list of correction evaluators from a
    :external+correctionlib:py:class:`correctionlib.highlevel.CorrectionSet` object given
    a list of *names*. The *names* can refer to either simple or compound
    corrections.

    :param correction_set: evaluator provided by :external+correctionlib:doc:`index`
    :type correction_set: :external+correctionlib:py:class:`correctionlib.highlevel.CorrectionSet`
    :param names: List of names of corrections to be applied
    :type names: list
    :raises RuntimeError: If a requested correction in *names* is not available
    :return: List of compounded corrections, see
        :external+correctionlib:py:class:`correctionlib.highlevel.CorrectionSet`
    :rtype: list[Any]
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
    """Evaluate a :external+correctionlib:py:class:`correctionlib.highlevel.Correction`
    using one or more :external+ak:py:class:`awkward arrays <ak.Array>` as inputs.

    :param evaluator: Evaluator instance
    :type evaluator: :external+correctionlib:py:class:`correctionlib.highlevel.Correction`
    :raises ValueError: If no :external+ak:py:class:`awkward arrays <ak.Array>` are provided
    :return: The correction factor derived from the input arrays
    :rtype: float
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

@calibrator(
    uses={
        "nJet", "Jet.pt", "Jet.eta", "Jet.phi", "Jet.mass", "Jet.area", "Jet.rawFactor",
        "Jet.jetId", "fixedGridRhoFastjetAll", "Rho.fixedGridRhoFastjetAll",
        attach_coffea_behavior,
    },
    produces={
        "Jet.pt", "Jet.mass", "Jet.rawFactor",
    },
    # custom uncertainty sources, defaults to config when empty
    uncertainty_sources=None,
    # toggle for propagation to MET
    propagate_met=True,
    # function to determine the correction file
    get_jec_file=(lambda self, external_files: external_files.jet_jerc),
    # function to determine the jec configuration dict
    get_jec_config=(lambda self: self.config_inst.x.jec),
)
def jec(
    self: Calibrator,
    events: ak.Array,
    min_pt_met_prop: float = 15.0,
    max_eta_met_prop: float = 5.2,
    **kwargs,
) -> ak.Array:
    """Performs the jet energy corrections and uncertainty shifts using the 
    :external+correctionlib:doc:`index`, optionally
    propagating the changes to the MET.

    Requires an external file in the config under ``jet_jerc``:

    .. code-block:: python

        cfg.x.external_files = DotDict.wrap({
            "jet_jerc": "/afs/cern.ch/user/m/mrieger/public/mirrors/jsonpog-integration-f018adfb/POG/JME/2017_UL/jet_jerc.json.gz",  # noqa
        })

    *get_jec_file* can be adapted in a subclass in case it is stored differently in the
    external files

    The jec configuration should be an auxiliary entry in the config, specifying the correction
    details under "jec":

    .. code-block:: python

        cfg.x.jec = {
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
        }

    *get_jec_config* can be adapted in a subclass in case it is stored differently in the config.

    If running on data, the datasets must have an auxiliary field *jec_era* defined, e.g. "RunF",
    or an auxiliary field *era*, e.g. "F".

    This instance of :py:class:`~columnflow.calibration.base.Calibrator` is
    initialized with the following parameters by default:

    :*uses*: ``"nJet"``, ``"Jet.pt"``, ``"Jet.eta"``, ``"Jet.phi"``, ``"Jet.mass"``,
        ``"Jet.area"``, ``"Jet.rawFactor"``, ``"Jet.jetId"``, ``"fixedGridRhoFastjetAll"``,
        ``"Rho.fixedGridRhoFastjetAll"``,
        :py:func:`~columnflow.production.util.attach_coffea_behavior`
    :*produces*: ``"Jet.pt"``, ``"Jet.mass"``, ``"Jet.rawFactor"``.
        If *propagate_met* is ``True``, also produces columns for the original
        MET values (RawMET) and corrected MET (MET). Additionally produces columns
        corresponding to JEC up and down variations for all previously
        mentioned columns except for Jet.rawFactor.

    :uncertainty_sources: None,

    :propagate_met: ``True``
    
    :get_jec_file: 
        .. code-block:: python

            lambda self, external_files: external_files.jet_jerc

    :get_jec_config: 
        .. code-block:: python

            lambda self: self.config_inst.x.jec
    
    :param self: This :py:class:`~columnflow.calibration.base.Calibrator` instance
    :type self: :py:class:`~columnflow.calibration.base.Calibrator`
    
    :param events: awkward array containing events to process
    :type events: :external+ak:py:class:`ak.Array`

    :param min_pt_met_prop: If *propagate_met* variable is ``True`` propagate the
        updated jet values to the missing transverse energy (MET) using
        :py:func:`~columnflow.calibration.util.propagate_met` for events where
        ``met.pt > *min_pt_met_prop*``. Defaults to ``15.0``.
    :type min_pt_met_prop: float, optional
    :param max_eta_met_prop: If *propagate_met* variable is ``True`` propagate
        the updated jet values to the missing transverse energy (MET) using
        :py:func:`~columnflow.calibration.util.propagate_met` for events where
        ``met.eta > *min_eta_met_prop*``. Defaults to ``5.2``.
    :return: awkward array containing new columns with corrected ``Jet.pt`` and
        ``Jet.mass``, as well as the relative difference between raw and corrected
        pt ``Jet.rawFactor``. Additionally contains columns for JEC up and down
        variations, see produces section
    :rtype: :external+ak:py:class:`ak.Array`
    """
    # calculate uncorrected pt, mass
    events = set_ak_column_f32(events, "Jet.pt_raw", events.Jet.pt * (1 - events.Jet.rawFactor))
    events = set_ak_column_f32(events, "Jet.mass_raw", events.Jet.mass * (1 - events.Jet.rawFactor))

    def correct_jets(pt, area, eta, rho, evaluator_key="jec"):
        # variable naming convention
        variable_map = {
            "JetA": area,
            "JetEta": eta,
            "JetPt": pt,
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
        if "fixedGridRhoFastjetAll" in events.fields else
        events.Rho.fixedGridRhoFastjetAll
    )

    # correct jets with only a subset of correction levels
    # (for calculating TypeI MET correction)
    if self.propagate_met:
        # get correction factors
        jec_factors_subset_type1_met = correct_jets(
            pt=events.Jet.pt_raw,
            eta=events.Jet.eta,
            area=events.Jet.area,
            rho=rho,
            evaluator_key="jec_subset_type1_met",
        )

        # temporarily apply the new factors with only subset of corrections
        events = set_ak_column_f32(events, "Jet.pt", events.Jet.pt_raw * jec_factors_subset_type1_met)
        events = set_ak_column_f32(events, "Jet.mass", events.Jet.mass_raw * jec_factors_subset_type1_met)
        events = self[attach_coffea_behavior](events, collections=["Jet"], **kwargs)

        # store pt and phi of the full jet system for MET propagation, including a selection in raw info
        # see https://twiki.cern.ch/twiki/bin/view/CMS/JECAnalysesRecommendations?rev=19#Minimum_jet_selection_cuts
        met_prop_mask = (events.Jet.pt_raw > min_pt_met_prop) & (abs(events.Jet.eta) < max_eta_met_prop)
        jetsum = events.Jet[met_prop_mask].sum(axis=1)
        jetsum_pt_subset_type1_met = jetsum.pt
        jetsum_phi_subset_type1_met = jetsum.phi

    # factors for full jet correction with all levels
    jec_factors = correct_jets(
        pt=events.Jet.pt_raw,
        eta=events.Jet.eta,
        area=events.Jet.area,
        rho=rho,
        evaluator_key="jec",
    )

    # apply full jet correction
    events = set_ak_column_f32(events, "Jet.pt", events.Jet.pt_raw * jec_factors)
    events = set_ak_column_f32(events, "Jet.mass", events.Jet.mass_raw * jec_factors)
    events = set_ak_column_f32(events, "Jet.rawFactor", (1 - events.Jet.pt_raw / events.Jet.pt))
    events = self[attach_coffea_behavior](events, collections=["Jet"], **kwargs)

    # nominal met propagation
    if self.propagate_met:
        # get pt and phi of all jets after correcting
        jetsum = events.Jet[met_prop_mask].sum(axis=1)
        jetsum_pt_all_levels = jetsum.pt
        jetsum_phi_all_levels = jetsum.phi

        # propagate changes to MET, starting from jets corrected with subset of JEC levels
        # (recommendation is to propagate only L2 corrections and onwards)
        met_pt, met_phi = propagate_met(
            jetsum_pt_subset_type1_met,
            jetsum_phi_subset_type1_met,
            jetsum_pt_all_levels,
            jetsum_phi_all_levels,
            events.RawMET.pt,
            events.RawMET.phi,
        )
        events = set_ak_column_f32(events, "MET.pt", met_pt)
        events = set_ak_column_f32(events, "MET.phi", met_phi)

    # jet energy uncertainty components
    for name, evaluator in self.evaluators["junc"].items():
        # get uncertainty
        jec_uncertainty = ak_evaluate(
            evaluator,
            events.Jet.eta,
            events.Jet.pt_raw,
        )

        # apply jet uncertainty shifts
        events = set_ak_column_f32(events, f"Jet.pt_jec_{name}_up", events.Jet.pt * (1.0 + jec_uncertainty))
        events = set_ak_column_f32(events, f"Jet.pt_jec_{name}_down", events.Jet.pt * (1.0 - jec_uncertainty))
        events = set_ak_column_f32(events, f"Jet.mass_jec_{name}_up", events.Jet.mass * (1.0 + jec_uncertainty))
        events = set_ak_column_f32(events, f"Jet.mass_jec_{name}_down", events.Jet.mass * (1.0 - jec_uncertainty))

        # propagate shifts to MET
        if self.propagate_met:
            jet_pt_up = events.Jet[met_prop_mask][f"pt_jec_{name}_up"]
            jet_pt_down = events.Jet[met_prop_mask][f"pt_jec_{name}_down"]
            met_pt_up, met_phi_up = propagate_met(
                jetsum_pt_all_levels,
                jetsum_phi_all_levels,
                jet_pt_up,
                events.Jet[met_prop_mask].phi,
                met_pt,
                met_phi,
            )
            met_pt_down, met_phi_down = propagate_met(
                jetsum_pt_all_levels,
                jetsum_phi_all_levels,
                jet_pt_down,
                events.Jet[met_prop_mask].phi,
                met_pt,
                met_phi,
            )
            events = set_ak_column_f32(events, f"MET.pt_jec_{name}_up", met_pt_up)
            events = set_ak_column_f32(events, f"MET.pt_jec_{name}_down", met_pt_down)
            events = set_ak_column_f32(events, f"MET.phi_jec_{name}_up", met_phi_up)
            events = set_ak_column_f32(events, f"MET.phi_jec_{name}_down", met_phi_down)

    return events


@jec.init
def jec_init(self: Calibrator) -> None:
    """:py:meth:`init` function for :py:func:`~.jec`
    :py:class:`~columnflow.calibration.base.Calibrator`.
    Adds JEC uncertainty shifts to the list of produced columns.

    If member variable *uncertainty_source* is ``None``, load the full list
    of jec uncertainties from the associated ``config`` instance.

    If the member variable *propagate_met* is ``True``, add also MET and RawMET
    as well as the corresponding jec variations to the set of columns to be produced.

    :param self: :py:class:`~columnflow.calibration.base.Calibrator` instance
    :type self:  :py:class:`~columnflow.calibration.base.Calibrator`
    """
    jec_cfg = self.get_jec_config()

    sources = self.uncertainty_sources
    if sources is None:
        sources = jec_cfg.uncertainty_sources

    # add shifted jet variables
    self.produces |= {
        f"Jet.{shifted_var}_jec_{junc_name}_{junc_dir}"
        for shifted_var in ("pt", "mass")
        for junc_name in sources
        for junc_dir in ("up", "down")
    }

    # add MET variables
    if self.propagate_met:
        self.uses |= {"RawMET.pt", "RawMET.phi"}
        self.produces |= {"MET.pt", "MET.phi"}

        # add shifted MET variables
        self.produces |= {
            f"MET.{shifted_var}_jec_{junc_name}_{junc_dir}"
            for shifted_var in ("pt", "phi")
            for junc_name in sources
            for junc_dir in ("up", "down")
        }


@jec.requires
def jec_requires(self: Calibrator, reqs: dict) -> None:
    """Add external files bundle (for JEC text files) to dependencies.

    Adds the requirements for task :py:class:`~columnflow.tasks.external.BundleExternalFiles`
    as keyword ``external_files`` to the dictionary of requirements *reqs*.

    :param self: :py:class:`~columnflow.calibration.base.Calibrator` instance
    :type self: :py:class:`~columnflow.calibration.base.Calibrator`
    :param reqs: Requirement dictionary for this
        :py:class:`~columnflow.calibration.base.Calibrator` instance
    :type reqs:  dict
    """
    if "external_files" in reqs:
        return

    from columnflow.tasks.external import BundleExternalFiles
    reqs["external_files"] = BundleExternalFiles.req(self.task)


@jec.setup
def jec_setup(self: Calibrator, reqs: dict, inputs: dict) -> None:
    """Load the correct jec files using the :py:func:`from_string` method of the
    :external+correctionlib:py:class:`correctionlib.highlevel.CorrectionSet`
    function and apply the corrections as needed.

    :param self: This :py:class:`~columnflow.calibration.base.Calibrator` instance
    :type self: :py:class:`~columnflow.calibration.base.Calibrator`
    :param reqs: Requirement dictionary for this
        :py:class:`~columnflow.calibration.base.Calibrator` instance
    :type reqs: dict
    :param inputs: Additional inputs, currently not used
    :type inputs: dict
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


#
# jet energy resolution smearing
#

@calibrator(
    uses={
        "nJet", "Jet.pt", "Jet.eta", "Jet.phi", "Jet.mass", "Jet.genJetIdx",
        "Rho.fixedGridRhoFastjetAll", "fixedGridRhoFastjetAll",
        "nGenJet", "GenJet.pt", "GenJet.eta", "GenJet.phi",
        "MET.pt", "MET.phi",
        attach_coffea_behavior,
    },
    produces={
        "Jet.pt", "Jet.mass",
        "Jet.pt_unsmeared", "Jet.mass_unsmeared",
        "Jet.pt_jer_up", "Jet.pt_jer_down", "Jet.mass_jer_up", "Jet.mass_jer_down",
        "MET.pt", "MET.phi",
        "MET.pt_jer_up", "MET.pt_jer_down", "MET.phi_jer_up", "MET.phi_jer_down",
    },
    # toggle for propagation to MET
    propagate_met=True,
    # only run on mc
    mc_only=True,
    # function to determine the correction file
    get_jer_file=(lambda self, external_files: external_files.jet_jerc),
    # function to determine the jer configuration dict
    get_jer_config=(lambda self: self.config_inst.x.jer),
)
def jer(self: Calibrator, events: ak.Array, **kwargs) -> ak.Array:
    """Applies the jet energy resolution smearing in MC and calculates the
    associated uncertainty shifts using the :external+correctionlib:doc:`index`,
    following the recommendations given in
    https://twiki.cern.ch/twiki/bin/viewauth/CMS/JetResolution.

    Requires an external file in the config under ``jet_jerc``:

    .. code-block:: python

        cfg.x.external_files = DotDict.wrap({
            "jet_jerc": "/afs/cern.ch/user/m/mrieger/public/mirrors/jsonpog-integration-f018adfb/POG/JME/2017_UL/jet_jerc.json.gz",  # noqa
        })

    *get_jer_file* can be adapted in a subclass in case it is stored differently in the
    external files

    The jer configuration should be an auxiliary entry in the config, specifying the correction
    details under ``jer``:

    .. code-block:: python

        cfg.x.jer = {
            "campaign": "Summer19UL17",
            "version": "JRV2",
            "jet_type": "AK4PFchs",
        },

    *get_jer_config* can be adapted in a subclass in case it is stored differently in the config.

    Throws an error if running on data.

    This instance of :py:class:`~columnflow.calibration.base.Calibrator` is
    initialized with the following parameters by default:

    :*uses*: ``"nJet"``, ``"Jet.pt"``, ``"Jet.eta"``, ``"Jet.phi"``,
        ``"Jet.mass"``, ``"Jet.genJetIdx"``, ``"Rho.fixedGridRhoFastjetAll"``,
        ``"fixedGridRhoFastjetAll"``, ``"nGenJet"``, ``"GenJet.pt"``,
        ``"GenJet.eta"``, ``"GenJet.phi"``, ``"MET.pt"``, ``"MET.phi"``,
        :py:func:`~columnflow.production.util.attach_coffea_behavior`
    :*produces*: Smeared Jet values (``"Jet.pt"``, ``"Jet.mass"``),
        as well as the original values (``"Jet.pt_unsmeared"``,
        ``"Jet.mass_unsmeared"``).

        If *propagate_met* is ``True``, also produces columns for the original
        MET values (``"MET.pt_unmeared"``, ``"MET.phi_unmeared"``) and
        corrected MET (MET). Additionally produces columns
        corresponding to JEC up and down variations for all previously
        mentioned columns except for unsmeared values.

    :propagate_met: ``True``
    :mc_only: ``True``

    
    :get_jer_file: 
        .. code-block:: python

            lambda self, external_files: external_files.jet_jerc

    :get_jer_config: 
        .. code-block:: python
        
            lambda self: self.config_inst.x.jer
    
    :param self: This :py:class:`~columnflow.calibration.base.Calibrator` instance
    :type self: :py:class:`~columnflow.calibration.base.Calibrator`
    
    :param events: awkward array containing events to process
    :type events: :external+ak:py:class:`ak.Array`

    :return: awkward array containing new columns with smeared ``Jet.pt`` and
        ``Jet.mass``, as well as the original unsmeared values.
        Additionally contains columns for JER up and down variations,
        see produces section
    :rtype: :external+ak:py:class:`ak.Array`
    """
    # fail when running on data
    if self.dataset_inst.is_data:
        raise ValueError("attempt to apply jet energy resolution smearing in data")

    # save the unsmeared properties in case they are needed later
    events = set_ak_column_f32(events, "Jet.pt_unsmeared", events.Jet.pt)
    events = set_ak_column_f32(events, "Jet.mass_unsmeared", events.Jet.mass)

    # use event numbers in chunk to seed random number generator
    # TODO: use deterministic seeds!
    rand_gen = np.random.Generator(np.random.SFC64(events.event.to_list()))

    # obtain rho, which might be located at different routes, depending on the nano version
    rho = (
        events.fixedGridRhoFastjetAll
        if "fixedGridRhoFastjetAll" in events.fields else
        events.Rho.fixedGridRhoFastjetAll
    )

    # pt resolution
    jer = ak_evaluate(
        self.evaluators["jer"],
        events.Jet.eta,
        events.Jet.pt,
        rho,
    )

    # JER scale factors and systematic variations
    jersf = {
        syst: ak_evaluate(
            self.evaluators["sf"],
            events.Jet.eta,
            syst,
        )
        for syst in ("nom", "up", "down")
    }

    # array with all JER scale factor variations as an additional axis
    # (note: axis needs to be regular for broadcasting to work correctly)
    jersf = ak.concatenate(
        [jersf[syst][..., None] for syst in ("nom", "up", "down")],
        axis=-1,
    )

    # -- stochastic smearing

    # normally distributed random numbers according to JER
    jer_random_normal = ak_random(0, jer, rand_func=rand_gen.normal)

    # scale random numbers according to JER SF
    jersf2_m1 = jersf ** 2 - 1
    add_smear = np.sqrt(ak.where(jersf2_m1 < 0, 0, jersf2_m1))

    # broadcast over JER SF variations
    jer_random_normal, jersf_z = ak.broadcast_arrays(jer_random_normal, add_smear)

    # compute smearing factors (stochastic method)
    smear_factors_stochastic = 1.0 + jer_random_normal * add_smear

    # -- scaling method (using gen match)

    # mask negative gen jet indices (= no gen match)
    valid_gen_jet_idxs = ak.mask(events.Jet.genJetIdx, events.Jet.genJetIdx >= 0)

    # pad list of gen jets to prevent index error on match lookup
    padded_gen_jets = ak.pad_none(events.GenJet, ak.max(valid_gen_jet_idxs) + 1)

    # gen jets that match the reconstructed jets
    matched_gen_jets = padded_gen_jets[valid_gen_jet_idxs]

    # compute the relative (reco - gen) pt difference
    pt_relative_diff = (events.Jet.pt - matched_gen_jets.pt) / events.Jet.pt

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
        jetsum = events.Jet.sum(axis=1)
        jetsum_pt_before = jetsum.pt
        jetsum_phi_before = jetsum.phi

    # apply the smearing factors to the pt and mass
    # (note: apply variations first since they refer to the original pt)
    events = set_ak_column_f32(events, "Jet.pt_jer_up", events.Jet.pt * smear_factors[:, :, 1])
    events = set_ak_column_f32(events, "Jet.mass_jer_up", events.Jet.mass * smear_factors[:, :, 1])
    events = set_ak_column_f32(events, "Jet.pt_jer_down", events.Jet.pt * smear_factors[:, :, 2])
    events = set_ak_column_f32(events, "Jet.mass_jer_down", events.Jet.mass * smear_factors[:, :, 2])
    events = set_ak_column_f32(events, "Jet.pt", events.Jet.pt * smear_factors[:, :, 0])
    events = set_ak_column_f32(events, "Jet.mass", events.Jet.mass * smear_factors[:, :, 0])

    # recover coffea behavior
    events = self[attach_coffea_behavior](events, collections=["Jet"], **kwargs)

    # met propagation
    if self.propagate_met:
        # save unsmeared quantities
        events = set_ak_column_f32(events, "MET.pt_unsmeared", events.MET.pt)
        events = set_ak_column_f32(events, "MET.phi_unsmeared", events.MET.phi)

        # get pt and phi of all jets after correcting
        jetsum = events.Jet.sum(axis=1)
        jetsum_pt_after = jetsum.pt
        jetsum_phi_after = jetsum.phi

        # propagate changes to MET
        met_pt, met_phi = propagate_met(
            jetsum_pt_before,
            jetsum_phi_before,
            jetsum_pt_after,
            jetsum_phi_after,
            events.MET.pt,
            events.MET.phi,
        )
        events = set_ak_column_f32(events, "MET.pt", met_pt)
        events = set_ak_column_f32(events, "MET.phi", met_phi)

        # syst variations on top of corrected MET
        met_pt_up, met_phi_up = propagate_met(
            jetsum_pt_after,
            jetsum_phi_after,
            events.Jet.pt_jer_up,
            events.Jet.phi,
            met_pt,
            met_phi,
        )
        met_pt_down, met_phi_down = propagate_met(
            jetsum_pt_after,
            jetsum_phi_after,
            events.Jet.pt_jer_down,
            events.Jet.phi,
            met_pt,
            met_phi,
        )
        events = set_ak_column_f32(events, "MET.pt_jer_up", met_pt_up)
        events = set_ak_column_f32(events, "MET.pt_jer_down", met_pt_down)
        events = set_ak_column_f32(events, "MET.phi_jer_up", met_phi_up)
        events = set_ak_column_f32(events, "MET.phi_jer_down", met_phi_down)

    return events


@jer.init
def jer_init(self: Calibrator) -> None:
    """:py:meth:`init` function for :py:func:`~.jer`
    :py:class:`~columnflow.calibration.base.Calibrator`.
    Adds all MET related columns to ``produces`` set if *propagate_met* is ``True``,
    see produces section in :py:func:`~.jer`.

    If the member variable *propagate_met* is ``True``, add also MET and RawMET
    as well as the corresponding jec variations to the set of columns to be produced.

    :param self: :py:class:`~columnflow.calibration.base.Calibrator` instance
    :type self:  :py:class:`~columnflow.calibration.base.Calibrator`
    """
    if not self.propagate_met:
        return

    self.uses |= {
        "MET.pt", "MET.phi",
    }
    self.produces |= {
        "MET.pt", "MET.phi", "MET.pt_jer_up", "MET.pt_jer_down", "MET.phi_jer_up",
        "MET.phi_jer_down", "MET.pt_unsmeared", "MET.phi_unsmeared",
    }


@jer.requires
def jer_requires(self: Calibrator, reqs: dict) -> None:
    """Add external files bundle (for JER text files) to dependencies.

    Adds the requirements for task :py:class:`~columnflow.tasks.external.BundleExternalFiles`
    as keyword ``external_files`` to the dictionary of requirements *reqs*.

    :param self: :py:class:`~columnflow.calibration.base.Calibrator` instance
    :type self: :py:class:`~columnflow.calibration.base.Calibrator`
    :param reqs: Requirement dictionary for this
        :py:class:`~columnflow.calibration.base.Calibrator` instance
    :type reqs:  dict
    """
    if "external_files" in reqs:
        return

    from columnflow.tasks.external import BundleExternalFiles
    reqs["external_files"] = BundleExternalFiles.req(self.task)


@jer.setup
def jer_setup(self: Calibrator, reqs: dict, inputs: dict) -> None:
    """Determine correct JER files for task based on config/dataset and inject them
    into the calibrator function call.

    Load the correct JER files using the :py:func:`from_string` method of the
    :external+correctionlib:py:class:`correctionlib.highlevel.CorrectionSet`
    function and apply the corrections as needed.

    :param self: This :py:class:`~columnflow.calibration.base.Calibrator` instance
    :type self: :py:class:`~columnflow.calibration.base.Calibrator`
    :param reqs: Requirement dictionary for this
        :py:class:`~columnflow.calibration.base.Calibrator` instance
    :type reqs: dict
    :param inputs: Additional inputs, currently not used
    :type inputs: dict
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


#
# single calibrator for doing both JEC and JER smearing
#

@calibrator(
    uses={jec, jer},
    produces={jec, jer},
    # toggle for propagation to MET
    propagate_met=None,
    # functions to determine configs and files
    get_jec_file=None,
    get_jec_config=None,
    get_jer_file=None,
    get_jer_config=None,
)
def jets(self: Calibrator, events: ak.Array, **kwargs) -> ak.Array:
    """Instance of :py:class:`~columnflow.calibration.base.Calibrator` that
    does all relevant calibrations for jets, i.e. JEC and JER.
    For more information, see :py:func:`~.jec` and :py:func:`~.jer`.

    This instance of :py:class:`~columnflow.calibration.base.Calibrator` is
    initialized with the following parameters by default:

    :*uses*: Same as the two base Calibrators, see :py:func:`~.jec` 
        and :py:func:`~.jer`.
    :*produces*: Same as the two base Calibrators, see :py:func:`~.jec`
        and :py:func:`~.jer`.

    :*propagate_met*: ``None``

    :get_jec_file: ``None``
    :get_jec_config: ``None``
    :get_jer_file: ``None``
    :get_jer_config: ``None``

    :param self: :py:class:`~columnflow.calibration.base.Calibrator` class in which
        this function is embedded
    :type self: :py:class:`~columnflow.calibration.base.Calibrator`

    :param events: awkward array containing events to process
    :type events: :external+ak:py:class:`ak.Array`

    :return: awkward array containing new JEC and JER related columns, see
        :py:func:`~.jec` and :py:func:`~.jer`
    :rtype: :external+ak:py:class:`ak.Array`
    """
    # apply jet energy corrections
    events = self[jec](events, **kwargs)

    # apply jer smearing on MC only
    if self.dataset_inst.is_mc:
        events = self[jer](events, **kwargs)

    return events


@jets.init
def jets_init(self: Calibrator) -> None:
    """:py:meth:`init` function for :py:func:`~.jets`
    :py:class:`~columnflow.calibration.base.Calibrator`.
    
    If *propagate_met*, *get_jec_file*, *get_jec_config*, *get_jec_file*,
    *get_jec_config* are set (i.e. not ``None``), propagate these settings
    to the underlying :py:func:`~.jec` and :py:func:`~.jer` Calibrators.
    Otherwise, use their defaults. 

    :param self: :py:class:`~columnflow.calibration.base.Calibrator` instance
    :type self:  :py:class:`~columnflow.calibration.base.Calibrator`
    """

    # forward argument to the producers
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
