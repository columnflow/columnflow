# coding: utf-8

"""
Calibration methods for jets using :external+coffea:doc:`coffea <installation>`
functions.
"""

import os
import functools

import law

from columnflow.util import maybe_import, memoize, InsertableDict
from columnflow.calibration import Calibrator, calibrator
from columnflow.calibration.util import propagate_met, ak_random
from columnflow.production.util import attach_coffea_behavior
from columnflow.columnar_util import set_ak_column
from typing import Iterable, Callable, Type

np = maybe_import("numpy")
ak = maybe_import("awkward")

coffea_extractor = maybe_import("coffea.lookup_tools.extractor")
coffea_jetmet_tools = maybe_import("coffea.jetmet_tools")
coffea_txt_converters = maybe_import("coffea.lookup_tools.txt_converters")


#
# first, some utility functions
#

set_ak_column_f32 = functools.partial(set_ak_column, value_type=np.float32)


def get_basenames(struct: Iterable) -> Iterable:
    """
    Replace full file paths in an arbitrary struct by the file basenames.

    The function loops through the structure and extracts the base name using a combination of
    :py:func:`os.path.splitext` and :py:func:`os.path.basename`. The loop itself is done using the
    :external+law:py:func:`law.util.map_struct` function.

    :param struct: Iterable of arbitrary nested structure containing full file paths

    :return: Iterable of same structure as *struct* containing only basenames of paths.
    """
    return law.util.map_struct(
        lambda p: os.path.splitext(os.path.basename(p[0] if isinstance(p, tuple) else p))[0],
        struct,
    )


@memoize
def get_lookup_provider(
    files: list,
    conversion_func: Callable,
    provider_cls: Type,
    names: list[str or tuple[str, str]] = None,
) -> Type:
    """
    Create a coffea helper object for looking up information in files of various formats.

    This function reads in the *files* containing lookup tables (e.g. JEC text files), extracts the
    table of values ("weights") using the conversion function *conversion_func* implemented in
    coffea, and uses them to construct a helper object of type *provider_cls* that can be passed
    event data to yield the lookup values (e.g. a
    :external+coffea:py:class:`~coffea.jetmet_tools.FactorizedJetCorrector` or
    :external+coffea:py:class:`~coffea.jetmet_tools.JetCorrectionUncertainty`).

    Optionally, a list of *names* can be supplied to select only a subset of weight tables for
    constructing the provider object (the default is to use all of them). This is intended to be
    useful for e.g. selecting only a particular set of jet energy uncertainties from an
    "UncertaintySources" file. By convention, the *names* always start with the basename of the file
    that contains the corresponding weight table.

    Entries in *names* may also be tuples of the form (*src_name*, *dst_name*), in which case the
    *src_name* will be replaced by *dst_name* when passing the names to the *provider_cls*.

    The user must ensure that the *files* can be parsed by the *conversion_func* supplied, and that
    the information contained in the files is meaningful in connection with the *provider_cls*.

    :param files: List of files containing lookup tables (e.g. JEC text files).
    :param conversion_func:  ``Callable`` that extracts the table of weights from the files in
        *files*. Must return an *Iterable* that provides a :py:meth:`items` method that returns a
        structure like (name, type), value
    :param provider_cls: Class method that is used to construct the *provider* instance that finally
        provides the weights for the events. Examples:
        :external+coffea:py:class:`~coffea.jetmet_tools.FactorizedJetCorrector`,
        :external+coffea:py:class:`~coffea.jetmet_tools.JetCorrectionUncertainty`
    :param names: Optional list of weight names to include, see text above.
    :raises ValueError: If *names* contains weight names that are not present in the source file
    :return: helper class that provides the weights for the events of same type as *provider_cls*
            (e.g.
            :external+coffea:py:class:`~coffea.jetmet_tools.FactorizedJetCorrector`,
            :external+coffea:py:class:`~coffea.jetmet_tools.JetCorrectionUncertainty`)
    """
    # the extractor reads the information contained in the files
    extractor = coffea_extractor.extractor()

    # files contain one or more lookup tables, each identified by a name
    all_names = []
    for file_ in files:
        # the actual file parsing is done here
        weights = conversion_func(file_)
        for (name, type_), value in weights.items():
            extractor.add_weight_set(name, type_, value)
            all_names.append(name)

    extractor.finalize()

    # if user provided explicit names, check that corresponding
    # weight tables have been read
    if names is not None:
        src_dst_names = [n if isinstance(n, tuple) else (n, n) for n in names]
        unknown_names = set(src_name for src_name, _ in src_dst_names) - set(all_names)
        if unknown_names:
            unknown_names = ", ".join(sorted(list(unknown_names)))
            available = ", ".join(sorted(list(all_names)))
            raise ValueError(
                f"no weight tables found for the following names: {unknown_names}, "
                f"available: {available}",
            )
        # TODO: I don't think the code works correctly if *names* is a list of
        # strings, since further down below the code explicitly needs a tuple
        # structure. We will probably need something like the following here

        # names = src_dst_names
    else:
        names = [(n, n) for n in all_names]

    # the evaluator does the actual lookup for each separate name
    evaluator = extractor.make_evaluator()

    # the provider combines lookup results from multiple names
    provider = provider_cls(**{
        dst_name: evaluator[src_name]
        for src_name, dst_name in names
    })

    return provider


#
# Jet energy corrections
#

@calibrator(
    uses={
        "Jet.pt", "Jet.eta", "Jet.phi", "Jet.mass", "Jet.area", "Jet.rawFactor",
        "Jet.jetId",
        "Rho.fixedGridRhoFastjetAll", "fixedGridRhoFastjetAll",
        attach_coffea_behavior,
    },
    produces={
        "Jet.pt", "Jet.mass", "Jet.rawFactor",
    },
    # custom uncertainty sources, defaults to config when empty
    uncertainty_sources=None,
    # toggle for propagation to MET
    propagate_met=True,
)
def jec_coffea(
    self: Calibrator,
    events: ak.Array,
    min_pt_met_prop: float = 15.0,
    max_eta_met_prop: float = 5.2,
    **kwargs,
) -> ak.Array:
    """
    Apply jet energy corrections and calculate shifts for jet energy uncertainty sources.

    :param events: awkward array containing events to process
    :param min_pt_met_prop: If *propagate_met* variable is ``True`` propagate the updated jet values
        to the missing transverse energy (MET) using
        :py:func:`~columnflow.calibration.util.propagate_met` for events where
        ``met.pt > min_pt_met_prop``.
    :param max_eta_met_prop: If *propagate_met* variable is ``True`` propagate the updated jet
        values to the missing transverse energy (MET) using
        :py:func:`~columnflow.calibration.util.propagate_met` for events where
        ``met.eta > min_eta_met_prop``.
    """
    # calculate uncorrected pt, mass
    events = set_ak_column_f32(events, "Jet.pt_raw", events.Jet.pt * (1 - events.Jet.rawFactor))
    events = set_ak_column_f32(events, "Jet.mass_raw", events.Jet.mass * (1 - events.Jet.rawFactor))

    # build/retrieve lookup providers for JECs and uncertainties
    # NOTE: could also be moved to `jec_setup`, but keep here in case the provider ever needs
    #       to change based on the event content (JEC change in the middle of a run)
    jec_provider = get_lookup_provider(
        self.jec_files,
        coffea_txt_converters.convert_jec_txt_file,
        coffea_jetmet_tools.FactorizedJetCorrector,
        names=self.jec_names,
    )
    jec_provider_only_l1 = get_lookup_provider(
        self.jec_files_only_l1,
        coffea_txt_converters.convert_jec_txt_file,
        coffea_jetmet_tools.FactorizedJetCorrector,
        names=self.jec_names_only_l1,
    )
    if self.junc_names:
        junc_provider = get_lookup_provider(
            self.junc_files,
            coffea_txt_converters.convert_junc_txt_file,
            coffea_jetmet_tools.JetCorrectionUncertainty,
            names=self.junc_names,
        )

    # obtain rho, which might be located at different routes, depending on the nano version
    rho = (
        events.fixedGridRhoFastjetAll
        if "fixedGridRhoFastjetAll" in events.fields else
        events.Rho.fixedGridRhoFastjetAll
    )

    # look up JEC correction factors
    jec_factors = jec_provider.getCorrection(
        JetEta=events.Jet.eta,
        JetPt=events.Jet.pt_raw,
        JetA=events.Jet.area,
        Rho=rho,
    )
    jec_factors_only_l1 = jec_provider_only_l1.getCorrection(
        JetEta=events.Jet.eta,
        JetPt=events.Jet.pt_raw,
        JetA=events.Jet.area,
        Rho=rho,
    )

    # apply the new factors with only L1 corrections
    events = set_ak_column_f32(events, "Jet.pt", events.Jet.pt_raw * jec_factors_only_l1)
    events = set_ak_column_f32(events, "Jet.mass", events.Jet.mass_raw * jec_factors_only_l1)
    events = self[attach_coffea_behavior](events, collections=["Jet"], **kwargs)

    # store pt and phi of the full jet system for MET propagation, including a selection in raw info
    # see https://twiki.cern.ch/twiki/bin/view/CMS/JECAnalysesRecommendations?rev=19#Minimum_jet_selection_cuts
    if self.propagate_met:
        met_prop_mask = (events.Jet.pt_raw > min_pt_met_prop) & (abs(events.Jet.eta) < max_eta_met_prop)
        jetsum = events.Jet[met_prop_mask].sum(axis=1)
        jet_pt_only_l1 = jetsum.pt
        jet_phi_only_l1 = jetsum.phi

    # full jet correction with all levels
    events = set_ak_column_f32(events, "Jet.pt", events.Jet.pt_raw * jec_factors)
    events = set_ak_column_f32(events, "Jet.mass", events.Jet.mass_raw * jec_factors)
    events = set_ak_column_f32(events, "Jet.rawFactor", (1 - events.Jet.pt_raw / events.Jet.pt))
    events = self[attach_coffea_behavior](events, collections=["Jet"], **kwargs)

    # nominal met propagation
    if self.propagate_met:
        # get pt and phi of all jets after correcting
        jetsum = events.Jet[met_prop_mask].sum(axis=1)
        jet_pt_all_levels = jetsum.pt
        jet_phi_all_levels = jetsum.phi

        # propagate changes from L2 corrections and onwards (i.e. no L1) to MET
        met_pt, met_phi = propagate_met(
            jet_pt_only_l1,
            jet_phi_only_l1,
            jet_pt_all_levels,
            jet_phi_all_levels,
            events.RawMET.pt,
            events.RawMET.phi,
        )
        events = set_ak_column_f32(events, "MET.pt", met_pt)
        events = set_ak_column_f32(events, "MET.phi", met_phi)

    # look up JEC uncertainties
    if self.junc_names:
        jec_uncertainties = junc_provider.getUncertainty(
            JetEta=events.Jet.eta,
            JetPt=events.Jet.pt_raw,
        )
        for name, jec_unc_factors in jec_uncertainties:
            # jec_unc_factors[I_EVT][I_JET][I_VAR]
            events = set_ak_column_f32(events, f"Jet.pt_jec_{name}_up", events.Jet.pt * jec_unc_factors[:, :, 0])
            events = set_ak_column_f32(events, f"Jet.pt_jec_{name}_down", events.Jet.pt * jec_unc_factors[:, :, 1])
            events = set_ak_column_f32(events, f"Jet.mass_jec_{name}_up", events.Jet.mass * jec_unc_factors[:, :, 0])
            events = set_ak_column_f32(events, f"Jet.mass_jec_{name}_down", events.Jet.mass * jec_unc_factors[:, :, 1])

            # shifted MET propagation
            if self.propagate_met:
                jet_pt_up = events.Jet[met_prop_mask][f"pt_jec_{name}_up"]
                jet_pt_down = events.Jet[met_prop_mask][f"pt_jec_{name}_down"]
                met_pt_up, met_phi_up = propagate_met(
                    jet_pt_all_levels,
                    jet_phi_all_levels,
                    jet_pt_up,
                    events.Jet[met_prop_mask].phi,
                    met_pt,
                    met_phi,
                )
                met_pt_down, met_phi_down = propagate_met(
                    jet_pt_all_levels,
                    jet_phi_all_levels,
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


@jec_coffea.init
def jec_coffea_init(self: Calibrator) -> None:
    sources = self.uncertainty_sources
    if sources is None:
        sources = self.config_inst.x.jec.uncertainty_sources

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


@jec_coffea.requires
def jec_coffea_requires(self: Calibrator, reqs: dict) -> None:
    if "external_files" in reqs:
        return

    from columnflow.tasks.external import BundleExternalFiles
    reqs["external_files"] = BundleExternalFiles.req(self.task)


@jec_coffea.setup
def jec_coffea_setup(self: Calibrator, reqs: dict, inputs: dict, reader_targets: InsertableDict) -> None:
    """
    Determine correct JEC files for task based on config/dataset and inject them into the calibrator
    function call.

    :param reqs: Requirement dictionary for this :py:class:`~columnflow.calibration.Calibrator`
        instance.
    :param inputs: Additional inputs, currently not used.
    :param reader_targets: TODO: add docs

    :raises ValueError: If module is provided with more than one JEC uncertainty source file.
    """
    # get external files bundle that contains JEC text files
    bundle = reqs["external_files"]

    # make selector for JEC text files based on sample type (and era for data)
    if self.dataset_inst.is_data:
        jec_era = self.dataset_inst.get_aux("jec_era", None)
        # if no special JEC era is specified, infer based on 'era'
        if jec_era is None:
            jec_era = "Run" + self.dataset_inst.get_aux("era")

        resolve_samples = lambda x: x.data[jec_era]
    else:
        resolve_samples = lambda x: x.mc

    # store jec files with all correction levels
    self.jec_files = [
        t.path
        for t in resolve_samples(bundle.files.jec).values()
    ]
    self.jec_names = list(zip(
        get_basenames(self.jec_files),
        get_basenames(resolve_samples(self.config_inst.x.external_files.jec).values()),
    ))

    # store jec files with only L1* corrections for MET propagation
    self.jec_files_only_l1 = [
        t.path
        for level, t in resolve_samples(bundle.files.jec).items()
        if level.startswith("L1")
    ]
    self.jec_names_only_l1 = list(zip(
        get_basenames(self.jec_files_only_l1),
        get_basenames([
            src
            for level, src in resolve_samples(self.config_inst.x.external_files.jec).items()
            if level.startswith("L1")
        ]),
    ))

    # store uncertainty
    self.junc_files = [
        t.path
        for t in resolve_samples(bundle.files.junc)
    ]
    self.junc_names = list(zip(
        get_basenames(self.junc_files),
        get_basenames(resolve_samples(self.config_inst.x.external_files.junc)),
    ))

    # ensure exactly one 'UncertaintySources' file is passed
    if len(self.junc_names) != 1:
        raise ValueError(
            f"expected exactly one 'UncertaintySources' file, got {len(self.junc_names)}",
        )

    sources = self.uncertainty_sources
    if sources is None:
        sources = self.config_inst.x.jec.uncertainty_sources

    # update the weight names to include the uncertainty sources specified in the config
    self.junc_names = [
        (f"{basename}_{src}", f"{orig_basename}_{src}")
        for basename, orig_basename in self.junc_names
        for src in sources
    ]


# custom jec calibrator that only runs nominal correction
jec_coffea_nominal = jec_coffea.derive("jec_coffea_nominal", cls_dict={"uncertainty_sources": []})


#
# Jet energy resolution smearing
#

@calibrator(
    uses={
        "Jet.pt", "Jet.eta", "Jet.phi", "Jet.mass", "Jet.genJetIdx",
        "Rho.fixedGridRhoFastjetAll", "fixedGridRhoFastjetAll",
        "GenJet.pt", "GenJet.eta", "GenJet.phi",
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
)
def jer_coffea(self: Calibrator, events: ak.Array, **kwargs) -> ak.Array:
    """
    Apply jet energy resolution smearing and calculate shifts for Jet Energy Resolution (JER) scale
    factor variations.

    Follows the recommendations given in https://twiki.cern.ch/twiki/bin/viewauth/CMS/JetResolution.

    The module applies the scale factors associated to the JER and performs the stochastic smearing
    to make the energy resolution in simulation more realistic.

    :param events: awkward array containing events to process
    """
    # save the unsmeared properties in case they are needed later
    events = set_ak_column_f32(events, "Jet.pt_unsmeared", events.Jet.pt)
    events = set_ak_column_f32(events, "Jet.mass_unsmeared", events.Jet.mass)

    # use event numbers in chunk to seed random number generator
    # TODO: use seeds!
    rand_gen = np.random.Generator(np.random.SFC64(events.event.to_list()))

    # obtain rho, which might be located at different routes, depending on the nano version
    rho = (
        events.fixedGridRhoFastjetAll
        if "fixedGridRhoFastjetAll" in events.fields else
        events.Rho.fixedGridRhoFastjetAll
    )

    # build/retrieve lookup providers for JECs and uncertainties
    # NOTE: could also be moved to `jer_setup`, but keep here in case the provider ever needs
    #       to change based on the event content (JER change in the middle of a run)
    jer_provider = get_lookup_provider(
        self.jer_files,
        coffea_txt_converters.convert_jr_txt_file,
        coffea_jetmet_tools.JetResolution,
        names=self.jer_names,
    )
    jersf_provider = get_lookup_provider(
        self.jersf_files,
        coffea_txt_converters.convert_jersf_txt_file,
        coffea_jetmet_tools.JetResolutionScaleFactor,
        names=self.jersf_names,
    )

    # look up jet energy resolutions
    # jer[I_EVT][I_JET]
    jer = ak.materialized(jer_provider.getResolution(
        JetEta=events.Jet.eta,
        JetPt=events.Jet.pt,
        Rho=rho,
    ))

    # look up jet energy resolution scale factors
    # jersf[I_EVT][I_JET][I_VAR]
    jersf = jersf_provider.getScaleFactor(
        JetEta=events.Jet.eta,
        JetPt=events.Jet.pt,
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
        jet_pt_before = jetsum.pt
        jet_phi_before = jetsum.phi

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
        jet_pt_after = jetsum.pt
        jet_phi_after = jetsum.phi

        # propagate changes to MET
        met_pt, met_phi = propagate_met(
            jet_pt_before,
            jet_phi_before,
            jet_pt_after,
            jet_phi_after,
            events.MET.pt,
            events.MET.phi,
        )
        met_pt_up, met_phi_up = propagate_met(
            jet_pt_after,
            jet_phi_after,
            events.Jet.pt_jer_up,
            events.Jet.phi,
            met_pt,
            met_phi,
        )
        met_pt_down, met_phi_down = propagate_met(
            jet_pt_after,
            jet_phi_after,
            events.Jet.pt_jer_down,
            events.Jet.phi,
            met_pt,
            met_phi,
        )
        events = set_ak_column_f32(events, "MET.pt", met_pt)
        events = set_ak_column_f32(events, "MET.phi", met_phi)
        events = set_ak_column_f32(events, "MET.pt_jer_up", met_pt_up)
        events = set_ak_column_f32(events, "MET.pt_jer_down", met_pt_down)
        events = set_ak_column_f32(events, "MET.phi_jer_up", met_phi_up)
        events = set_ak_column_f32(events, "MET.phi_jer_down", met_phi_down)

    return events


@jer_coffea.init
def jer_coffea_init(self: Calibrator) -> None:
    if not self.propagate_met:
        return

    self.uses |= {
        "MET.pt", "MET.phi",
    }
    self.produces |= {
        "MET.pt", "MET.phi", "MET.pt_jer_up", "MET.pt_jer_down", "MET.phi_jer_up",
        "MET.phi_jer_down", "MET.pt_unsmeared", "MET.phi_unsmeared",
    }


@jer_coffea.requires
def jer_coffea_requires(self: Calibrator, reqs: dict) -> None:
    if "external_files" in reqs:
        return

    from columnflow.tasks.external import BundleExternalFiles
    reqs["external_files"] = BundleExternalFiles.req(self.task)


@jer_coffea.setup
def jer_coffea_setup(
    self: Calibrator,
    reqs: dict, inputs: dict,
    reader_targets: InsertableDict,
) -> None:
    """
    Determine correct JER files for task based on config/dataset and inject them into the calibrator
    function call.

    :param reqs: Requirement dictionary for this :py:class:`~columnflow.calibration.Calibrator`
        instance.
    :param inputs: Additional inputs, currently not used.
    :param reader_targets: TODO: add docs.

    :raises ValueError: If module is provided with more than one JER uncertainty source file.
    """
    # get external files bundle that contains JR text files
    bundle = reqs["external_files"]

    resolve_sample = lambda x: x.mc

    # pass text files to calibrator method
    for key in ("jer", "jersf"):
        # pass the paths to the text files that contain the corrections/uncertainties
        files = [
            t.path for t in resolve_sample(bundle.files[key])
        ]
        setattr(self, f"{key}_files", files)

        # also pass a list of tuples encoding the correspondence between the
        # file basenames on disk (as determined by `BundleExternalFiles`) and the
        # original file basenames (needed by coffea to identify the weights correctly)
        basenames = get_basenames(files)
        orig_basenames = get_basenames(resolve_sample(self.config_inst.x.external_files[key]))
        setattr(self, f"{key}_names", list(zip(basenames, orig_basenames)))

        # ensure exactly one file is passed
        if len(files) != 1:
            raise ValueError(
                f"Expected exactly one file for key '{key}', got {len(files)}.",
            )


#
# General jets calibrator
#

@calibrator(
    uses={jec_coffea, jer_coffea},
    produces={jec_coffea, jer_coffea},
    # toggle for propagation to MET
    propagate_met=True,
)
def jets_coffea(self: Calibrator, events: ak.Array, **kwargs) -> ak.Array:
    """
    Instance of :py:class:`~columnflow.calibration.Calibrator` that does all relevant calibrations
    for jets, i.e. JEC and JER. For more information, see :py:class:`~.jec_coffea` and
    :py:class:`~.jer_coffea`.

    :param events: awkward array containing events to process.
    """
    # apply jet energy corrections
    events = self[jec_coffea](events, **kwargs)

    # apply jer smearing on MC only
    if self.dataset_inst.is_mc:
        events = self[jer_coffea](events, **kwargs)

    return events


@jets_coffea.init
def jets_coffea_init(self: Calibrator) -> None:
    # forward the propagate_met argument to the producers
    self.deps_kwargs[jec_coffea] = {"propagate_met": self.propagate_met}
    self.deps_kwargs[jer_coffea] = {"propagate_met": self.propagate_met}
