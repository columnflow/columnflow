# coding: utf-8

"""
Calibration methods for jets
"""

import os
from typing import Tuple

import law

from columnflow.util import maybe_import, memoize
from columnflow.calibration import Calibrator, calibrator
from columnflow.production.util import attach_coffea_behavior
from columnflow.columnar_util import set_ak_column

np = maybe_import("numpy")
ak = maybe_import("awkward")
coffea = maybe_import("coffea")

coffea_extractor = maybe_import("coffea.lookup_tools.extractor")
coffea_jetmet_tools = maybe_import("coffea.jetmet_tools")
coffea_txt_converters = maybe_import("coffea.lookup_tools.txt_converters")


#
# first, some utility functions
#

@memoize
def get_lookup_provider(files, conversion_func, provider_cls, names=None):
    """
    Create a coffea helper object for looking up information in files of various formats.

    This function reads in the *files* containing lookup tables (e.g. JEC text files), extracts
    the table of values ("weights") using the conversion function *conversion_func* implemented
    in coffea, and uses them to construct a helper object of type *provider_cls* that can be
    passed event data to yield the lookup values (e.g. a :py:class:`FactorizedJetCorrector` or
    :py:class:`JetCorrectionUncertainty`).

    Optionally, a list of *names* can be supplied to select only a subset of weight tables
    for constructing the provider object (the default is to use all of them). This is intended
    to be useful for e.g. selecting only a particular set of jet energy uncertainties from an
    "UncertaintySources" file. By convention, the *names* always start with the basename of the
    file that contains the corresponding weight table.

    Entries in *names* may also be tuples of the form (*src_name*, *dst_name*), in which case the
    *src_name* will be replaced by *dst_name* when passing the names to the *provider_cls*.

    The user must ensure that the *files* can be parsed by the *conversion_func* supplied, and that
    the information contained in the files is meaningful in connection with the *provider_cls*.
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


def get_basenames(struct):
    """Replace full file paths in an arbitrary struct by the file basenames"""
    return law.util.map_struct(
        lambda p: os.path.splitext(os.path.basename(p[0] if isinstance(p, tuple) else p))[0],
        struct,
    )


# https://github.com/scikit-hep/awkward/issues/489\#issuecomment-711090923
def ak_random(*args, rand_func):
    """Return an awkward array filled with random numbers. The *args* must be broadcastable
    awkward arrays and will be passed as positional arguments to *rand_func* to obtain the
    random numbers."""
    from awkward import Array
    from awkward.layout import ListOffsetArray64

    args = ak.broadcast_arrays(*args)

    if hasattr(args[0].layout, "offsets"):
        # convert to flat numpy arrays
        np_args = [np.asarray(a.layout.content) for a in args]

        # pass flat arrays to random function and get random values
        np_randvals = rand_func(*np_args)

        # convert back to awkward array
        return Array(ListOffsetArray64(args[0].layout.offsets, ak.layout.NumpyArray(np_randvals)))
    else:
        # pass args directly (this may fail for some array types)
        # TODO: make function more general
        np_randvals = rand_func(*args)
        return ak.from_numpy(np_randvals)


# helper to compute new MET based on per-jet pts and phis before and after a correction
def prop_met(
    jet_pt1: ak.Array,
    jet_phi1: ak.Array,
    jet_pt2: ak.Array,
    jet_phi2: ak.Array,
    met_pt1: ak.Array,
    met_phi1: ak.Array,
) -> Tuple[ak.Array, ak.Array]:
    # build px and py sums before and after
    jet_px1 = jet_pt1 * np.cos(jet_phi1)
    jet_py1 = jet_pt1 * np.sin(jet_phi1)
    jet_px2 = jet_pt2 * np.cos(jet_phi2)
    jet_py2 = jet_pt2 * np.sin(jet_phi2)

    # build sums instead per-jet info was passed
    if jet_pt1.ndim > 1:
        jet_px1 = ak.sum(jet_px1, axis=1)
        jet_py1 = ak.sum(jet_py1, axis=1)
        jet_px2 = ak.sum(jet_px2, axis=1)
        jet_py2 = ak.sum(jet_py2, axis=1)

    # propagate to met
    met_px2 = met_pt1 * np.cos(met_phi1) - (jet_px2 - jet_px1)
    met_py2 = met_pt1 * np.sin(met_phi1) - (jet_py2 - jet_py1)

    # compute new components
    met_pt2 = (met_px2**2.0 + met_py2**2.0)**0.5
    met_phi2 = np.arctan(met_py2 / met_px2)

    return met_pt2, met_phi2


#
# Jet energy corrections
#

@calibrator(
    uses={
        "nJet", "Jet.pt", "Jet.eta", "Jet.phi", "Jet.mass", "Jet.area", "Jet.rawFactor",
        "Jet.jetId",
        "MET.pt", "MET.phi",
        "fixedGridRhoFastjetAll",
        attach_coffea_behavior,
    },
    produces={
        "Jet.pt", "Jet.mass", "Jet.rawFactor", "MET.pt", "MET.phi",
    },
    # custom uncertainty sources, defaults to config when empty
    uncertainty_sources=None,
)
def jec(self: Calibrator, events: ak.Array, **kwargs) -> ak.Array:
    """
    Apply jet energy corrections and calculate shifts for jet energy uncertainty sources.
    """
    # calculate uncorrected pt, mass
    events = set_ak_column(events, "Jet.pt_raw", events.Jet.pt * (1 - events.Jet.rawFactor))
    events = set_ak_column(events, "Jet.mass_raw", events.Jet.mass * (1 - events.Jet.rawFactor))

    # build/retrieve lookup providers for JECs and uncertainties
    # NOTE: could also be moved to `jec_setup`, but keep here in case the provider ever needs
    #       to change based on the event content (JEC change in the middle of a run)
    jec_provider = get_lookup_provider(
        self.jec_files,
        coffea_txt_converters.convert_jec_txt_file,
        coffea_jetmet_tools.FactorizedJetCorrector,
        names=self.jec_names,
    )
    if self.junc_names:
        junc_provider = get_lookup_provider(
            self.junc_files,
            coffea_txt_converters.convert_junc_txt_file,
            coffea_jetmet_tools.JetCorrectionUncertainty,
            names=self.junc_names,
        )

    # look up JEC correction factors
    jec_factors = jec_provider.getCorrection(
        JetEta=events.Jet.eta,
        JetPt=events.Jet.pt_raw,
        JetA=events.Jet.area,
        Rho=events.fixedGridRhoFastjetAll,
    )

    # store pt and phi of the full jet system
    jetsum = ak.sum(events.Jet, axis=1)
    jet_pt1 = jetsum.pt
    jet_phi1 = jetsum.phi

    # store corrected pt, mass and recalculate rawFactor
    events = set_ak_column(events, "Jet.pt", events.Jet.pt_raw * jec_factors)
    events = set_ak_column(events, "Jet.mass", events.Jet.mass_raw * jec_factors)
    events = set_ak_column(events, "Jet.rawFactor", (1 - events.Jet.pt_raw / events.Jet.pt))

    # recover coffea behavior
    events = self[attach_coffea_behavior](events, colletions=["Jet"], **kwargs)

    # get pt and phi of all jets after correcting
    jetsum = ak.sum(events.Jet, axis=1)
    jet_pt2 = jetsum.pt
    jet_phi2 = jetsum.phi

    # propagate changes to MET
    met_pt2, met_phi2 = prop_met(jet_pt1, jet_phi1, jet_pt2, jet_phi2, events.MET.pt, events.MET.phi)
    events = set_ak_column(events, "MET.pt", met_pt2)
    events = set_ak_column(events, "MET.phi", met_phi2)

    # look up JEC uncertainties
    if self.junc_names:
        jec_uncertainties = junc_provider.getUncertainty(
            JetEta=events.Jet.eta,
            JetPt=events.Jet.pt_raw,
        )
        for name, jec_unc_factors in jec_uncertainties:
            # jec_unc_factors[I_EVT][I_JET][I_VAR]
            events = set_ak_column(events, f"Jet.pt_jec_{name}_up", events.Jet.pt * jec_unc_factors[:, :, 0])
            events = set_ak_column(events, f"Jet.pt_jec_{name}_down", events.Jet.pt * jec_unc_factors[:, :, 1])
            events = set_ak_column(events, f"Jet.mass_jec_{name}_up", events.Jet.mass * jec_unc_factors[:, :, 0])
            events = set_ak_column(events, f"Jet.mass_jec_{name}_down", events.Jet.mass * jec_unc_factors[:, :, 1])

            # MET propagation
            jet_pt_up = events.Jet[f"pt_{name}_up"]
            jet_pt_down = events.Jet[f"pt_{name}_down"]
            met_pt_up, met_phi_up = prop_met(jet_pt2, jet_phi2, jet_pt_up, jet_phi2, met_pt2, met_phi2)
            met_pt_down, met_phi_down = prop_met(jet_pt2, jet_phi2, jet_pt_down, jet_phi2, met_pt2, met_phi2)
            events = set_ak_column(events, f"MET.pt_jec_{name}_up", met_pt_up)
            events = set_ak_column(events, f"MET.pt_jec_{name}_down", met_pt_down)
            events = set_ak_column(events, f"MET.phi_jec_{name}_up", met_phi_up)
            events = set_ak_column(events, f"MET.phi_jec_{name}_down", met_phi_down)

    return events


@jec.init
def jec_init(self: Calibrator) -> None:
    """
    Add JEC uncertainty shifts to the list of produced columns.
    """
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

    # add shifted MET variables
    self.produces |= {
        f"MET.{shifted_var}_jec_{junc_name}_{junc_dir}"
        for shifted_var in ("pt", "phi")
        for junc_name in sources
        for junc_dir in ("up", "down")
    }


@jec.requires
def jec_requires(self: Calibrator, reqs: dict) -> None:
    """
    Add external files bundle (for JEC text files) to dependencies.
    """
    if "external_files" in reqs:
        return

    from columnflow.tasks.external import BundleExternalFiles
    reqs["external_files"] = BundleExternalFiles.req(self.task)


@jec.setup
def jec_setup(self: Calibrator, reqs: dict, inputs: dict) -> None:
    """
    Determine correct JEC files for task based on config/dataset and inject them
    into the calibrator function call.
    """

    # get external files bundle that contains JEC text files
    bundle = reqs["external_files"]

    # make selector for JEC text files based on sample type (and era for data)
    if self.dataset_inst.is_data:
        resolve_sample = lambda x: x.data[self.dataset_inst.x.jec_era]
    else:
        resolve_sample = lambda x: x.mc

    # pass text files to calibrator method
    for key in ("jec", "junc"):
        # pass the paths to the text files that contain the corrections/uncertainties
        files = [
            t.path for t in resolve_sample(bundle.files[key])
        ]
        setattr(self, f"{key}_files", files)

        # also pass a list of tuples encoding the correspondence between the original
        # file basenames and filenames on disk (as determined by `BundleExternalFiles`)
        # and the original file basenames (needed by coffea tools to handle the
        # weights correctly)
        basenames = get_basenames(files)
        orig_basenames = get_basenames(resolve_sample(self.config_inst.x.external_files[key]))
        setattr(self, f"{key}_names", list(zip(basenames, orig_basenames)))

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


#
# Jet energy resolution smearing
#

@calibrator(
    uses={
        "nJet", "Jet.pt", "Jet.eta", "Jet.phi", "Jet.mass", "Jet.genJetIdx",
        "fixedGridRhoFastjetAll",
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
)
def jer(self: Calibrator, events: ak.Array, **kwargs) -> ak.Array:
    """
    Apply jet energy resolution smearing and calculate shifts for JER scale factor variations.
    Follows the recommendations given in https://twiki.cern.ch/twiki/bin/viewauth/CMS/JetResolution.
    """

    # complain when running on data
    if self.dataset_inst.is_data:
        raise ValueError("attempt to apply jet energy resolution smearing in data")

    # save the unsmeared properties in case they are needed later
    events = set_ak_column(events, "Jet.pt_unsmeared", events.Jet.pt)
    events = set_ak_column(events, "Jet.mass_unsmeared", events.Jet.mass)

    # use event numbers in chunk to seed random number generator
    # TODO: use seeds!
    rand_gen = np.random.Generator(np.random.SFC64(events.event.to_list()))

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
        Rho=events.fixedGridRhoFastjetAll,
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
    jetsum = ak.sum(events.Jet, axis=1)
    jet_pt1 = jetsum.pt
    jet_phi1 = jetsum.phi

    # apply the smearing factors to the pt and mass
    # (note: apply variations first since they refer to the original pt)
    events = set_ak_column(events, "Jet.pt_jer_up", events.Jet.pt * smear_factors[:, :, 1])
    events = set_ak_column(events, "Jet.mass_jer_up", events.Jet.mass * smear_factors[:, :, 1])
    events = set_ak_column(events, "Jet.pt_jer_down", events.Jet.pt * smear_factors[:, :, 2])
    events = set_ak_column(events, "Jet.mass_jer_down", events.Jet.mass * smear_factors[:, :, 2])
    events = set_ak_column(events, "Jet.pt", events.Jet.pt * smear_factors[:, :, 0])
    events = set_ak_column(events, "Jet.mass", events.Jet.mass * smear_factors[:, :, 0])

    # recover coffea behavior
    events = self[attach_coffea_behavior](events, colletions=["Jet"], **kwargs)

    # get pt and phi of all jets after correcting
    jetsum = ak.sum(events.Jet, axis=1)
    jet_pt2 = jetsum.pt
    jet_phi2 = jetsum.phi

    # propagate changes to MET
    met_pt2, met_phi2 = prop_met(jet_pt1, jet_phi1, jet_pt2, jet_phi2, events.MET.pt, events.MET.phi)
    met_pt_up, met_phi_up = prop_met(jet_pt2, jet_phi2, events.Jet.pt_jer_up, jet_phi2, met_pt2, met_phi2)
    met_pt_down, met_phi_down = prop_met(jet_pt2, jet_phi2, events.Jet.pt_jer_down, jet_phi2, met_pt2, met_phi2)
    events = set_ak_column(events, "MET.pt", met_pt2)
    events = set_ak_column(events, "MET.phi", met_phi2)
    events = set_ak_column(events, "MET.pt_jer_up", met_pt_up)
    events = set_ak_column(events, "MET.pt_jer_down", met_pt_down)
    events = set_ak_column(events, "MET.phi_jer_up", met_phi_up)
    events = set_ak_column(events, "MET.phi_jer_down", met_phi_down)

    return events


@jer.requires
def jer_requires(self: Calibrator, reqs: dict) -> None:
    """
    Add external files bundle (for JR text files) to dependencies.
    """
    if "external_files" in reqs:
        return

    from columnflow.tasks.external import BundleExternalFiles
    reqs["external_files"] = BundleExternalFiles.req(self.task)


@jer.setup
def jer_setup(self: Calibrator, reqs: dict, inputs: dict) -> None:
    """
    Determine correct JR files for task based on config/dataset and inject them
    into the calibrator function call.
    """

    # get external files bundle that contains JR text files
    bundle = reqs["external_files"]

    # make selector for JEC text files based on sample type
    if self.dataset_inst.is_data:
        raise ValueError("attempt to setup jet energy resolution smearing in data")
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

@calibrator(uses={jec, jer}, produces={jec, jer})
def jets(self: Calibrator, events: ak.Array, **kwargs) -> ak.Array:
    # apply jet energy corrections
    events = self[jec](events, **kwargs)

    # apply jer smearing on MC only
    if self.dataset_inst.is_mc:
        events = self[jer](events, **kwargs)

    return events


@jets.init
def jets_init(self: Calibrator) -> None:
    if getattr(self, "dataset_inst", None) and self.dataset_inst.is_mc:
        self.uses |= {jer}
        self.produces |= {jer}
