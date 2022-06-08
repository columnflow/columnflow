# coding: utf-8

"""
Calibration methods for testing purposes.
"""
import os

from ap.util import maybe_import, memoize
from ap.calibration import calibrator
from ap.columnar_util import set_ak_column

from coffea.lookup_tools.extractor import extractor as coffea_extractor
from coffea.jetmet_tools import FactorizedJetCorrector, JetCorrectionUncertainty

from coffea.lookup_tools.txt_converters import (convert_jec_txt_file, convert_junc_txt_file)

np = maybe_import("numpy")
ak = maybe_import("awkward")


def get_lookup_provider(files, conversion_func, provider_cls, names=None):
    """
    Create a coffea helper object for looking up information in files of various formats.

    This function reads in the `files` containing lookup tables (e.g. JEC text files), extracts
    the table of values ("weights") using the conversion function `conversion_func` implemented
    in coffea, and uses them to construct a helper object of type `provider_cls` that can be
    passed event data to yield the lookup values (e.g. a `FactorizedJetCorrector` or
    `JetCorrectionUncertainty`).

    Optionally, a list of `names` can be supplied to select only a subset of weight tables
    for constructing the provider object (the default is to use all of them). This is intended
    to be useful for e.g. selecting only a particular set of jet energy uncertainties from an
    "UncertaintySources" file. By convention, the `names` always start with the basename of the
    file that contains the corresponding weight table.

    The user must ensure that the `files` can be parsed by the `conversion_func` supplied, and that
    the information contained in the files is meaningful in connection with the `provider_cls`.
    """

    # the extractor reads the information contained in the files
    extractor = coffea_extractor()

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
        unknown_names = set(names) - set(all_names)
        if unknown_names:
            unknown_names = ", ".join(sorted(list(unknown_names)))
            available = ", ".join(sorted(list(all_names)))
            raise ValueError(f"No weight tables found for the following names: {unknown_names}. Available: {available}")
    else:
        names = all_names

    # the evaluator does the actual lookup for each separate name
    evaluator = extractor.make_evaluator()

    # the provider combines lookup results from multiple names
    provider = provider_cls(**{
        name: evaluator[name]
        for name in names
    })

    return provider


def get_jec_files(base_dir, version, sample, jet_type, names):
    """Convenience function for constructing the paths to JEC files."""
    return [
        os.path.join(base_dir, f"{version}_{sample}", f"{version}_{sample}_{name}_{jet_type}.txt")
        for name in names
    ]


@memoize
def get_jec_providers(config, dataset):
    """Construct the lookup helpers used by the calibrator. Expensive, so memoized for efficiency."""

    # get the global JEC configuration (version, levels, etc.)
    jec = config.get_aux("jec")

    # determine the correct JEC files for the dataset
    if dataset.is_data:
        # look up data era
        #sample = "Run2018A"  # TODO: implement data eras  # noqa
        raise NotImplementedError("JEC calibrator for data has not been implemented yet.")
    else:
        sample = "MC"

    jec_files = get_jec_files(
        jec["txt_file_path"], jec["version"], sample, jec["jet_type"], jec["levels"],
    )
    junc_files = get_jec_files(
        jec["txt_file_path"], jec["version"], sample, jec["jet_type"], ["UncertaintySources"],
    )

    # build the lookup providers
    return {
        "jec": get_lookup_provider(
            jec_files,
            convert_jec_txt_file,
            FactorizedJetCorrector,
        ),
        "junc": get_lookup_provider(
            junc_files,
            convert_junc_txt_file,
            JetCorrectionUncertainty,
            # only select uncertainties of interest
            names=[
                "_".join([os.path.basename(junc_files[0]).split('.')[0], junc_src])
                for junc_src in jec["uncertainty_sources"]
            ],
        ),
    }


@calibrator(
    uses={"nJet", "Jet.pt", "Jet.eta", "Jet.area", "Jet.mass", "Jet.rawFactor", "fixedGridRhoFastjetAll"},
    produces=set((
        "Jet.pt", "Jet.mass", "Jet.rawFactor",
    ) + tuple(
        f"Jet.pt_{junc_name}_{junc_dir}"
        for junc_name in ("Total", ) for junc_dir in ('up', 'dn')
        # TODO: different sources depend on campaign -> how to resolve dynamically?
    )),
)
def jec(events, **kwargs):
    """Apply jet energy corrections and calculate shifts for jet energy uncertainty sources."""

    # calculate uncorrected pt, mass
    set_ak_column(events, "Jet.pt_raw", events.Jet.pt * (1 - events.Jet.rawFactor))
    set_ak_column(events, "Jet.mass_raw", events.Jet.mass * (1 - events.Jet.rawFactor))

    jec_providers = get_jec_providers(kwargs["config_inst"], kwargs["dataset_inst"])

    # retrieve JEC correction factors
    jec_factors = jec_providers["jec"].getCorrection(
        JetEta=events.Jet.eta,
        JetPt=events.Jet.pt_raw,
        JetA=events.Jet.area,
        Rho=events.fixedGridRhoFastjetAll,
    )

    # store corrected pt, mass and recalculate rawFactor
    set_ak_column(events, "Jet.pt", events.Jet.pt_raw * jec_factors)
    set_ak_column(events, "Jet.mass", events.Jet.mass_raw * jec_factors)
    set_ak_column(events, "Jet.rawFactor", (1 - events.Jet.pt_raw / events.Jet.pt))

    # [('SourceName', [[up_val down_val]_jet1 ... ]), ...]
    jec_uncertainties = jec_providers["junc"].getUncertainty(
        JetEta=events.Jet.eta,
        JetPt=events.Jet.pt_raw,
    )
    for name, jec_unc_factors in jec_uncertainties:
        # jec_unc_factors[I_EVT][I_JET][I_VAR]
        set_ak_column(events, f"Jet.pt_{name}_up", events.Jet.pt * jec_unc_factors[:, :, 0])
        set_ak_column(events, f"Jet.pt_{name}_dn", events.Jet.pt * jec_unc_factors[:, :, 1])

    return events
