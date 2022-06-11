# coding: utf-8

"""
Calibration methods for testing purposes.
"""
import os

import law

from ap.util import maybe_import, memoize
from ap.calibration import calibrator
from ap.columnar_util import set_ak_column

np = maybe_import("numpy")
ak = maybe_import("awkward")
coffea = maybe_import("coffea")

coffea_extractor = maybe_import("coffea.lookup_tools.extractor")
coffea_jetmet_tools = maybe_import("coffea.jetmet_tools")
coffea_txt_converters = maybe_import("coffea.lookup_tools.txt_converters")


@memoize
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

    Entries in `names` may also be tuples of the form (`src_name`, `dst_name`), in which case the
    `src_name` will be replaced by `dst_name` when passing the names to the `provider_cls`.

    The user must ensure that the `files` can be parsed by the `conversion_func` supplied, and that
    the information contained in the files is meaningful in connection with the `provider_cls`.
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
            raise ValueError(f"No weight tables found for the following names: {unknown_names}. Available: {available}")
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


@calibrator(
    uses={"nJet", "Jet.pt", "Jet.eta", "Jet.area", "Jet.mass", "Jet.rawFactor", "fixedGridRhoFastjetAll"},
    produces={
        "Jet.pt", "Jet.mass", "Jet.rawFactor",
    },
)
def jec(events, config_inst, dataset_inst, jec_files, junc_files, jec_names, junc_names, **kwargs):
    """Apply jet energy corrections and calculate shifts for jet energy uncertainty sources."""

    # calculate uncorrected pt, mass
    set_ak_column(events, "Jet.pt_raw", events.Jet.pt * (1 - events.Jet.rawFactor))
    set_ak_column(events, "Jet.mass_raw", events.Jet.mass * (1 - events.Jet.rawFactor))

    # build/retrieve lookup providers for JECs and uncertainties
    jec_provider = get_lookup_provider(
        jec_files,
        coffea_txt_converters.convert_jec_txt_file,
        coffea_jetmet_tools.FactorizedJetCorrector,
        names=jec_names,
    )
    junc_provider = get_lookup_provider(
        junc_files,
        coffea_txt_converters.convert_junc_txt_file,
        coffea_jetmet_tools.JetCorrectionUncertainty,
        names=junc_names,
    )

    # look up JEC correction factors
    jec_factors = jec_provider.getCorrection(
        JetEta=events.Jet.eta,
        JetPt=events.Jet.pt_raw,
        JetA=events.Jet.area,
        Rho=events.fixedGridRhoFastjetAll,
    )

    # store corrected pt, mass and recalculate rawFactor
    set_ak_column(events, "Jet.pt", events.Jet.pt_raw * jec_factors)
    set_ak_column(events, "Jet.mass", events.Jet.mass_raw * jec_factors)
    set_ak_column(events, "Jet.rawFactor", (1 - events.Jet.pt_raw / events.Jet.pt))

    # look up JEC uncertainties
    jec_uncertainties = junc_provider.getUncertainty(
        JetEta=events.Jet.eta,
        JetPt=events.Jet.pt_raw,
    )
    for name, jec_unc_factors in jec_uncertainties:
        # jec_unc_factors[I_EVT][I_JET][I_VAR]
        set_ak_column(events, f"Jet.pt_{name}_up", events.Jet.pt * jec_unc_factors[:, :, 0])
        set_ak_column(events, f"Jet.pt_{name}_dn", events.Jet.pt * jec_unc_factors[:, :, 1])
        set_ak_column(events, f"Jet.mass_{name}_up", events.Jet.mass * jec_unc_factors[:, :, 0])
        set_ak_column(events, f"Jet.mass_{name}_dn", events.Jet.mass * jec_unc_factors[:, :, 1])

    return events


@jec.update
def update(self, config_inst, **kwargs):
    """Add JEC uncertainty shifts to the list of produced columns."""
    for shifted_var in ("pt", "mass"):
        self.produces |= {
            f"Jet.{shifted_var}_{junc_name}_{junc_dir}"
            for junc_name in config_inst.x.jec.uncertainty_sources
            for junc_dir in ('up', 'dn')
        }


@jec.requires
def requires(self, task, reqs):
    """Add external files bundle (for JEC text files) to dependencies."""
    if "external_files" not in reqs:
        from ap.tasks.external import BundleExternalFiles
        reqs["external_files"] = BundleExternalFiles.req(task)
    return reqs


def get_basenames(struct):
    """Replace full file paths in an arbitrary struct by the file basenames"""
    return law.util.map_struct(
        lambda p: os.path.splitext(os.path.basename(p[0] if isinstance(p, tuple) else p))[0],
        struct,
    )


@jec.setup
def setup(self, task, inputs, call_kwargs, **kwargs):
    """Determine correct JEC files for task based on config/dataset and inject them
    into the calibrator function call."""

    # get external files bundle that contains JEC text files
    bundle = task.requires()["calibrator"]["external_files"]

    # make selector for JEC text files based on sample type (and era for data)
    if task.dataset_inst.is_data:
        raise NotImplementedError("JEC for data has not been implemented yet.")
        era = "RunA"  # task.dataset_inst.x.era  # TODO: implement data eras
        resolve_sample = lambda x: x.data[era]
    else:
        resolve_sample = lambda x: x.mc

    # pass text files to calibrator method
    for key in ("jec", "junc"):
        # pass the paths to the text files that contain the corrections/uncertainties
        files = call_kwargs[f"{key}_files"] = [
            t.path for t in resolve_sample(bundle.files[key])
        ]

        # also pass a list of tuples encoding the correspondence between the original
        # file basenames and filenames on disk (as determined by `BundleExternalFiles`)
        # and the original file basenames (needed by coffea tools to handle the
        # weights correctly)
        basenames = get_basenames(files)
        orig_basenames = get_basenames(resolve_sample(task.config_inst.x.external_files[key]))
        call_kwargs[f"{key}_names"] = list(zip(basenames, orig_basenames))

    # ensure exactly one 'UncertaintySources' file is passed
    if len(call_kwargs["junc_names"]) != 1:
        raise ValueError(
            f"Expected exactly one 'UncertaintySources' file, got {len(call_kwargs['junc_names'])}",
        )

    # update the weight names to include the uncertainty sources specified in the config
    call_kwargs["junc_names"] = [
        (f"{basename}_{src}", f"{orig_basename}_{src}")
        for basename, orig_basename in call_kwargs["junc_names"]
        for src in task.config_inst.x.jec["uncertainty_sources"]
    ]
