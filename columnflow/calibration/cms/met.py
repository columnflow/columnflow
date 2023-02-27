# coding: utf-8

"""
MET corrections.
"""

from columnflow.calibration import Calibrator, calibrator
from columnflow.util import maybe_import
from columnflow.columnar_util import set_ak_column


np = maybe_import("numpy")
ak = maybe_import("awkward")


@calibrator(
    uses={"run", "PV.npvs", "MET.pt", "MET.phi"},
    produces={"MET.pt", "MET.phi"},
    # function to determine the correction file
    get_met_file=(lambda self, external_files: external_files.met_phi_corr),
    # function to determine met correction config
    get_met_config=(lambda self: self.config_inst.x.met_phi_correction_set),
)
def met_phi(self: Calibrator, events: ak.Array, **kwargs) -> ak.Array:
    """
    Performs the MET phi (type II) correction using the correctionlib. Requires an external file in
    the config under ``met_phi_corr``:

    .. code-block:: python

        cfg.x.external_files = DotDict.wrap({
            "met_phi_corr": "/afs/cern.ch/user/m/mrieger/public/mirrors/jsonpog-integration-f018adfb/POG/JME/2017_UL/met.json.gz",  # noqa
        })

    *get_met_file* can be adapted in a subclass in case it is stored differently in the external
    files.

    The name of the correction set should be present as an auxiliary entry in the config:

    .. code-block:: python

        cfg.x.met_phi_correction_set = "{variable}_metphicorr_pfmet_{data_source}"

    where "variable" and "data_source" are placeholders that are inserted in the calibrator setup.
    *get_met_correction_set* can be adapted in a subclass in case it is stored differently in the
    config.
    """
    args = (
        events.MET.pt,
        events.MET.phi,
        ak.values_astype(events.PV.npvs, np.float32),
        ak.values_astype(events.run, np.float32),
    )
    corr_pt = self.met_pt_corrector.evaluate(*args)
    corr_phi = self.met_phi_corrector.evaluate(*args)

    events = set_ak_column(events, "MET.pt", corr_pt, value_type=np.float32)
    events = set_ak_column(events, "MET.phi", corr_phi, value_type=np.float32)

    return events


@met_phi.requires
def met_phi_requires(self: Calibrator, reqs: dict) -> None:
    if "external_files" in reqs:
        return

    from columnflow.tasks.external import BundleExternalFiles
    reqs["external_files"] = BundleExternalFiles.req(self.task)


@met_phi.setup
def met_phi_setup(self: Calibrator, reqs: dict, inputs: dict) -> None:
    bundle = reqs["external_files"]

    # create the pt and phi correctors
    import correctionlib
    correction_set = correctionlib.CorrectionSet.from_string(
        self.get_met_file(bundle.files).load(formatter="gzip").decode("utf-8"),
    )
    name_tmpl = self.get_met_config()
    self.met_pt_corrector = correction_set[name_tmpl.format(
        variable="pt",
        data_source=self.dataset_inst.data_source,
    )]
    self.met_phi_corrector = correction_set[name_tmpl.format(
        variable="phi",
        data_source=self.dataset_inst.data_source,
    )]
