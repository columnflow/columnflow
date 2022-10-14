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
)
def met_phi(self: Calibrator, events: ak.Array, **kwargs) -> ak.Array:
    """
    Performs the MET phi (type II) correction using the correctionlib. Requires an external file in
    the config as (e.g.):

    .. code-block:: python

        "met_phi_corr": ("/afs/cern.ch/user/m/mrieger/public/mirrors/cms-met/metphi_corrs_all.json.gz", "v1")
    """
    args = (
        events.MET.pt,
        events.MET.phi,
        ak.values_astype(events.PV.npvs, np.float32),
        ak.values_astype(events.run, np.float32),
    )
    corr_pt = self.met_pt_corrector.evaluate(*args)
    corr_phi = self.met_phi_corrector.evaluate(*args)

    events = set_ak_column(events, "MET.pt", corr_pt)
    events = set_ak_column(events, "MET.phi", corr_phi)

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
        bundle.files.met_phi_corr.load(formatter="gzip").decode("utf-8"),
    )
    self.met_pt_corrector = correction_set[self.config_inst.x.met_phi_correction_set.format(
        variable="pt",
        data_source=self.dataset_inst.data_source,
    )]
    self.met_phi_corrector = correction_set[self.config_inst.x.met_phi_correction_set.format(
        variable="phi",
        data_source=self.dataset_inst.data_source,
    )]
