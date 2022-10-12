# coding: utf-8

"""
MET corrections.
"""

from columnflow.calibration import Calibrator, calibrator
from columnflow.util import maybe_import
from columnflow.columnar_util import set_ak_column

ak = maybe_import("awkward")


@calibrator(
    uses={"run", "PV.npvs", "MET.pt", "MET.phi"},
    produces={"MET.pt", "MET.phi"},
)
def met_phi(self: Calibrator, events: ak.Array, **kwargs) -> ak.Array:
    """
    Performs the MET phi (type II) correction using the external corrector tool. Requires the
    corrector tool external to be loaded as an external file in the config as:

    .. code-block:: python

        "met_phi_corr": ("https://mrieger.web.cern.ch/snippets/met_phi_correction.py", "v1"),
    """
    corr_pt, corr_phi = self.met_phi_corrector(
        uncor_pt=events.MET.pt,
        uncor_phi=events.MET.phi,
        npv=events.PV.npvs,
        run=events.run,
    )

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

    # create the corrector object
    pkg = bundle.files.met_phi_corr.load(formatter="python")
    METPhiCorrector = pkg.METPhiCorrector
    METCampaign = pkg.Campaign

    # determine the campaign
    if self.config_inst.campaign.x.year == 2016:
        met_campaign = METCampaign.UL_2016 if self.dataset_inst.is_data else METCampaign.UL_2016_APV
    elif self.config_inst.campaign.x.year == 2017:
        met_campaign = METCampaign.UL_2017
    elif self.config_inst.campaign.x.year == 2018:
        met_campaign = METCampaign.UL_2018
    else:
        raise ValueError(f"no MET phi correction campaign defined for config {self.config_inst}")

    self.met_phi_corrector = METPhiCorrector(met_campaign, self.dataset_inst.is_data)
