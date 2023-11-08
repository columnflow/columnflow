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
    Performs the MET phi (type II) correction using the
    :external+correctionlib:doc:`index` for events there the
    uncorrected MET pt is below the beam energy (extracted from ``config_inst.campaign.ecm * 0.5``).
    Requires an external file in the config under ``met_phi_corr``:

    .. code-block:: python

        cfg.x.external_files = DotDict.wrap({
            "met_phi_corr": "/afs/cern.ch/work/m/mrieger/public/mirrors/jsonpog-integration-9ea86c4c/POG/JME/2017_UL/met.json.gz",  # noqa
        })

    *get_met_file* can be adapted in a subclass in case it is stored differently in the external
    files.

    The name of the correction set should be present as an auxiliary entry in the config:

    .. code-block:: python

        cfg.x.met_phi_correction_set = "{variable}_metphicorr_pfmet_{data_source}"

    where "variable" and "data_source" are placeholders that are inserted in the
    calibrator setup :py:meth:`~.met_phi.setup_func`.
    *get_met_correction_set* can be adapted in a subclass in case it is stored
    differently in the config.

    :param events: awkward array containing events to process
    """
    # copy the intial pt and phi values
    corr_pt = np.array(events.MET.pt, dtype=np.float32)
    corr_phi = np.array(events.MET.phi, dtype=np.float32)

    # select only events where MET pt is below the expected beam energy
    mask = events.MET.pt < (0.5 * self.config_inst.campaign.ecm)

    # arguments for evaluation
    args = (
        events.MET.pt[mask],
        events.MET.phi[mask],
        ak.values_astype(events.PV.npvs[mask], np.float32),
        ak.values_astype(events.run[mask], np.float32),
    )

    # evaluate and insert
    corr_pt[mask] = self.met_pt_corrector.evaluate(*args)
    corr_phi[mask] = self.met_phi_corrector.evaluate(*args)

    # save the corrected values
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
def met_phi_setup(self: Calibrator, reqs: dict, inputs: dict, reader_targets: dict) -> None:
    """
    Load the correct met files using the :py:func:`from_string` method of the
    :external+correctionlib:py:class:`correctionlib.highlevel.CorrectionSet`
    function and apply the corrections as needed.

    :param reqs: Requirement dictionary for this :py:class:`~columnflow.calibration.Calibrator`
        instance
    :param inputs: Additional inputs, currently not used.
    :param reader_targets: Additional targets, currently not used.
    """
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

    # check versions
    if self.met_pt_corrector.version not in (1,):
        raise Exception(f"unsuppprted met pt corrector version {self.met_pt_corrector.version}")
    if self.met_phi_corrector.version not in (1,):
        raise Exception(f"unsuppprted met phi corrector version {self.met_phi_corrector.version}")
