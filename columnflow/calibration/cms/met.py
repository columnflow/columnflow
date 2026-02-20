# coding: utf-8

"""
MET corrections.
"""

from __future__ import annotations

import functools
from dataclasses import dataclass, field

import law

from columnflow.calibration import Calibrator
from columnflow.util import maybe_import, load_correction_set, DotDict
from columnflow.columnar_util import set_ak_column
from columnflow.types import Any

np = maybe_import("numpy")
ak = maybe_import("awkward")


# helpers
set_ak_column_f32 = functools.partial(set_ak_column, value_type=np.float32)


class _met_phi_base(Calibrator):
    """"
    Common base class for MET phi calibrators.
    """

    exposed = False

    # function to determine the correction file
    get_met_file = lambda self, external_files: external_files.met_phi_corr

    # function to determine met correction config
    get_met_config = lambda self: self.config_inst.x.met_phi_correction

    def requires_func(self, task: law.Task, reqs: dict[str, DotDict[str, Any]], **kwargs) -> None:
        if "external_files" in reqs:
            return

        from columnflow.tasks.external import BundleExternalFiles
        reqs["external_files"] = BundleExternalFiles.req(task)


#
# Run 2 implementation
#

@dataclass
class METPhiConfigRun2:
    correction_set_template: str = r"{variable}_metphicorr_pfmet_{data_source}"
    met_name: str = "MET"
    keep_uncorrected: bool = False


@_met_phi_base.calibrator(exposed=True)
def met_phi_run2(self: Calibrator, events: ak.Array, **kwargs) -> ak.Array:
    """
    Performs the MET phi (type II) correction using :external+correctionlib:doc:`index`. Events whose uncorrected MET pt
    is below the beam energy (extracted from ``config_inst.campaign.ecm * 0.5``) are skipped. Requires an external file
    in the config under ``met_phi_corr``:

    .. code-block:: python

        cfg.x.external_files = DotDict.wrap({
            "met_phi_corr": "/afs/cern.ch/work/m/mrieger/public/mirrors/jsonpog-integration-406118ec/POG/JME/2022_Summer22EE/met_xyCorrections_2022_2022EE.json.gz",  # noqa
        })

    *get_met_file* can be adapted in a subclass in case it is stored differently in the external files.

    The calibrator should be configured with an :py:class:`METPhiConfigRun2` as an auxiliary entry in the config named
    ``met_phi_correction``. *get_met_config* can be adapted in a subclass in case it is stored differently in the
    config. Exemplary config entry:

    .. code-block:: python

        from columnflow.calibration.cms.met import METPhiConfigRun2
        cfg.x.met_phi_correction = METPhiConfigRun2(
            met_name="MET",
            correction_set_template="{variable}_metphicorr_pfmet_{data_source}",
            keep_uncorrected=False,
        )

    "variable" and "data_source" are placeholders that will be replace with "pt" or "phi", and the data source of the
    current dataset, respectively.

    Resources:
        - https://twiki.cern.ch/twiki/bin/view/CMS/MissingETRun2Corrections?rev=79#xy_Shift_Correction_MET_phi_modu
    """
    # get met columns
    met_name = self.met_config.met_name
    met = events[met_name]

    # store uncorrected values if requested
    if self.met_config.keep_uncorrected:
        events = set_ak_column_f32(events, f"{met_name}.pt_metphi_uncorrected", met.pt)
        events = set_ak_column_f32(events, f"{met_name}.phi_metphi_uncorrected", met.phi)

    # copy the intial pt and phi values
    corr_pt = np.array(met.pt, dtype=np.float32)
    corr_phi = np.array(met.phi, dtype=np.float32)

    # select only events where MET pt is below the expected beam energy
    mask = met.pt < (0.5 * self.config_inst.campaign.ecm)

    # arguments for evaluation
    args = (
        met.pt[mask],
        met.phi[mask],
        ak.values_astype(events.PV.npvs[mask], np.float32),
        ak.values_astype(events.run[mask], np.float32),
    )

    # evaluate and insert
    corr_pt[mask] = self.met_pt_corrector.evaluate(*args)
    corr_phi[mask] = self.met_phi_corrector.evaluate(*args)

    # save the corrected values
    events = set_ak_column_f32(events, f"{met_name}.pt", corr_pt)
    events = set_ak_column_f32(events, f"{met_name}.phi", corr_phi)

    return events


@met_phi_run2.init
def met_phi_run2_init(self: Calibrator, **kwargs) -> None:
    self.met_config = self.get_met_config()

    # set used columns
    self.uses.update({"run", "PV.npvs", f"{self.met_config.met_name}.{{pt,phi}}"})

    # set produced columns
    self.produces.add(f"{self.met_config.met_name}.{{pt,phi}}")
    if self.met_config.keep_uncorrected:
        self.produces.add(f"{self.met_config.met_name}.{{pt,phi}}_metphi_uncorrected")


@met_phi_run2.setup
def met_phi_run2_setup(
    self: Calibrator,
    task: law.Task,
    reqs: dict[str, DotDict[str, Any]],
    inputs: dict[str, Any],
    reader_targets: law.util.InsertableDict,
    **kwargs,
) -> None:
    # create the pt and phi correctors
    met_file = self.get_met_file(reqs["external_files"].files)
    correction_set = load_correction_set(met_file)

    name_tmpl = self.met_config.correction_set_template
    self.met_pt_corrector = correction_set[name_tmpl.format(
        variable="pt",
        data_source=self.dataset_inst.data_source,
    )]
    self.met_phi_corrector = correction_set[name_tmpl.format(
        variable="phi",
        data_source=self.dataset_inst.data_source,
    )]


#
# Run 3 implementation
#

@dataclass
class METPhiConfig:
    correction_set: str = "met_xy_corrections"
    met_name: str = "PuppiMET"
    met_type: str = "PuppiMET"
    keep_uncorrected: bool = False
    # variations (intrinsic method uncertainties) for pt and phi
    pt_phi_variations: dict[str, str] | None = field(default_factory=lambda: {
        "stat_xdn": "metphi_statx_down",
        "stat_xup": "metphi_statx_up",
        "stat_ydn": "metphi_staty_down",
        "stat_yup": "metphi_staty_up",
    })
    # other variations (external uncertainties)
    variations: dict[str, str] | None = field(default_factory=lambda: {
        "pu_dn": "minbias_xs_down",
        "pu_up": "minbias_xs_up",
    })


@_met_phi_base.calibrator(exposed=True)
def met_phi(self: Calibrator, events: ak.Array, **kwargs) -> ak.Array:
    """
    Performs the MET phi (type II) correction using :external+correctionlib:doc:`index`. Events whose uncorrected MET pt
    is below the beam energy (extracted from ``config_inst.campaign.ecm * 0.5``) are skipped. Requires an external file
    in the config under ``met_phi_corr``:

    .. code-block:: python

        cfg.x.external_files = DotDict.wrap({
            "met_phi_corr": "/afs/cern.ch/work/m/mrieger/public/mirrors/jsonpog-integration-406118ec/POG/JME/2022_Summer22EE/met_xyCorrections_2022_2022EE.json.gz",  # noqa
        })

    *get_met_file* can be adapted in a subclass in case it is stored differently in the external files.

    The calibrator should be configured with an :py:class:`METPhiConfig` as an auxiliary entry in the config named
    ``met_phi_correction``. *get_met_config* can be adapted in a subclass in case it is stored differently in the
    config. Exemplary config entry:

    .. code-block:: python

        from columnflow.calibration.cms.met import METPhiConfig
        cfg.x.met_phi_correction = METPhiConfig(
            correction_set="met_xy_corrections",
            met_name="PuppiMET",
            met_type="PuppiMET",
            keep_uncorrected=False,
            # mappings of method variation to column (pt/phi) postfixes
            pt_phi_variations={
                "stat_xdn": "metphi_statx_down",
                "stat_xup": "metphi_statx_up",
                "stat_ydn": "metphi_staty_down",
                "stat_yup": "metphi_staty_up",
            },
            variations={
                "pu_dn": "minbias_xs_down",
                "pu_up": "minbias_xs_up",
            },
        )
    """
    # get met
    met_name = self.met_config.met_name
    met = events[met_name]

    # store uncorrected values if requested
    if self.met_config.keep_uncorrected:
        events = set_ak_column_f32(events, f"{met_name}.pt_metphi_uncorrected", met.pt)
        events = set_ak_column_f32(events, f"{met_name}.phi_metphi_uncorrected", met.phi)

    # correct only events where MET pt is below the expected beam energy
    mask = met.pt < (0.5 * self.config_inst.campaign.ecm * 1000)  # convert TeV to GeV

    # gather variables
    variable_map = {
        "met_type": self.met_config.met_type,
        "epoch": f"{self.config_inst.campaign.x.year}{self.config_inst.campaign.x.postfix}",
        "dtmc": "DATA" if self.dataset_inst.is_data else "MC",
        "met_pt": ak.values_astype(met.pt[mask], np.float32),
        "met_phi": ak.values_astype(met.phi[mask], np.float32),
        "npvGood": ak.values_astype(events.PV.npvsGood[mask], np.float32),
    }

    # evaluate pt and phi separately
    for var in ["pt", "phi"]:
        # remember initial values
        vals_orig = np.array(met[var], dtype=np.float32)
        # loop over general variations, then pt/phi variations
        # (needed since the JME correction file is inconsistent in how intrinsic and external variations are treated)
        general_vars = {"nom": ""}
        if self.dataset_inst.is_mc:
            general_vars.update(self.met_config.variations or {})
        for variation, postfix in general_vars.items():
            pt_phi_vars = {"": ""}
            if variation == "nom" and self.dataset_inst.is_mc:
                pt_phi_vars.update(self.met_config.pt_phi_variations or {})
            for pt_phi_variation, pt_phi_postfix in pt_phi_vars.items():
                _postfix = postfix or pt_phi_postfix
                out_var = f"{var}{_postfix and '_' + _postfix}"
                # prepare evaluator inputs
                _variable_map = {
                    **variable_map,
                    "pt_phi": f"{var}{pt_phi_variation and '_' + pt_phi_variation}",
                    "variation": variation,
                }
                inputs = [_variable_map[inp.name] for inp in self.met_corrector.inputs]
                # evaluate and create new column
                corr_vals = np.array(vals_orig)
                corr_vals[mask] = self.met_corrector(*inputs)
                events = set_ak_column_f32(events, f"{met_name}.{out_var}", corr_vals)

    return events


@met_phi.init
def met_phi_init(self: Calibrator, **kwargs) -> None:
    self.met_config = self.get_met_config()

    # set used columns
    self.uses.update({"PV.npvsGood", f"{self.met_config.met_name}.{{pt,phi}}"})

    # set produced columns
    self.produces.add(f"{self.met_config.met_name}.{{pt,phi}}")
    if self.dataset_inst.is_mc:
        for postfix in {**(self.met_config.pt_phi_variations or {}), **(self.met_config.variations or {})}.values():
            self.produces.add(f"{self.met_config.met_name}.{{pt,phi}}_{postfix}")
    if self.met_config.keep_uncorrected:
        self.produces.add(f"{self.met_config.met_name}.{{pt,phi}}_metphi_uncorrected")


@met_phi.setup
def met_phi_setup(
    self: Calibrator,
    task: law.Task,
    reqs: dict[str, DotDict[str, Any]],
    inputs: dict[str, Any],
    reader_targets: law.util.InsertableDict,
    **kwargs,
) -> None:
    # load the corrector
    met_file = self.get_met_file(reqs["external_files"].files)
    correction_set = load_correction_set(met_file)
    self.met_corrector = correction_set[self.met_config.correction_set]
