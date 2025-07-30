# coding: utf-8

"""
MET corrections.
"""

from __future__ import annotations

import law

from dataclasses import dataclass

from columnflow.calibration import Calibrator, calibrator
from columnflow.util import maybe_import, load_correction_set, DotDict
from columnflow.columnar_util import set_ak_column
from columnflow.types import Any

np = maybe_import("numpy")
ak = maybe_import("awkward")


@dataclass
class MetPhiConfig:
    variable_config: dict[str, tuple[str]]
    correction_set: str = "met_xy_corrections"
    met_name: str = "PuppiMET"
    met_type: str = "MET"
    keep_uncorrected: bool = False

    @classmethod
    def new(
        cls,
        obj: MetPhiConfig | tuple[str, list[str]] | tuple[str, list[str], str],
    ) -> MetPhiConfig:
        # purely for backwards compatibility with the old string format
        if isinstance(obj, cls):
            return obj
        if isinstance(obj, str):
            return cls(correction_set=obj, variable_config={"pt": ("pt",), "phi": ("phi",)})
        if isinstance(obj, dict):
            return cls(**obj)
        raise ValueError(f"cannot convert {obj} to MetPhiConfig")


@calibrator(
    uses={"run", "PV.npvs"},
    # function to determine the correction file
    get_met_file=(lambda self, external_files: external_files.met_phi_corr),
    # function to determine met correction config
    get_met_config=(lambda self: MetPhiConfig.new(self.config_inst.x.met_phi_correction)),
)
def met_phi(self: Calibrator, events: ak.Array, **kwargs) -> ak.Array:
    """
    Performs the MET phi (type II) correction using the
    :external+correctionlib:doc:`index` for events there the
    uncorrected MET pt is below the beam energy (extracted from ``config_inst.campaign.ecm * 0.5``).
    Requires an external file in the config under ``met_phi_corr``:

    .. code-block:: python

        cfg.x.external_files = DotDict.wrap({
            "met_phi_corr": "/afs/cern.ch/user/m/mfrahm/public/mirrors/jsonpog-integration-406118ec/POG/JME/2022_Summer22EE/met_xyCorrections_2022_2022EE.json.gz",  # noqa
        })

    *get_met_file* can be adapted in a subclass in case it is stored differently in the external
    files.

    The met_phi Calibrator should be configured with an auxiliary entry in the config that can contain:
    - the name of the correction set
    - the name of the MET column
    - the MET type that is passed as an input to the correction set
    - a boolean flag to keep the uncorrected MET pt and phi values as additional output columns
    - a dictionary that maps the input variable names ("pt", "phi") to a list
    of output variable names that should be produced.

    Exemplary config entry:

    .. code-block:: python
        cfg.x.met_phi_correction = {
            "met_name": "PuppiMET",
            "met_type": "MET",
            "correction_set": "met_xy_corrections",
            "keep_uncorrected": False,
            "variable_config": {
                "pt": (
                    "pt",
                    "pt_stat_yup",
                    "pt_stat_ydn",
                    "pt_stat_xup",
                    "pt_stat_xdn",
                ),
                "phi": (
                    "phi",
                    "phi_stat_yup",
                    "phi_stat_ydn",
                    "phi_stat_xup",
                    "phi_stat_xdn",
                ),
            },
        }

    The `correction_set` value can also contain the placeholders "variable" and "data_source"
    that are replaced in the calibrator setup :py:meth:`~.met_phi.setup_func`.

    *get_met_config* can be adapted in a subclass in case it is stored differently in the config.

    Resources:
    - https://twiki.cern.ch/twiki/bin/viewauth/CMS/MissingETRun2Corrections#xy_Shift_Correction_MET_phi_modu (r79)

    :param events: awkward array containing events to process
    """
    # get Met columns
    met = events[self.met_config.met_name]

    # correct only events where MET pt is below the expected beam energy
    mask = met.pt < (0.5 * self.config_inst.campaign.ecm * 1000)  # convert TeV to GeV

    variable_map = {
        "met_type": self.met_config.met_type,
        "epoch": f"{self.config_inst.campaign.x.year}{self.config_inst.campaign.x.postfix}",
        "dtmc": "DATA" if self.dataset_inst.is_data else "MC",
        "variation": "nom",
        "met_pt": ak.values_astype(met.pt[mask], np.float32),
        "met_phi": ak.values_astype(met.phi[mask], np.float32),
        "npvGood": ak.values_astype(events.PV.npvsGood, np.float32),
        "npvs": ak.values_astype(events.PV.npvs, np.float32),  # needed for old-style corrections
        "run": ak.values_astype(events.run, np.float32),
    }

    for variable, outp_variables in self.met_config.variable_config.items():
        met_corrector = self.met_correctors[variable]
        if self.met_config.keep_uncorrected:
            events = set_ak_column(
                events,
                f"{self.met_config.met_name}.{variable}_uncorrected",
                met[variable],
                value_type=np.float32,
            )
        for out_var in outp_variables:
            # copy initial value every time
            # NOTE: this needs to be within the loop to ensure that the output values are not
            # overwritten by the next iteration
            corr_var = np.array(met[variable], dtype=np.float32)

            # get the input variables for the correction
            variable_map_syst = {
                **variable_map,
                "pt_phi": out_var,
            }
            inputs = [variable_map_syst[inp.name] for inp in met_corrector.inputs]

            # insert the corrected values
            corr_var[mask] = met_corrector(*inputs)

            # save the corrected values
            events = set_ak_column(events, f"{self.met_config.met_name}.{out_var}", corr_var, value_type=np.float32)

    return events


@met_phi.init
def met_phi_init(self: Calibrator, **kwargs) -> None:
    """
    Initialize the :py:attr:`met_pt_corrector` and :py:attr:`met_phi_corrector` attributes.
    """
    self.met_config = self.get_met_config()
    self.uses.add(f"{self.met_config.met_name}.{{pt,phi}}")
    for variable in self.met_config.variable_config.keys():
        if self.met_config.keep_uncorrected:
            self.produces.add(f"{self.met_config.met_name}.{variable}_uncorrected")
        for out_var in self.met_config.variable_config[variable]:
            # add the produced columns to the uses set
            self.produces.add(f"{self.met_config.met_name}.{out_var}")


@met_phi.requires
def met_phi_requires(
    self: Calibrator,
    task: law.Task,
    reqs: dict[str, DotDict[str, Any]],
    **kwargs,
) -> None:
    if "external_files" in reqs:
        return

    from columnflow.tasks.external import BundleExternalFiles
    reqs["external_files"] = BundleExternalFiles.req(task)


@met_phi.setup
def met_phi_setup(
    self: Calibrator,
    task: law.Task,
    reqs: dict[str, DotDict[str, Any]],
    inputs: dict[str, Any],
    reader_targets: law.util.InsertableDict,
    **kwargs,
) -> None:
    """
    Load the correct met files using the :py:func:`from_string` method of the
    :external+correctionlib:py:class:`correctionlib.highlevel.CorrectionSet`
    function and apply the corrections as needed.

    :param reqs: Requirement dictionary for this :py:class:`~columnflow.calibration.Calibrator`
        instance
    :param inputs: Additional inputs, currently not used.
    :param reader_targets: Additional targets, currently not used.
    """
    # create the pt and phi correctors
    met_file = self.get_met_file(reqs["external_files"].files)
    correction_set = load_correction_set(met_file)

    # self.met_config = self.get_met_config()
    name_tmpl = self.met_config.correction_set
    self.met_correctors = {
        variable: correction_set[name_tmpl.format(
            variable=variable,
            data_source=self.dataset_inst.data_source,
        )] for variable in self.met_config.variable_config.keys()
    }

    # # check versions
    # if self.met_pt_corrector.version not in (1,):
    #     raise Exception(f"unsuppprted met pt corrector version {self.met_pt_corrector.version}")
    # if self.met_phi_corrector.version not in (1,):
    #     raise Exception(f"unsuppprted met phi corrector version {self.met_phi_corrector.version}")
