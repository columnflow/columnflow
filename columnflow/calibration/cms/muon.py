# coding: utf-8

"""
Muon calibration methods.
"""

from __future__ import annotations

import functools
import dataclasses
import inspect

import law

from columnflow.calibration import Calibrator, calibrator
from columnflow.columnar_util import TAFConfig, set_ak_column, IF_MC
from columnflow.util import maybe_import, load_correction_set, import_file, DotDict
from columnflow.types import Any

ak = maybe_import("awkward")
np = maybe_import("numpy")


logger = law.logger.get_logger(__name__)

# helper
set_ak_column_f32 = functools.partial(set_ak_column, value_type=np.float32)


@dataclasses.dataclass
class MuonSRConfig(TAFConfig):
    """
    Container class to configure muon momentum scale and resolution corrections. Example:

    .. code-block:: python

        cfg.x.muon_sr = MuonSRConfig(
            systs=["scale_up", "scale_down", "res_up", "res_down"],
        )
    """
    systs: list[str] = dataclasses.field(default_factory=lambda: ["scale_up", "scale_down", "res_up", "res_down"])


@calibrator(
    uses={
        "Muon.{pt,eta,phi,mass,charge}",
        IF_MC("event", "luminosityBlock", "Muon.nTrackerLayers"),
    },
    # uncertainty variations added in init
    produces={"Muon.pt"},
    # whether to produce also uncertainties
    with_uncertainties=True,
    # functions to determine the correction and tool files
    get_muon_sr_file=(lambda self, external_files: external_files.muon_sr),
    get_muon_sr_tool_file=(lambda self, external_files: external_files.muon_sr_tools),
    # function to determine the muon config
    get_muon_sr_config=(lambda self: self.config_inst.x.muon_sr),
    # if the original pt columns should be stored as "pt_sr_uncorrected"
    store_original=False,
)
def muon_sr(
    self: Calibrator,
    events: ak.Array,
    **kwargs,
) -> ak.Array:
    """
    Calibrator for muon scale and resolution smearing. Requires two external file in the config under the ``muon_sr``
    and ``muon_sr_tools`` keys, pointing to the json correction file and the "MuonScaRe" tools script, respectively,

    .. code-block:: python

        cfg.x.external_files = DotDict.wrap({
            "muon_sr": "/cvmfs/cms-griddata.cern.ch/cat/metadata/MUO/Run3-22CDSep23-Summer22-NanoAODv12/2025-08-14/muon_scalesmearing.json.gz",  # noqa
            "muon_sr_tools": "/path/to/MuonScaRe.py",
        })

    and a :py:class:`MuonSRConfig` configuration object in the auxiliary field ``muon_sr``,

    .. code-block:: python

        from columnflow.calibration.cms.muon import MuonSRConfig
        cfg.x.muon_sr = MuonSRConfig(
            systs=["scale_up", "scale_down", "res_up", "res_down"],
        )

    *get_muon_sr_file*, *get_muon_sr_tool_file* and *get_muon_sr_config* can be adapted in a subclass in case they are
    stored differently in the config.

    Resources:

        - https://gitlab.cern.ch/cms-muonPOG/muonscarekit
        - https://cms-analysis-corrections.docs.cern.ch/corrections_era/Run3-22CDSep23-Summer22-NanoAODv12/MUO/latest/#muon_scalesmearingjsongz # noqa
    """
    # store the original pt column if requested
    if self.store_original:
        events = set_ak_column(events, "Muon.pt_sr_uncorrected", events.Muon.pt)

    # apply scale correction to data
    if self.dataset_inst.is_data:
        pt_scale_corr = self.muon_sr_tools.pt_scale(
            1,
            events.Muon.pt,
            events.Muon.eta,
            events.Muon.phi,
            events.Muon.charge,
            self.muon_correction_set,
            nested=True,
        )
        events = set_ak_column_f32(events, "Muon.pt", pt_scale_corr)

    # apply scale and resolution correction to mc
    if self.dataset_inst.is_mc:
        pt_scale_corr = self.muon_sr_tools.pt_scale(
            0,
            events.Muon.pt,
            events.Muon.eta,
            events.Muon.phi,
            events.Muon.charge,
            self.muon_correction_set,
            nested=True,
        )
        pt_scale_res_corr = self.muon_sr_tools.pt_resol(
            pt_scale_corr,
            events.Muon.eta,
            events.Muon.phi,
            events.Muon.nTrackerLayers,
            events.event,
            events.luminosityBlock,
            self.muon_correction_set,
            rnd_gen="np",
            nested=True,
        )
        events = set_ak_column_f32(events, "Muon.pt", pt_scale_res_corr)

        # apply scale and resolution uncertainties to mc
        if self.with_uncertainties and self.muon_cfg.systs:
            for syst in self.muon_cfg.systs:
                # the sr tools use up/dn naming
                sr_direction = {"up": "up", "down": "dn"}[syst.rsplit("_", 1)[-1]]

                # exact behavior depends on syst itself
                if syst in {"scale_up", "scale_down"}:
                    pt_syst = self.muon_sr_tools.pt_scale_var(
                        pt_scale_res_corr,
                        events.Muon.eta,
                        events.Muon.phi,
                        events.Muon.charge,
                        sr_direction,
                        self.muon_correction_set,
                        nested=True,
                    )
                    events = set_ak_column_f32(events, f"Muon.pt_{syst}", pt_syst)

                elif syst in {"res_up", "res_down"}:
                    pt_syst = self.muon_sr_tools.pt_resol_var(
                        pt_scale_corr,
                        pt_scale_res_corr,
                        events.Muon.eta,
                        sr_direction,
                        self.muon_correction_set,
                        nested=True,
                    )
                    events = set_ak_column_f32(events, f"Muon.pt_{syst}", pt_syst)

                else:
                    logger.error(f"{self.cls_name} calibrator received unknown systematic '{syst}', skipping")

    return events


@muon_sr.init
def muon_sr_init(self: Calibrator, **kwargs) -> None:
    self.muon_cfg = self.get_muon_sr_config()

    # add produced columns with unceratinties if requested
    if self.dataset_inst.is_mc and self.with_uncertainties and self.muon_cfg.systs:
        for syst in self.muon_cfg.systs:
            self.produces.add(f"Muon.pt_{syst}")

    # original column
    if self.store_original:
        self.produces.add("Muon.pt_sr_uncorrected")


@muon_sr.requires
def muon_sr_requires(
    self: Calibrator,
    task: law.Task,
    reqs: dict[str, DotDict[str, Any]],
    **kwargs,
) -> None:
    if "external_files" in reqs:
        return

    from columnflow.tasks.external import BundleExternalFiles
    reqs["external_files"] = BundleExternalFiles.req(task)


@muon_sr.setup
def muon_sr_setup(
    self: Calibrator,
    task: law.Task,
    reqs: dict[str, DotDict[str, Any]],
    inputs: dict[str, Any],
    reader_targets: law.util.InsertableDict,
    **kwargs,
) -> None:
    # load the correction set
    muon_sr_file = self.get_muon_sr_file(reqs["external_files"].files)
    self.muon_correction_set = load_correction_set(muon_sr_file)

    # also load the tools as an external package
    muon_sr_tool_file = self.get_muon_sr_tool_file(reqs["external_files"].files)
    self.muon_sr_tools = import_file(muon_sr_tool_file.abspath)

    # silence printing of the filter_boundaries function
    spec = inspect.getfullargspec(self.muon_sr_tools.filter_boundaries)
    if "silent" in spec.args or "silent" in spec.kwonlyargs:
        self.muon_sr_tools.filter_boundaries = functools.partial(self.muon_sr_tools.filter_boundaries, silent=True)


muon_sr_nominal = muon_sr.derive("muon_sr_nominal", cls_dict={"with_uncertainties": False})
