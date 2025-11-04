# coding: utf-8

"""
CMS-specific calibrators applying electron and photon energy scale and smearing.

1. Scale corrections are applied to data.
2. Resolution smearing is applied to simulation.
3. Both scale and resolution uncertainties are applied to simulation.

Resources:
  - https://twiki.cern.ch/twiki/bin/viewauth/CMS/EgammSFandSSRun3#Scale_And_Smearings_Correctionli
  - https://egammapog.docs.cern.ch/Run3/SaS
  - https://cms-analysis-corrections.docs.cern.ch/corrections_era/Run3-22CDSep23-Summer22-NanoAODv12/EGM/2025-10-22
"""

from __future__ import annotations

import functools
import dataclasses

import law

from columnflow.calibration import Calibrator, calibrator
from columnflow.calibration.util import ak_random
from columnflow.util import maybe_import, load_correction_set, DotDict
from columnflow.columnar_util import TAFConfig, set_ak_column, full_like
from columnflow.types import Any

ak = maybe_import("awkward")
np = maybe_import("numpy")


logger = law.logger.get_logger(__name__)

# helper
set_ak_column_f32 = functools.partial(set_ak_column, value_type=np.float32)


@dataclasses.dataclass
class EGammaCorrectionConfig(TAFConfig):
    """
    Container class to describe energy scaling and smearing configurations. Example:

    .. code-block:: python

        cfg.x.ess = EGammaCorrectionConfig(
            scale_correction_set="Scale",
            scale_compound=True,
            smear_syst_correction_set="SmearAndSyst",
            systs=["scale_down", "scale_up", "smear_down", "smear_up"],
        )
    """
    scale_correction_set: str
    smear_syst_correction_set: str
    scale_compound: bool = False
    smear_syst_compound: bool = False
    systs: list[str] = dataclasses.field(default_factory=list)
    corrector_kwargs: dict[str, Any] = dataclasses.field(default_factory=dict)


@calibrator(
    exposed=False,
    # used and produced columns are defined dynamically in init function
    with_uncertainties=True,
    collection_name=None,  # to be set in derived classes to "Electron" or "Photon"
    get_scale_smear_config=None,  # to be set in derived classes
    get_correction_file=None,  # to be set in derived classes
    deterministic_seed_index=-1,  # use deterministic seeds for random smearing when >=0
    store_original=False,  # if original columns (pt, energyErr) should be stored as "*_uncorrected"
)
def _egamma_scale_smear(self: Calibrator, events: ak.Array, **kwargs) -> ak.Array:
    # gather inputs
    coll = events[self.collection_name]

    if ak.sum(ak.num(coll.pt, axis=-1)) == 0:
        # skip chunk if no objects are present
        return events

    variable_map = {
        "run": events.run,
        "pt": coll.pt,
        "ScEta": coll.superclusterEta,
        "r9": coll.r9,
        "seedGain": coll.seedGain,
        **self.cfg.corrector_kwargs,
    }
    def get_inputs(corrector, **additional_variables):
        _variable_map = variable_map | additional_variables
        return (_variable_map[inp.name] for inp in corrector.inputs if inp.name in _variable_map)

    # apply scale correction to data
    if self.dataset_inst.is_data:
        # store uncorrected values before correcting
        if self.store_original:
            events = set_ak_column(events, f"{self.collection_name}.pt_scale_uncorrected", coll.pt)
            events = set_ak_column(events, f"{self.collection_name}.energyErr_scale_uncorrected", coll.energyErr)

        # get scaled pt
        scale = self.scale_corrector.evaluate("scale", *get_inputs(self.scale_corrector))
        pt_scaled = coll.pt * scale

        # get scaled energy error
        smear = self.smear_syst_corrector.evaluate("smear", *get_inputs(self.smear_syst_corrector, pt=pt_scaled))
        energy_err_scaled = (((coll.energyErr)**2 + (coll.energy * smear)**2) * scale)**0.5

        # store columns
        events = set_ak_column_f32(events, f"{self.collection_name}.pt", pt_scaled)
        events = set_ak_column_f32(events, f"{self.collection_name}.energyErr", energy_err_scaled)

    # apply smearing to MC
    if self.dataset_inst.is_mc:
        # store uncorrected values before correcting
        if self.store_original:
            events = set_ak_column(events, f"{self.collection_name}.pt_smear_uncorrected", coll.pt)
            events = set_ak_column(events, f"{self.collection_name}.energyErr_smear_uncorrected", coll.energyErr)

        # compute random variables in the shape of the collection once
        rnd_args = (full_like(coll.pt, 0.0), full_like(coll.pt, 1.0))
        if self.use_deterministic_seeds:
            rnd_args += (coll.deterministic_seed,)
            rand_func = self.deterministic_normal
        else:
            # TODO: bit generator could be configurable
            rand_func = np.random.Generator(np.random.SFC64((events.event).to_list())).normal
        rnd = ak_random(*rnd_args, rand_func=rand_func)

        # helper to compute smeared pt and energy error values given a syst
        def apply_smearing(syst):
            # get smeared pt
            smear = self.smear_syst_corrector.evaluate(syst, *get_inputs(self.smear_syst_corrector))
            smear_factor = 1.0 + smear * rnd
            pt_smeared = coll.pt * smear_factor
            # get smeared energy error
            energy_err_smeared = (((coll.energyErr)**2 + (coll.energy * smear)**2) * smear_factor)**0.5
            # return both
            return pt_smeared, energy_err_smeared

        # compute and store columns
        pt_smeared, energy_err_smeared = apply_smearing("smear")
        events = set_ak_column_f32(events, f"{self.collection_name}.pt", pt_smeared)
        events = set_ak_column_f32(events, f"{self.collection_name}.energyErr", energy_err_smeared)

        # apply scale and smearing uncertainties to MC
        if self.with_uncertainties and self.cfg.systs:
            for syst in self.cfg.systs:
                # exact behavior depends on syst itself
                if syst in {"scale_up", "scale_down"}:
                    # compute scale with smeared pt and apply muliplicatively to smeared values
                    scale = self.smear_syst_corrector.evaluate(syst, *get_inputs(self.smear_syst_corrector, pt=pt_smeared))  # noqa: E501
                    events = set_ak_column_f32(events, f"{self.collection_name}.pt_{syst}", pt_smeared * scale)
                    events = set_ak_column_f32(events, f"{self.collection_name}.energyErr_{syst}", energy_err_smeared * scale)  # noqa: E501

                elif syst in {"smear_up", "smear_down"}:
                    # compute smearing variations on original variables with same method as above
                    pt_smeared_syst, energy_err_smeared_syst = apply_smearing(syst)
                    events = set_ak_column_f32(events, f"{self.collection_name}.pt_{syst}", pt_smeared_syst)
                    events = set_ak_column_f32(events, f"{self.collection_name}.energyErr_{syst}", energy_err_smeared_syst)  # noqa: E501

                else:
                    logger.error(f"{self.cls_name} calibrator received unknown systematic '{syst}', skipping")

    return events


@_egamma_scale_smear.init
def _egamma_scale_smear_init(self: Calibrator, **kwargs) -> None:
    # store the config
    self.cfg = self.get_scale_smear_config()

    # update used columns
    self.uses |= {"run", f"{self.collection_name}.{{pt,eta,phi,mass,energyErr,superclusterEta,r9,seedGain}}"}

    # update produced columns
    if self.dataset_inst.is_data:
        self.produces |= {f"{self.collection_name}.{{pt,energyErr}}"}
        if self.store_original:
            self.produces |= {f"{self.collection_name}.{{pt,energyErr}}_scale_uncorrected"}
    else:
        self.produces |= {f"{self.collection_name}.{{pt,energyErr}}"}
        if self.store_original:
            self.produces |= {f"{self.collection_name}.{{pt,energyErr}}_smear_uncorrected"}
        if self.with_uncertainties:
            for syst in self.cfg.systs:
                self.produces |= {f"{self.collection_name}.{{pt,energyErr}}_{syst}"}


@_egamma_scale_smear.requires
def _egamma_scale_smear_requires(self, task: law.Task, reqs: dict[str, DotDict[str, Any]], **kwargs) -> None:
    if "external_files" in reqs:
        return

    from columnflow.tasks.external import BundleExternalFiles
    reqs["external_files"] = BundleExternalFiles.req(task)


@_egamma_scale_smear.setup
def _egamma_scale_smear_setup(
    self,
    task: law.Task,
    reqs: dict[str, DotDict[str, Any]],
    inputs: dict[str, Any],
    reader_targets: law.util.InsertableDict,
    **kwargs,
) -> None:
    # get and load the correction file
    corr_file = self.get_correction_file(reqs["external_files"].files)
    corr_set = load_correction_set(corr_file)

    # setup the correctors
    get_set = lambda set_name, compound: (corr_set.compound if compound else corr_set)[set_name]
    self.scale_corrector = get_set(self.cfg.scale_correction_set, self.cfg.scale_compound)
    self.smear_syst_corrector = get_set(self.cfg.smear_syst_correction_set, self.cfg.smear_syst_compound)

    # use deterministic seeds for random smearing if requested
    self.use_deterministic_seeds = self.deterministic_seed_index >= 0
    if self.use_deterministic_seeds:
        idx = self.deterministic_seed_index
        bit_generator = np.random.SFC64

        def _deterministic_normal(loc, scale, seed, idx_offset=0):
            return np.asarray([
                np.random.Generator(bit_generator(_seed)).normal(_loc, _scale, size=idx + 1 + idx_offset)[-1]
                for _loc, _scale, _seed in zip(loc, scale, seed)
            ])

        # each systematic is to be evaluated with the same random number so use a fixed offset
        self.deterministic_normal = functools.partial(_deterministic_normal, idx_offset=0)


electron_scale_smear = _egamma_scale_smear.derive(
    "electron_scale_smear",
    cls_dict={
        "collection_name": "Electron",
        "get_scale_smear_config": lambda self: self.config_inst.x.ess,
        "get_correction_file": lambda self, external_files: external_files.electron_ss,
    },
)

photon_scale_smear = _egamma_scale_smear.derive(
    "photon_scale_smear",
    cls_dict={
        "collection_name": "Photon",
        "get_scale_smear_config": lambda self: self.config_inst.x.gss,
        "get_correction_file": lambda self, external_files: external_files.photon_ss,
    },
)
