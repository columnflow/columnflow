# coding: utf-8

"""
Tau energy correction methods.
"""

from __future__ import annotations

import functools
import itertools
import dataclasses

import law

from columnflow.calibration import Calibrator, calibrator
from columnflow.calibration.util import propagate_met
from columnflow.util import maybe_import, load_correction_set, DotDict
from columnflow.columnar_util import TAFConfig, set_ak_column, flat_np_view, layout_ak_array, full_like
from columnflow.types import Any

ak = maybe_import("awkward")
np = maybe_import("numpy")


# helper
set_ak_column_f32 = functools.partial(set_ak_column, value_type=np.float32)


@dataclasses.dataclass
class TECConfig(TAFConfig):
    tagger: str
    correction_set: str = "tau_energy_scale"
    corrector_kwargs: dict[str, Any] = dataclasses.field(default_factory=dict)

    @classmethod
    def new(cls, obj: TECConfig | tuple[str] | dict[str, str]) -> TECConfig:
        # purely for backwards compatibility with the old tuple format that accepted the two
        # working point values
        if isinstance(obj, tuple) and len(obj) == 2:
            obj = dict(zip(["wp", "wp_VSe"], obj))
        if isinstance(obj, dict):
            return cls(corrector_kwargs=obj)
        return obj


@calibrator(
    uses={"Tau.{pt,eta,phi,mass,charge,genPartFlav,decayMode}"},
    produces={"Tau.{pt,mass}"},
    # whether to produce also uncertainties
    with_uncertainties=True,
    # toggle for propagation to MET
    propagate_met=True,
    # name of the met collection to use
    met_name="MET",
    # only run on mc
    mc_only=True,
    # function to determine the correction file
    get_tau_file=(lambda self, external_files: external_files.tau_sf),
    # function to determine the tec config
    get_tec_config=(lambda self: TECConfig.new(self.config_inst.x.tec)),
)
def tec(
    self: Calibrator,
    events: ak.Array,
    **kwargs,
) -> ak.Array:
    """
    Calibrator for tau energy. Requires an external file in the config under ``tau_sf``, e.g.

    .. code-block:: python

        cfg.x.external_files = DotDict.wrap({
            "tau_sf": "/afs/cern.ch/work/m/mrieger/public/mirrors/jsonpog-integration-6ce37404/POG/TAU/2017_UL/tau.json.gz",  # noqa
        })

    and a :py:class:`TECConfig` configuration object named ``tec``,

    .. code-block:: python

        # run 3 example
        from columnflow.calibration.cms.tau import TECConfig

        cfg.x.tec = TECConfig(
            tagger="DeepTau2018v2p5",
            corrector_kwargs={"wp": "Tight", "wp_VSe": "Tight"},
        )

    *get_tau_file* and *get_tec_config* can be adapted in a subclass in case they are stored
    differently in the config.

    .. note::

        In case you also perform the propagation from jet energy calibrations to MET, please check if the propagation of
        tau energy calibrations to MET is required in your analysis!

    Resources:
    https://twiki.cern.ch/twiki/bin/view/CMS/TauIDRecommendationForRun2?rev=113
    https://gitlab.cern.ch/cms-nanoAOD/jsonpog-integration/-/blob/849c6a6efef907f4033715d52290d1a661b7e8f9/POG/TAU
    """
    # fail when running on data
    if self.dataset_inst.is_data:
        raise ValueError("attempt to apply tau energy corrections in data")

    # create mask to select taus with supported decay modes
    match = events.Tau.genPartFlav
    dm = events.Tau.decayMode
    dm_mask = (dm == 0) | (dm == 1) | (dm == 10) | (dm == 11)

    # prepare inputs
    variable_map = {
        "pt": events.Tau.pt[dm_mask],
        "eta": events.Tau.eta[dm_mask],
        "dm": dm[dm_mask],
        "genmatch": match[dm_mask],
        "id": self.tec_cfg.tagger,
        **self.tec_cfg.corrector_kwargs,
    }
    inputs = [
        variable_map[inp.name] for inp in self.tec_corrector.inputs
        if inp.name in variable_map
    ]

    # helper to get scales
    def get_scales(syst):
        scales = flat_np_view(full_like(dm_mask, 1.0, dtype=np.float32))
        scales[flat_np_view(dm_mask)] = flat_np_view(self.tec_corrector(*inputs, syst))
        return layout_ak_array(scales, events.Tau)

    # nominal correction
    scales_nom = get_scales("nom")

    # varied corrections
    if self.with_uncertainties:
        scales_up = get_scales("up")
        scales_down = get_scales("down")

    # custom adjustment: reset where the matching value is unhandled
    reset_mask = (match < 1) | (match > 5)
    if ak.any(reset_mask):
        scales_nom = ak.where(reset_mask, 1.0, scales_nom)
        if self.with_uncertainties:
            scales_up = ak.where(reset_mask, 1.0, scales_up)
            scales_down = ak.where(reset_mask, 1.0, scales_down)

    # create varied collections per decay mode
    if self.with_uncertainties:
        for (match_mask, match_name), _dm, (direction, scales) in itertools.product(
            [(match == 5, "jet"), ((match == 1) | (match == 3), "e")],
            [0, 1, 10, 11],
            [("up", scales_up), ("down", scales_down)],
        ):
            # copy pt and mass
            pt_flat = flat_np_view(events.Tau.pt, copy=True)
            mass_flat = flat_np_view(events.Tau.mass, copy=True)

            # correct pt and mass for taus with that gen match and decay mode
            mask = match_mask & (dm == _dm)
            mask_flat = flat_np_view(mask)
            pt_flat[mask_flat] *= flat_np_view(scales[mask])
            mass_flat[mask_flat] *= flat_np_view(scales[mask])

            # save columns
            postfix = f"tec_{match_name}_dm{_dm}_{direction}"
            events = set_ak_column_f32(events, f"Tau.pt_{postfix}", layout_ak_array(pt_flat, events.Tau))
            events = set_ak_column_f32(events, f"Tau.mass_{postfix}", layout_ak_array(mass_flat, events.Tau))

            # propagate changes to MET
            if self.propagate_met:
                met_pt_varied, met_phi_varied = propagate_met(
                    events.Tau.pt,
                    events.Tau.phi,
                    events.Tau[f"pt_{postfix}"],
                    events.Tau.phi,
                    events[self.met_name].pt,
                    events[self.met_name].phi,
                )
                events = set_ak_column_f32(events, f"{self.met_name}.pt_{postfix}", met_pt_varied)
                events = set_ak_column_f32(events, f"{self.met_name}.phi_{postfix}", met_phi_varied)

    # apply the nominal correction
    tau_sum_before = events.Tau.sum(axis=1)
    events = set_ak_column_f32(events, "Tau.pt", events.Tau.pt * scales_nom)
    events = set_ak_column_f32(events, "Tau.mass", events.Tau.mass * scales_nom)

    # propagate changes to MET
    if self.propagate_met:
        met_pt, met_phi = propagate_met(
            tau_sum_before.pt,
            tau_sum_before.phi,
            events.Tau.pt,
            events.Tau.phi,
            events[self.met_name].pt,
            events[self.met_name].phi,
        )
        events = set_ak_column_f32(events, f"{self.met_name}.pt", met_pt)
        events = set_ak_column_f32(events, f"{self.met_name}.phi", met_phi)

    return events


@tec.init
def tec_init(self: Calibrator, **kwargs) -> None:
    self.tec_cfg = self.get_tec_config()

    # add nominal met columns of propagating nominal tec
    if self.propagate_met:
        self.uses.add(f"{self.met_name}.{{pt,phi}}")
        self.produces.add(f"{self.met_name}.{{pt,phi}}")

    # add columns with unceratinties if requested
    if self.with_uncertainties:
        # also check if met propagation is enabled
        src_fields = ["Tau.pt", "Tau.mass"]
        if self.propagate_met:
            src_fields += [f"{self.met_name}.{var}" for var in ["pt", "phi"]]

        self.produces |= {
            f"{field}_tec_{{jet,e}}_dm{{0,1,10,11}}_{{up,down}}"
            for field in src_fields
        }


@tec.requires
def tec_requires(
    self: Calibrator,
    task: law.Task,
    reqs: dict[str, DotDict[str, Any]],
    **kwargs,
) -> None:
    if "external_files" in reqs:
        return

    from columnflow.tasks.external import BundleExternalFiles
    reqs["external_files"] = BundleExternalFiles.req(task)


@tec.setup
def tec_setup(
    self: Calibrator,
    task: law.Task,
    reqs: dict[str, DotDict[str, Any]],
    inputs: dict[str, Any],
    reader_targets: law.util.InsertableDict,
    **kwargs,
) -> None:
    # create the tec corrector
    tau_file = self.get_tau_file(reqs["external_files"].files)
    self.tec_corrector = load_correction_set(tau_file)[self.tec_cfg.correction_set]

    # check versions
    assert self.tec_corrector.version in {0, 1, 2}


tec_nominal = tec.derive("tec_nominal", cls_dict={"with_uncertainties": False})
