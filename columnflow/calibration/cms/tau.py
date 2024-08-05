# coding: utf-8

"""
Tau energy correction methods.
"""

from __future__ import annotations

import functools
import itertools
from dataclasses import dataclass, field

from columnflow.calibration import Calibrator, calibrator
from columnflow.calibration.util import propagate_met
from columnflow.util import maybe_import, InsertableDict
from columnflow.columnar_util import set_ak_column, flat_np_view, ak_copy
from columnflow.types import Any

ak = maybe_import("awkward")
np = maybe_import("numpy")


# helper
set_ak_column_f32 = functools.partial(set_ak_column, value_type=np.float32)


@dataclass
class TECConfig:
    tagger: str
    correction_set: str = "tau_energy_scale"
    corrector_kwargs: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def new(
        cls,
        obj: TECConfig | tuple[str] | dict[str, str],
    ) -> TECConfig:
        # purely for backwards compatibility with the old tuple format that accepted the two
        # working point values
        if isinstance(obj, tuple) and len(obj) == 2:
            obj = dict(zip(["wp", "wp_VSe"], obj))
        if isinstance(obj, dict):
            return cls(corrector_kwargs=obj)
        return obj


@calibrator(
    uses={
        # nano columns
        "nTau", "Tau.pt", "Tau.eta", "Tau.phi", "Tau.mass", "Tau.charge", "Tau.genPartFlav",
        "Tau.decayMode",
    },
    produces={
        "Tau.pt", "Tau.mass",
    },
    # whether to produce also uncertainties
    with_uncertainties=True,
    # toggle for propagation to MET
    propagate_met=True,
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

    Resources:
    https://twiki.cern.ch/twiki/bin/view/CMS/TauIDRecommendationForRun2?rev=113
    https://gitlab.cern.ch/cms-nanoAOD/jsonpog-integration/-/blob/849c6a6efef907f4033715d52290d1a661b7e8f9/POG/TAU
    """
    # fail when running on data
    if self.dataset_inst.is_data:
        raise ValueError("attempt to apply tau energy corrections in data")

    # the correction tool only supports flat arrays, so convert inputs to flat np view first
    pt = flat_np_view(events.Tau.pt, axis=1)
    mass = flat_np_view(events.Tau.mass, axis=1)
    eta = flat_np_view(events.Tau.eta, axis=1)
    dm = flat_np_view(events.Tau.decayMode, axis=1)
    match = flat_np_view(events.Tau.genPartFlav, axis=1)

    # get the scale factors for the four supported decay modes
    dm_mask = (dm == 0) | (dm == 1) | (dm == 10) | (dm == 11)

    # prepare arguments
    variable_map = {
        "pt": pt[dm_mask],
        "eta": eta[dm_mask],
        "dm": dm[dm_mask],
        "genmatch": match[dm_mask],
        "id": self.tec_config.tagger,
        **self.tec_config.corrector_kwargs,
    }
    args = tuple(
        variable_map[inp.name] for inp in self.tec_corrector.inputs
        if inp.name in variable_map
    )

    # nominal correction
    scales_nom = np.ones_like(dm_mask, dtype=np.float32)
    scales_nom[dm_mask] = self.tec_corrector(*args, "nom")

    # varied corrections
    if self.with_uncertainties:
        scales_up = np.ones_like(dm_mask, dtype=np.float32)
        scales_up[dm_mask] = self.tec_corrector(*args, "up")
        scales_down = np.ones_like(dm_mask, dtype=np.float32)
        scales_down[dm_mask] = self.tec_corrector(*args, "down")

    # custom adjustment 1: reset where the matching value is unhandled
    # custom adjustment 2: reset electrons faking taus where the pt is too small
    mask1 = (match < 1) | (match > 5)
    mask2 = ((match == 1) | (match == 3)) & (pt <= 20.0)

    # apply reset masks
    mask = mask1 | mask2
    scales_nom[mask] = 1.0
    if self.with_uncertainties:
        scales_up[mask] = 1.0
        scales_down[mask] = 1.0

    # create varied collections per decay mode
    if self.with_uncertainties:
        for (match_mask, match_name), _dm, (direction, scales) in itertools.product(
            [(match == 5, "jet"), ((match == 1) | (match == 3), "e")],
            [0, 1, 10, 11],
            [("up", scales_up), ("down", scales_down)],
        ):
            # copy pt and mass
            pt_varied = ak_copy(events.Tau.pt)
            mass_varied = ak_copy(events.Tau.mass)
            pt_view = flat_np_view(pt_varied, axis=1)
            mass_view = flat_np_view(mass_varied, axis=1)

            # correct pt and mass for taus with that gen match and decay mode
            mask = match_mask & (dm == _dm)
            pt_view[mask] *= scales[mask]
            mass_view[mask] *= scales[mask]

            # save columns
            postfix = f"tec_{match_name}_dm{_dm}_{direction}"
            events = set_ak_column_f32(events, f"Tau.pt_{postfix}", pt_varied)
            events = set_ak_column_f32(events, f"Tau.mass_{postfix}", mass_varied)

            # propagate changes to MET
            if self.propagate_met:
                met_pt_varied, met_phi_varied = propagate_met(
                    events.Tau.pt,
                    events.Tau.phi,
                    pt_varied,
                    events.Tau.phi,
                    events.MET.pt,
                    events.MET.phi,
                )
                events = set_ak_column_f32(events, f"MET.pt_{postfix}", met_pt_varied)
                events = set_ak_column_f32(events, f"MET.phi_{postfix}", met_phi_varied)

    # apply the nominal correction
    # note: changes are applied to the views and directly propagate to the original ak arrays
    # and do not need to be inserted into the events chunk again
    tau_sum_before = events.Tau.sum(axis=1)
    pt *= scales_nom
    mass *= scales_nom

    # propagate changes to MET
    if self.propagate_met:
        met_pt, met_phi = propagate_met(
            tau_sum_before.pt,
            tau_sum_before.phi,
            events.Tau.pt,
            events.Tau.phi,
            events.MET.pt,
            events.MET.phi,
        )
        events = set_ak_column_f32(events, "MET.pt", met_pt)
        events = set_ak_column_f32(events, "MET.phi", met_phi)

    return events


@tec.init
def tec_init(self: Calibrator) -> None:
    self.tec_config: TECConfig = self.get_tec_config()

    # add nominal met columns of propagating nominal tec
    if self.propagate_met:
        self.uses |= {"MET.pt", "MET.phi"}
        self.produces |= {"MET.pt", "MET.phi"}

    # add columns with unceratinties if requested
    if self.with_uncertainties:
        # also check if met propagation is enabled
        src_fields = ["Tau.pt", "Tau.mass"]
        if self.propagate_met:
            src_fields += ["MET.pt", "MET.phi"]

        self.produces |= {
            f"{field}_tec_{match}_dm{dm}_{direction}"
            for field, match, dm, direction in itertools.product(
                src_fields,
                ["jet", "e"],
                [0, 1, 10, 11],
                ["up", "down"],
            )
        }


@tec.requires
def tec_requires(self: Calibrator, reqs: dict) -> None:
    from columnflow.tasks.external import BundleExternalFiles
    reqs["external_files"] = BundleExternalFiles.req(self.task)


@tec.setup
def tec_setup(self: Calibrator, reqs: dict, inputs: dict, reader_targets: InsertableDict) -> None:
    bundle = reqs["external_files"]

    # create the tec corrector
    import correctionlib
    correctionlib.highlevel.Correction.__call__ = correctionlib.highlevel.Correction.evaluate
    correction_set = correctionlib.CorrectionSet.from_string(
        self.get_tau_file(bundle.files).load(formatter="gzip").decode("utf-8"),
    )
    self.tec_corrector = correction_set[self.tec_config.correction_set]

    # check versions
    assert self.tec_corrector.version in [0, 1]


tec_nominal = tec.derive("tec_nominal", cls_dict={"with_uncertainties": False})
