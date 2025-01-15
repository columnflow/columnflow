# coding: utf-8

"""
Egamma energy correction methods.
Source: https://twiki.cern.ch/twiki/bin/view/CMS/EgammSFandSSRun3#Scale_And_Smearings_Correctionli
"""

from __future__ import annotations

import abc
import functools
import itertools
import law
from dataclasses import dataclass, field

from columnflow.calibration import Calibrator, calibrator
from columnflow.calibration.util import ak_random
from columnflow.util import maybe_import, InsertableDict
from columnflow.columnar_util import (
    set_ak_column, flat_np_view, ak_copy, optional_column,
)
from columnflow.types import Any

ak = maybe_import("awkward")
np = maybe_import("numpy")


# helper
set_ak_column_f32 = functools.partial(set_ak_column, value_type=np.float32)


@dataclass
class EGammaCorrectionConfig:
    correction_set: str = "Scale"
    corrector_kwargs: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def new(
        cls,
        obj: EGammaCorrectionConfig | tuple[str] | dict[str, str],
    ) -> EGammaCorrectionConfig:
        # purely for backwards compatibility with the old tuple format that accepted the two
        # working point values
        if isinstance(obj, tuple) and len(obj) == 2:
            obj = dict(zip(["wp", "wp_VSe"], obj))
        if isinstance(obj, dict):
            return cls(corrector_kwargs=obj)
        return obj


class egamma_scale_corrector(Calibrator):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._with_uncertainties = True

    # whether to produce also uncertainties
    @property
    def with_uncertainties(self) -> bool:
        return self._with_uncertainties
    
    @with_uncertainties.setter
    def with_uncertainties(self, value: bool):
        self._with_uncertainties = value
    
    @property
    @abc.abstractmethod
    def source_field(self) -> str:
        """Fields required for the current calibrator."""
        ...

    @abc.abstractmethod
    def get_correction_file(self, external_files: law.FileTargetCollection) -> law.LocalFile:
        """Function to retrieve the correction file from the external files.

        :param external_files: File target containing the files as requested
            in the current config instance under ``config_inst.x.external_files``
        """
        ...
    
    @abc.abstractmethod
    def get_scale_config(self) -> EGammaCorrectionConfig:
        """Function to retrieve the configuration for the photon energy correction."""
        ...

    def call_func(
        self: Calibrator,
        events: ak.Array,
        **kwargs,
    ) -> ak.Array:
        """
        Calibrator for photon and electron energy scales.
        Requires an external file in the config under ``electronSS`` or ``photon_SS``,
        e.g.

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
        
        # from IPython import embed
        # embed(header="entering photon energy calibration")
        # if no raw pt (i.e. pt for any corrections) is available, use the nominal pt
        if not "rawPt" in events[self.source_field].fields:
            events = set_ak_column_f32(
                events, f"{self.source_field}.rawPt", events[self.source_field].pt
            )
        # the correction tool only supports flat arrays, so convert inputs to flat np view first
        # corrections are always applied to the raw pt - this is important if more than
        # one correction is applied in a row
        pt_eval = flat_np_view(events[self.source_field].rawPt, axis=1)

        # the final corrections must be applied to the current pt though
        pt_application = flat_np_view(events[self.source_field].pt, axis=1)

        broadcasted_run = ak.broadcast_arrays(
            events[self.source_field].pt, events.run
        )
        run = flat_np_view(broadcasted_run[1], axis=1)
        gain = flat_np_view(events[self.source_field].seedGain, axis=1)
        sceta = flat_np_view(events[self.source_field].superclusterEta, axis=1)
        r9 = flat_np_view(events[self.source_field].r9, axis=1)

        # prepare arguments
        # we use pt as et since there depends in linear (following the recoomendations)
        # (energy is part of the LorentzVector behavior)
        variable_map = {
            "et": pt_eval,
            "eta": sceta,
            "gain": gain,
            "r9": r9,
            "run": run,
            **self.scale_config.corrector_kwargs,
        }
        args = tuple(
            variable_map[inp.name] for inp in self.scale_corrector.inputs
            if inp.name in variable_map
        )

        # varied corrections are only applied to MC
        if self.with_uncertainties and self.dataset_inst.is_mc:
            scale_uncertainties = self.scale_corrector("total_uncertainty", *args)
            scales_up = (1 + scale_uncertainties)
            scales_down = (1 - scale_uncertainties)

            for (direction, scales) in [("up", scales_up), ("down", scales_down)]:
                # copy pt and mass
                pt_varied = ak_copy(events[self.source_field].pt)
                pt_view = flat_np_view(pt_varied, axis=1)

                # apply the scale variation
                pt_view *= scales

                # save columns
                postfix = f"scale_{direction}"
                events = set_ak_column_f32(
                    events, f"{self.source_field}.pt_{postfix}", pt_varied
                )

                
        # apply the nominal correction
        # note: changes are applied to the views and directly propagate to the original ak arrays
        # and do not need to be inserted into the events chunk again
        # EGamma energy correction is ONLY applied to DATA
        if self.dataset_inst.is_data:
            scales_nom = self.scale_corrector("total_correction", *args)
            pt_application *= scales_nom

        return events


    def init_func(self: Calibrator) -> None:
        self.uses |= {
            # nano columns
            f"n{self.source_field}", f"{self.source_field}.{{seedGain,pt,superclusterEta,r9}}",
            "run",
            optional_column(f"{self.source_field}.rawPt"),
        }
        self.produces |= {
            f"{self.source_field}.pt",
            optional_column(f"{self.source_field}.rawPt"),
        }
        self.scale_config: EGammaCorrectionConfig = self.get_scale_config()

        # if we do not calculate uncertainties, this module
        # should only run on observed DATA
        self.data_only = not self.with_uncertainties

        # add columns with unceratinties if requested
        # photon scale _uncertainties_ are only available for MC
        if self.with_uncertainties and self.dataset_inst.is_mc:
            src_fields = [f"{self.source_field}.pt"]            
            self.produces |= {
                f"{field}_scale_{direction}"
                for field, direction in itertools.product(
                    src_fields,
                    ["up", "down"],
                )
            }


    def requires_func(self: Calibrator, reqs: dict) -> None:
        from columnflow.tasks.external import BundleExternalFiles
        reqs["external_files"] = BundleExternalFiles.req(self.task)


    def setup_func(
        self: Calibrator,
        reqs: dict,
        inputs: dict,
        reader_targets: InsertableDict
    ) -> None:
        bundle = reqs["external_files"]

        # create the tec corrector
        import correctionlib
        correctionlib.highlevel.Correction.__call__ = correctionlib.highlevel.Correction.evaluate
        correction_set = correctionlib.CorrectionSet.from_string(
            self.get_correction_file(bundle.files).load(formatter="gzip").decode("utf-8"),
        )
        # from IPython import embed
        # embed(header="entering pec setup")
        self.scale_corrector = correction_set[self.scale_config.correction_set]

        # check versions
        assert self.scale_corrector.version in [0, 1, 2]


pec = egamma_scale_corrector.derive(
    "pec", cls_dict={
        "source_field": "Photon",
        "with_uncertainties": True,
        "get_correction_file": (lambda self, external_files: external_files.photon_ss),
        "get_scale_config": (lambda self: EGammaCorrectionConfig.new(self.config_inst.x.pec)),
    }
)

class egamma_resolution_corrector(Calibrator):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._with_uncertainties = True
        # smearing of the energy resolution is only applied to MC
        self.mc_only=True

        # use deterministic seeds for random smearing and
        # take the "index"-th random number per seed when not -1
        self._deterministic_seed_index=-1

    # whether to produce also uncertainties
    @property
    def with_uncertainties(self) -> bool:
        return self._with_uncertainties
    
    @with_uncertainties.setter
    def with_uncertainties(self, value: bool):
        self._with_uncertainties = value

    @property
    def deterministic_seed_index(self) -> int:
        return self._deterministic_seed_index
    
    @deterministic_seed_index.setter
    def deterministic_seed_index(self, value: int):
        self._deterministic_seed_index = value

    @property
    @abc.abstractmethod
    def source_field(self) -> str:
        """Fields required for the current calibrator."""
        ...

    @abc.abstractmethod
    def get_correction_file(self, external_files: law.FileTargetCollection) -> law.LocalFile:
        """Function to retrieve the correction file from the external files.

        :param external_files: File target containing the files as requested
            in the current config instance under ``config_inst.x.external_files``
        """
        ...
    
    @abc.abstractmethod
    def get_resolution_config(self) -> EGammaCorrectionConfig:
        """Function to retrieve the configuration for the photon energy correction."""
        ...

    def call_func(
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

        # if no raw pt (i.e. pt for any corrections) is available, use the nominal pt
        if not "rawPt" in events[self.source_field].fields:
            events = set_ak_column_f32(
                events, f"{self.source_field}.rawPt", events[self.source_field].pt
            )

        # the correction tool only supports flat arrays, so convert inputs to flat np view first

        sceta = flat_np_view(events[self.source_field].superclusterEta, axis=1)
        r9 = flat_np_view(events[self.source_field].r9, axis=1)

        # prepare arguments
        # we use pt as et since there depends in linear (following the recoomendations)
        # (energy is part of the LorentzVector behavior)
        variable_map = {
            "eta": sceta,
            "r9": r9,
            **self.resolution_config.corrector_kwargs,
        }
        args = tuple(
            variable_map[inp.name] for inp in self.resolution_corrector.inputs
            if inp.name in variable_map
        )

        # calculate the smearing scale
        rho = self.resolution_corrector("rho", *args)

        # -- stochastic smearing
        # normally distributed random numbers according to EGamma resolution

        # varied corrections
        if self.with_uncertainties and self.dataset_inst.is_mc:
            rho_unc = self.resolution_corrector("err_rho", *args)
            smearing_up = (
                ak_random(
                    0, rho + rho_unc, events[self.source_field].deterministic_seed,
                    rand_func=self.deterministic_normal
                )
                if self.deterministic_seed_index >= 0
                else ak_random(0, rho + rho_unc, rand_func=np.random.Generator(
                    np.random.SFC64(events.event.to_list())).normal,
                )
            )

            smearing_down = (
                ak_random(
                    0, rho - rho_unc, events[self.source_field].deterministic_seed,
                    rand_func=self.deterministic_normal
                )
                if self.deterministic_seed_index >= 0
                else ak_random(0, rho - rho_unc, rand_func=np.random.Generator(
                    np.random.SFC64(events.event.to_list())).normal,
                )
            )

            for (direction, smear) in [("up", smearing_up), ("down", smearing_down)]:
                # copy pt and mass
                pt_varied = ak_copy(events[self.source_field].pt)
                pt_view = flat_np_view(pt_varied, axis=1)

                # from IPython import embed
                # embed(header=f"about to apply smearing for {direction} direction")
                # apply the scale variation
                # cast ak to numpy array for convenient usage of *=
                pt_view *= smear.to_numpy()

                # save columns
                postfix = f"res_{direction}"
                events = set_ak_column_f32(
                    events, f"{self.source_field}.pt_{postfix}", pt_varied
                )

                
        # apply the nominal correction
        # note: changes are applied to the views and directly propagate to the original ak arrays
        # and do not need to be inserted into the events chunk again
        # EGamma energy resolution correction is ONLY applied to MC
        if self.dataset_inst.is_mc:
            smearing = (
                ak_random(0, rho, events[self.source_field].deterministic_seed, rand_func=self.deterministic_normal)
                if self.deterministic_seed_index >= 0
                else ak_random(0, rho, rand_func=np.random.Generator(
                    np.random.SFC64(events.event.to_list())).normal,
                )
            )
            # the final corrections must be applied to the current pt though
            pt = flat_np_view(events[self.source_field].pt, axis=1)
            pt *= smearing.to_numpy()

        return events

    def init_func(self: Calibrator) -> None:
        self.uses |= {
        # nano columns
        f"n{self.source_field}", f"{self.source_field}.{{pt,superclusterEta,r9}}",
        optional_column(f"{self.source_field}.rawPt"),
        }
        self.produces |= {
            f"{self.source_field}.pt",
            optional_column(f"{self.source_field}.rawPt"),
        }
        
        self.resolution_config: EGammaCorrectionConfig = self.get_resolution_config()

        # add columns with unceratinties if requested
        # photon scale _uncertainties_ are only available for MC
        if self.with_uncertainties and self.dataset_inst.is_mc:
            # also check if met propagation is enabled
            src_fields = [f"{self.source_field}.pt"]
            
            self.produces |= {
                f"{field}_res_{direction}"
                for field, direction in itertools.product(
                    src_fields,
                    ["up", "down"],
                )
            }


    def requires_func(self: Calibrator, reqs: dict) -> None:
        from columnflow.tasks.external import BundleExternalFiles
        reqs["external_files"] = BundleExternalFiles.req(self.task)


    def setup_func(self: Calibrator, reqs: dict, inputs: dict, reader_targets: InsertableDict) -> None:
        bundle = reqs["external_files"]

        # create the tec corrector
        import correctionlib
        correctionlib.highlevel.Correction.__call__ = correctionlib.highlevel.Correction.evaluate
        correction_set = correctionlib.CorrectionSet.from_string(
            self.get_correction_file(bundle.files).load(formatter="gzip").decode("utf-8"),
        )
        self.resolution_corrector = correction_set[self.resolution_config.correction_set]

        # check versions
        assert self.resolution_corrector.version in [0, 1, 2]

        # use deterministic seeds for random smearing if requested
        if self.deterministic_seed_index >= 0:
            idx = self.deterministic_seed_index
            bit_generator = np.random.SFC64
            def deterministic_normal(loc, scale, seed):
                return np.asarray([
                    np.random.Generator(bit_generator(_seed)).normal(_loc, _scale, size=idx + 1)[-1]
                    for _loc, _scale, _seed in zip(loc, scale, seed)
                ])
            self.deterministic_normal = deterministic_normal


per = egamma_resolution_corrector.derive(
    "per", cls_dict={
        "source_field": "Photon",
        "with_uncertainties": True,
        # function to determine the correction file
        "get_correction_file": (lambda self, external_files: external_files.photon_ss),
        # function to determine the tec config
        "get_resolution_config": (lambda self: EGammaCorrectionConfig.new(self.config_inst.x.per)),
    },
)

@calibrator(
    uses={per, pec},
    produces={per, pec},
    with_uncertainties=True,
    get_correction_file=None,
    get_scale_config=None,
    get_resolution_config=None,
    deterministic_seed_index=-1,
)
def photons(self: Calibrator, events: ak.Array, **kwargs) -> ak.Array:
    """
    Calibrator for photons. This calibrator runs the energy scale and resolution calibrators
    for photons.

    Careful! Always apply resolution before scale corrections for MC.
    """
    if self.dataset_inst.is_mc:
        events = self[per](events, **kwargs)

    if self.with_uncertainties or self.dataset_inst.is_data:
        events = self[pec](events, **kwargs)

    return events

@photons.init
def photons_init(self: Calibrator) -> None:
    # forward argument to the producers

    if not pec in self.deps_kwargs:
        self.deps_kwargs[pec] = dict()
    if not per in self.deps_kwargs:
        self.deps_kwargs[per] = dict()
    self.deps_kwargs[pec]["with_uncertainties"] = self.with_uncertainties
    self.deps_kwargs[per]["with_uncertainties"] = self.with_uncertainties
    
    self.deps_kwargs[per]["deterministic_seed_index"] = self.deterministic_seed_index
    if self.get_correction_file is not None:
        self.deps_kwargs[pec]["get_correction_file"] = self.get_correction_file
        self.deps_kwargs[per]["get_correction_file"] = self.get_correction_file

    if self.get_resolution_config is not None:
        self.deps_kwargs[per]["get_resolution_config"] = self.get_resolution_config
    if self.get_scale_config is not None:
        self.deps_kwargs[pec]["get_scale_config"] = self.get_scale_config

photons_nominal = photons.derive("photons_nominal", cls_dict={"with_uncertainties": False})


eer = egamma_resolution_corrector.derive(
    "eer", cls_dict={
        "source_field": "Electron",
        # calculation of superclusterEta for electrons requires the deltaEtaSC
        "uses": {"Electron.deltaEtaSC"},
        "with_uncertainties": True,
        # function to determine the correction file
        "get_correction_file": (lambda self, external_files: external_files.electron_ss),
        # function to determine the tec config
        "get_resolution_config": (lambda self: EGammaCorrectionConfig.new(self.config_inst.x.eer)),
    },
)

eec = egamma_scale_corrector.derive(
    "eec", cls_dict={
        "source_field": "Electron",
        # calculation of superclusterEta for electrons requires the deltaEtaSC
        "uses": {"Electron.deltaEtaSC"},
        "with_uncertainties": True,
        "get_correction_file": (lambda self, external_files: external_files.electron_ss),
        "get_scale_config": (lambda self: EGammaCorrectionConfig.new(self.config_inst.x.eec)),
    }
)


@calibrator(
    uses={eer, eec},
    produces={eer, eec},
    with_uncertainties=True,
    get_correction_file=None,
    get_scale_config=None,
    get_resolution_config=None,
    deterministic_seed_index=-1,
)
def electrons(self: Calibrator, events: ak.Array, **kwargs) -> ak.Array:
    """
    Calibrator for photons. This calibrator runs the energy scale and resolution calibrators
    for photons.

    Careful! Always apply resolution before scale corrections for MC.
    """
    if self.dataset_inst.is_mc:
        events = self[eer](events, **kwargs)

    if self.with_uncertainties or self.dataset_inst.is_data:
        events = self[eec](events, **kwargs)

    return events

@electrons.init
def electrons_init(self: Calibrator) -> None:
    # forward argument to the producers

    if not eec in self.deps_kwargs:
        self.deps_kwargs[eec] = dict()
    if not eer in self.deps_kwargs:
        self.deps_kwargs[eer] = dict()
    self.deps_kwargs[eec]["with_uncertainties"] = self.with_uncertainties
    self.deps_kwargs[eer]["with_uncertainties"] = self.with_uncertainties
    
    self.deps_kwargs[eer]["deterministic_seed_index"] = self.deterministic_seed_index
    if self.get_correction_file is not None:
        self.deps_kwargs[eec]["get_correction_file"] = self.get_correction_file
        self.deps_kwargs[eer]["get_correction_file"] = self.get_correction_file

    if self.get_resolution_config is not None:
        self.deps_kwargs[eer]["get_resolution_config"] = self.get_resolution_config
    if self.get_scale_config is not None:
        self.deps_kwargs[eec]["get_scale_config"] = self.get_scale_config

electrons_nominal = photons.derive("electrons_nominal", cls_dict={"with_uncertainties": False})