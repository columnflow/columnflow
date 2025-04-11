# coding: utf-8

"""
Egamma energy correction methods.
Source: https://twiki.cern.ch/twiki/bin/view/CMS/EgammSFandSSRun3#Scale_And_Smearings_Correctionli
"""

from __future__ import annotations

import abc
import functools
import law
from dataclasses import dataclass, field

from columnflow.calibration import Calibrator, calibrator
from columnflow.calibration.util import ak_random
from columnflow.util import maybe_import, load_correction_set, DotDict
from columnflow.columnar_util import set_ak_column, flat_np_view, ak_copy, optional_column
from columnflow.types import Any

ak = maybe_import("awkward")
np = maybe_import("numpy")


# helper
set_ak_column_f32 = functools.partial(set_ak_column, value_type=np.float32)


@dataclass
class EGammaCorrectionConfig:
    correction_set: str = "Scale"
    compound: bool = False
    corrector_strings: dict[str] = field(default_factory=dict)
    corrector_kwargs: dict[str, Any] = field(default_factory=dict)


class egamma_scale_corrector(Calibrator):

    with_uncertainties = True
    """Switch to control whether uncertainties are calculated."""

    @property
    @abc.abstractmethod
    def source_field(self) -> str:
        """Fields required for the current calibrator."""
        ...

    @abc.abstractmethod
    def get_correction_file(self, external_files: law.FileTargetCollection) -> law.LocalFileTarget:
        """Function to retrieve the correction file from the external files.

        :param external_files: File target containing the files as requested
            in the current config instance under ``config_inst.x.external_files``
        """
        ...

    @abc.abstractmethod
    def get_scale_config(self) -> EGammaCorrectionConfig:
        """Function to retrieve the configuration for the photon energy correction."""
        ...

    def call_func(self, events: ak.Array, **kwargs) -> ak.Array:
        """
        Apply energy corrections to EGamma objects in the events array.

        This implementation follows the recommendations from the EGamma POG:
        https://twiki.cern.ch/twiki/bin/view/CMS/EgammSFandSSRun3#Scale_And_Smearings_Example

        Derivatives of this base class require additional member variables and
        functions:

        - *source_field*: The field name of the EGamma objects in the events array (i.e. `Electron` or `Photon`).
        - *get_correction_file*: Function to retrieve the correction file, e.g.from the list ,of external files in the current `config_inst`.
        - *get_scale_config*: Function to retrieve the configuration for the energy correction.
            This config must be an instance of :py:class:`~columnflow.calibration.cms.egamma.EGammaCorrectionConfig`.

        If no raw pt (i.e., pt before any corrections) is available, use the nominal pt.
        The correction tool only supports flat arrays, so inputs are converted to a flat numpy view first.
        Corrections are always applied to the raw pt, which is important if more than one correction is applied in a
        row. The final corrections must be applied to the current pt.

        If :py:attr:`with_uncertainties` is set to `True`, the scale uncertainties are calculated.
        The scale uncertainties are only available for simulated data.

        :param events: The events array containing EGamma objects.
        :return: The events array with applied scale corrections.

        :notes:
            - Varied corrections are only applied to Monte Carlo (MC) data.
            - EGamma energy correction is only applied to real data.
            - Changes are applied to the views and directly propagate to the original awkward arrays.
        """

        # if no raw pt (i.e. pt for any corrections) is available, use the nominal pt
        if "rawPt" not in events[self.source_field].fields:
            events = set_ak_column_f32(
                events, f"{self.source_field}.rawPt", events[self.source_field].pt,
            )

        # the correction tool only supports flat arrays, so convert inputs to flat np view first
        # corrections are always applied to the raw pt - this is important if more than
        # one correction is applied in a row
        pt_eval = flat_np_view(events[self.source_field].rawPt, axis=1)

        # the final corrections must be applied to the current pt though
        pt_application = flat_np_view(events[self.source_field].pt, axis=1)

        broadcasted_run = ak.broadcast_arrays(events[self.source_field].pt, events.run)
        run = flat_np_view(broadcasted_run[1], axis=1)
        gain = flat_np_view(events[self.source_field].seedGain, axis=1)
        sceta = flat_np_view(events[self.source_field].superclusterEta, axis=1)
        r9 = flat_np_view(events[self.source_field].r9, axis=1)

        # prepare arguments
        # (energy is part of the LorentzVector behavior)
        variable_map = {
            "et": pt_eval,
            "eta": sceta,
            "gain": gain,
            "r9": r9,
            "run": run,
            "seedGain": gain,
            "pt": pt_eval,
            "AbsScEta": np.abs(sceta),
            "ScEta": sceta,
            **self.scale_config.corrector_kwargs,
        }
        args = tuple(
            variable_map[inp.name] for inp in self.scale_corrector.inputs
            if inp.name in variable_map
        )

        # varied corrections are only applied to MC
        if self.with_uncertainties and self.dataset_inst.is_mc:
            scale_uncertainties = self.scale_corrector.evaluate(self.scale_config.corrector_strings["type"], *args)
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
                    events, f"{self.source_field}.pt_{postfix}", pt_varied,
                )

        # apply the nominal correction
        # note: changes are applied to the views and directly propagate to the original ak arrays
        # and do not need to be inserted into the events chunk again
        # EGamma energy correction is ONLY applied to DATA
        if self.dataset_inst.is_data:
            scales_nom = self.scale_corrector("total_correction", *args)
            pt_application *= scales_nom

        return events

    def init_func(self, **kwargs) -> None:
        """Function to initialize the calibrator.

        Sets the required and produced columns for the calibrator.
        """
        self.uses |= {
            # nano columns
            f"{self.source_field}.{{seedGain,pt,superclusterEta,r9}}",
            "run",
            optional_column(f"{self.source_field}.rawPt"),
        }
        self.produces |= {
            f"{self.source_field}.pt",
            optional_column(f"{self.source_field}.rawPt"),
        }

        # if we do not calculate uncertainties, this module
        # should only run on observed DATA
        self.data_only = not self.with_uncertainties

        # add columns with unceratinties if requested
        # photon scale _uncertainties_ are only available for MC
        if self.with_uncertainties and self.dataset_inst.is_mc:
            self.produces |= {f"{self.source_field}.pt_scale_{{up,down}}"}

    def requires_func(self, task: law.Task, reqs: dict[str, DotDict[str, Any]], **kwargs) -> None:
        """Function to add necessary requirements.

        This function add the :py:class:`~columnflow.tasks.external.BundleExternalFiles`
        task to the requirements.

        :param reqs: Dictionary of requirements.
        """
        if "external_files" in reqs:
            return

        from columnflow.tasks.external import BundleExternalFiles
        reqs["external_files"] = BundleExternalFiles.req(task)

    def setup_func(
        self,
        task: law.Task,
        reqs: dict[str, DotDict[str, Any]],
        inputs: dict[str, Any],
        reader_targets: law.util.InsertableDict,
        **kwargs,
    ) -> None:
        """Setup function before event chunk loop.

        This function loads the correction file and sets up the correction tool.
        Additionally, the *scale_config* is retrieved.

        :param reqs: Dictionary with resolved requirements.
        :param inputs: Dictionary with inputs (not used).
        :param reader_targets: Dictionary for optional additional columns to load
            (not used).
        """
        self.scale_config = self.get_scale_config()
        # create the egamma corrector
        corr_file = self.get_correction_file(reqs["external_files"].files)
        # init and extend the correction set
        corr_set = load_correction_set(corr_file)
        if self.scale_config.compound:
            corr_set = corr_set.compound
        self.scale_corrector = corr_set[self.scale_config.correction_set]


class egamma_resolution_corrector(Calibrator):

    with_uncertainties = True
    """Switch to control whether uncertainties are calculated."""

    # smearing of the energy resolution is only applied to MC
    mc_only = True
    """This calibrator is only applied to simulated data."""

    deterministic_seed_index = -1
    """ use deterministic seeds for random smearing and
    take the "index"-th random number per seed when not -1
    """

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

    def call_func(self, events: ak.Array, **kwargs) -> ak.Array:
        """
        Apply energy resolution corrections to EGamma objects in the events array.

        This implementation follows the recommendations from the EGamma POG:
        https://twiki.cern.ch/twiki/bin/view/CMS/EgammSFandSSRun3#Scale_And_Smearings_Example

        Derivatives of this base class require additional member variables and
        functions:

        - *source_field*: The field name of the EGamma objects in the events array (i.e. `Electron` or `Photon`).
        - *get_correction_file*: Function to retrieve the correction file, e.g.
            from the list of external files in the current `config_inst`.
        - *get_resolution_config*: Function to retrieve the configuration for the energy resolution correction.
            This config must be an instance of :py:class:`~columnflow.calibration.cms.egamma.EGammaCorrectionConfig`.

        If no raw pt (i.e., pt before any corrections) is available, use the nominal pt.
        The correction tool only supports flat arrays, so inputs are converted to a flat numpy view first.
        Corrections are always applied to the raw pt, which is important if more than one correction is applied in a
        row. The final corrections must be applied to the current pt.

        If :py:attr:`with_uncertainties` is set to `True`, the resolution uncertainties are calculated.

        If :py:attr:`deterministic_seed_index` is set to a value greater than or equal to 0, deterministic seeds
        are used for random smearing. The "index"-th random number per seed is taken for the nominal resolution
        correction. The "index+1"-th random number per seed is taken for the up variation and the "index+2"-th random
        number per seed is taken for the down variation.

        :param events: The events array containing EGamma objects.
        :return: The events array with applied resolution corrections.

        :notes:
            - Energy resolution correction are only to be applied to simulation.
            - Changes are applied to the views and directly propagate to the original awkward arrays.
        """

        # if no raw pt (i.e. pt for any corrections) is available, use the nominal pt
        if "rawPt" not in events[self.source_field].fields:
            events = set_ak_column_f32(
                events, f"{self.source_field}.rawPt", ak_copy(events[self.source_field].pt),
            )

        # the correction tool only supports flat arrays, so convert inputs to flat np view first
        sceta = flat_np_view(events[self.source_field].superclusterEta, axis=1)
        r9 = flat_np_view(events[self.source_field].r9, axis=1)
        flat_seeds = flat_np_view(events[self.source_field].deterministic_seed, axis=1)

        pt = flat_np_view(events[self.source_field].rawPt, axis=1)

        # prepare arguments
        variable_map = {
            "AbsScEta": np.abs(sceta),
            "eta": sceta,
            "r9": r9,
            "pt": pt,
            **self.resolution_cfg.corrector_kwargs,
        }

        args = tuple(
            variable_map[inp.name]
            for inp in self.resolution_corrector.inputs
            if inp.name in variable_map
        )

        # calculate the smearing scale
        # as mentioned in the example above, allows us to apply them directly to the MC simulation.
        rho = self.resolution_corrector.evaluate(self.resolution_cfg.corrector_strings["type"], *args)

        # -- stochastic smearing
        # normally distributed random numbers according to EGamma resolution

        # varied corrections
        if self.with_uncertainties and self.dataset_inst.is_mc:
            rho_unc = self.resolution_corrector(self.resolution_cfg.corrector_strings["unc_type"], *args)
            random_normal_number = functools.partial(ak_random, 0, 1)
            smearing_func = lambda rng_array, variation: rng_array * variation + 1

            smearing_up = (
                smearing_func(
                    random_normal_number(flat_seeds, rand_func=self.deterministic_normal),
                    rho + rho_unc,
                )
                if self.deterministic_seed_index >= 0
                else smearing_func(
                    random_normal_number(rand_func=np.random.Generator(np.random.SFC64(events.event.to_list())).normal),
                    rho + rho_unc,
                )
            )

            smearing_down = (
                smearing_func(
                    random_normal_number(flat_seeds, rand_func=self.deterministic_normal),
                    rho - rho_unc,
                )
                if self.deterministic_seed_index >= 0
                else smearing_func(
                    random_normal_number(rand_func=np.random.Generator(np.random.SFC64(events.event.to_list())).normal),
                    rho - rho_unc,
                )
            )

            for (direction, smear) in [("up", smearing_up), ("down", smearing_down)]:
                # copy pt and mass
                pt_varied = ak_copy(events[self.source_field].pt)
                pt_view = flat_np_view(pt_varied, axis=1)

                # apply the scale variation
                # cast ak to numpy array for convenient usage of *=
                pt_view *= smear.to_numpy()

                # save columns
                postfix = f"res_{direction}"
                events = set_ak_column_f32(
                    events, f"{self.source_field}.pt_{postfix}", pt_varied,
                )

        # apply the nominal correction
        # note: changes are applied to the views and directly propagate to the original ak arrays
        # and do not need to be inserted into the events chunk again
        # EGamma energy resolution correction is ONLY applied to MC
        if self.dataset_inst.is_mc:
            smearing = (
                ak_random(1, rho, flat_seeds, rand_func=self.deterministic_normal)
                if self.deterministic_seed_index >= 0
                else ak_random(1, rho, rand_func=np.random.Generator(
                    np.random.SFC64(events.event.to_list())).normal,
                )
            )
            # the final corrections must be applied to the current pt though
            pt = flat_np_view(events[self.source_field].pt, axis=1)
            pt *= smearing.to_numpy()

        return events

    def init_func(self, **kwargs) -> None:
        """Function to initialize the calibrator.

        Sets the required and produced columns for the calibrator.
        """
        self.uses |= {
            # nano columns
            f"{self.source_field}.{{pt,superclusterEta,r9}}",
            optional_column(f"{self.source_field}.rawPt"),
        }
        self.produces |= {
            f"{self.source_field}.pt",
            optional_column(f"{self.source_field}.rawPt"),
        }

        # add columns with unceratinties if requested
        if self.with_uncertainties and self.dataset_inst.is_mc:
            self.produces |= {f"{self.source_field}.pt_res_{{up,down}}"}

    def requires_func(self, task: law.Task, reqs: dict[str, DotDict[str, Any]], **kwargs) -> None:
        """Function to add necessary requirements.

        This function add the :py:class:`~columnflow.tasks.external.BundleExternalFiles`
        task to the requirements.

        :param reqs: Dictionary of requirements.
        """
        if "external_files" in reqs:
            return

        from columnflow.tasks.external import BundleExternalFiles
        reqs["external_files"] = BundleExternalFiles.req(task)

    def setup_func(
        self,
        task: law.Task,
        reqs: dict[str, DotDict[str, Any]],
        inputs: dict[str, Any],
        reader_targets: law.util.InsertableDict,
        **kwargs,
    ) -> None:
        """Setup function before event chunk loop.

        This function loads the correction file and sets up the correction tool.
        Additionally, the *resolution_config* is retrieved.
        If :py:attr:`deterministic_seed_index` is set to a value greater than or equal to 0,
        random generator based on object-specific random seeds are setup.

        :param reqs: Dictionary with resolved requirements.
        :param inputs: Dictionary with inputs (not used).
        :param reader_targets: Dictionary for optional additional columns to load
            (not used).
        """
        self.resolution_cfg = self.get_resolution_config()
        # create the egamma corrector
        corr_file = self.get_correction_file(reqs["external_files"].files)
        corr_set = load_correction_set(corr_file)
        if self.resolution_cfg.compound:
            corr_set = corr_set.compound
        self.resolution_corrector = corr_set[self.resolution_cfg.correction_set]

        # use deterministic seeds for random smearing if requested
        if self.deterministic_seed_index >= 0:
            idx = self.deterministic_seed_index
            bit_generator = np.random.SFC64

            def deterministic_normal(loc, scale, seed, idx_offset=0):
                return np.asarray([
                    np.random.Generator(bit_generator(_seed)).normal(_loc, _scale, size=idx + 1 + idx_offset)[-1]
                    for _loc, _scale, _seed in zip(loc, scale, seed)
                ])
            self.deterministic_normal = functools.partial(deterministic_normal, idx_offset=0)
            self.deterministic_normal_up = functools.partial(deterministic_normal, idx_offset=1)
            self.deterministic_normal_down = functools.partial(deterministic_normal, idx_offset=2)


pec = egamma_scale_corrector.derive(
    "pec", cls_dict={
        "source_field": "Photon",
        "with_uncertainties": True,
        "get_correction_file": (lambda self, external_files: external_files.photon_ss),
        "get_scale_config": (lambda self: self.config_inst.x.pec),
    },
)

per = egamma_resolution_corrector.derive(
    "per", cls_dict={
        "source_field": "Photon",
        "with_uncertainties": True,
        # function to determine the correction file
        "get_correction_file": (lambda self, external_files: external_files.photon_ss),
        # function to determine the tec config
        "get_resolution_config": (lambda self: self.config_inst.x.per),
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
def photons(self, events: ak.Array, **kwargs) -> ak.Array:
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


@photons.pre_init
def photons_pre_init(self, **kwargs) -> None:
    # forward argument to the producers
    if pec not in self.deps_kwargs:
        self.deps_kwargs[pec] = dict()
    if per not in self.deps_kwargs:
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
        "get_resolution_config": (lambda self: self.config_inst.x.eer),
    },
)

eec = egamma_scale_corrector.derive(
    "eec", cls_dict={
        "source_field": "Electron",
        # calculation of superclusterEta for electrons requires the deltaEtaSC
        "uses": {"Electron.deltaEtaSC"},
        "with_uncertainties": True,
        "get_correction_file": (lambda self, external_files: external_files.electron_ss),
        "get_scale_config": (lambda self: self.config_inst.x.eec),
    },
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
def electrons(self, events: ak.Array, **kwargs) -> ak.Array:
    """
    Calibrator for electrons. This calibrator runs the energy scale and resolution calibrators
    for electrons.

    Careful! Always apply resolution before scale corrections for MC.
    """
    if self.dataset_inst.is_mc:
        events = self[eer](events, **kwargs)

    if self.with_uncertainties or self.dataset_inst.is_data:
        events = self[eec](events, **kwargs)

    return events


@electrons.pre_init
def electrons_pre_init(self, **kwargs) -> None:
    # forward argument to the producers
    if eec not in self.deps_kwargs:
        self.deps_kwargs[eec] = dict()
    if eer not in self.deps_kwargs:
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
