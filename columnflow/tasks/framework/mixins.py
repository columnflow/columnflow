# coding: utf-8

"""
Lightweight mixins task classes.
"""

from __future__ import annotations

import gc
import time
import itertools
from collections import Counter

import luigi
import law
import order as od

from columnflow.types import Sequence, Any
from columnflow.tasks.framework.base import AnalysisTask, ConfigTask, RESOLVE_DEFAULT
from columnflow.calibration import Calibrator
from columnflow.selection import Selector
from columnflow.production import Producer
from columnflow.ml import MLModel
from columnflow.inference import InferenceModel
from columnflow.util import maybe_import


ak = maybe_import("awkward")


class CalibratorMixin(ConfigTask):

    calibrator = luigi.Parameter(
        default=RESOLVE_DEFAULT,
        description="the name of the calibrator to be applied; default: value of the "
        "'default_calibrator' config",
    )

    # decibes whether the task itself runs the calibrator and implements its shifts
    register_calibrator_shifts = False

    @classmethod
    def get_calibrator_inst(cls, calibrator, kwargs=None):
        calibrator_cls = Calibrator.get_cls(calibrator)
        if not calibrator_cls.exposed:
            raise RuntimeError(f"cannot use unexposed calibrator '{calibrator}' in {cls.__name__}")

        inst_dict = cls.get_calibrator_kwargs(**kwargs) if kwargs else None
        return calibrator_cls(inst_dict=inst_dict)

    @classmethod
    def resolve_param_values(cls, params):
        params = super().resolve_param_values(params)

        config_inst = params.get("config_inst")
        if config_inst:
            # add the default calibrator when empty
            params["calibrator"] = cls.resolve_config_default(
                params,
                params.get("calibrator"),
                container=config_inst,
                default_str="default_calibrator",
                multiple=False,
            )
            params["calibrator_inst"] = cls.get_calibrator_inst(params["calibrator"], params)

        return params

    @classmethod
    def get_known_shifts(cls, config_inst: od.Config, params: dict) -> tuple[set[str], set[str]]:
        shifts, upstream_shifts = super().get_known_shifts(config_inst, params)

        # get the calibrator, update it and add its shifts
        calibrator_inst = params.get("calibrator_inst")
        if calibrator_inst:
            if cls.register_calibrator_shifts:
                shifts |= calibrator_inst.all_shifts
            else:
                upstream_shifts |= calibrator_inst.all_shifts

        return shifts, upstream_shifts

    @classmethod
    def req_params(cls, inst: law.Task, **kwargs) -> dict:
        # prefer --calibrator set on task-level via cli
        kwargs["_prefer_cli"] = law.util.make_set(kwargs.get("_prefer_cli", [])) | {"calibrator"}

        return super().req_params(inst, **kwargs)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # cache for calibrator inst
        self._calibrator_inst = None

    @property
    def calibrator_inst(self):
        if self._calibrator_inst is None:
            self._calibrator_inst = self.get_calibrator_inst(self.calibrator, {"task": self})

            # overwrite the sandbox when set
            if self._calibrator_inst.sandbox:
                self.sandbox = self._calibrator_inst.sandbox

        return self._calibrator_inst

    def store_parts(self):
        parts = super().store_parts()
        parts.insert_before("version", "calibrator", f"calib__{self.calibrator}")
        return parts


class CalibratorsMixin(ConfigTask):

    calibrators = law.CSVParameter(
        default=(RESOLVE_DEFAULT,),
        description="comma-separated names of calibrators to be applied; default: value of the "
        "'default_calibrator' config in a 1-tuple",
        brace_expand=True,
        parse_empty=True,
    )

    # decibes whether the task itself runs the calibrators and implements their shifts
    register_calibrators_shifts = False

    @classmethod
    def get_calibrator_insts(cls, calibrators, kwargs=None):
        inst_dict = cls.get_calibrator_kwargs(**kwargs) if kwargs else None

        insts = []
        for calibrator in calibrators:
            calibrator_cls = Calibrator.get_cls(calibrator)
            if not calibrator_cls.exposed:
                raise RuntimeError(
                    f"cannot use unexposed calibrator '{calibrator}' in {cls.__name__}",
                )
            insts.append(calibrator_cls(inst_dict=inst_dict))

        return insts

    @classmethod
    def resolve_param_values(cls, params):
        params = super().resolve_param_values(params)

        config_inst = params.get("config_inst")
        if config_inst:
            params["calibrators"] = cls.resolve_config_default_and_groups(
                params,
                params.get("calibrators"),
                container=config_inst,
                default_str="default_calibrator",
                groups_str="calibrator_groups",
            )
            params["calibrator_insts"] = cls.get_calibrator_insts(params["calibrators"], params)

        return params

    @classmethod
    def get_known_shifts(cls, config_inst: od.Config, params: dict) -> tuple[set[str], set[str]]:
        shifts, upstream_shifts = super().get_known_shifts(config_inst, params)

        # get the calibrators, update them and add their shifts
        for calibrator_inst in params.get("calibrator_insts") or []:
            if cls.register_calibrators_shifts:
                shifts |= calibrator_inst.all_shifts
            else:
                upstream_shifts |= calibrator_inst.all_shifts

        return shifts, upstream_shifts

    @classmethod
    def req_params(cls, inst: law.Task, **kwargs) -> dict:
        # prefer --calibrators set on task-level via cli
        kwargs["_prefer_cli"] = law.util.make_set(kwargs.get("_prefer_cli", [])) | {"calibrators"}

        return super().req_params(inst, **kwargs)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # cache for calibrator insts
        self._calibrator_insts = None

    @property
    def calibrator_insts(self):
        if self._calibrator_insts is None:
            self._calibrator_insts = self.get_calibrator_insts(self.calibrators, {"task": self})
        return self._calibrator_insts

    def store_parts(self):
        parts = super().store_parts()

        part = "__".join(self.calibrators[:5])
        if len(self.calibrators) > 5:
            part += f"__{law.util.create_hash(self.calibrators[5:])}"
        parts.insert_before("version", "calibrators", f"calib__{part or 'none'}")

        return parts


class SelectorMixin(ConfigTask):

    selector = luigi.Parameter(
        default=RESOLVE_DEFAULT,
        description="the name of the selector to be applied; default: value of the "
        "'default_selector' config",
    )

    # decibes whether the task itself runs the selector and implements its shifts
    register_selector_shifts = False

    @classmethod
    def get_selector_inst(cls, selector, kwargs=None):
        selector_cls = Selector.get_cls(selector)
        if not selector_cls.exposed:
            raise RuntimeError(f"cannot use unexposed selector '{selector}' in {cls.__name__}")

        inst_dict = cls.get_selector_kwargs(**kwargs) if kwargs else None
        return selector_cls(inst_dict=inst_dict)

    @classmethod
    def resolve_param_values(cls, params):
        params = super().resolve_param_values(params)

        # add the default selector when empty
        config_inst = params.get("config_inst")
        if config_inst:
            params["selector"] = cls.resolve_config_default(
                params,
                params.get("selector"),
                container=config_inst,
                default_str="default_selector",
                multiple=False,
            )
            params["selector_inst"] = cls.get_selector_inst(params["selector"], params)

        return params

    @classmethod
    def get_known_shifts(cls, config_inst: od.Config, params: dict) -> tuple[set[str], set[str]]:
        shifts, upstream_shifts = super().get_known_shifts(config_inst, params)

        # get the selector, update it and add its shifts
        selector_inst = params.get("selector_inst")
        if selector_inst:
            if cls.register_selector_shifts:
                shifts |= selector_inst.all_shifts
            else:
                upstream_shifts |= selector_inst.all_shifts

        return shifts, upstream_shifts

    @classmethod
    def req_params(cls, inst: law.Task, **kwargs) -> dict:
        # prefer --selector set on task-level via cli
        kwargs["_prefer_cli"] = law.util.make_set(kwargs.get("_prefer_cli", [])) | {"selector"}

        return super().req_params(inst, **kwargs)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # cache for selector inst
        self._selector_inst = None

    @property
    def selector_inst(self):
        if self._selector_inst is None:
            self._selector_inst = self.get_selector_inst(self.selector, {"task": self})

            # overwrite the sandbox when set
            if self._selector_inst.sandbox:
                self.sandbox = self._selector_inst.sandbox

        return self._selector_inst

    def store_parts(self):
        parts = super().store_parts()
        parts.insert_before("version", "selector", f"sel__{self.selector}")
        return parts


class SelectorStepsMixin(SelectorMixin):

    selector_steps = law.CSVParameter(
        default=(),
        description="a subset of steps of the selector to apply; uses all steps when empty; "
        "empty default",
        brace_expand=True,
        parse_empty=True,
    )

    exclude_params_repr_empty = {"selector_steps"}

    selector_steps_order_sensitive = False

    @classmethod
    def resolve_param_values(cls, params: dict[str, Any]) -> dict[str, Any]:
        params = super().resolve_param_values(params)

        # apply selector_steps_groups and default_selector_steps from config
        config_inst = params.get("config_inst")
        if config_inst:
            params["selector_steps"] = cls.resolve_config_default_and_groups(
                params,
                params.get("selector_steps"),
                container=config_inst,
                default_str="default_selector_steps",
                groups_str="selector_step_groups",
            )

        # sort selector steps when the order does not matter
        if not cls.selector_steps_order_sensitive and "selector_steps" in params:
            params["selector_steps"] = tuple(sorted(params["selector_steps"]))

        return params

    @classmethod
    def req_params(cls, inst: law.Task, **kwargs) -> dict:
        # prefer --selector-steps set on task-level via cli
        kwargs["_prefer_cli"] = law.util.make_set(kwargs.get("_prefer_cli", [])) | {"selector_steps"}

        return super().req_params(inst, **kwargs)

    def store_parts(self) -> law.util.InsertableDict:
        parts = super().store_parts()

        steps = self.selector_steps
        if not self.selector_steps_order_sensitive:
            steps = sorted(steps)
        if steps:
            parts["selector"] += "__steps_" + "_".join(steps)

        return parts


class ProducerMixin(ConfigTask):

    producer = luigi.Parameter(
        default=RESOLVE_DEFAULT,
        description="the name of the producer to be applied; default: value of the "
        "'default_producer' config",
    )

    # decibes whether the task itself runs the producer and implements its shifts
    register_producer_shifts = False

    @classmethod
    def get_producer_inst(cls, producer, kwargs=None):
        producer_cls = Producer.get_cls(producer)
        if not producer_cls.exposed:
            raise RuntimeError(f"cannot use unexposed producer '{producer}' in {cls.__name__}")

        inst_dict = cls.get_producer_kwargs(**kwargs) if kwargs else None
        return producer_cls(inst_dict=inst_dict)

    @classmethod
    def resolve_param_values(cls, params):
        params = super().resolve_param_values(params)

        # add the default producer when empty
        config_inst = params.get("config_inst")
        if config_inst:
            params["producer"] = cls.resolve_config_default(
                params,
                params.get("producer"),
                container=config_inst,
                default_str="default_producer",
                multiple=False,
            )
            params["producer_inst"] = cls.get_producer_inst(params["producer"], params)

        return params

    @classmethod
    def get_known_shifts(cls, config_inst: od.Config, params: dict) -> tuple[set[str], set[str]]:
        shifts, upstream_shifts = super().get_known_shifts(config_inst, params)

        # get the producer, update it and add its shifts
        producer_inst = params.get("producer_inst")
        if producer_inst:
            if cls.register_producer_shifts:
                shifts |= producer_inst.all_shifts
            else:
                upstream_shifts |= producer_inst.all_shifts

        return shifts, upstream_shifts

    @classmethod
    def req_params(cls, inst: law.Task, **kwargs) -> dict:
        # prefer --producer set on task-level via cli
        kwargs["_prefer_cli"] = law.util.make_set(kwargs.get("_prefer_cli", [])) | {"producer"}

        return super().req_params(inst, **kwargs)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # cache for producer inst
        self._producer_inst = None

    @property
    def producer_inst(self):
        if self._producer_inst is None:
            self._producer_inst = self.get_producer_inst(self.producer, {"task": self})

            # overwrite the sandbox when set
            if self._producer_inst.sandbox:
                self.sandbox = self._producer_inst.sandbox

        return self._producer_inst

    def store_parts(self):
        parts = super().store_parts()
        producer = f"prod__{self.producer}" if self.producer != law.NO_STR else "none"
        parts.insert_before("version", "producer", producer)
        return parts


class ProducersMixin(ConfigTask):

    producers = law.CSVParameter(
        default=(RESOLVE_DEFAULT,),
        description="comma-separated names of producers to be applied; empty default",
        brace_expand=True,
        parse_empty=True,
    )

    # decibes whether the task itself runs the producers and implements their shifts
    register_producers_shifts = False

    @classmethod
    def get_producer_insts(cls, producers, kwargs=None):
        inst_dict = cls.get_producer_kwargs(**kwargs) if kwargs else None

        insts = []
        for producer in producers:
            producer_cls = Producer.get_cls(producer)
            if not producer_cls.exposed:
                raise RuntimeError(f"cannot use unexposed producer '{producer}' in {cls.__name__}")
            insts.append(producer_cls(inst_dict=inst_dict))

        return insts

    @classmethod
    def resolve_param_values(cls, params):
        params = super().resolve_param_values(params)

        config_inst = params.get("config_inst")
        if config_inst:
            params["producers"] = cls.resolve_config_default_and_groups(
                params,
                params.get("producers"),
                container=config_inst,
                default_str="default_producer",
                groups_str="producer_groups",
            )
            params["producer_insts"] = cls.get_producer_insts(params["producers"], params)

        return params

    @classmethod
    def get_known_shifts(cls, config_inst: od.Config, params: dict) -> tuple[set[str], set[str]]:
        shifts, upstream_shifts = super().get_known_shifts(config_inst, params)

        # get the producers, update them and add their shifts
        for producer_inst in params.get("producer_insts") or []:
            if cls.register_producers_shifts:
                shifts |= producer_inst.all_shifts
            else:
                upstream_shifts |= producer_inst.all_shifts

        return shifts, upstream_shifts

    @classmethod
    def req_params(cls, inst: law.Task, **kwargs) -> dict:
        # prefer --producers set on task-level via cli
        kwargs["_prefer_cli"] = law.util.make_set(kwargs.get("_prefer_cli", [])) | {"producers"}

        return super().req_params(inst, **kwargs)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # cache for producer insts
        self._producer_insts = None

    @property
    def producer_insts(self):
        if self._producer_insts is None:
            self._producer_insts = self.get_producer_insts(self.producers, {"task": self})
        return self._producer_insts

    def store_parts(self):
        parts = super().store_parts()

        part = "none"
        if self.producers:
            part = "__".join(self.producers[:5])
            if len(self.producers) > 5:
                part += f"__{law.util.create_hash(self.producers[5:])}"
        parts.insert_before("version", "producers", f"prod__{part or 'none'}")

        return parts


class MLModelMixinBase(AnalysisTask):

    ml_model = luigi.Parameter(
        description="the name of the ML model to be applied",
    )

    exclude_params_repr_empty = {"ml_model"}

    @classmethod
    def req_params(cls, inst: law.Task, **kwargs) -> dict:
        # prefer --ml-model set on task-level via cli
        kwargs["_prefer_cli"] = law.util.make_set(kwargs.get("_prefer_cli", [])) | {"ml_model"}

        return super().req_params(inst, **kwargs)

    @classmethod
    def get_ml_model_inst(
        cls,
        ml_model: str,
        analysis_inst: od.Analysis,
        requested_configs: list[str] | None = None,
        **kwargs,
    ) -> MLModel:
        ml_model_inst = MLModel.get_cls(ml_model)(analysis_inst, **kwargs)

        if requested_configs:
            configs = ml_model_inst.training_configs(list(requested_configs))
            if configs:
                ml_model_inst._setup(configs)

        return ml_model_inst

    def events_used_in_training(
        self,
        config_inst: od.Config,
        dataset_inst: od.Dataset,
        shift_inst: od.Shift,
    ) -> bool:
        # evaluate whether the events for the combination of dataset_inst and shift_inst
        # shall be used in the training
        return (
            dataset_inst in self.ml_model_inst.datasets(config_inst) and
            not shift_inst.has_tag("disjoint_from_nominal")
        )


class MLModelTrainingMixin(MLModelMixinBase):

    configs = law.CSVParameter(
        default=(),
        description="comma-separated names of analysis config to use; should only contain a single "
        "name in case the ml model is bound to a single config; when empty, the ml model is "
        "expected to fully define the configs it uses; empty default",
        brace_expand=True,
        parse_empty=True,
    )
    calibrators = law.MultiCSVParameter(
        default=(),
        description="multiple comma-separated sequences of names of calibrators to apply, "
        "separated by ':'; each sequence corresponds to a config in --configs; when empty, the "
        "'default_calibrator' setting of each config is used if set, or the model is expected to "
        "fully define the calibrators it requires upstream; empty default",
        brace_expand=True,
        parse_empty=True,
    )
    selectors = law.CSVParameter(
        default=(),
        description="comma-separated names of selectors to apply; each selector corresponds to a "
        "config in --configs; when empty, the 'default_selector' setting of each config is used if "
        "set, or the ml model is expected to fully define the selector it uses requires upstream; "
        "empty default",
        brace_expand=True,
        parse_empty=True,
    )
    producers = law.MultiCSVParameter(
        default=(),
        description="multiple comma-separated sequences of names of producers to apply, "
        "separated by ':'; each sequence corresponds to a config in --configs; when empty, the "
        "'default_producer' setting of each config is used if set, or ml model is expected to "
        "fully define the producers it requires upstream; empty default",
        brace_expand=True,
        parse_empty=True,
    )

    @classmethod
    def resolve_calibrators(
        cls,
        ml_model_inst: MLModel,
        params: dict[str, Any],
    ) -> tuple[tuple[str]]:
        calibrators = params.get("calibrators") or ((),)

        # broadcast to configs
        n_configs = len(ml_model_inst.config_insts)
        if len(calibrators) == 1 and n_configs != 1:
            calibrators = tuple(calibrators * n_configs)

        # apply calibrators_groups and default_calibrator from the config
        calibrators = tuple(
            ConfigTask.resolve_config_default_and_groups(
                params,
                calibrators[i],
                container=config_inst,
                default_str="default_calibrator",
                groups_str="calibrator_groups",
            )
            for i, config_inst in enumerate(ml_model_inst.config_insts)
        )

        # validate number of sequences
        if len(calibrators) != n_configs:
            raise Exception(
                f"MLModel '{ml_model_inst.cls_name}' uses {n_configs} configs but received "
                f"{len(calibrators)} calibrator sequences",
            )

        # final check by model
        calibrators = tuple(
            tuple(ml_model_inst.training_calibrators(config_inst, list(_calibrators)))
            for config_inst, _calibrators in zip(ml_model_inst.config_insts, calibrators)
        )

        # instantiate them once
        for config_inst, _calibrators in zip(ml_model_inst.config_insts, calibrators):
            init_kwargs = law.util.merge_dicts(params, {"config_inst": config_inst})
            for calibrator in _calibrators:
                CalibratorMixin.get_calibrator_inst(calibrator, kwargs=init_kwargs)

        return calibrators

    @classmethod
    def resolve_selectors(
        cls,
        ml_model_inst: MLModel,
        params: dict[str, Any],
    ) -> tuple[str]:
        selectors = params.get("selectors") or (None,)

        # broadcast to configs
        n_configs = len(ml_model_inst.config_insts)
        if len(selectors) == 1 and n_configs != 1:
            selectors = tuple(selectors * n_configs)

        # use config defaults
        selectors = tuple(
            ConfigTask.resolve_config_default(
                params,
                selectors[i],
                container=config_inst,
                default_str="default_selector",
                multiple=False,
            )
            for i, config_inst in enumerate(ml_model_inst.config_insts)
        )

        # validate sequence length
        if len(selectors) != n_configs:
            raise Exception(
                f"MLModel '{ml_model_inst.cls_name}' uses {n_configs} configs but received "
                f"{len(selectors)} selectors",
            )

        # final check by model
        selectors = tuple(
            ml_model_inst.training_selector(config_inst, selector)
            for config_inst, selector in zip(ml_model_inst.config_insts, selectors)
        )

        # instantiate them once
        for config_inst, selector in zip(ml_model_inst.config_insts, selectors):
            init_kwargs = law.util.merge_dicts(params, {"config_inst": config_inst})
            SelectorMixin.get_selector_inst(selector, kwargs=init_kwargs)

        return selectors

    @classmethod
    def resolve_producers(
        cls,
        ml_model_inst: MLModel,
        params: dict[str, Any],
    ) -> tuple[tuple[str]]:
        producers = params.get("producers") or ((),)

        # broadcast to configs
        n_configs = len(ml_model_inst.config_insts)
        if len(producers) == 1 and n_configs != 1:
            producers = tuple(producers * n_configs)

        # apply producers_groups and default_producer from the config
        producers = tuple(
            ConfigTask.resolve_config_default_and_groups(
                params,
                producers[i],
                container=config_inst,
                default_str="default_producer",
                groups_str="producer_groups",
            )
            for i, config_inst in enumerate(ml_model_inst.config_insts)
        )

        # validate number of sequences
        if len(producers) != n_configs:
            raise Exception(
                f"MLModel '{ml_model_inst.cls_name}' uses {n_configs} configs but received "
                f"{len(producers)} producer sequences",
            )

        # final check by model
        producers = tuple(
            tuple(ml_model_inst.training_producers(config_inst, list(_producers)))
            for config_inst, _producers in zip(ml_model_inst.config_insts, producers)
        )

        # instantiate them once
        for config_inst, _producers in zip(ml_model_inst.config_insts, producers):
            init_kwargs = law.util.merge_dicts(params, {"config_inst": config_inst})
            for producer in _producers:
                ProducerMixin.get_producer_inst(producer, kwargs=init_kwargs)

        return producers

    @classmethod
    def resolve_param_values(cls, params: dict[str, Any]) -> dict[str, Any]:
        params = super().resolve_param_values(params)

        if "analysis_inst" in params and "ml_model" in params:
            analysis_inst = params["analysis_inst"]

            # NOTE: we could try to implement resolving the default ml_model here
            ml_model_inst = cls.get_ml_model_inst(params["ml_model"], analysis_inst)
            params["ml_model_inst"] = ml_model_inst

            # resolve configs
            _configs = params.get("configs", ())
            params["configs"] = tuple(ml_model_inst.training_configs(list(_configs)))
            if not params["configs"]:
                raise Exception(
                    f"MLModel '{ml_model_inst.cls_name}' received configs '{_configs}' to define "
                    "training configs, but did not define any",
                )
            ml_model_inst._set_configs(params["configs"])

            # resolve calibrators
            params["calibrators"] = cls.resolve_calibrators(ml_model_inst, params)

            # resolve selectors
            params["selectors"] = cls.resolve_selectors(ml_model_inst, params)

            # resolve producers
            params["producers"] = cls.resolve_producers(ml_model_inst, params)

            # call the model's setup hook
            ml_model_inst._setup()

        return params

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # get the ML model instance
        self.ml_model_inst = self.get_ml_model_inst(
            self.ml_model,
            self.analysis_inst,
            configs=list(self.configs),
        )

    def store_parts(self) -> law.util.InsertableDict:
        parts = super().store_parts()
        # since MLTraining is no CalibratorsMixin, SelectorMixin, ProducerMixin, ConfigTask,
        # all these parts are missing in the `store_parts`

        configs_repr = "__".join(self.configs[:5])

        if len(self.configs) > 5:
            configs_repr += f"_{law.util.create_hash(self.configs[5:])}"

        parts.insert_after("task_family", "configs", configs_repr)

        for label, fct_names in [
            ("calib", self.calibrators),
            ("sel", tuple((sel,) for sel in self.selectors)),
            ("prod", self.producers),
        ]:
            if not fct_names or not any(fct_names):
                fct_names = ["none"]
            elif len(set(fct_names)) == 1:
                # when functions are the same per config, only use them once
                fct_names = fct_names[0]
                n_fct_per_config = str(len(fct_names))
            else:
                # when functions differ between configs, flatten
                n_fct_per_config = "".join(str(len(x)) for x in fct_names)
                fct_names = tuple(fct_name for fct_names_cfg in fct_names for fct_name in fct_names_cfg)

            part = "__".join(fct_names[:2])

            if len(fct_names) > 2:
                part += f"_{n_fct_per_config}_{law.util.create_hash(fct_names[2:])}"

            parts.insert_before("version", label, f"{label}__{part}")

        if self.ml_model_inst:
            parts.insert_before("version", "ml_model", f"ml__{self.ml_model_inst.cls_name}")

        return parts


class MLModelMixin(ConfigTask, MLModelMixinBase):

    ml_model = luigi.Parameter(
        default=RESOLVE_DEFAULT,
        description="the name of the ML model to be applied; default: value of the "
        "'default_ml_model' config",
    )

    allow_empty_ml_model = True

    exclude_params_repr_empty = {"ml_model"}

    @classmethod
    def resolve_param_values(cls, params: dict[str, Any]) -> dict[str, Any]:
        params = super().resolve_param_values(params)

        # add the default ml model when empty
        if "analysis_inst" in params and "config_inst" in params:
            analysis_inst = params["analysis_inst"]
            config_inst = params["config_inst"]

            params["ml_model"] = cls.resolve_config_default(
                params,
                params.get("ml_model"),
                container=config_inst,
                default_str="default_ml_model",
                multiple=False,
            )

            # initialize it once to trigger its set_config hook which might, in turn,
            # add objects to the config itself
            if params.get("ml_model") not in (None, law.NO_STR):
                params["ml_model_inst"] = cls.get_ml_model_inst(
                    params["ml_model"],
                    analysis_inst,
                    requested_configs=[config_inst],
                )
            elif not cls.allow_empty_ml_model:
                raise Exception(f"no ml_model configured for {cls.task_family}")

        return params

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # get the ML model instance
        self.ml_model_inst = None
        if self.ml_model != law.NO_STR:
            self.ml_model_inst = self.get_ml_model_inst(
                self.ml_model,
                self.analysis_inst,
                requested_configs=[self.config_inst],
            )

    def store_parts(self) -> law.util.InsertableDict:
        parts = super().store_parts()
        if self.ml_model_inst:
            parts.insert_before("version", "ml_model", f"ml__{self.ml_model_inst.cls_name}")
        return parts


class MLModelDataMixin(MLModelMixin):

    allow_empty_ml_model = False

    def store_parts(self) -> law.util.InsertableDict:
        parts = super().store_parts()

        # replace the ml_model entry
        store_name = self.ml_model_inst.store_name or self.ml_model_inst.cls_name
        parts.insert_before("ml_model", "ml_data", f"ml__{store_name}")
        parts.pop("ml_model")

        return parts


class MLModelsMixin(ConfigTask):

    ml_models = law.CSVParameter(
        default=(RESOLVE_DEFAULT,),
        description="comma-separated names of ML models to be applied; empty default",
        brace_expand=True,
        parse_empty=True,
    )

    allow_empty_ml_models = True

    exclude_params_repr_empty = {"ml_models"}

    @classmethod
    def resolve_param_values(cls, params: dict[str, Any]) -> dict[str, Any]:
        params = super().resolve_param_values(params)

        analysis_inst = params.get("analysis_inst")
        config_inst = params.get("config_inst")
        if analysis_inst and config_inst:
            # apply ml_model_groups and default_ml_model from the config
            params["ml_models"] = cls.resolve_config_default_and_groups(
                params,
                params.get("ml_models"),
                container=config_inst,
                default_str="default_ml_model",
                groups_str="ml_model_groups",
            )

            # special case: initialize them once to trigger their set_config hook
            if params.get("ml_models"):
                params["ml_model_insts"] = [
                    MLModelMixinBase.get_ml_model_inst(
                        ml_model,
                        analysis_inst,
                        requested_configs=[config_inst],
                    )
                    for ml_model in params["ml_models"]
                ]
            elif not cls.allow_empty_ml_models:
                raise Exception(f"no ml_models configured for {cls.task_family}")

        return params

    @classmethod
    def req_params(cls, inst: law.Task, **kwargs) -> dict:
        # prefer --ml-models set on task-level via cli
        kwargs["_prefer_cli"] = law.util.make_set(kwargs.get("_prefer_cli", [])) | {"ml_models"}

        return super().req_params(inst, **kwargs)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # get the ML model instances
        self.ml_model_insts = [
            MLModelMixinBase.get_ml_model_inst(
                ml_model,
                self.analysis_inst,
                requested_configs=[self.config_inst],
            )
            for ml_model in self.ml_models
        ]

    def store_parts(self) -> law.util.InsertableDict:
        parts = super().store_parts()

        if self.ml_model_insts:
            part = "__".join(self.ml_models)
            parts.insert_before("version", "ml_models", f"ml__{part}")

        return parts


class InferenceModelMixin(ConfigTask):

    inference_model = luigi.Parameter(
        default=RESOLVE_DEFAULT,
        description="the name of the inference model to be used; default: value of the "
        "'default_inference_model' config",
    )

    @classmethod
    def resolve_param_values(cls, params: dict[str, Any]) -> dict[str, Any]:
        params = super().resolve_param_values(params)

        # add the default inference model when empty
        config_inst = params.get("config_inst")
        if config_inst:
            params["inference_model"] = cls.resolve_config_default(
                params,
                params.get("inference_model"),
                container=config_inst,
                default_str="default_inference_model",
                multiple=False,
            )

        return params

    @classmethod
    def get_inference_model_inst(cls, inference_model: str, config_inst: od.Config) -> InferenceModel:
        return InferenceModel.get_cls(inference_model)(config_inst)

    @classmethod
    def req_params(cls, inst: law.Task, **kwargs) -> dict:
        # prefer --inference-model set on task-level via cli
        kwargs["_prefer_cli"] = law.util.make_set(kwargs.get("_prefer_cli", [])) | {"inference_model"}

        return super().req_params(inst, **kwargs)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # get the inference model instance
        self.inference_model_inst = self.get_inference_model_inst(self.inference_model, self.config_inst)

    def store_parts(self) -> law.util.InsertableDict:
        parts = super().store_parts()
        if self.inference_model != law.NO_STR:
            parts.insert_before("version", "inf_model", f"inf__{self.inference_model}")
        return parts


class CategoriesMixin(ConfigTask):

    categories = law.CSVParameter(
        default=(),
        description="comma-separated category names or patterns to select; can also be the key of "
        "a mapping defined in 'category_groups' auxiliary data of the config; when empty, uses the "
        "auxiliary data enty 'default_categories' when set; empty default",
        brace_expand=True,
        parse_empty=True,
    )

    default_categories = None
    allow_empty_categories = False

    @classmethod
    def resolve_param_values(cls, params):
        params = super().resolve_param_values(params)

        if "config_inst" not in params:
            return params
        config_inst = params["config_inst"]

        # resolve categories
        if "categories" in params:
            # when empty, use the config default
            if not params["categories"] and config_inst.x("default_categories", ()):
                params["categories"] = tuple(config_inst.x.default_categories)

            # when still empty and default categories are defined, use them instead
            if not params["categories"] and cls.default_categories:
                params["categories"] = tuple(cls.default_categories)

            # resolve them
            categories = cls.find_config_objects(
                params["categories"],
                config_inst,
                od.Category,
                config_inst.x("category_groups", {}),
                deep=True,
            )

            # complain when no categories were found
            if not categories and not cls.allow_empty_categories:
                raise ValueError(f"no categories found matching {params['categories']}")

            params["categories"] = tuple(categories)

        return params

    @property
    def categories_repr(self):
        if len(self.categories) == 1:
            return self.categories[0]

        return f"{len(self.categories)}_{law.util.create_hash(sorted(self.categories))}"


class VariablesMixin(ConfigTask):

    variables = law.CSVParameter(
        default=(),
        description="comma-separated variable names or patterns to select; can also be the key of "
        "a mapping defined in the 'variable_group' auxiliary data of the config; when empty, uses "
        "all variables of the config; empty default",
        brace_expand=True,
        parse_empty=True,
    )

    default_variables = None
    allow_empty_variables = False

    @classmethod
    def resolve_param_values(cls, params):
        params = super().resolve_param_values(params)

        if "config_inst" not in params:
            return params
        config_inst = params["config_inst"]

        # resolve variables
        if "variables" in params:
            # when empty, use the config default
            if not params["variables"] and config_inst.x("default_variables", ()):
                params["variables"] = tuple(config_inst.x.default_variables)

            # when still empty and default variables are defined, use them instead
            if not params["variables"] and cls.default_variables:
                params["variables"] = tuple(cls.default_variables)

            # resolve them
            if params["variables"]:
                # first, split into single- and multi-dimensional variables
                single_vars = []
                multi_var_parts = []
                for variable in params["variables"]:
                    parts = cls.split_multi_variable(variable)
                    if len(parts) == 1:
                        single_vars.append(variable)
                    else:
                        multi_var_parts.append(parts)

                # resolve single variables
                variables = cls.find_config_objects(
                    single_vars,
                    config_inst,
                    od.Variable,
                    config_inst.x("variable_groups", {}),
                )

                # for each multi-variable, resolve each part separately and create the full
                # combinatorics of all possibly pattern-resolved parts
                for parts in multi_var_parts:
                    resolved_parts = [
                        cls.find_config_objects(
                            part,
                            config_inst,
                            od.Variable,
                            config_inst.x("variable_groups", {}),
                        )
                        for part in parts
                    ]
                    variables.extend([
                        cls.join_multi_variable(_parts)
                        for _parts in itertools.product(*resolved_parts)
                    ])
            else:
                # fallback to using all known variables
                variables = config_inst.variables.names()

            # complain when no variables were found
            if not variables and not cls.allow_empty_variables:
                raise ValueError(f"no variables found matching {params['variables']}")

            params["variables"] = tuple(variables)

        return params

    @classmethod
    def split_multi_variable(cls, variable: str) -> tuple[str]:
        """
        Splits a multi-dimensional *variable* given in the format ``"var_a[-var_b[-...]]"`` into
        separate variable names using a delimiter (``"-"``) and returns a tuple.
        """
        return tuple(variable.split("-"))

    @classmethod
    def join_multi_variable(cls, variables: Sequence[str]) -> str:
        """
        Joins the name of multiple *variables* using a delimiter (``"-"``) into a single string
        that represents a multi-dimensional variable and returns it.
        """
        return "-".join(map(str, variables))

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # if enabled, split names of multi-dimensional parameters into tuples
        self.variable_tuples = {
            var_name: self.split_multi_variable(var_name)
            for var_name in self.variables
        }

    @property
    def variables_repr(self):
        if len(self.variables) == 1:
            return self.variables[0]

        return f"{len(self.variables)}_{law.util.create_hash(sorted(self.variables))}"


class DatasetsProcessesMixin(ConfigTask):

    datasets = law.CSVParameter(
        default=(),
        description="comma-separated dataset names or patters to select; can also be the key of a "
        "mapping defined in the 'dataset_groups' auxiliary data of the config; when empty, uses "
        "all datasets registered in the config that contain any of the selected --processes; empty "
        "default",
        brace_expand=True,
        parse_empty=True,
    )
    processes = law.CSVParameter(
        default=(),
        description="comma-separated process names or patterns for filtering processes; can also "
        "be the key of a mapping defined in the 'process_groups' auxiliary data of the config; "
        "uses all processes of the config when empty; empty default",
        brace_expand=True,
        parse_empty=True,
    )

    allow_empty_datasets = False
    allow_empty_processes = False

    @classmethod
    def resolve_param_values(cls, params):
        params = super().resolve_param_values(params)

        if "config_inst" not in params:
            return params
        config_inst = params["config_inst"]

        # resolve processes
        if "processes" in params:
            if params["processes"]:
                processes = cls.find_config_objects(
                    params["processes"],
                    config_inst,
                    od.Process,
                    config_inst.x("process_groups", {}),
                    deep=True,
                )
            else:
                processes = config_inst.processes.names()

            # complain when no processes were found
            if not processes and not cls.allow_empty_processes:
                raise ValueError(f"no processes found matching {params['processes']}")

            params["processes"] = tuple(processes)
            params["process_insts"] = [config_inst.get_process(p) for p in params["processes"]]

        # resolve datasets
        if "datasets" in params:
            if params["datasets"]:
                datasets = cls.find_config_objects(
                    params["datasets"],
                    config_inst,
                    od.Dataset,
                    config_inst.x("dataset_groups", {}),
                )
            elif "processes" in params:
                # pick all datasets that contain any of the requested (sub) processes
                sub_process_insts = sum((
                    [proc for proc, _, _ in process_inst.walk_processes(include_self=True)]
                    for process_inst in map(config_inst.get_process, params["processes"])
                ), [])
                datasets = [
                    dataset_inst.name
                    for dataset_inst in config_inst.datasets
                    if any(map(dataset_inst.has_process, sub_process_insts))
                ]

            # complain when no datasets were found
            if not datasets and not cls.allow_empty_datasets:
                raise ValueError(f"no datasets found matching {params['datasets']}")

            params["datasets"] = tuple(datasets)
            params["dataset_insts"] = [config_inst.get_dataset(d) for d in params["datasets"]]

        return params

    @classmethod
    def get_known_shifts(cls, config_inst, params):
        shifts, upstream_shifts = super().get_known_shifts(config_inst, params)

        # add shifts of all datasets to upstream ones
        for dataset_inst in params.get("dataset_insts") or []:
            if dataset_inst.is_mc:
                upstream_shifts |= set(dataset_inst.info.keys())

        return shifts, upstream_shifts

    @property
    def datasets_repr(self):
        if len(self.datasets) == 1:
            return self.datasets[0]

        return f"{len(self.datasets)}_{law.util.create_hash(sorted(self.datasets))}"

    @property
    def processes_repr(self):
        if len(self.processes) == 1:
            return self.processes[0]

        return f"{len(self.processes)}_{law.util.create_hash(self.processes)}"


class ShiftSourcesMixin(ConfigTask):

    shift_sources = law.CSVParameter(
        default=(),
        description="comma-separated shift source names (without direction) or patterns to select; "
        "can also be the key of a mapping defined in the 'shift_group' auxiliary data of the "
        "config; default: ()",
        brace_expand=True,
        parse_empty=True,
    )

    allow_empty_shift_sources = False

    @classmethod
    def resolve_param_values(cls, params):
        params = super().resolve_param_values(params)

        if "config_inst" not in params:
            return params
        config_inst = params["config_inst"]

        # resolve shift sources
        if "shift_sources" in params:
            # convert to full shift first to do the object finding
            shifts = cls.find_config_objects(
                cls.expand_shift_sources(params["shift_sources"]),
                config_inst,
                od.Shift,
                config_inst.x("shift_groups", {}),
            )

            # complain when no shifts were found
            if not shifts and not cls.allow_empty_shift_sources:
                raise ValueError(f"no shifts found matching {params['shift_sources']}")

            # convert back to sources
            params["shift_sources"] = tuple(cls.reduce_shifts(shifts))

        return params

    @classmethod
    def expand_shift_sources(cls, sources: Sequence[str] | set[str]) -> list[str]:
        return sum(([f"{s}_up", f"{s}_down"] for s in sources), [])

    @classmethod
    def reduce_shifts(cls, shifts: Sequence[str] | set[str]) -> list[str]:
        return list(set(od.Shift.split_name(shift)[0] for shift in shifts))

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.shifts = self.expand_shift_sources(self.shift_sources)

    @property
    def shift_sources_repr(self):
        if len(self.shift_sources) == 1:
            return self.shift_sources[0]

        return f"{len(self.shift_sources)}_{law.util.create_hash(sorted(self.shift_sources))}"


class EventWeightMixin(ConfigTask):

    @classmethod
    def get_known_shifts(cls, config_inst: od.Config, params: dict) -> tuple[set[str], set[str]]:
        shifts, upstream_shifts = super().get_known_shifts(config_inst, params)

        # add shifts introduced by event weights
        if config_inst.has_aux("event_weights"):
            for shift_insts in config_inst.x.event_weights.values():
                shifts |= {shift_inst.name for shift_inst in shift_insts}

        # optionally also for weights defined by a dataset
        if "dataset" in params:
            requested_dataset = params.get("dataset")
            if requested_dataset not in (None, law.NO_STR):
                dataset_inst = config_inst.get_dataset(requested_dataset)
                if dataset_inst.has_aux("event_weights"):
                    for shift_insts in dataset_inst.x.event_weights.values():
                        shifts |= {shift_inst.name for shift_inst in shift_insts}

        return shifts, upstream_shifts


class ChunkedIOMixin(AnalysisTask):

    check_finite_output = luigi.BoolParameter(
        default=False,
        significant=False,
        description="when True, checks whether output arrays only contain finite values before "
        "writing to them to file",
    )
    check_overlapping_inputs = luigi.BoolParameter(
        default=False,
        significant=False,
        description="when True, checks whether columns if input arrays overlap in at least one field",
    )

    exclude_params_req = {"check_finite_output", "check_overlapping_inputs"}

    @classmethod
    def raise_if_not_finite(cls, ak_array: ak.Array) -> None:
        """
        Checks whether all values in array *ak_array* are finite.

        The check is performed using the :external+numpy:py:func:`numpy.isfinite` function.

        :param ak_array: Array with events to check.
        :raises ValueError: If any value in *ak_array* is not finite.
        """
        import numpy as np
        from columnflow.columnar_util import get_ak_routes

        for route in get_ak_routes(ak_array):
            if ak.any(~np.isfinite(ak.flatten(route.apply(ak_array), axis=None))):
                raise ValueError(
                    f"found one or more non-finite values in column '{route.column}' "
                    f"of array {ak_array}",
                )

    @classmethod
    def raise_if_overlapping(cls, ak_arrays: Sequence[ak.Array]) -> None:
        """
        Checks whether fields of *ak_arrays* overlap.

        :param ak_arrays: Arrays with fields to check.
        :raises ValueError: If at least one overlap is found.
        """
        from columnflow.columnar_util import get_ak_routes

        # when less than two arrays are given, there cannot be any overlap
        if len(ak_arrays) < 2:
            return

        # determine overlapping routes
        counts = Counter(sum(map(get_ak_routes, ak_arrays), []))
        overlapping_routes = [r for r, c in counts.items() if c > 1]

        # raise
        if overlapping_routes:
            raise ValueError(
                f"found {len(overlapping_routes)} overlapping columns across {len(ak_arrays)} "
                f"columns: {','.join(overlapping_routes)}",
            )

    def iter_chunked_io(self, *args, **kwargs):
        from columnflow.columnar_util import ChunkedIOHandler

        # get the chunked io handler from first arg or create a new one with all args
        if len(args) == 1 and isinstance(args[0], ChunkedIOHandler):
            handler = args[0]
        else:
            handler = ChunkedIOHandler(*args, **kwargs)

        # iterate in the handler context
        with handler:
            self.chunked_io = handler
            msg = f"iterate through {handler.n_entries} events in {handler.n_chunks} chunks ..."
            try:
                # measure runtimes excluding IO
                loop_durations = []
                for obj in self.iter_progress(handler, max(handler.n_chunks, 1), msg=msg):
                    t1 = time.perf_counter()

                    # yield the object provided by the handler
                    yield obj

                    # save the runtime
                    loop_durations.append(time.perf_counter() - t1)

                # print runtimes
                self.publish_message(
                    "event processing in loop body took "
                    f"{law.util.human_duration(seconds=sum(loop_durations))}",
                )

            finally:
                self.chunked_io = None

        # eager, overly cautious gc
        del handler
        gc.collect()
