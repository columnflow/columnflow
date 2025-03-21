# coding: utf-8

"""
Lightweight mixins task classes.
"""

from __future__ import annotations

import time
import itertools
from collections import Counter

import luigi
import law
import order as od

from columnflow.types import Any, Iterable, Sequence
from columnflow.tasks.framework.base import ConfigTask, DatasetTask, TaskShifts, RESOLVE_DEFAULT
from columnflow.tasks.framework.parameters import SettingsParameter, DerivableInstParameter, DerivableInstsParameter
from columnflow.calibration import Calibrator
from columnflow.selection import Selector
from columnflow.production import Producer
from columnflow.weight import WeightProducer
from columnflow.ml import MLModel
from columnflow.inference import InferenceModel
from columnflow.columnar_util import Route, ColumnCollection, ChunkedIOHandler
from columnflow.util import maybe_import, DotDict

ak = maybe_import("awkward")

logger_dev = law.logger.get_logger(f"{__name__}-dev")


class ArrayFunctionClassMixin(ConfigTask):

    def array_function_cls_repr(self, array_function) -> str:
        """
        Central definition of how to obtain representation of array function from the name

        :param array_function: name of the array function (NOTE: change to class?)
        :return: sring representation of the array function
        """
        return str(array_function)


class ArrayFunctionInstanceMixin(DatasetTask):

    def _array_function_post_init(self, **kwargs) -> None:
        """
        Post-initialization method for all known task array functions.
        """
        return None

    def array_function_inst_repr(self, array_function_inst) -> None:
        return str(array_function_inst)


class CalibratorClassMixin(ArrayFunctionClassMixin):
    """
    Mixin to include and access single :py:class:`~columnflow.calibration.Calibrator` class.
    """

    calibrator = luigi.Parameter(
        default=RESOLVE_DEFAULT,
        description="the name of the calibrator to be applied; default: value of the "
        "'default_calibrator' analysis aux",
    )

    @classmethod
    def resolve_param_values_pre_init(cls, params: dict[str, Any]) -> dict[str, Any]:
        params = super().resolve_param_values_pre_init(params)

        # resolve the default class if necessary
        if (container := cls._get_config_container(params)):
            params["calibrator"] = cls.resolve_config_default(
                param=params.get("calibrator"),
                task_params=params,
                container=container,
                default_str="default_calibrator",
                multi_strategy="same",
            )

        return params

    @classmethod
    def req_params(cls, inst: law.Task, **kwargs) -> dict[str, Any]:
        # prefer --calibrator set on task-level via cli
        kwargs["_prefer_cli"] = law.util.make_set(kwargs.get("_prefer_cli", [])) | {"calibrator"}
        return super().req_params(inst, **kwargs)

    @property
    def calibrator_repr(self) -> str:
        """
        Return a string representation of the calibrator class.
        """
        return self.array_function_cls_repr(self.calibrator, Calibrator)

    def store_parts(self) -> law.util.InsertableDict:
        """
        :return: Dictionary with parts that will be translated into an output directory path.
        """
        parts = super().store_parts()
        parts.insert_after(self.config_store_anchor, "calibrator", f"calib__{self.calibrator_repr}")
        return parts

    @classmethod
    def get_config_lookup_keys(
        cls,
        inst_or_params: CalibratorClassMixin | dict[str, Any],
    ) -> law.util.InsertiableDict:
        keys = super().get_config_lookup_keys(inst_or_params)

        # add the calibrator name
        calibrator = (
            inst_or_params.get("calibrator")
            if isinstance(inst_or_params, dict)
            else getattr(inst_or_params, "calibrator", None)
        )
        if calibrator not in (law.NO_STR, None, ""):
            keys["calibrator"] = f"calib_{calibrator}"

        return keys


class CalibratorMixin(ArrayFunctionInstanceMixin, CalibratorClassMixin):
    """
    Mixin to include and access a single :py:class:`~columnflow.calibration.Calibrator` instance.
    """

    calibrator_inst = DerivableInstParameter(
        default=None,
        visibility=luigi.parameter.ParameterVisibility.PRIVATE,
    )

    exclude_params_index = {"calibrator_inst"}
    exclude_params_repr = {"calibrator_inst"}
    exclude_params_sandbox = {"calibrator_inst"}
    exclude_params_remote_workflow = {"calibrator_inst"}

    # decides whether the task itself invokes the calibrator
    invokes_calibrator = False

    @classmethod
    def get_calibrator_dict(cls, params: dict[str, Any]) -> dict[str, Any]:
        return cls.get_array_function_dict(params)

    @classmethod
    def build_calibrator_inst(
        cls,
        calibrator: str,
        params: dict[str, Any] | None = None,
    ) -> Calibrator:
        """
        Instantiate and return the :py:class:`~columnflow.calibration.Calibrator` instance.

        :param calibrator: Name of the calibrator class to instantiate.
        :param params: Arguments forwarded to the calibrator constructor.
        :raises RuntimeError: If the calibrator class is not
            :py:attr:`~columnflow.calibration.Calibrator.exposed`.
        :return: The calibrator instance.
        """
        calibrator_cls = Calibrator.get_cls(calibrator)
        if not calibrator_cls.exposed:
            raise RuntimeError(f"cannot use unexposed calibrator '{calibrator}' in {cls.__name__}")

        inst_dict = cls.get_calibrator_dict(params) if params else None
        return calibrator_cls(inst_dict=inst_dict)

    @classmethod
    def resolve_instances(cls, params: dict[str, Any], shifts: TaskShifts) -> dict[str, Any]:
        # add the calibrator instance
        if not params.get("calibrator_inst"):
            params["calibrator_inst"] = cls.build_calibrator_inst(params["calibrator"], params)

        params = super().resolve_instances(params, shifts)

        return params

    @classmethod
    def get_known_shifts(
        cls,
        params: dict[str, Any],
        shifts: TaskShifts,
    ) -> None:
        """
        Updates the set of known *shifts* implemented by *this* and upstream tasks.

        :param config_inst: Config instance.
        :param params: Dictionary of task parameters.
        :param shifts: TaskShifts object to adjust.
        """
        # get the calibrator, update it and add its shifts
        calibrator_shifts = params["calibrator_inst"].all_shifts
        (shifts.local if cls.invokes_calibrator else shifts.upstream).update(calibrator_shifts)

        super().get_known_shifts(params, shifts)

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

        # overwrite the sandbox when set
        if self.invokes_calibrator and (sandbox := self.calibrator_inst.get_sandbox()):
            self.reset_sandbox(sandbox)

    def _array_function_post_init(self, **kwargs) -> None:
        self.calibrator_inst.run_post_init(task=self, **kwargs)
        super()._array_function_post_init(**kwargs)

    @property
    def calibrator_repr(self) -> str:
        """
        Return a string representation of the calibrator instance.
        """
        return self.array_function_inst_repr(self.calibrator_inst)

    def find_keep_columns(self, collection: ColumnCollection) -> set[Route]:
        """
        Finds the columns to keep based on the *collection*.

        :param collection: The collection of columns.
        :return: Set of columns to keep.
        """
        columns = super().find_keep_columns(collection)

        if collection == ColumnCollection.ALL_FROM_CALIBRATOR:
            columns |= self.calibrator_inst.produced_columns

        return columns


class CalibratorClassesMixin(ArrayFunctionClassMixin):
    """
    Mixin to include and access multiple :py:class:`~columnflow.calibration.Calibrator` classes.
    """

    calibrators = law.CSVParameter(
        default=(RESOLVE_DEFAULT,),
        description="comma-separated names of calibrators to be applied; default: value of the "
        "'default_calibrator' analysis aux",
        brace_expand=True,
        parse_empty=True,
    )

    @classmethod
    def resolve_param_values_pre_init(
        cls,
        params: law.util.InsertableDict[str, Any],
    ) -> law.util.InsertableDict[str, Any]:
        params = super().resolve_param_values_pre_init(params)

        # resolve the default classes if necessary
        if (container := cls._get_config_container(params)):
            params["calibrators"] = cls.resolve_config_default_and_groups(
                param=params.get("calibrators"),
                task_params=params,
                container=container,
                default_str="default_calibrator",
                groups_str="calibrator_groups",
                multi_strategy="same",
            )

        return params

    @classmethod
    def req_params(cls, inst: law.Task, **kwargs) -> dict[str, Any]:
        # prefer --calibrators set on task-level via cli
        kwargs["_prefer_cli"] = law.util.make_set(kwargs.get("_prefer_cli", [])) | {"calibrators"}
        return super().req_params(inst, **kwargs)

    @property
    def calibrators_repr(self) -> str:
        """
        Return a string representation of the calibrators.
        """
        calibs_repr = "none"
        if self.calibrators:
            reprs = list(map(self.array_function_cls_repr, self.calibrators))
            calibs_repr = "__".join(reprs[:5])
            if len(reprs) > 5:
                calibs_repr += f"__{law.util.create_hash(reprs[5:])}"
        return calibs_repr

    def store_parts(self) -> law.util.InsertableDict:
        """
        :return: Dictionary with parts that will be translated into an output directory path.
        """
        parts = super().store_parts()
        parts.insert_after(self.config_store_anchor, "calibrators", f"calib__{self.calibrators_repr}")
        return parts


class CalibratorsMixin(ArrayFunctionInstanceMixin, CalibratorClassesMixin):
    """
    Mixin to include multiple :py:class:`~columnflow.calibration.Calibrator` instances into tasks.
    """

    calibrator_insts = DerivableInstsParameter(
        default=(),
        visibility=luigi.parameter.ParameterVisibility.PRIVATE,
    )

    exclude_params_index = {"calibrator_insts"}
    exclude_params_repr = {"calibrator_insts"}
    exclude_params_sandbox = {"calibrator_insts"}
    exclude_params_remote_workflow = {"calibrator_insts"}

    @classmethod
    def get_calibrator_dict(cls, params: dict[str, Any]) -> dict[str, Any]:
        return cls.get_array_function_dict(params)

    @classmethod
    def build_calibrator_insts(
        cls,
        calibrators: Iterable[str],
        params: dict[str, Any] | None = None,
    ) -> list[Calibrator]:
        """
        Instantiate and return multiple :py:class:`~columnflow.calibration.Calibrator` instances.

        :param calibrators: Name of the calibrator class to instantiate.
        :param params: Arguments forwarded to the calibrator constructors.
        :raises RuntimeError: If any calibrator class is not
            :py:attr:`~columnflow.calibration.Calibrator.exposed`.
        :return: The list of calibrator instances.
        """
        inst_dict = cls.get_calibrator_dict(params) if params else None

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
    def resolve_instances(cls, params: dict[str, Any], shifts: TaskShifts) -> dict[str, Any]:
        # add the calibrator instances
        if not params.get("calibrator_insts"):
            params["calibrator_insts"] = cls.build_calibrator_insts(params["calibrators"], params)

        params = super().resolve_instances(params, shifts)

        return params

    @classmethod
    def get_known_shifts(
        cls,
        params: dict[str, Any],
        shifts: TaskShifts,
    ) -> None:
        """
        Updates the set of known *shifts* implemented by *this* and upstream tasks.

        :param params: Dictionary of task parameters.
        :param shifts: TaskShifts object to adjust.
        """
        # get the calibrators, update them and add their shifts
        for calibrator_inst in params["calibrator_insts"]:
            shifts.upstream |= calibrator_inst.all_shifts

        super().get_known_shifts(params, shifts)

    def _array_function_post_init(self, **kwargs) -> None:
        for calibrator_inst in self.calibrator_insts or []:
            calibrator_inst.run_post_init(task=self, **kwargs)
        super()._array_function_post_init(**kwargs)

    @property
    def calibrators_repr(self) -> str:
        """
        Return a string representation of the calibrators.
        """
        calibs_repr = "none"
        if self.calibrators:
            reprs = list(map(self.array_function_inst_repr, self.calibrator_insts))
            calibs_repr = "__".join(reprs[:5])
            if len(reprs) > 5:
                calibs_repr += f"__{law.util.create_hash(reprs[5:])}"
        return calibs_repr

    def find_keep_columns(self, collection: ColumnCollection) -> set[Route]:
        """
        Finds the columns to keep based on the *collection*.

        :param collection: The collection of columns.
        :return: Set of columns to keep.
        """
        columns = super().find_keep_columns(collection)

        if collection == ColumnCollection.ALL_FROM_CALIBRATORS:
            columns |= set.union(*(
                calibrator_inst.produced_columns
                for calibrator_inst in self.calibrator_insts
            ))

        return columns


class SelectorClassMixin(ArrayFunctionClassMixin):
    """
    Mixin to include and access single :py:class:`~columnflow.selection.Selector` class.
    """

    selector = luigi.Parameter(
        default=RESOLVE_DEFAULT,
        description="the name of the selector to be applied; default: value of the "
        "'default_selector' analysis aux",
    )
    selector_steps = law.CSVParameter(
        default=(),
        description="a subset of steps of the selector to apply; uses all steps when empty; "
        "default: empty",
        brace_expand=True,
        parse_empty=True,
    )

    selector_steps_order_sensitive = False

    exclude_params_repr_empty = {"selector_steps"}

    @classmethod
    def resolve_param_values_pre_init(cls, params: dict[str, Any]) -> dict[str, Any]:
        params = super().resolve_param_values_pre_init(params)

        if (container := cls._get_config_container(params)):
            # resolve the default class if necessary
            params["selector"] = cls.resolve_config_default(
                param=params.get("selector"),
                task_params=params,
                container=container,
                default_str="default_selector",
                multi_strategy="same",
            )

            # apply selector_steps_groups and default_selector_steps from config
            if "selector_steps" in params:
                params["selector_steps"] = cls.resolve_config_default_and_groups(
                    param=params.get("selector_steps"),
                    task_params=params,
                    container=container,
                    default_str="default_selector_steps",
                    groups_str="selector_step_groups",
                    multi_strategy="same",
                )

        # sort selector steps when the order does not matter
        if params.get("selector_steps") and not cls.selector_steps_order_sensitive:
            params["selector_steps"] = tuple(sorted(params["selector_steps"]))

        return params

    @classmethod
    def req_params(cls, inst: law.Task, **kwargs) -> dict[str, Any]:
        # prefer --selector and --selector-steps set on task-level via cli
        kwargs["_prefer_cli"] = law.util.make_set(kwargs.get("_prefer_cli", [])) | {
            "selector",
            "selector_steps",
        }
        return super().req_params(inst, **kwargs)

    @property
    def selector_repr(self) -> str:
        """
        Return a string representation of the selector class.
        """
        sel_repr = self.array_function_cls_repr(self.selector)
        steps = self.selector_steps
        if steps and not self.selector_steps_order_sensitive:
            steps = sorted(steps)
        if steps:
            sel_repr += "__steps_" + "_".join(steps)
        return sel_repr

    def store_parts(self) -> law.util.InsertableDict:
        """
        :return: Dictionary with parts that will be translated into an output directory path.
        """
        parts = super().store_parts()
        parts.insert_after(self.config_store_anchor, "selector", f"sel__{self.selector_repr}")
        return parts

    @classmethod
    def get_config_lookup_keys(
        cls,
        inst_or_params: SelectorClassMixin | dict[str, Any],
    ) -> law.util.InsertiableDict:
        keys = super().get_config_lookup_keys(inst_or_params)

        # add the selector name
        selector = (
            inst_or_params.get("selector")
            if isinstance(inst_or_params, dict)
            else getattr(inst_or_params, "selector", None)
        )
        if selector not in (law.NO_STR, None, ""):
            keys["selector"] = f"sel_{selector}"

        return keys


class SelectorMixin(ArrayFunctionInstanceMixin, SelectorClassMixin):
    """
    Mixin to include and access a single :py:class:`~columnflow.selection.Selector` instance.
    """

    selector_inst = DerivableInstParameter(
        default=None,
        visibility=luigi.parameter.ParameterVisibility.PRIVATE,
    )

    exclude_params_index = {"selector_inst"}
    exclude_params_repr = {"selector_inst"}
    exclude_params_sandbox = {"selector_inst"}
    exclude_params_remote_workflow = {"selector_inst"}

    # decides whether the task itself invokes the selector
    invokes_selector = False

    @classmethod
    def get_selector_dict(cls, params: dict[str, Any]) -> dict[str, Any]:
        return cls.get_array_function_dict(params)

    @classmethod
    def build_selector_inst(cls, selector: str, params: dict[str, Any] | None = None) -> Selector:
        """
        Instantiate and return the :py:class:`~columnflow.selection.Selector` instance.

        :param selector: Name of the selector class to instantiate.
        :param params: Arguments forwarded to the selector constructor.
        :raises RuntimeError: If the selector class is not
            :py:attr:`~columnflow.selection.Selector.exposed`.
        :return: The selector instance.
        """
        selector_cls = Selector.get_cls(selector)
        if not selector_cls.exposed:
            raise RuntimeError(f"cannot use unexposed selector '{selector}' in {cls.__name__}")

        inst_dict = cls.get_selector_dict(params) if params else None
        return selector_cls(inst_dict=inst_dict)

    @classmethod
    def resolve_instances(cls, params: dict[str, Any], shifts: TaskShifts) -> dict[str, Any]:
        # add the selector instance
        if not params.get("selector_inst"):
            params["selector_inst"] = cls.build_selector_inst(params["selector"], params)

        params = super().resolve_instances(params, shifts)

        return params

    @classmethod
    def get_known_shifts(
        cls,
        params: dict[str, Any],
        shifts: TaskShifts,
    ) -> None:
        """
        Updates the set of known *shifts* implemented by *this* and upstream tasks.

        :param params: Dictionary of task parameters.
        :param shifts: TaskShifts object to adjust.
        """
        # get the selector, update it and add its shifts
        selector_shifts = params["selector_inst"].all_shifts
        (shifts.local if cls.invokes_selector else shifts.upstream).update(selector_shifts)

        super().get_known_shifts(params, shifts)

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

        # overwrite the sandbox when set
        if self.invokes_selector and (sandbox := self.selector_inst.get_sandbox()):
            self.reset_sandbox(sandbox)

    def _array_function_post_init(self, **kwargs) -> None:
        self.selector_inst.run_post_init(task=self, **kwargs)
        super()._array_function_post_init(**kwargs)

    @property
    def selector_repr(self) -> str:
        """
        Return a string representation of the selector instance.
        """
        sel_repr = self.array_function_inst_repr(self.selector_inst)
        # add representation of steps only if this class does not invoke the selector itself
        if not self.invokes_selector:
            steps = self.selector_steps
            if steps and not self.selector_steps_order_sensitive:
                steps = sorted(steps)
            if steps:
                sel_repr += "__steps_" + "_".join(steps)

        return sel_repr

    def find_keep_columns(self, collection: ColumnCollection) -> set[Route]:
        """
        Finds the columns to keep based on the *collection*.

        :param collection: The collection of columns.
        :return: Set of columns to keep.
        """
        columns = super().find_keep_columns(collection)

        if collection == ColumnCollection.ALL_FROM_SELECTOR:
            columns |= self.selector_inst.produced_columns

        return columns


class ProducerClassMixin(ArrayFunctionClassMixin):
    """
    Mixin to include and access single :py:class:`~columnflow.production.Producer` class.
    """

    producer = luigi.Parameter(
        default=RESOLVE_DEFAULT,
        description="the name of the producer to be applied; default: value of the "
        "'default_producer' analysis aux",
    )

    @classmethod
    def resolve_param_values_pre_init(cls, params: dict[str, Any]) -> dict[str, Any]:
        params = super().resolve_param_values_pre_init(params)

        # resolve the default class if necessary
        if (container := cls._get_config_container(params)):
            params["producer"] = cls.resolve_config_default(
                param=params.get("producer"),
                task_params=params,
                container=container,
                default_str="default_producer",
                multi_strategy="same",
            )

        return params

    @classmethod
    def req_params(cls, inst: law.Task, **kwargs) -> dict[str, Any]:
        # prefer --producer set on task-level via cli
        kwargs["_prefer_cli"] = law.util.make_set(kwargs.get("_prefer_cli", [])) | {"producer"}
        return super().req_params(inst, **kwargs)

    @property
    def producer_repr(self) -> str:
        """
        Return a string representation of the producer class.
        """
        return self.array_function_cls_repr(self.producer)

    def store_parts(self) -> law.util.InsertableDict:
        """
        :return: Dictionary with parts that will be translated into an output directory path.
        """
        parts = super().store_parts()
        parts.insert_after(self.config_store_anchor, "producer", f"prod__{self.producer_repr}")
        return parts

    @classmethod
    def get_config_lookup_keys(
        cls,
        inst_or_params: ProducerClassMixin | dict[str, Any],
    ) -> law.util.InsertiableDict:
        keys = super().get_config_lookup_keys(inst_or_params)

        # add the producer name
        producer = (
            inst_or_params.get("producer")
            if isinstance(inst_or_params, dict)
            else getattr(inst_or_params, "producer", None)
        )
        if producer not in (law.NO_STR, None, ""):
            keys["producer"] = f"prod_{producer}"

        return keys


class ProducerMixin(ArrayFunctionInstanceMixin, ProducerClassMixin):
    """
    Mixin to include and access a single :py:class:`~columnflow.production.Producer` instance.
    """

    producer_inst = DerivableInstParameter(
        default=None,
        visibility=luigi.parameter.ParameterVisibility.PRIVATE,
    )

    exclude_params_index = {"producer_inst"}
    exclude_params_repr = {"producer_inst"}
    exclude_params_sandbox = {"producer_inst"}
    exclude_params_remote_workflow = {"producer_inst"}

    # decides whether the task itself invokes the producer
    invokes_producer = False

    @classmethod
    def get_producer_dict(cls, params: dict[str, Any]) -> dict[str, Any]:
        return cls.get_array_function_dict(params)

    @classmethod
    def build_producer_inst(
        cls,
        producer: str,
        params: dict[str, Any] | None = None,
    ) -> Producer:
        """
        Instantiate and return the :py:class:`~columnflow.production.Producer` instance.

        :param producer: Name of the producer class to instantiate.
        :param params: Arguments forwarded to the producer constructor.
        :raises RuntimeError: If the producer class is not
            :py:attr:`~columnflow.production.Producer.exposed`.
        :return: The producer instance.
        """
        producer_cls = Producer.get_cls(producer)
        if not producer_cls.exposed:
            raise RuntimeError(f"cannot use unexposed producer '{producer}' in {cls.__name__}")

        inst_dict = cls.get_producer_dict(params) if params else None
        return producer_cls(inst_dict=inst_dict)

    @classmethod
    def resolve_instances(cls, params: dict[str, Any], shifts: TaskShifts) -> dict[str, Any]:
        # add the producer instance
        if not params.get("producer_inst"):
            params["producer_inst"] = cls.build_producer_inst(params["producer"], params)

        params = super().resolve_instances(params, shifts)

        return params

    @classmethod
    def get_known_shifts(
        cls,
        params: dict[str, Any],
        shifts: TaskShifts,
    ) -> None:
        """
        Updates the set of known *shifts* implemented by *this* and upstream tasks.

        :param params: Dictionary of task parameters.
        :param shifts: TaskShifts object to adjust.
        """
        # get the producer, update it and add its shifts
        producer_shifts = params["producer_inst"].all_shifts
        (shifts.local if cls.invokes_producer else shifts.upstream).update(producer_shifts)

        super().get_known_shifts(params, shifts)

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

        # overwrite the sandbox when set
        if self.invokes_producer and (sandbox := self.producer_inst.get_sandbox()):
            self.reset_sandbox(sandbox)

    def _array_function_post_init(self, **kwargs) -> None:
        self.producer_inst.run_post_init(task=self, **kwargs)
        super()._array_function_post_init(**kwargs)

    @property
    def producer_repr(self) -> str:
        """
        Return a string representation of the producer instance.
        """
        return self.array_function_inst_repr(self.producer_inst)

    def find_keep_columns(self, collection: ColumnCollection) -> set[Route]:
        """
        Finds the columns to keep based on the *collection*.

        :param collection: The collection of columns.
        :return: Set of columns to keep.
        """
        columns = super().find_keep_columns(collection)

        if collection == ColumnCollection.ALL_FROM_CALIBRATOR:
            columns |= self.producer_inst.produced_columns

        return columns


class ProducerClassesMixin(ArrayFunctionClassMixin):
    """
    Mixin to include and access multiple :py:class:`~columnflow.production.Producer` classes.
    """

    producers = law.CSVParameter(
        default=(RESOLVE_DEFAULT,),
        description="comma-separated names of producers to be applied; default: value of the "
        "'default_producer' analysis aux",
        brace_expand=True,
        parse_empty=True,
    )

    @classmethod
    def resolve_param_values_pre_init(
        cls,
        params: law.util.InsertableDict[str, Any],
    ) -> law.util.InsertableDict[str, Any]:
        params = super().resolve_param_values_pre_init(params)

        # resolve the default classes if necessary
        if (container := cls._get_config_container(params)):
            params["producers"] = cls.resolve_config_default_and_groups(
                param=params.get("producers"),
                task_params=params,
                container=container,
                default_str="default_producer",
                groups_str="producer_groups",
                multi_strategy="same",
            )

        return params

    @classmethod
    def req_params(cls, inst: law.Task, **kwargs) -> dict[str, Any]:
        # prefer --producers set on task-level via cli
        kwargs["_prefer_cli"] = law.util.make_set(kwargs.get("_prefer_cli", [])) | {"producers"}
        return super().req_params(inst, **kwargs)

    @property
    def producers_repr(self) -> str:
        """
        Return a string representation of the producers.
        """
        prods_repr = "none"
        if self.producers:
            reprs = list(map(self.array_function_cls_repr, self.producers))
            prods_repr = "__".join(reprs[:5])
            if len(reprs) > 5:
                prods_repr += f"__{law.util.create_hash(reprs[5:])}"
        return prods_repr

    def store_parts(self) -> law.util.InsertableDict:
        """
        :return: Dictionary with parts that will be translated into an output directory path.
        """
        parts = super().store_parts()
        parts.insert_after(self.config_store_anchor, "producers", f"prod__{self.producers_repr}")
        return parts


class ProducersMixin(ArrayFunctionInstanceMixin, ProducerClassesMixin):
    """
    Mixin to include multiple :py:class:`~columnflow.production.Producer` instances into tasks.
    """

    producer_insts = DerivableInstsParameter(
        default=(),
        visibility=luigi.parameter.ParameterVisibility.PRIVATE,
    )

    exclude_params_index = {"producer_insts"}
    exclude_params_repr = {"producer_insts"}
    exclude_params_sandbox = {"producer_insts"}
    exclude_params_remote_workflow = {"producer_insts"}

    @classmethod
    def get_producer_dict(cls, params: dict[str, Any]) -> dict[str, Any]:
        return cls.get_array_function_dict(params)

    @classmethod
    def build_producer_insts(
        cls,
        producers: Iterable[str],
        params: dict[str, Any] | None = None,
    ) -> list[Producer]:
        """
        Instantiate and return multiple :py:class:`~columnflow.production.Producer` instances.

        :param producers: Name of the producer class to instantiate.
        :param params: Arguments forwarded to the producer constructors.
        :raises RuntimeError: If any producer class is not
            :py:attr:`~columnflow.production.Producer.exposed`.
        :return: The list of producer instances.
        """
        inst_dict = cls.get_producer_dict(params) if params else None

        insts = []
        for producer in producers:
            producer_cls = Producer.get_cls(producer)
            if not producer_cls.exposed:
                raise RuntimeError(
                    f"cannot use unexposed producer '{producer}' in {cls.__name__}",
                )
            insts.append(producer_cls(inst_dict=inst_dict))

        return insts

    @classmethod
    def resolve_instances(cls, params: dict[str, Any], shifts: TaskShifts) -> dict[str, Any]:
        # add the producer instances
        if not params.get("producer_insts"):
            params["producer_insts"] = cls.build_producer_insts(params["producers"], params)

        params = super().resolve_instances(params, shifts)

        return params

    @classmethod
    def get_known_shifts(
        cls,
        params: dict[str, Any],
        shifts: TaskShifts,
    ) -> None:
        """
        Updates the set of known *shifts* implemented by *this* and upstream tasks.

        :param params: Dictionary of task parameters.
        :param shifts: TaskShifts object to adjust.
        """
        # get the producers, update them and add their shifts
        for producer_inst in params["producer_insts"]:
            shifts.upstream |= producer_inst.all_shifts

        super().get_known_shifts(params, shifts)

    def _array_function_post_init(self, **kwargs) -> None:
        for producer_inst in self.producer_insts or []:
            producer_inst.run_post_init(task=self, **kwargs)
        super()._array_function_post_init(**kwargs)

    @property
    def producers_repr(self) -> str:
        """
        Return a string representation of the producers.
        """
        prods_repr = "none"
        if self.producers:
            reprs = list(map(self.array_function_inst_repr, self.producer_insts))
            prods_repr = "__".join(reprs[:5])
            if len(reprs) > 5:
                prods_repr += f"__{law.util.create_hash(reprs[5:])}"
        return prods_repr

    def find_keep_columns(self, collection: ColumnCollection) -> set[Route]:
        """
        Finds the columns to keep based on the *collection*.

        :param collection: The collection of columns.
        :return: Set of columns to keep.
        """
        columns = super().find_keep_columns(collection)

        if collection == ColumnCollection.ALL_FROM_CALIBRATORS:
            columns |= set.union(*(
                producer_inst.produced_columns
                for producer_inst in self.producer_insts
            ))

        return columns


class MLModelMixinBase(ConfigTask):
    """
    Base mixin to include a machine learning application into tasks.

    Inheriting from this mixin will allow a task to instantiate and access a
    :py:class:`~columnflow.ml.MLModel` instance with name *ml_model*, which is an input parameter
    for this task.
    """

    ml_model = luigi.Parameter(
        description="the name of the ML model to be applied",
    )
    ml_model_settings = SettingsParameter(
        default=DotDict(),
        description="settings passed to the init function of the ML model",
    )
    ml_model_inst = DerivableInstParameter(
        default=None,
        visibility=luigi.parameter.ParameterVisibility.PRIVATE,
    )
    exclude_params_index = {"ml_model_inst"}
    exclude_params_repr = {"ml_model_inst"}
    exclude_params_sandbox = {"ml_model_inst"}
    exclude_params_remote_workflow = {"ml_model_inst"}

    exclude_params_repr_empty = {"ml_model"}

    @property
    def ml_model_repr(self):
        """
        Returns a string representation of the ML model instance.
        """
        return str(self.ml_model_inst)

    @classmethod
    def req_params(cls, inst: law.Task, **kwargs) -> dict[str, Any]:
        """
        Get the required parameters for the task, preferring the ``--ml-model`` set on task-level
        via CLI.

        This method first checks if the ``--ml-model`` parameter is set at the task-level via the command line.
        If it is, this parameter is preferred and added to the '_prefer_cli' key in the kwargs dictionary.
        The method then calls the 'req_params' method of the superclass with the updated kwargs.

        :param inst: The current task instance.
        :param kwargs: Additional keyword arguments that may contain parameters for the task.
        :return: A dictionary of parameters required for the task.
        """
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
        """
        Get requested *ml_model* instance.

        This method retrieves the requested *ml_model* instance.
        If *requested_configs* are provided, they are used for the training of
        the ML application.

        :param ml_model: Name of :py:class:`~columnflow.ml.MLModel` to load.
        :param analysis_inst: Forward this analysis inst to the init function of new MLModel sub class.
        :param requested_configs: Configs needed for the training of the ML application.
        :param kwargs: Additional keyword arguments to forward to the :py:class:`~columnflow.ml.MLModel` instance.
        :return: :py:class:`~columnflow.ml.MLModel` instance.
        """

        ml_model_inst: MLModel = MLModel.get_cls(ml_model)(analysis_inst, **kwargs)
        if requested_configs:
            configs = ml_model_inst.training_configs(list(requested_configs))
            if configs:
                ml_model_inst._setup(configs)

        return ml_model_inst

    def events_used_in_training(
        self,
        config_inst: od.config.Config,
        dataset_inst: od.dataset.Dataset,
        shift_inst: od.shift.Shift,
    ) -> bool:
        """
        Evaluate whether the events for the combination of *dataset_inst* and
        *shift_inst* shall be used in the training.

        This method checks if the *dataset_inst* is in the set of datasets of
        the current `ml_model_inst` based on the given *config_inst*. Additionally,
        the function checks that the *shift_inst* does not have the tag
        `"disjoint_from_nominal"`.

        :param config_inst: The configuration instance.
        :param dataset_inst: The dataset instance.
        :param shift_inst: The shift instance.
        :return: True if the events shall be used in the training, False otherwise.
        """
        # evaluate whether the events for the combination of dataset_inst and shift_inst
        # shall be used in the training
        return (
            dataset_inst in self.ml_model_inst.datasets(config_inst) and
            not shift_inst.has_tag("disjoint_from_nominal")
        )


class MLModelTrainingMixin(
    MLModelMixinBase,
    ProducerClassesMixin,
    SelectorClassMixin,
    CalibratorClassesMixin,
):
    """
    A mixin class for training machine learning models.
    """
    single_config = False

    @classmethod
    def resolve_instances(cls, params: dict[str, Any], shifts: TaskShifts) -> dict[str, Any]:
        # NOTE: we can only build TAF insts from the MLModel after ml_model_inst is set
        if not cls.resolution_task_class:
            raise ValueError(f"resolution_task_class must be set for multi-config task {cls.task_family}")

        cls.get_known_shifts(params, shifts)

        ml_model_inst = params["ml_model_inst"]
        for config_inst, dataset_insts in ml_model_inst.used_datasets.items():
            for dataset_inst in dataset_insts:
                # NOTE: we need to copy here, because otherwise taf inits will only be triggered once
                _params = {
                    **params,
                    "config_inst": config_inst,
                    "config": config_inst.name,
                    "dataset": dataset_inst.name,
                }
                logger_dev.debug(
                    f"building taf insts for {ml_model_inst.cls_name} {config_inst.name}, {dataset_inst.name}",
                )
                cls.resolution_task_class.resolve_instances(_params, shifts)
                cls.resolution_task_class.get_known_shifts(_params, shifts)

        params["known_shifts"] = shifts

        return params

    @classmethod
    def resolve_param_values_pre_init(cls, params: dict[str, Any]) -> dict[str, Any]:
        """
        Resolve the parameter values for the given parameters.

        This method retrieves the parameters and resolves the ML model instance and the configs.
        It also calls the model's setup hook.

        :param params: A dictionary of parameters that may contain the analysis instance and ML model.
        :return: A dictionary containing the resolved parameters.
        :raises Exception: If the ML model instance received configs to define training configs,
            but did not define any.
        """
        # NOTE: we need to resolve ml_model_inst before CSPs because the ml_model_inst itself defines
        # used CSPs and datasets
        params = super().resolve_param_values_pre_init(params)

        if "analysis_inst" not in params or "ml_model" not in params:
            raise ValueError("analysis_inst and ml_model need to be set to resolve the ml_model_inst")

        analysis_inst = params["analysis_inst"]

        # NOTE: we could try to implement resolving the default ml_model here
        # NOTE: why not implement the config resoluting in get_ml_model_inst instead?
        ml_model_inst = cls.get_ml_model_inst(
            params["ml_model"],
            analysis_inst,
            parameters=params["ml_model_settings"],
        )
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

        # call the model's setup hook
        ml_model_inst._setup()

        # resolve CSPs based on the MLModel
        params["calibrators"] = law.util.make_tuple(
            ml_model_inst.training_calibrators(analysis_inst, params["calibrators"]),
        )
        params["selector"] = ml_model_inst.training_selector(analysis_inst, params["selector"])
        params["producers"] = law.util.make_tuple(
            ml_model_inst.training_producers(analysis_inst, params["producers"]),
        )

        return params

    def store_parts(self) -> law.util.InsertableDict[str, str]:
        """
        Generate a dictionary of store parts for the current instance.

        This method extends the base method to include the ML model parameter.

        :return: An InsertableDict containing the store parts.
        """
        parts = super().store_parts()

        if self.ml_model_inst:
            parts.insert_before("version", "ml_model", f"ml__{self.ml_model_repr}")

        return parts


class MLModelMixin(MLModelMixinBase):
    """
    A mixin for tasks that require a single machine learning model, e.g. for evaluation.
    """

    ml_model = luigi.Parameter(
        default=RESOLVE_DEFAULT,
        description="the name of the ML model to be applied; default: value of the "
        "'default_ml_model' analysis aux",
    )

    allow_empty_ml_model = True

    exclude_params_repr_empty = {"ml_model"}

    @classmethod
    def resolve_param_values_pre_init(cls, params: dict[str, Any]) -> dict[str, Any]:
        params = super().resolve_param_values_pre_init(params)

        # # add the default ml model when empty
        params["ml_model"] = cls.resolve_config_default(
            param=params.get("ml_model"),
            task_params=params,
            container=params["analysis_inst"],
            default_str="default_ml_model",
            multi_strategy="same",
        )

        # when both config_inst and ml_model are set, initialize the ml_model_inst
        if all(params.get(x) not in (None, law.NO_STR) for x in ("config_inst", "ml_model")):
            if not params.get("ml_model_inst"):
                params["ml_model_inst"] = cls.get_ml_model_inst(
                    params["ml_model"],
                    params["analysis_inst"],
                    requested_configs=[params["config_inst"]],
                )
        elif not cls.allow_empty_ml_model:
            raise Exception(f"no ml_model configured for {cls.task_family}")

        return params

    def store_parts(self) -> law.util.InsertableDict:
        parts = super().store_parts()

        if self.ml_model_inst:
            parts.insert_before("version", "ml_model", f"ml__{self.ml_model_repr}")

        return parts

    def find_keep_columns(self, collection: ColumnCollection) -> set[Route]:
        columns = super().find_keep_columns(collection)

        if collection == ColumnCollection.ALL_FROM_ML_EVALUATION and self.ml_model_inst:
            columns |= set.union(*self.ml_model_inst.produced_columns().values())

        return columns


class PreparationProducerMixin(ArrayFunctionInstanceMixin, MLModelMixin):
    preparation_producer_inst = DerivableInstParameter(
        default=None,
        visibility=luigi.parameter.ParameterVisibility.PRIVATE,
    )
    exclude_params_index = {"preparation_producer_inst"}
    exclude_params_repr = {"preparation_producer_inst"}
    exclude_params_sandbox = {"preparation_producer_inst"}
    exclude_params_remote_workflow = {"preparation_producer_inst"}

    invokes_preparation_producer = False

    @classmethod
    def get_producer_dict(cls, params: dict[str, Any]) -> dict[str, Any]:
        return cls.get_array_function_dict(params)

    # @property
    # def preparation_producer(self):
    #     return self.ml_model_inst.preparation_producer(self.analysis_inst)

    build_producer_inst = ProducerMixin.build_producer_inst

    @classmethod
    def resolve_instances(cls, params: dict[str, Any], shifts: TaskShifts) -> dict[str, Any]:
        ml_model_inst = params["ml_model_inst"]
        preparation_producer = ml_model_inst.preparation_producer(params["analysis_inst"])
        # add the producer instance
        if not params.get("preparation_producer_inst"):
            params["preparation_producer_inst"] = cls.build_producer_inst(preparation_producer, params)

        params = super().resolve_instances(params, shifts)

        return params


class MLModelDataMixin(PreparationProducerMixin):
    single_config = True
    allow_empty_ml_model = False

    def store_parts(self) -> law.util.InsertableDict:
        parts = super().store_parts()

        # replace the ml_model entry
        store_name = self.ml_model_inst.store_name or self.ml_model_repr
        parts.insert_before("ml_model", "ml_data", f"ml__{store_name}")
        parts.pop("ml_model")

        return parts


class MLModelsMixin(ConfigTask):

    ml_models = law.CSVParameter(
        default=(RESOLVE_DEFAULT,),
        description="comma-separated names of ML models to be applied; default: value of the "
        "'default_ml_model' config",
        brace_expand=True,
        parse_empty=True,
    )

    allow_empty_ml_models = True

    exclude_params_repr_empty = {"ml_models"}

    @property
    def ml_models_repr(self) -> str:
        """
        Returns a string representation of the ML models.
        """
        return "__".join(map(str, self.ml_model_insts))

    @classmethod
    def resolve_param_values_pre_init(cls, params: dict[str, Any]) -> dict[str, Any]:
        # NOTE: at the moment, the ml_models will be initialized before CSPs are initialized
        params = super().resolve_param_values_pre_init(params)

        if (container := cls._get_config_container(params)):
            # apply ml_model_groups and default_ml_model from the config
            params["ml_models"] = cls.resolve_config_default_and_groups(
                param=params.get("ml_models"),
                task_params=params,
                container=container,
                default_str="default_ml_model",
                groups_str="ml_model_groups",
                multi_strategy="same",
            )

            # special case: initialize them once to trigger their set_config hook
            if params.get("ml_models"):
                params["ml_model_insts"] = [
                    MLModelMixinBase.get_ml_model_inst(
                        ml_model,
                        params["analysis_inst"],
                        requested_configs=[params["config"]] if cls.has_single_config() else params["configs"],
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

    @property
    def ml_model_insts(self) -> list[MLModel]:
        if self._ml_model_insts is None:
            self._ml_model_insts = [
                MLModelMixinBase.get_ml_model_inst(
                    ml_model,
                    self.analysis_inst,
                    requested_configs=[self.config] if self.single_config else self.config,
                )
                for ml_model in self.ml_models
            ]
        return self._ml_model_insts

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # cache for ml model insts
        self._ml_model_insts = None

    def store_parts(self) -> law.util.InsertableDict:
        parts = super().store_parts()

        if self.ml_model_insts:
            parts.insert_before("version", "ml_models", f"ml__{self.ml_models_repr}")

        return parts

    def find_keep_columns(self, collection: ColumnCollection) -> set[Route]:
        columns = super().find_keep_columns(collection)

        if collection == ColumnCollection.ALL_FROM_ML_EVALUATION:
            columns |= set.union(*(
                set.union(*model_inst.produced_columns().values())
                for model_inst in self.ml_model_insts
            ))

        return columns


class WeightProducerClassMixin(ArrayFunctionClassMixin):
    """
    Mixin to include and access single :py:class:`~columnflow.weight.WeightProducer` class.
    """

    weight_producer = luigi.Parameter(
        default=RESOLVE_DEFAULT,
        description="the name of the weight producer to be applied; default: value of the "
        "'default_weight_producer' config",
    )

    @classmethod
    def resolve_param_values_pre_init(cls, params: dict[str, Any]) -> dict[str, Any]:
        params = super().resolve_param_values_pre_init(params)

        # resolve the default class if necessary
        if (container := cls._get_config_container(params)):
            params["weight_producer"] = cls.resolve_config_default(
                param=params.get("weight_producer"),
                task_params=params,
                container=container,
                default_str="default_weight_producer",
                multi_strategy="same",
            )

        return params

    @classmethod
    def req_params(cls, inst: law.Task, **kwargs) -> dict[str, Any]:
        # prefer --weight-producer set on task-level via cli
        kwargs["_prefer_cli"] = law.util.make_set(kwargs.get("_prefer_cli", [])) | {"weight_producer"}
        return super().req_params(inst, **kwargs)

    @property
    def weight_producer_repr(self) -> str:
        """
        Return a string representation of the weight producer class.
        """
        return self.array_function_cls_repr(self.weight_producer)

    def store_parts(self) -> law.util.InsertableDict:
        """
        :return: Dictionary with parts that will be translated into an output directory path.
        """
        parts = super().store_parts()
        parts.insert_after(self.config_store_anchor, "weight_producer", f"weight__{self.weight_producer_repr}")
        return parts

    @classmethod
    def get_config_lookup_keys(
        cls,
        inst_or_params: WeightProducerClassMixin | dict[str, Any],
    ) -> law.util.InsertiableDict:
        keys = super().get_config_lookup_keys(inst_or_params)

        # add the weight producer name
        producer = (
            inst_or_params.get("weight_producer")
            if isinstance(inst_or_params, dict)
            else getattr(inst_or_params, "weight_producer", None)
        )
        if producer not in (law.NO_STR, None, ""):
            keys["weight_producer"] = f"weight_{producer}"

        return keys


class WeightProducerMixin(ArrayFunctionInstanceMixin, WeightProducerClassMixin):
    """
    Mixin to include and access a single :py:class:`~columnflow.weight.WeightProducer` instance.
    """

    weight_producer_inst = DerivableInstParameter(
        default=None,
        visibility=luigi.parameter.ParameterVisibility.PRIVATE,
    )

    exclude_params_index = {"weight_producer_inst"}
    exclude_params_repr = {"weight_producer_inst"}
    exclude_params_sandbox = {"weight_producer_inst"}
    exclude_params_remote_workflow = {"weight_producer_inst"}

    # decides whether the task itself invokes the weight_producer
    invokes_weight_producer = False

    @classmethod
    def get_weight_producer_dict(cls, params: dict[str, Any]) -> dict[str, Any]:
        return cls.get_array_function_dict(params)

    @classmethod
    def build_weight_producer_inst(
        cls,
        weight_producer: str,
        params: dict[str, Any] | None = None,
    ) -> Producer:
        """
        Instantiate and return the :py:class:`~columnflow.weight.WeightProducer` instance.

        :param producer: Name of the weight producer class to instantiate.
        :param params: Arguments forwarded to the weight producer constructor.
        :raises RuntimeError: If the weight producer class is not
            :py:attr:`~columnflow.weight.WeightProducer.exposed`.
        :return: The weight producer instance.
        """
        weight_producer_cls = WeightProducer.get_cls(weight_producer)
        if not weight_producer_cls.exposed:
            raise RuntimeError(
                f"cannot use unexposed weight_producer '{weight_producer}' in {cls.__name__}",
            )

        inst_dict = cls.get_weight_producer_dict(params) if params else None
        return weight_producer_cls(inst_dict=inst_dict)

    @classmethod
    def resolve_instances(cls, params: dict[str, Any], shifts: TaskShifts) -> dict[str, Any]:
        # add the weight producer instance
        if not params.get("weight_producer_inst"):
            params["weight_producer_inst"] = cls.build_weight_producer_inst(
                params["weight_producer"],
                params,
            )

        params = super().resolve_instances(params, shifts)

        return params

    @classmethod
    def get_known_shifts(
        cls,
        params: dict[str, Any],
        shifts: TaskShifts,
    ) -> None:
        """
        Updates the set of known *shifts* implemented by *this* and upstream tasks.

        :param params: Dictionary of task parameters.
        :param shifts: TaskShifts object to adjust.
        """
        # get the weight producer, update it and add its shifts
        weight_producer_shifts = params["weight_producer_inst"].all_shifts
        (shifts.local if cls.invokes_weight_producer else shifts.upstream).update(weight_producer_shifts)

        super().get_known_shifts(params, shifts)

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

        # overwrite the sandbox when set
        if self.invokes_weight_producer and (sandbox := self.weight_producer_inst.get_sandbox()):
            self.reset_sandbox(sandbox)

    def _array_function_post_init(self, **kwargs) -> None:
        self.weight_producer_inst.run_post_init(task=self, **kwargs)
        super()._array_function_post_init(**kwargs)

    @property
    def weight_producer_repr(self) -> str:
        """
        Return a string representation of the weight producer instance.
        """
        return self.array_function_inst_repr(self.weight_producer_inst)


class InferenceModelClassMixin(ConfigTask):

    inference_model = luigi.Parameter(
        default=RESOLVE_DEFAULT,
        description="the name of the inference model to be used; default: value of the "
        "'default_inference_model' config",
    )

    @classmethod
    def resolve_param_values_pre_init(cls, params: dict[str, Any]) -> dict[str, Any]:
        params = super().resolve_param_values_pre_init(params)

        # add the default inference model when empty
        if (container := cls._get_config_container(params)):
            params["inference_model"] = cls.resolve_config_default(
                param=params.get("inference_model"),
                task_params=params,
                container=container,
                default_str="default_inference_model",
                multi_strategy="same",
            )

        return params

    @classmethod
    def req_params(cls, inst: law.Task, **kwargs) -> dict:
        # prefer --inference-model set on task-level via cli
        kwargs["_prefer_cli"] = law.util.make_set(kwargs.get("_prefer_cli", [])) | {"inference_model"}

        return super().req_params(inst, **kwargs)

    @property
    def inference_model_repr(self):
        return str(self.inference_model)

    def store_parts(self) -> law.util.InsertableDict:
        """
        :return: Dictionary with parts that will be translated into an output directory path.
        """
        parts = super().store_parts()
        if self.inference_model != law.NO_STR:
            parts.insert_after(self.config_store_anchor, "inf_model", f"inf__{self.inference_model_repr}")
        return parts


class InferenceModelMixin(InferenceModelClassMixin):

    inference_model_inst = DerivableInstParameter(
        default=None,
        visibility=luigi.parameter.ParameterVisibility.PRIVATE,
    )

    exclude_params_index = {"inference_model_inst"}
    exclude_params_repr = {"inference_model_inst"}
    exclude_params_sandbox = {"inference_model_inst"}
    exclude_params_remote_workflow = {"inference_model_inst"}

    @classmethod
    def build_inference_model_inst(
        cls,
        inference_model: str,
        config_insts: list[od.Config],
        **kwargs,
    ) -> InferenceModel:
        """
        Instantiate and return the :py:class:`~columnflow.inference.InferenceModel` instance.

        :param inference_model: Name of the inference model class to instantiate.
        :param config_insts: List of configuration objects that are passed to the inference model constructor.
        :param kwargs: Additional keywork arguments forwarded to the inference model constructor.
        :return: The inference model instance.
        """
        inference_model_cls = InferenceModel.get_cls(inference_model)
        return inference_model_cls(config_insts, **kwargs)

    @classmethod
    def resolve_param_values_post_init(cls, params: dict[str, Any]) -> dict[str, Any]:
        params = super().resolve_param_values_post_init(params)

        # add the inference model instance
        if not params.get("inference_model_inst") and params.get("inference_model"):
            if cls.has_single_config():
                if (config_inst := params.get("config_inst")):
                    params["inference_model_inst"] = cls.build_inference_model_inst(
                        params["inference_model"],
                        [config_inst],
                    )
            elif (config_insts := params.get("config_insts")):
                params["inference_model_inst"] = cls.build_inference_model_inst(
                    params["inference_model"],
                    config_insts,
                )

        return params

    @classmethod
    def resolve_instances(cls, params: dict[str, Any], shifts: TaskShifts) -> dict[str, Any]:
        if not cls.resolution_task_class:
            raise ValueError(f"resolution_task_class must be set for multi-config task {cls.task_family}")

        cls.get_known_shifts(params, shifts)

        # we loop over all configs/datasets, but return initial params
        inference_model_cls = InferenceModel.get_cls(params["inference_model"])
        for i, config_inst in enumerate(params["config_insts"]):
            datasets = inference_model_cls.used_datasets(config_inst)

            for dataset in datasets:
                # NOTE: we need to copy here, because otherwise taf inits will only be triggered once
                _params = {
                    **params,
                    "config_inst": config_inst,
                    "config": config_inst.name,
                    "dataset": dataset,
                }
                logger_dev.debug(f"building taf insts for {config_inst.name}, {dataset}")
                cls.resolution_task_class.resolve_instances(_params, shifts)
                cls.resolution_task_class.get_known_shifts(_params, shifts)

        params["known_shifts"] = shifts

        return params


class CategoriesMixin(ConfigTask):

    categories = law.CSVParameter(
        default=(RESOLVE_DEFAULT,),
        description="comma-separated category names or patterns to select; can also be the key of a mapping defined in "
        "'category_groups' auxiliary data of the config; when empty, uses the auxiliary data enty 'default_categories' "
        "when set; empty default",
        brace_expand=True,
        parse_empty=True,
    )

    default_categories = None
    allow_empty_categories = False

    @classmethod
    def resolve_param_values_post_init(cls, params: dict[str, Any]) -> dict[str, Any]:
        params = super().resolve_param_values_post_init(params)
        if "analysis_inst" not in params or "config_insts" not in params:
            return params

        # resolve categories
        if (categories := params.get("categories", law.no_value)) != law.no_value:
            # when empty, use the ones defined on class level
            if categories in ((), (RESOLVE_DEFAULT,)) and cls.default_categories:
                categories = tuple(cls.default_categories)

            # additional resolution and expansion requires a config
            if (container := cls._get_config_container(params)):
                # when still empty, get the config default
                categories = cls.resolve_config_default(
                    param=params.get("categories"),
                    task_params=params,
                    container=container,
                    default_str="default_categories",
                    # groups_str="category_groups",
                    multi_strategy="union",
                )
                # resolve them
                categories = cls.find_config_objects(
                    names=categories,
                    container=container,
                    object_cls=od.Category,
                    groups_str="category_groups",
                    deep=True,
                    multi_strategy="intersection",
                )

            # complain when no categories were found
            if not categories and not cls.allow_empty_categories:
                raise ValueError(f"no categories found matching {params['categories']}")

            params["categories"] = tuple(categories)

        return params

    @property
    def categories_repr(self) -> str:
        if len(self.categories) == 1:
            return self.categories[0]
        return f"{len(self.categories)}_{law.util.create_hash(sorted(self.categories))}"


class VariablesMixin(ConfigTask):

    variables = law.CSVParameter(
        default=(RESOLVE_DEFAULT,),
        description="comma-separated variable names or patterns to select; can also be the key "
        "of a mapping defined in the 'variable_group' auxiliary data of the config; when empty, "
        "uses all variables of the config; empty default",
        brace_expand=True,
        parse_empty=True,
    )

    default_variables = None
    allow_empty_variables = False
    allow_missing_variables = False

    @classmethod
    def resolve_param_values_post_init(cls, params: dict[str, Any]) -> dict[str, Any]:
        params = super().resolve_param_values_post_init(params)

        if "analysis_inst" not in params or "config_insts" not in params:
            return params

        # resolve variables
        if (variables := params.get("variables", law.no_value)) != law.no_value:
            # when empty, use the ones defined on class level
            if variables in ((), (RESOLVE_DEFAULT,)) and cls.default_variables:
                variables = tuple(cls.default_variables)

            # additional resolution and expansion requires a config
            if (container := cls._get_config_container(params)):
                # when still empty, get the config default
                variables = cls.resolve_config_default_and_groups(
                    param=params.get("variables"),
                    task_params=params,
                    container=container,
                    default_str="default_variables",
                    groups_str="variable_groups",
                    multi_strategy="union",
                )
                # since there can be multi-dimensional variables, resolve each part separately
                resolved_variables = set()
                for variable in variables:
                    resolved_parts = [
                        cls.find_config_objects(
                            names=part,
                            container=container,
                            object_cls=od.Variable,
                            groups_str="variable_groups",
                            multi_strategy="intersection",
                        )
                        for part in cls.split_multi_variable(variable)
                    ]
                    # build combinatrics
                    resolved_variables.update(map(cls.join_multi_variable, itertools.product(*resolved_parts)))
                variables = resolved_variables

            # when still empty, fallback to using all known variables
            if not variables:
                variables = sorted(set.intersection(*(set(c.variables.names()) for c in law.util.make_list(container))))

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

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

        # if enabled, split names of multi-dimensional parameters into tuples
        self.variable_tuples = {
            var_name: self.split_multi_variable(var_name)
            for var_name in self.variables
        }

    @property
    def variables_repr(self) -> str:
        if len(self.variables) == 1:
            return self.variables[0]
        return f"{len(self.variables)}_{law.util.create_hash(sorted(self.variables))}"


class DatasetsProcessesMixin(ConfigTask):
    # single_config = True
    datasets = law.CSVParameter(
        default=(),
        description="comma-separated dataset names or patters to select; can also be the key of a mapping defined in "
        "the 'dataset_groups' auxiliary data of the config; when empty, uses all datasets registered in the config "
        "that contain any of the selected --processes; empty default",
        brace_expand=True,
        parse_empty=True,
    )
    datasets_multi = law.MultiCSVParameter(
        default=(),
        description="multiple comma-separated dataset names or patters to select per config object, each separated by "
        "a colon; when only one sequence is passed, it is applied to all configs; values can also be the key of a "
        "mapping defined in " "the 'dataset_groups' auxiliary data of the specific config; when empty, uses all "
        "datasets registered in the config that contain any of the selected --processes; empty default",
        brace_expand=True,
        parse_empty=True,
    )
    processes = law.CSVParameter(
        default=(),
        description="comma-separated process names or patterns for filtering processes; can also be the key of a "
        "mapping defined in the 'process_groups' auxiliary data of the config; uses all processes of the config when "
        "empty; empty default",
        brace_expand=True,
        parse_empty=True,
    )
    processes_multi = law.MultiCSVParameter(
        default=(),
        description="multiple comma-separated process names or patters for filtering processing per config object, "
        "each separated by a colon; when only one sequence is passed, it is applied to all configs; values can also be "
        "the key of a mapping defined in the 'process_groups' auxiliary data of the specific config; uses all "
        "processes of the config when empty; empty default",
        brace_expand=True,
        parse_empty=True,
    )

    allow_empty_datasets = False
    allow_empty_processes = False

    @classmethod
    def modify_task_attributes(cls) -> None:
        super().modify_task_attributes()
        # single/multi config adjustments in case the switch has been specified
        if isinstance(cls.single_config, bool) and getattr(cls, "datasets_multi", None) is not None:
            if not cls.has_single_config():
                cls.datasets = cls.datasets_multi
                cls.processes = cls.processes_multi
            cls.datasets_multi = None
            cls.processes_multi = None

    @classmethod
    def resolve_param_values_pre_init(cls, params: dict[str, Any]) -> dict[str, Any]:
        params = super().resolve_param_values_pre_init(params)

        # helper to resolve processes and datasets for one config
        def resolve(config_inst: od.Config, processes: Any, datasets: Any) -> tuple[list[str], list[str]]:
            if processes != law.no_value:
                processes_orig = processes
                if processes:
                    processes = cls.find_config_objects(
                        names=processes,
                        container=config_inst,
                        object_cls=od.Process,
                        groups_str="process_groups",
                        deep=True,
                    )
                else:
                    processes = config_inst.processes.names()
                if not processes and not cls.allow_empty_processes:
                    raise ValueError(f"no processes found matching {processes_orig}")
            if datasets != law.no_value:
                datasets_orig = datasets
                if datasets:
                    datasets = cls.find_config_objects(
                        names=datasets,
                        container=config_inst,
                        object_cls=od.Dataset,
                        groups_str="dataset_groups",
                    )
                elif processes and processes != law.no_value:
                    # pick all datasets that contain any of the requested (sub)processes
                    sub_process_insts = sum((
                        [proc for proc, _, _ in process_inst.walk_processes(include_self=True)]
                        for process_inst in map(config_inst.get_process, processes)
                    ), [])
                    datasets = [
                        dataset_inst.name for dataset_inst in config_inst.datasets
                        if any(map(dataset_inst.has_process, sub_process_insts))
                    ]
                if not datasets and not cls.allow_empty_datasets:
                    raise ValueError(f"no datasets found matching {datasets_orig}")

            return (processes, datasets)

        # get processes and datasets
        single_config = cls.has_single_config()
        processes = (params.get("processes", law.no_value),) if single_config else params.get("processes", ())
        datasets = (params.get("datasets", law.no_value),) if single_config else params.get("datasets", ())

        # "broadcast" to match number of configs
        config_insts = params.get("config_insts")
        processes = cls.broadcast_to_configs(processes, "processes", len(config_insts))
        datasets = cls.broadcast_to_configs(datasets, "datasets", len(config_insts))

        # perform resolution per config
        multi_processes = []
        multi_datasets = []
        for config_inst, _processes, _datasets in zip(config_insts, processes, datasets):
            _processes, _datasets = resolve(config_inst, _processes, _datasets)
            multi_processes.append(tuple(_processes) if _processes != law.no_value else None)
            multi_datasets.append(tuple(_datasets) if _datasets != law.no_value else None)

        # store params
        params["processes"] = multi_processes[0] if single_config else tuple(multi_processes)
        params["datasets"] = multi_datasets[0] if single_config else tuple(multi_datasets)

        # store instances
        params["process_insts"] = {
            config_inst: list(map(config_inst.get_process, processes))
            for config_inst, processes in zip(config_insts, multi_processes)
        }
        params["dataset_insts"] = {
            config_inst: list(map(config_inst.get_dataset, datasets))
            for config_inst, datasets in zip(config_insts, multi_datasets)
        }
        return params

    @classmethod
    def resolve_instances(cls, params: dict[str, Any], shifts: TaskShifts) -> dict[str, Any]:
        if not cls.resolution_task_class:
            raise ValueError(f"resolution_task_class must be set for multi-config task {cls.task_family}")

        cls.get_known_shifts(params, shifts)

        # we loop over all configs/datasets, but return initial params
        for i, config_inst in enumerate(params["config_insts"]):
            if cls.has_single_config():
                datasets = params["datasets"]
            else:
                datasets = params["datasets"][i]

            for dataset in datasets:
                # NOTE: we need to copy here, because otherwise taf inits will only be triggered once
                _params = {
                    **params,
                    "config_inst": config_inst,
                    "config": config_inst.name,
                    "dataset": dataset,
                }
                logger_dev.debug(f"building taf insts for {config_inst.name}, {dataset}")
                cls.resolution_task_class.resolve_instances(_params, shifts)
                cls.resolution_task_class.get_known_shifts(_params, shifts)

        params["known_shifts"] = shifts

        return params

    @classmethod
    def get_known_shifts(
        cls,
        params: dict[str, Any],
        shifts: TaskShifts,
    ) -> None:
        """
        Updates the set of known *shifts* implemented by *this* and upstream tasks.

        :param params: Dictionary of task parameters.
        :param shifts: TaskShifts object to adjust.
        """
        # add shifts of all datasets to upstream ones
        for config_inst, dataset_insts in params["dataset_insts"].items():
            for dataset_inst in dataset_insts:
                if dataset_inst.is_mc:
                    shifts.upstream |= set(dataset_inst.info.keys())

        super().get_known_shifts(params, shifts)

    @property
    def datasets_repr(self) -> str:
        return self._multi_sequence_repr(self.datasets, sort=True)

    @property
    def processes_repr(self) -> str:
        return self._multi_sequence_repr(self.processes, sort=True)


class ShiftSourcesMixin(ConfigTask):
    shift_sources = law.CSVParameter(
        default=(),
        description="comma-separated shift source names (without direction) or patterns to select; can also be the key "
        "of a mapping defined in the 'shift_group' auxiliary data of the config; default: ()",
        brace_expand=True,
        parse_empty=True,
    )

    allow_empty_shift_sources = False

    @classmethod
    def resolve_param_values_post_init(cls, params: dict[str, Any]) -> dict[str, Any]:
        params = super().resolve_param_values_post_init(params)

        # resolve shift sources
        if (container := cls._get_config_container(params)) and "shift_sources" in params:
            shifts = cls.find_config_objects(
                names=cls.expand_shift_sources(params["shift_sources"]),
                container=container,
                object_cls=od.Shift,
                groups_str="shift_groups",
                multi_strategy="union",  # or "intersection"?
            )

            # convert back to sources and validate
            sources = []
            if shifts:
                sources = cls.reduce_shifts(shifts)

                # # reduce shifts based on known shifts
                if "known_shifts" not in params:
                    raise ValueError("known_shifts must be set before resolving shift sources")
                sources = [
                    source for source in sources
                    if (
                        f"{source}_up" in params["known_shifts"].upstream and
                        f"{source}_down" in params["known_shifts"].upstream
                    )
                ]

            # complain when no sources were found
            if not sources and not cls.allow_empty_shift_sources:
                raise ValueError(f"no shifts found matching {params['shift_sources']}")

            # store them
            params["shift_sources"] = tuple(sources)

        return params

    @classmethod
    def expand_shift_sources(cls, sources: Sequence[str] | set[str]) -> list[str]:
        return sum(([f"{s}_up", f"{s}_down"] for s in sources), [])

    @classmethod
    def reduce_shifts(cls, shifts: Sequence[str] | set[str]) -> list[str]:
        return list(set(od.Shift.split_name(shift)[0] for shift in shifts))

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

        self.shifts = self.expand_shift_sources(self.shift_sources)

    @property
    def shift_sources_repr(self) -> str:
        if not self.shift_sources:
            return "none"
        if len(self.shift_sources) == 1:
            return self.shift_sources[0]
        return f"{len(self.shift_sources)}_{law.util.create_hash(sorted(self.shift_sources))}"

    def store_parts(self) -> law.util.InsertableDict:
        parts = super().store_parts()
        parts.insert_before("calibrators", "shift_sources", f"shifts__{self.shift_sources_repr}")
        return parts


class DatasetShiftSourcesMixin(ShiftSourcesMixin, DatasetTask):

    # disable the shift parameter
    shift = None
    effective_shift = None
    allow_empty_shift = True

    # allow empty sources, i.e., using only nominal
    allow_empty_shift_sources = True


class ChunkedIOMixin(ConfigTask):

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

    # define default chunk and pool sizes that can be adjusted per inheriting task
    default_chunk_size = ChunkedIOHandler.default_chunk_size
    default_pool_size = ChunkedIOHandler.default_pool_size

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
        # get the chunked io handler from first arg or create a new one with all args
        if len(args) == 1 and isinstance(args[0], ChunkedIOHandler):
            handler = args[0]
        else:
            # default chunk and pool sizes
            for key in ["chunk_size", "pool_size"]:
                if kwargs.get(key) is None:
                    # get the default from the config, defaulting to the class default
                    kwargs[key] = law.config.get_expanded_int(
                        "analysis",
                        f"{self.task_family}__chunked_io_{key}",
                        getattr(self, f"default_{key}"),
                    )
                # when still not set, remove it and let the handler decide using its defaults
                if kwargs.get(key) is None:
                    kwargs.pop(key, None)
            # create the handler
            handler = ChunkedIOHandler(*args, **kwargs)

        # iterate in the handler context
        with handler:
            self.chunked_io = handler
            msg = f"iterate through {handler.n_entries:_} events in {handler.n_chunks} chunks ..."
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

        # eager cleanup
        del handler


class HistHookMixin(ConfigTask):

    hist_hooks = law.CSVParameter(
        default=(),
        description="names of functions in the config's auxiliary dictionary 'hist_hooks' that are "
        "invoked before plotting to update a potentially nested dictionary of histograms; "
        "default: empty",
    )

    def invoke_hist_hooks(
        self,
        hists: dict[od.Config, dict[od.Process, Any]],
    ) -> dict[od.Config, dict[od.Process, Any]]:
        """
        Invoke hooks to modify histograms before further processing such as plotting.
        """
        if not self.hist_hooks:
            return hists

        # apply hooks in order
        for hook in self.hist_hooks:
            if hook in {None, "", law.NO_STR}:
                continue

            # get the hook
            # TODO: is this actually generalizable / provided via functions by @mafrahm?
            func = None
            if self.has_single_config():
                # check the config, fallback to the analysis
                if not (func := self.config_inst.x("hist_hooks", {}).get(hook)):
                    func = self.analysis_inst.x("hist_hooks", {}).get(hook)
            else:
                # only check the analysis
                func = self.analysis_inst.x("hist_hooks", {}).get(hook)
            if not func:
                raise KeyError(
                    f"hist hook '{hook}' not found in 'hist_hooks' for {self.config_mode()} config task {self!r}",
                )

            # validate it
            if not callable(func):
                raise TypeError(f"hist hook '{hook}' is not callable: {func}")

            # invoke it
            self.publish_message(f"invoking hist hook '{hook}'")
            hists = func(self, hists)

        return hists

    @property
    def hist_hooks_repr(self) -> str:
        """
        Return a string representation of the hist hooks.
        """
        hooks = [hook for hook in self.hist_hooks if hook not in {None, "", law.NO_STR}]

        hooks_repr = "__".join(hooks[:5])
        if len(hooks) > 5:
            hooks_repr += f"__{law.util.create_hash(hooks[5:])}"

        return hooks_repr
