# coding: utf-8

"""
Lightweight mixins task classes.
"""

from __future__ import annotations

import time
import copy
import itertools
from collections import Counter

import luigi
import law
import order as od

from columnflow.tasks.framework.base import (
    AnalysisTask, ConfigTask, DatasetTask, TaskShifts, RESOLVE_DEFAULT,
)
from columnflow.tasks.framework.parameters import (
    SettingsParameter, TaskArrayFunctionParameter, TaskArrayFunctionsParameter,
)
from columnflow.calibration import Calibrator
from columnflow.selection import Selector
from columnflow.production import Producer
from columnflow.weight import WeightProducer
from columnflow.ml import MLModel
from columnflow.inference import InferenceModel
from columnflow.columnar_util import Route, ColumnCollection, ChunkedIOHandler
from columnflow.util import maybe_import, DotDict
from columnflow.types import Sequence, Any, Iterable, Union

ak = maybe_import("awkward")


logger = law.logger.get_logger(__name__)


class ArrayFunctionInstanceMixin(DatasetTask):

    def _array_function_post_init(self, **kwargs) -> None:
        """
        Post-initialization method for all known task array functions.
        """
        return None


class CalibratorClassMixin(ConfigTask):
    """
    Mixin to include and access single :py:class:`~columnflow.calibration.Calibrator` class.
    """

    calibrator = luigi.Parameter(
        default=RESOLVE_DEFAULT,
        description="the name of the calibrator to be applied; default: value of the "
        "'default_calibrator' config",
    )

    @classmethod
    def resolve_param_values(cls, params: dict[str, Any]) -> dict[str, Any]:
        params = super().resolve_param_values(params)

        # resolve the default class if necessary
        if (config_inst := params.get("config_inst")):
            params["calibrator"] = cls.resolve_config_default(
                params,
                params.get("calibrator"),
                container=config_inst,
                default_str="default_calibrator",
                multiple=False,
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
        return str(self.calibrator)

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

    calibrator_inst = TaskArrayFunctionParameter(
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
    def resolve_param_values(cls, params: dict[str, Any]) -> dict[str, Any]:
        params = super().resolve_param_values(params)

        # add the calibrator instance
        if not params.get("calibrator_inst"):
            params["calibrator_inst"] = cls.build_calibrator_inst(params["calibrator"], params)

        return params

    @classmethod
    def get_known_shifts(
        cls,
        config_inst: od.Config,
        params: dict[str, Any],
        shifts: TaskShifts,
    ) -> None:
        """
        Updates the set of known *shifts* implemented by *this* and upstream tasks

        :param config_inst: Config instance.
        :param params: Dictionary of task parameters.
        :param shifts: TaskShifts object to adjust.
        """
        # get the calibrator, update it and add its shifts
        calibrator_shifts = params["calibrator_inst"].all_shifts
        (shifts.local if cls.invokes_calibrator else shifts.upstream).update(calibrator_shifts)

        super().get_known_shifts(config_inst, params, shifts)

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

        # overwrite the sandbox when set
        if self.invokes_calibrator and (sandbox := self.calibrator_inst.get_sandbox()):
            self.reset_sandbox(sandbox)

    def _array_function_post_init(self, **kwargs) -> None:
        super()._array_function_post_init(**kwargs)
        self.calibrator_inst.run_post_init(task=self, **kwargs)

    @property
    def calibrator_repr(self) -> str:
        """
        Return a string representation of the calibrator instance.
        """
        return str(self.calibrator_inst)

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


class CalibratorClassesMixin(ConfigTask):
    """
    Mixin to include and access multiple :py:class:`~columnflow.calibration.Calibrator` classes.
    """

    calibrators = law.CSVParameter(
        default=(RESOLVE_DEFAULT,),
        description="comma-separated names of calibrators to be applied; default: value of the "
        "'default_calibrator' config",
        brace_expand=True,
        parse_empty=True,
    )

    @classmethod
    def resolve_param_values(
        cls,
        params: law.util.InsertableDict[str, Any],
    ) -> law.util.InsertableDict[str, Any]:
        params = super().resolve_param_values(params)

        # resolve the default classes if necessary
        if (config_inst := params.get("config_inst")):
            params["calibrators"] = cls.resolve_config_default_and_groups(
                params,
                params.get("calibrators"),
                container=config_inst,
                default_str="default_calibrator",
                groups_str="calibrator_groups",
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
            reprs = list(map(str, self.calibrators))
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

    calibrator_insts = TaskArrayFunctionsParameter(
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
    def resolve_param_values(cls, params: dict[str, Any]) -> dict[str, Any]:
        params = super().resolve_param_values(params)

        # add the calibrator instances
        if not params.get("calibrator_insts"):
            params["calibrator_insts"] = cls.build_calibrator_insts(params["calibrators"], params)

        return params

    @classmethod
    def get_known_shifts(
        cls,
        config_inst: od.Config,
        params: dict[str, Any],
        shifts: TaskShifts,
    ) -> None:
        """
        Updates the set of known *shifts* implemented by *this* and upstream tasks

        :param config_inst: Config instance.
        :param params: Dictionary of task parameters.
        :param shifts: TaskShifts object to adjust.
        """
        # get the calibrators, update them and add their shifts
        for calibrator_inst in params["calibrator_insts"]:
            shifts.upstream |= calibrator_inst.all_shifts

        super().get_known_shifts(config_inst, params, shifts)

    def _array_function_post_init(self, **kwargs) -> None:
        super()._array_function_post_init(**kwargs)
        for calibrator_inst in self.calibrator_insts or []:
            calibrator_inst.run_post_init(task=self, **kwargs)

    @property
    def calibrators_repr(self) -> str:
        """
        Return a string representation of the calibrators.
        """
        calibs_repr = "none"
        if self.calibrators:
            reprs = list(map(str, self.calibrator_insts))
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


class SelectorClassMixin(ConfigTask):
    """
    Mixin to include and access single :py:class:`~columnflow.selection.Selector` class.
    """

    selector = luigi.Parameter(
        default=RESOLVE_DEFAULT,
        description="the name of the selector to be applied; default: value of the "
        "'default_selector' config",
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
    def resolve_param_values(cls, params: dict[str, Any]) -> dict[str, Any]:
        params = super().resolve_param_values(params)

        if (config_inst := params.get("config_inst")):
            # resolve the default class if necessary
            params["selector"] = cls.resolve_config_default(
                params,
                params.get("selector"),
                container=config_inst,
                default_str="default_selector",
                multiple=False,
            )

            # apply selector_steps_groups and default_selector_steps from config
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
        sel_repr = str(self.selector)
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

    selector_inst = TaskArrayFunctionParameter(
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
    def resolve_param_values(cls, params: dict[str, Any]) -> dict[str, Any]:
        params = super().resolve_param_values(params)

        # add the selector instance
        if not params.get("selector_inst"):
            params["selector_inst"] = cls.build_selector_inst(params["selector"], params)

        return params

    @classmethod
    def get_known_shifts(
        cls,
        config_inst: od.Config,
        params: dict[str, Any],
        shifts: TaskShifts,
    ) -> None:
        """
        Updates the set of known *shifts* implemented by *this* and upstream tasks

        :param config_inst: Config instance.
        :param params: Dictionary of task parameters.
        :param shifts: TaskShifts object to adjust.
        """
        # get the selector, update it and add its shifts
        selector_shifts = params["selector_inst"].all_shifts
        (shifts.local if cls.invokes_selector else shifts.upstream).update(selector_shifts)

        super().get_known_shifts(config_inst, params, shifts)

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

        # overwrite the sandbox when set
        if self.invokes_selector and (sandbox := self.selector_inst.get_sandbox()):
            self.reset_sandbox(sandbox)

    def _array_function_post_init(self, **kwargs) -> None:
        super()._array_function_post_init(**kwargs)
        self.selector_inst.run_post_init(task=self, **kwargs)

    @property
    def selector_repr(self) -> str:
        """
        Return a string representation of the selector instance.
        """
        sel_repr = str(self.selector_inst)
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


class ProducerClassMixin(ConfigTask):
    """
    Mixin to include and access single :py:class:`~columnflow.production.Producer` class.
    """

    producer = luigi.Parameter(
        default=RESOLVE_DEFAULT,
        description="the name of the producer to be applied; default: value of the "
        "'default_producer' config",
    )

    @classmethod
    def resolve_param_values(cls, params: dict[str, Any]) -> dict[str, Any]:
        params = super().resolve_param_values(params)

        # resolve the default class if necessary
        if (config_inst := params.get("config_inst")):
            params["producer"] = cls.resolve_config_default(
                params,
                params.get("producer"),
                container=config_inst,
                default_str="default_producer",
                multiple=False,
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
        return str(self.producer)

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

    producer_inst = TaskArrayFunctionParameter(
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
    def resolve_param_values(cls, params: dict[str, Any]) -> dict[str, Any]:
        params = super().resolve_param_values(params)

        # add the producer instance
        if not params.get("producer_inst"):
            params["producer_inst"] = cls.build_producer_inst(params["producer"], params)

        return params

    @classmethod
    def get_known_shifts(
        cls,
        config_inst: od.Config,
        params: dict[str, Any],
        shifts: TaskShifts,
    ) -> None:
        """
        Updates the set of known *shifts* implemented by *this* and upstream tasks

        :param config_inst: Config instance.
        :param params: Dictionary of task parameters.
        :param shifts: TaskShifts object to adjust.
        """
        # get the producer, update it and add its shifts
        producer_shifts = params["producer_inst"].all_shifts
        (shifts.local if cls.invokes_producer else shifts.upstream).update(producer_shifts)

        super().get_known_shifts(config_inst, params, shifts)

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

        # overwrite the sandbox when set
        if self.invokes_producer and (sandbox := self.producer_inst.get_sandbox()):
            self.reset_sandbox(sandbox)

    def _array_function_post_init(self, **kwargs) -> None:
        super()._array_function_post_init(**kwargs)
        self.producer_inst.run_post_init(task=self, **kwargs)

    @property
    def producer_repr(self) -> str:
        """
        Return a string representation of the producer instance.
        """
        return str(self.producer_inst)

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


class ProducerClassesMixin(ConfigTask):
    """
    Mixin to include and access multiple :py:class:`~columnflow.production.Producer` classes.
    """

    producers = law.CSVParameter(
        default=(RESOLVE_DEFAULT,),
        description="comma-separated names of producers to be applied; default: value of the "
        "'default_producer' config",
        brace_expand=True,
        parse_empty=True,
    )

    @classmethod
    def resolve_param_values(
        cls,
        params: law.util.InsertableDict[str, Any],
    ) -> law.util.InsertableDict[str, Any]:
        params = super().resolve_param_values(params)

        # resolve the default classes if necessary
        if (config_inst := params.get("config_inst")):
            params["producers"] = cls.resolve_config_default_and_groups(
                params,
                params.get("producers"),
                container=config_inst,
                default_str="default_producer",
                groups_str="producer_groups",
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
            reprs = list(map(str, self.producers))
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

    producer_insts = TaskArrayFunctionsParameter(
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
    def resolve_param_values(cls, params: dict[str, Any]) -> dict[str, Any]:
        params = super().resolve_param_values(params)

        # add the producer instances
        if not params.get("producer_insts"):
            params["producer_insts"] = cls.build_producer_insts(params["producers"], params)

        return params

    @classmethod
    def get_known_shifts(
        cls,
        config_inst: od.Config,
        params: dict[str, Any],
        shifts: TaskShifts,
    ) -> None:
        """
        Updates the set of known *shifts* implemented by *this* and upstream tasks

        :param config_inst: Config instance.
        :param params: Dictionary of task parameters.
        :param shifts: TaskShifts object to adjust.
        """
        # get the producers, update them and add their shifts
        for producer_inst in params["producer_insts"]:
            shifts.upstream |= producer_inst.all_shifts

        super().get_known_shifts(config_inst, params, shifts)

    def _array_function_post_init(self, **kwargs) -> None:
        super()._array_function_post_init(**kwargs)
        for producer_inst in self.producer_insts or []:
            producer_inst.run_post_init(task=self, **kwargs)

    @property
    def producers_repr(self) -> str:
        """
        Return a string representation of the producers.
        """
        prods_repr = "none"
        if self.producers:
            reprs = list(map(str, self.producer_insts))
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


# class MLModelMixinBase(AnalysisTask):
#     """
#     Base mixin to include a machine learning application into tasks.

#     Inheriting from this mixin will allow a task to instantiate and access a
#     :py:class:`~columnflow.ml.MLModel` instance with name *ml_model*, which is an input parameter
#     for this task.
#     """

#     ml_model = luigi.Parameter(
#         description="the name of the ML model to be applied",
#     )
#     ml_model_settings = SettingsParameter(
#         default=DotDict(),
#         description="settings passed to the init function of the ML model",
#     )

#     exclude_params_repr_empty = {"ml_model"}

#     @property
#     def ml_model_repr(self):
#         """
#         Returns a string representation of the ML model instance.
#         """
#         return str(self.ml_model_inst)

#     @classmethod
#     def req_params(cls, inst: law.Task, **kwargs) -> dict[str, Any]:
#         """
#         Get the required parameters for the task, preferring the ``--ml-model`` set on task-level
#         via CLI.

#         This method first checks if the ``--ml-model`` parameter is set at the task-level via the command line.
#         If it is, this parameter is preferred and added to the '_prefer_cli' key in the kwargs dictionary.
#         The method then calls the 'req_params' method of the superclass with the updated kwargs.

#         :param inst: The current task instance.
#         :param kwargs: Additional keyword arguments that may contain parameters for the task.
#         :return: A dictionary of parameters required for the task.
#         """
#         # prefer --ml-model set on task-level via cli
#         kwargs["_prefer_cli"] = law.util.make_set(kwargs.get("_prefer_cli", [])) | {"ml_model"}

#         return super().req_params(inst, **kwargs)

#     @classmethod
#     def get_ml_model_inst(
#         cls,
#         ml_model: str,
#         analysis_inst: od.Analysis,
#         requested_configs: list[str] | None = None,
#         **kwargs,
#     ) -> MLModel:
#         """
#         Get requested *ml_model* instance.

#         This method retrieves the requested *ml_model* instance.
#         If *requested_configs* are provided, they are used for the training of
#         the ML application.

#         :param ml_model: Name of :py:class:`~columnflow.ml.MLModel` to load.
#         :param analysis_inst: Forward this analysis inst to the init function of new MLModel sub class.
#         :param requested_configs: Configs needed for the training of the ML application.
#         :param kwargs: Additional keyword arguments to forward to the :py:class:`~columnflow.ml.MLModel` instance.
#         :return: :py:class:`~columnflow.ml.MLModel` instance.
#         """
#         ml_model_inst: MLModel = MLModel.get_cls(ml_model)(analysis_inst, **kwargs)

#         if requested_configs:
#             configs = ml_model_inst.training_configs(list(requested_configs))
#             if configs:
#                 ml_model_inst._setup(configs)

#         return ml_model_inst

#     def events_used_in_training(
#         self,
#         config_inst: od.config.Config,
#         dataset_inst: od.dataset.Dataset,
#         shift_inst: od.shift.Shift,
#     ) -> bool:
#         """
#         Evaluate whether the events for the combination of *dataset_inst* and
#         *shift_inst* shall be used in the training.

#         This method checks if the *dataset_inst* is in the set of datasets of
#         the current `ml_model_inst` based on the given *config_inst*. Additionally,
#         the function checks that the *shift_inst* does not have the tag
#         `"disjoint_from_nominal"`.

#         :param config_inst: The configuration instance.
#         :param dataset_inst: The dataset instance.
#         :param shift_inst: The shift instance.
#         :return: True if the events shall be used in the training, False otherwise.
#         """
#         # evaluate whether the events for the combination of dataset_inst and shift_inst
#         # shall be used in the training
#         return (
#             dataset_inst in self.ml_model_inst.datasets(config_inst) and
#             not shift_inst.has_tag("disjoint_from_nominal")
#         )


# class MLModelTrainingMixin(MLModelMixinBase):
#     """
#     A mixin class for training machine learning models.

#     This class provides parameters for configuring the training of machine learning models.
#     """

#     configs = law.CSVParameter(
#         default=(),
#         description="comma-separated names of analysis config to use; should only contain a single "
#         "name in case the ml model is bound to a single config; when empty, the ml model is "
#         "expected to fully define the configs it uses; empty default",
#         brace_expand=True,
#         parse_empty=True,
#     )
#     calibrators = law.MultiCSVParameter(
#         default=(),
#         description="multiple comma-separated sequences of names of calibrators to apply, "
#         "separated by ':'; each sequence corresponds to a config in --configs; when empty, the "
#         "'default_calibrator' setting of each config is used if set, or the model is expected to "
#         "fully define the calibrators it requires upstream; empty default",
#         brace_expand=True,
#         parse_empty=True,
#     )
#     selectors = law.CSVParameter(
#         default=(),
#         description="comma-separated names of selectors to apply; each selector corresponds to a "
#         "config in --configs; when empty, the 'default_selector' setting of each config is used if "
#         "set, or the ml model is expected to fully define the selector it uses requires upstream; "
#         "empty default",
#         brace_expand=True,
#         parse_empty=True,
#     )
#     producers = law.MultiCSVParameter(
#         default=(),
#         description="multiple comma-separated sequences of names of producers to apply, "
#         "separated by ':'; each sequence corresponds to a config in --configs; when empty, the "
#         "'default_producer' setting of each config is used if set, or ml model is expected to "
#         "fully define the producers it requires upstream; empty default",
#         brace_expand=True,
#         parse_empty=True,
#     )

#     @classmethod
#     def resolve_calibrators(
#         cls,
#         ml_model_inst: MLModel,
#         params: dict[str, Any],
#     ) -> tuple[tuple[str]]:
#         """
#         Resolve the calibrators for the given ML model instance.

#         This method retrieves the calibrators from the parameters *params* and
#         broadcasts them to the configs if necessary.
#         It also resolves `calibrator_groups` and `default_calibrator` from the config(s) associated
#         with this ML model instance, and validates the number of sequences.
#         Finally, it checks the retrieved calibrators against
#         the training calibrators of the model using
#         :py:meth:`~columnflow.ml.MLModel.training_calibrators` and instantiates them if necessary.

#         :param ml_model_inst: The ML model instance.
#         :param params: A dictionary of parameters that may contain the calibrators.
#         :return: A tuple of tuples containing the resolved calibrators.
#         :raises Exception: If the number of calibrator sequences does not match
#             the number of configs used by the ML model.
#         """
#         calibrators: Union[tuple[str], tuple[tuple[str]]] = params.get("calibrators") or ((),)

#         # broadcast to configs
#         n_configs = len(ml_model_inst.config_insts)
#         if len(calibrators) == 1 and n_configs != 1:
#             calibrators = tuple(calibrators * n_configs)

#         # apply calibrators_groups and default_calibrator from the config
#         calibrators = tuple(
#             ConfigTask.resolve_config_default_and_groups(
#                 params,
#                 calibrators[i],
#                 container=config_inst,
#                 default_str="default_calibrator",
#                 groups_str="calibrator_groups",
#             )
#             for i, config_inst in enumerate(ml_model_inst.config_insts)
#         )

#         # validate number of sequences
#         if len(calibrators) != n_configs:
#             raise Exception(
#                 f"MLModel '{ml_model_inst.cls_name}' uses {n_configs} configs but received "
#                 f"{len(calibrators)} calibrator sequences",
#             )

#         # final check by model
#         calibrators = tuple(
#             tuple(ml_model_inst.training_calibrators(config_inst, list(_calibrators)))
#             for config_inst, _calibrators in zip(ml_model_inst.config_insts, calibrators)
#         )

#         # instantiate them once
#         for config_inst, _calibrators in zip(ml_model_inst.config_insts, calibrators):
#             init_kwargs = law.util.merge_dicts(params, {"config_inst": config_inst})
#             for calibrator in _calibrators:
#                 CalibratorMixin.get_calibrator_inst(calibrator, kwargs=init_kwargs)

#         return calibrators

#     @classmethod
#     def resolve_selectors(
#         cls,
#         ml_model_inst: MLModel,
#         params: dict[str, Any],
#     ) -> tuple[str]:
#         """
#         Resolve the selectors for the given ML model instance.

#         This method retrieves the selectors from the parameters *params* and
#         broadcasts them to the configs if necessary.
#         It also resolves `default_selector` from the config(s) associated
#         with this ML model instance, validates the number of sequences.
#         Finally, it checks the retrieved selectors against the training selectors
#         of the model, using
#         :py:meth:`~columnflow.ml.MLModel.training_selector`, and instantiates them.

#         :param ml_model_inst: The ML model instance.
#         :param params: A dictionary of parameters that may contain the selectors.
#         :return: A tuple containing the resolved selectors.
#         :raises Exception: If the number of selector sequences does not match
#             the number of configs used by the ML model.
#         """
#         selectors = params.get("selectors") or (None,)

#         # broadcast to configs
#         n_configs = len(ml_model_inst.config_insts)
#         if len(selectors) == 1 and n_configs != 1:
#             selectors = tuple(selectors * n_configs)

#         # use config defaults
#         selectors = tuple(
#             ConfigTask.resolve_config_default(
#                 params,
#                 selectors[i],
#                 container=config_inst,
#                 default_str="default_selector",
#                 multiple=False,
#             )
#             for i, config_inst in enumerate(ml_model_inst.config_insts)
#         )

#         # validate sequence length
#         if len(selectors) != n_configs:
#             raise Exception(
#                 f"MLModel '{ml_model_inst.cls_name}' uses {n_configs} configs but received "
#                 f"{len(selectors)} selectors",
#             )

#         # final check by model
#         selectors = tuple(
#             ml_model_inst.training_selector(config_inst, selector)
#             for config_inst, selector in zip(ml_model_inst.config_insts, selectors)
#         )

#         # instantiate them once
#         for config_inst, selector in zip(ml_model_inst.config_insts, selectors):
#             init_kwargs = law.util.merge_dicts(params, {"config_inst": config_inst})
#             SelectorMixin.get_selector_inst(selector, kwargs=init_kwargs)

#         return selectors

#     @classmethod
#     def resolve_producers(
#         cls,
#         ml_model_inst: MLModel,
#         params: dict[str, Any],
#     ) -> tuple[tuple[str]]:
#         """
#         Resolve the producers for the given ML model instance.

#         This method retrieves the producers from the parameters *params* and
#         broadcasts them to the configs if necessary.
#         It also resolves `producer_groups` and `default_producer` from the config(s) associated
#         with this ML model instance, validates the number of sequences.
#         Finally, it checks the retrieved producers against the training producers
#         of the model, using
#         :py:meth:`~columnflow.ml.MLModel.training_producers`, and instantiates them.

#         :param ml_model_inst: The ML model instance.
#         :param params: A dictionary of parameters that may contain the producers.
#         :return: A tuple of tuples containing the resolved producers.
#         :raises Exception: If the number of producer sequences does not match
#             the number of configs used by the ML model.
#         """
#         producers = params.get("producers") or ((),)

#         # broadcast to configs
#         n_configs = len(ml_model_inst.config_insts)
#         if len(producers) == 1 and n_configs != 1:
#             producers = tuple(producers * n_configs)

#         # apply producers_groups and default_producer from the config
#         producers = tuple(
#             ConfigTask.resolve_config_default_and_groups(
#                 params,
#                 producers[i],
#                 container=config_inst,
#                 default_str="default_producer",
#                 groups_str="producer_groups",
#             )
#             for i, config_inst in enumerate(ml_model_inst.config_insts)
#         )

#         # validate number of sequences
#         if len(producers) != n_configs:
#             raise Exception(
#                 f"MLModel '{ml_model_inst.cls_name}' uses {n_configs} configs but received "
#                 f"{len(producers)} producer sequences",
#             )

#         # final check by model
#         producers = tuple(
#             tuple(ml_model_inst.training_producers(config_inst, list(_producers)))
#             for config_inst, _producers in zip(ml_model_inst.config_insts, producers)
#         )

#         # instantiate them once
#         for config_inst, _producers in zip(ml_model_inst.config_insts, producers):
#             init_kwargs = law.util.merge_dicts(params, {"config_inst": config_inst})
#             for producer in _producers:
#                 ProducerMixin.get_producer_inst(producer, kwargs=init_kwargs)

#         return producers

#     @classmethod
#     def resolve_param_values(cls, params: dict[str, Any]) -> dict[str, Any]:
#         """
#         Resolve the parameter values for the given parameters.

#         This method retrieves the parameters and resolves the ML model instance, configs,
#         calibrators, selectors, and producers. It also calls the model's setup hook.

#         :param params: A dictionary of parameters that may contain the analysis instance and ML model.
#         :return: A dictionary containing the resolved parameters.
#         :raises Exception: If the ML model instance received configs to define training configs,
#             but did not define any.
#         """
#         params = super().resolve_param_values(params)

#         if "analysis_inst" in params and "ml_model" in params:
#             analysis_inst = params["analysis_inst"]

#             # NOTE: we could try to implement resolving the default ml_model here
#             ml_model_inst = cls.get_ml_model_inst(
#                 params["ml_model"],
#                 analysis_inst,
#                 parameters=params["ml_model_settings"],
#             )
#             params["ml_model_inst"] = ml_model_inst

#             # resolve configs
#             _configs = params.get("configs", ())
#             params["configs"] = tuple(ml_model_inst.training_configs(list(_configs)))
#             if not params["configs"]:
#                 raise Exception(
#                     f"MLModel '{ml_model_inst.cls_name}' received configs '{_configs}' to define "
#                     "training configs, but did not define any",
#                 )
#             ml_model_inst._set_configs(params["configs"])

#             # resolve calibrators
#             params["calibrators"] = cls.resolve_calibrators(ml_model_inst, params)

#             # resolve selectors
#             params["selectors"] = cls.resolve_selectors(ml_model_inst, params)

#             # resolve producers
#             params["producers"] = cls.resolve_producers(ml_model_inst, params)

#             # call the model's setup hook
#             ml_model_inst._setup()

#         return params

#     def __init__(self, *args, **kwargs):
#         super().__init__(*args, **kwargs)
#         # get the ML model instance
#         self.ml_model_inst = self.get_ml_model_inst(
#             self.ml_model,
#             self.analysis_inst,
#             configs=list(self.configs),
#             parameters=self.ml_model_settings,
#         )

#     def store_parts(self) -> law.util.InsertableDict:
#         """
#         :return: Dictionary with parts that will be translated into an output directory path.
#         """
#         parts = super().store_parts()

#         # since MLTraining is no CalibratorsMixin, SelectorMixin, ProducerMixin, ConfigTask,
#         # all these parts are missing in the `store_parts`

#         configs_repr = "__".join(self.configs[:5])

#         if len(self.configs) > 5:
#             configs_repr += f"_{law.util.create_hash(self.configs[5:])}"

#         parts.insert_after("task_family", "configs", configs_repr)

#         for label, fct_names in [
#             ("calib", self.calibrators),
#             ("sel", tuple((sel,) for sel in self.selectors)),
#             ("prod", self.producers),
#         ]:
#             if not fct_names or not any(fct_names):
#                 fct_names = ["none"]
#             elif len(set(fct_names)) == 1:
#                 # when functions are the same per config, only use them once
#                 fct_names = fct_names[0]
#                 n_fct_per_config = str(len(fct_names))
#             else:
#                 # when functions differ between configs, flatten
#                 n_fct_per_config = "".join(str(len(x)) for x in fct_names)
#                 fct_names = tuple(fct_name for fct_names_cfg in fct_names for fct_name in fct_names_cfg)

#             part = "__".join(fct_names[:2])

#             if len(fct_names) > 2:
#                 part += f"_{n_fct_per_config}_{law.util.create_hash(fct_names[2:])}"

#             parts.insert_after(self.config_store_anchor, label, f"{label}__{part}")

#         if self.ml_model_inst:
#             parts.insert_after(self.config_store_anchor, "ml_model", f"ml__{self.ml_model_repr}")

#         return parts


# class MLModelMixin(ConfigTask, MLModelMixinBase):

#     ml_model = luigi.Parameter(
#         default=RESOLVE_DEFAULT,
#         description="the name of the ML model to be applied; default: value of the "
#         "'default_ml_model' config",
#     )

#     allow_empty_ml_model = True

#     exclude_params_repr_empty = {"ml_model"}

#     @classmethod
#     def resolve_param_values(cls, params: dict[str, Any]) -> dict[str, Any]:
#         params = super().resolve_param_values(params)

#         # add the default ml model when empty
#         if "analysis_inst" in params and "config_inst" in params:
#             analysis_inst = params["analysis_inst"]
#             config_inst = params["config_inst"]

#             params["ml_model"] = cls.resolve_config_default(
#                 params,
#                 params.get("ml_model"),
#                 container=config_inst,
#                 default_str="default_ml_model",
#                 multiple=False,
#             )

#             # initialize it once to trigger its set_config hook which might, in turn,
#             # add objects to the config itself
#             if params.get("ml_model") not in (None, law.NO_STR):
#                 params["ml_model_inst"] = cls.get_ml_model_inst(
#                     params["ml_model"],
#                     analysis_inst,
#                     requested_configs=[config_inst],
#                     parameters=params["ml_model_settings"],
#                 )
#             elif not cls.allow_empty_ml_model:
#                 raise Exception(f"no ml_model configured for {cls.task_family}")

#         return params

#     def __init__(self, *args, **kwargs):
#         super().__init__(*args, **kwargs)

#         # get the ML model instance
#         self.ml_model_inst = None
#         if self.ml_model != law.NO_STR:
#             self.ml_model_inst = self.get_ml_model_inst(
#                 self.ml_model,
#                 self.analysis_inst,
#                 requested_configs=[self.config_inst],
#                 parameters=self.ml_model_settings,
#             )

#     def store_parts(self) -> law.util.InsertableDict:
#         """
#         :return: Dictionary with parts that will be translated into an output directory path.
#         """
#         parts = super().store_parts()

#         if self.ml_model_inst:
#             parts.insert_after(self.config_store_anchor, "ml_model", f"ml__{self.ml_model_repr}")

#         return parts

#     def find_keep_columns(self, collection: ColumnCollection) -> set[Route]:
#         columns = super().find_keep_columns(collection)

#         if collection == ColumnCollection.ALL_FROM_ML_EVALUATION and self.ml_model_inst:
#             columns |= set.union(*self.ml_model_inst.produced_columns().values())

#         return columns


# class MLModelDataMixin(MLModelMixin):

#     allow_empty_ml_model = False

#     def store_parts(self) -> law.util.InsertableDict:
#         """
#         :return: Dictionary with parts that will be translated into an output directory path.
#         """
#         parts = super().store_parts()

#         # replace the ml_model entry
#         store_name = self.ml_model_inst.store_name or self.ml_model_repr
#         parts.insert_after(self.config_store_anchor, "ml_data", f"ml__{store_name}")
#         parts.pop("ml_model")

#         return parts


# class MLModelsMixin(ConfigTask):

#     ml_models = law.CSVParameter(
#         default=(RESOLVE_DEFAULT,),
#         description="comma-separated names of ML models to be applied; default: value of the "
#         "'default_ml_model' config",
#         brace_expand=True,
#         parse_empty=True,
#     )

#     allow_empty_ml_models = True

#     exclude_params_repr_empty = {"ml_models"}

#     @property
#     def ml_models_repr(self):
#         """Returns a string representation of the ML models."""
#         ml_models_repr = "__".join([str(model_inst) for model_inst in self.ml_model_insts])
#         return ml_models_repr

#     @classmethod
#     def resolve_param_values(cls, params: dict[str, Any]) -> dict[str, Any]:
#         params = super().resolve_param_values(params)

#         analysis_inst = params.get("analysis_inst")
#         config_inst = params.get("config_inst")
#         if analysis_inst and config_inst:
#             # apply ml_model_groups and default_ml_model from the config
#             params["ml_models"] = cls.resolve_config_default_and_groups(
#                 params,
#                 params.get("ml_models"),
#                 container=config_inst,
#                 default_str="default_ml_model",
#                 groups_str="ml_model_groups",
#             )

#             # special case: initialize them once to trigger their set_config hook
#             if params.get("ml_models"):
#                 params["ml_model_insts"] = [
#                     MLModelMixinBase.get_ml_model_inst(
#                         ml_model,
#                         analysis_inst,
#                         requested_configs=[config_inst],
#                     )
#                     for ml_model in params["ml_models"]
#                 ]
#             elif not cls.allow_empty_ml_models:
#                 raise Exception(f"no ml_models configured for {cls.task_family}")

#         return params

#     @classmethod
#     def req_params(cls, inst: law.Task, **kwargs) -> dict:
#         # prefer --ml-models set on task-level via cli
#         kwargs["_prefer_cli"] = law.util.make_set(kwargs.get("_prefer_cli", [])) | {"ml_models"}

#         return super().req_params(inst, **kwargs)

#     def __init__(self, *args, **kwargs):
#         super().__init__(*args, **kwargs)

#         # get the ML model instances
#         self.ml_model_insts = [
#             MLModelMixinBase.get_ml_model_inst(
#                 ml_model,
#                 self.analysis_inst,
#                 requested_configs=[self.config_inst],
#             )
#             for ml_model in self.ml_models
#         ]

#     def store_parts(self) -> law.util.InsertableDict:
#         """
#         :return: Dictionary with parts that will be translated into an output directory path.
#         """
#         parts = super().store_parts()

#         if self.ml_model_insts:
#             parts.insert_after(self.config_store_anchor, "ml_models", f"ml__{self.ml_models_repr}")

#         return parts

#     def find_keep_columns(self, collection: ColumnCollection) -> set[Route]:
#         columns = super().find_keep_columns(collection)

#         if collection == ColumnCollection.ALL_FROM_ML_EVALUATION:
#             columns |= set.union(*(
#                 set.union(*model_inst.produced_columns().values())
#                 for model_inst in self.ml_model_insts
#             ))

#         return columns


class WeightProducerClassMixin(ConfigTask):
    """
    Mixin to include and access single :py:class:`~columnflow.weight.WeightProducer` class.
    """

    weight_producer = luigi.Parameter(
        default=RESOLVE_DEFAULT,
        description="the name of the weight producer to be applied; default: value of the "
        "'default_weight_producer' config",
    )

    @classmethod
    def resolve_param_values(cls, params: dict[str, Any]) -> dict[str, Any]:
        params = super().resolve_param_values(params)

        # resolve the default class if necessary
        if (config_inst := params.get("config_inst")):
            params["weight_producer"] = cls.resolve_config_default(
                params,
                params.get("weight_producer"),
                container=config_inst,
                default_str="default_weight_producer",
                multiple=False,
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
        return str(self.weight_producer)

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

    weight_producer_inst = TaskArrayFunctionParameter(
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
    def resolve_param_values(cls, params: dict[str, Any]) -> dict[str, Any]:
        params = super().resolve_param_values(params)

        # add the weight producer instance
        if not params.get("weight_producer_inst"):
            params["weight_producer_inst"] = cls.build_weight_producer_inst(
                params["weight_producer"],
                params,
            )

        return params

    @classmethod
    def get_known_shifts(
        cls,
        config_inst: od.Config,
        params: dict[str, Any],
        shifts: TaskShifts,
    ) -> None:
        """
        Updates the set of known *shifts* implemented by *this* and upstream tasks

        :param config_inst: Config instance.
        :param params: Dictionary of task parameters.
        :param shifts: TaskShifts object to adjust.
        """
        # get the weight producer, update it and add its shifts
        weight_producer_shifts = params["weight_producer_inst"].all_shifts
        (shifts.local if cls.invokes_weight_producer else shifts.upstream).update(weight_producer_shifts)

        super().get_known_shifts(config_inst, params, shifts)

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

        # overwrite the sandbox when set
        if self.invokes_weight_producer and (sandbox := self.weight_producer_inst.get_sandbox()):
            self.reset_sandbox(sandbox)

    def _array_function_post_init(self, **kwargs) -> None:
        super()._array_function_post_init(**kwargs)
        self.weight_producer_inst.run_post_init(task=self, **kwargs)

    @property
    def weight_producer_repr(self) -> str:
        """
        Return a string representation of the weight producer instance.
        """
        return str(self.weight_producer_inst)


class InferenceModelClassMixin(ConfigTask):

    inference_model = luigi.Parameter(
        default=RESOLVE_DEFAULT,
        description="the name of the inference model to be used; default: value of the "
        "'default_inference_model' config",
    )

    @classmethod
    def resolve_param_values(cls, params: dict[str, Any]) -> dict[str, Any]:
        params = super().resolve_param_values(params)

        # add the default inference model when empty
        if (config_inst := params.get("config_inst")):
            params["inference_model"] = cls.resolve_config_default(
                params,
                params.get("inference_model"),
                container=config_inst,
                default_str="default_inference_model",
                multiple=False,
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

    @classmethod
    def get_inference_model_inst(
        cls,
        inference_model: str,
        config_inst: od.Config,
    ) -> InferenceModel:
        return InferenceModel.get_cls(inference_model)(config_inst)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # get the inference model instance
        self.inference_model_inst = self.get_inference_model_inst(self.inference_model, self.config_inst)


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
    def resolve_param_values(cls, params: dict[str, Any]) -> dict[str, Any]:
        params = super().resolve_param_values(params)

        # resolve categories
        if (config_inst := params.get("config_inst")) and "categories" in params:
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
    def categories_repr(self) -> str:
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
    allow_missing_variables = False

    @classmethod
    def resolve_param_values(cls, params: dict[str, Any]) -> dict[str, Any]:
        params = super().resolve_param_values(params)

        # resolve variables
        if (config_inst := params.get("config_inst")) and "variables" in params:
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
                    strict=not cls.allow_missing_variables,
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
                            strict=not cls.allow_missing_variables,
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

        # resolve processes
        if (config_inst := params.get("config_inst")) and "processes" in params:
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
        if config_inst and "datasets" in params:
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
    def get_known_shifts(
        cls,
        config_inst: od.Config,
        params: dict[str, Any],
        shifts: TaskShifts,
    ) -> None:
        """
        Updates the set of known *shifts* implemented by *this* and upstream tasks

        :param config_inst: Config instance.
        :param params: Dictionary of task parameters.
        :param shifts: TaskShifts object to adjust.
        """
        # add shifts of all datasets to upstream ones
        for dataset_inst in params.get("dataset_insts") or []:
            if dataset_inst.is_mc:
                shifts.upstream |= set(dataset_inst.info.keys())

        super().get_known_shifts(config_inst, params, shifts)

    @property
    def datasets_repr(self) -> str:
        if len(self.datasets) == 1:
            return self.datasets[0]
        return f"{len(self.datasets)}_{law.util.create_hash(sorted(self.datasets))}"

    @property
    def processes_repr(self) -> str:
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
    shift_validation_task_cls = None

    @classmethod
    def resolve_param_values(cls, params: dict[str, Any]) -> dict[str, Any]:
        params = super().resolve_param_values(params)

        # resolve shift sources
        if (config_inst := params.get("config_inst")) and "shift_sources" in params:
            # convert to full shift first to do the object finding
            shifts = cls.find_config_objects(
                cls.expand_shift_sources(params["shift_sources"]),
                config_inst,
                od.Shift,
                config_inst.x("shift_groups", {}),
            )

            # convert back to sources
            sources = cls.reduce_shifts(shifts)

            # when a validation task is set, is it to validate and reduce the requested shifts
            if cls.shift_validation_task_cls:
                sources = [
                    source for source in sources
                    if cls.validate_shift_source(params, source)
                ]

            # complain when no shifts were found
            if not shifts and not cls.allow_empty_shift_sources:
                raise ValueError(f"no shifts found matching {params['shift_sources']}")

            # convert back to sources
            params["shift_sources"] = tuple(sources)

        return params

    @classmethod
    def validate_shift_source(cls, params: dict[str, Any], source: str) -> bool:
        if not cls.shift_validation_task_cls:
            return True

        # run the task's parameter validation using the up shift
        _params = params | {"shift": f"{source}_up"}
        cls.shift_validation_task_cls.modify_param_values(_params)
        return _params["global_shift_inst"].source == source

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

    # allow only running on nominal
    allow_empty_shift_sources = True


class DatasetsProcessesShiftSourcesMixin(ShiftSourcesMixin, DatasetsProcessesMixin):

    @classmethod
    def validate_shift_source(cls, params: dict[str, Any], source: str) -> bool:
        if not cls.shift_validation_task_cls:
            return True

        # run the task's parameter validation using the up shift and all datasets
        for dataset in params["datasets"]:
            _params = params | {"shift": f"{source}_up", "dataset": dataset}
            cls.shift_validation_task_cls.modify_param_values(_params)
            if _params["global_shift_inst"].source == source:
                return True

        return False


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

    def invoke_hist_hooks(self, hists: dict) -> dict:
        """
        Invoke hooks to update histograms before plotting.
        """
        if not self.hist_hooks:
            return hists

        for hook in self.hist_hooks:
            if hook in {None, "", law.NO_STR}:
                continue

            # get the hook from the config instance
            hooks = self.config_inst.x("hist_hooks", {})
            if hook not in hooks:
                raise KeyError(
                    f"hist hook '{hook}' not found in 'hist_hooks' auxiliary entry of config",
                )
            func = hooks[hook]
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
        hooks = [hook for hook in self.hist_hooks if hook not in (None, "", law.NO_STR)]

        hooks_repr = "__".join(hooks[:5])
        if len(hooks) > 5:
            hooks_repr += f"__{law.util.create_hash(hooks[5:])}"

        return hooks_repr
