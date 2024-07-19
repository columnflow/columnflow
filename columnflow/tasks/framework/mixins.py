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

from columnflow.types import Sequence, Any, Iterable, Union
from columnflow.tasks.framework.base import AnalysisTask, ConfigTask, RESOLVE_DEFAULT
from columnflow.tasks.framework.parameters import SettingsParameter
from columnflow.calibration import Calibrator
from columnflow.selection import Selector
from columnflow.production import Producer
from columnflow.weight import WeightProducer
from columnflow.ml import MLModel
from columnflow.inference import InferenceModel
from columnflow.columnar_util import Route, ColumnCollection, ChunkedIOHandler
from columnflow.util import maybe_import, DotDict

ak = maybe_import("awkward")


logger = law.logger.get_logger(__name__)


class CalibratorMixin(ConfigTask):
    """
    Mixin to include a single :py:class:`~columnflow.calibration.Calibrator` into tasks.

    Inheriting from this mixin will give access to instantiate and access a
    :py:class:`~columnflow.calibration.Calibrator` instance with name *calibrator*,
    which is an input parameter for this task.
    """
    calibrator = luigi.Parameter(
        default=RESOLVE_DEFAULT,
        description="the name of the calibrator to be applied; default: value of the "
        "'default_calibrator' config",
    )
    calibrator.__annotations__ = " ".join("""
        the name of the calibrator to be applied; default: value of the
        'default_calibrator' config""".split())

    # decides whether the task itself runs the calibrator and implements its shifts
    register_calibrator_sandbox = False
    register_calibrator_shifts = False

    @classmethod
    def get_calibrator_inst(cls, calibrator: str, kwargs=None) -> Calibrator:
        """
        Initialize :py:class:`~columnflow.calibration.Calibrator` instance.

        Extracts relevant *kwargs* for this calibrator instance using the
        :py:meth:`~columnflow.tasks.framework.base.AnalaysisTask.get_calibrator_kwargs`
        method.
        After this process, the previously initialized instance of a
        :py:class:`~columnflow.calibration.Calibrator` with the name
        *calibrator* is initialized using the
        :py:meth:`~columnflow.util.DerivableMeta.get_cls` method with the
        relevant keyword arguments.

        :param calibrator: Name of the calibrator instance
        :param kwargs: Any set keyword argument that is potentially relevant for
            this :py:class:`~columnflow.calibration.Calibrator` instance
        :raises RuntimeError: if requested :py:class:`~columnflow.calibration.Calibrator` instance
            is not :py:attr:`~columnflow.calibration.Calibrator.exposed`
        :return: The initialized :py:class:`~columnflow.calibration.Calibrator`
            instance.
        """
        calibrator_cls: Calibrator = Calibrator.get_cls(calibrator)
        if not calibrator_cls.exposed:
            raise RuntimeError(f"cannot use unexposed calibrator '{calibrator}' in {cls.__name__}")

        inst_dict = cls.get_calibrator_kwargs(**kwargs) if kwargs else None
        return calibrator_cls(inst_dict=inst_dict)

    @classmethod
    def resolve_param_values(cls, params: dict[str, Any]) -> dict[str, Any]:
        """
        Resolve parameter values *params* relevant for the
        :py:class:`CalibratorMixin` and all classes it inherits from.

        Loads the ``config_inst`` and loads the parameter ``"calibrator"``.
        In case the parameter is not found, defaults to ``"default_calibrator"``.
        Finally, this function adds the keyword ``"calibrator_inst"``, which
        contains the :py:class:`~columnflow.calibration.Calibrator` instance
        obtained using :py:meth:`~.CalibratorMixin.get_calibrator_inst` method.

        :param params: Dictionary with parameters provided by the user at
            commandline level.
        :return: Dictionary of parameters that now includes new value for
            ``"calibrator_inst"``.
        """
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
    def get_known_shifts(cls, config_inst: od.Config, params: dict[str, Any]) -> tuple[set[str], set[str]]:
        """
        Adds set of shifts that the current ``calibrator_inst`` registers to the
        set of known ``shifts`` and ``upstream_shifts``.

        First, the set of ``shifts`` and ``upstream_shifts`` are obtained from
        the *config_inst* and the current set of parameters *params* using the
        ``get_known_shifts`` methods of all classes that :py:class:`CalibratorMixin`
        inherits from.
        Afterwards, check if the current ``calibrator_inst`` registers shifts.
        If :py:attr:`~CalibratorMixin.register_calibrator_shifts` is ``True``,
        add them to the current set of ``shifts``. Otherwise, add the
        shifts obtained from the ``calibrator_inst`` to ``upstream_shifts``.

        :param config_inst: Config instance for the current task.
        :param params: Dictionary containing the current set of parameters provided
            by the user at commandline level
        :return: Tuple with updated sets of ``shifts`` and ``upstream_shifts``.
        """
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
    def req_params(cls, inst: law.Task, **kwargs) -> dict[str, Any]:
        """
        Returns the required parameters for the task.
        It prefers `--calibrator` set on task-level via command line.

        :param inst: The current task instance.
        :param kwargs: Additional keyword arguments.
        :return: Dictionary of required parameters.
        """
        # prefer --calibrator set on task-level via cli
        kwargs["_prefer_cli"] = law.util.make_set(kwargs.get("_prefer_cli", [])) | {"calibrator"}

        return super().req_params(inst, **kwargs)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # cache for calibrator inst
        self._calibrator_inst = None

    @property
    def calibrator_inst(self) -> Calibrator:
        """
        Access current :py:class:`~columnflow.calibration.Calibrator` instance.

        This method loads the current :py:class:`~columnflow.calibration.Calibrator`
        *calibrator_inst* from the cache or initializes it.
        If the calibrator requests a specific ``sandbox``, set this sandbox as
        the environment for the current :py:class:`~law.task.base.Task`.

        :return: Current :py:class:`~columnflow.calibration.Calibrator` instance
        """
        if self._calibrator_inst is None:
            self._calibrator_inst = self.get_calibrator_inst(self.calibrator, {"task": self})

            # overwrite the sandbox when set
            if self.register_calibrator_sandbox:
                sandbox = self._calibrator_inst.get_sandbox()
                if sandbox:
                    self.sandbox = sandbox
                    # rebuild the sandbox inst when already initialized
                    if self._sandbox_initialized:
                        self._initialize_sandbox(force=True)

        return self._calibrator_inst

    @property
    def calibrator_repr(self):
        """
        Return a string representation of the calibrator.
        """
        return str(self.calibrator_inst)

    def store_parts(self) -> law.util.InsertableDict[str, str]:
        """
        Create parts to create the output path to store intermediary results
        for the current :py:class:`~law.task.base.Task`.

        This method calls :py:meth:`store_parts` of the ``super`` class and inserts
        `{"calibrator": "calib__{self.calibrator}"}` before keyword ``version``.
        For more information, see e.g. :py:meth:`~columnflow.tasks.framework.base.ConfigTask.store_parts`.

        :return: Updated parts to create output path to store intermediary results.
        """
        parts = super().store_parts()
        parts.insert_before("version", "calibrator", f"calib__{self.calibrator_repr}")
        return parts

    def find_keep_columns(self: ConfigTask, collection: ColumnCollection) -> set[Route]:
        """
        Finds the columns to keep based on the *collection*.

        If the collection is `ALL_FROM_CALIBRATOR`, it includes the columns produced by the calibrator.

        :param collection: The collection of columns.
        :return: Set of columns to keep.
        """
        columns = super().find_keep_columns(collection)

        if collection == ColumnCollection.ALL_FROM_CALIBRATOR:
            columns |= self.calibrator_inst.produced_columns

        return columns


class CalibratorsMixin(ConfigTask):
    """
    Mixin to include multiple :py:class:`~columnflow.calibration.Calibrator` instances into tasks.

    Inheriting from this mixin will allow a task to instantiate and access a set of
    :py:class:`~columnflow.calibration.Calibrator` instances with names *calibrators*,
    which is a comma-separated list of calibrator names and is an input parameter for this task.
    """

    calibrators = law.CSVParameter(
        default=(RESOLVE_DEFAULT,),
        description="comma-separated names of calibrators to be applied; default: value of the "
        "'default_calibrator' config",
        brace_expand=True,
        parse_empty=True,
    )

    # decides whether the task itself runs the calibrators and implements their shifts
    register_calibrators_shifts = False

    @classmethod
    def get_calibrator_insts(cls, calibrators: Iterable[str], kwargs=None) -> list[Calibrator]:
        """
        Get all requested *calibrators*.

        :py:class:`~columnflow.calibration.Calibrator` instances are either
        initalized or loaded from cache.

        :param calibrators: Names of Calibrators to load
        :param kwargs: Additional keyword arguments to forward to individual
            :py:class:`~columnflow.calibration.Calibrator` instances
        :raises RuntimeError: if requested calibrators are not
            :py:attr:`~columnflow.calibration.Calibrator.exposed`
        :return: List of :py:class:`~columnflow.calibration.Calibrator` instances.
        """
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
    def resolve_param_values(
        cls,
        params: law.util.InsertableDict[str, Any],
    ) -> law.util.InsertableDict[str, Any]:
        """
        Resolve values *params* and check against possible default values and
        calibrator groups.

        Check the values in *params* against the default value ``"default_calibrator"``
        and possible group definitions ``"calibrator_groups"`` in the current config inst.
        For more information, see
        :py:meth:`~columnflow.tasks.framework.base.ConfigTask.resolve_config_default_and_groups`.

        :param params: Parameter values to resolve
        :return: Dictionary of parameters that contains the list requested
            :py:class:`~columnflow.calibration.Calibrator` instances under the
            keyword ``"calibrator_insts"``. See :py:meth:`~.CalibratorsMixin.get_calibrator_insts`
            for more information.
        """
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
    def get_known_shifts(
        cls,
        config_inst: od.Config,
        params: dict[str, Any],
    ) -> tuple[set[str], set[str]]:
        """
        Adds set of all shifts that the list of ``calibrator_insts`` register to the
        set of known ``shifts`` and ``upstream_shifts``.

        First, the set of ``shifts`` and ``upstream_shifts`` are obtained from
        the *config_inst* and the current set of parameters *params* using the
        ``get_known_shifts`` methods of all classes that :py:class:`CalibratorsMixin`
        inherits from.
        Afterwards, loop through the list of :py:class:`~columnflow.calibration.Calibrator`
        and check if they register shifts.
        If :py:attr:`~CalibratorsMixin.register_calibrators_shifts` is ``True``,
        add them to the current set of ``shifts``. Otherwise, add the
        shifts to ``upstream_shifts``.

        :param config_inst: Config instance for the current task.
        :param params: Dictionary containing the current set of parameters provided
            by the user at commandline level
        :return: Tuple with updated sets of ``shifts`` and ``upstream_shifts``.
        """
        shifts, upstream_shifts = super().get_known_shifts(config_inst, params)

        # get the calibrators, update them and add their shifts
        for calibrator_inst in params.get("calibrator_insts") or []:
            if cls.register_calibrators_shifts:
                shifts |= calibrator_inst.all_shifts
            else:
                upstream_shifts |= calibrator_inst.all_shifts

        return shifts, upstream_shifts

    @classmethod
    def req_params(cls, inst: law.Task, **kwargs) -> dict[str, Any]:
        """
        Returns the required parameters for the task.

        It prefers ``--calibrators`` set on task-level via command line.

        :param inst: The current task instance.
        :param kwargs: Additional keyword arguments.
        :return: Dictionary of required parameters.
        """

        # prefer --calibrators set on task-level via cli
        kwargs["_prefer_cli"] = law.util.make_set(kwargs.get("_prefer_cli", [])) | {"calibrators"}

        return super().req_params(inst, **kwargs)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # cache for calibrator insts
        self._calibrator_insts = None

    @property
    def calibrator_insts(self) -> list[Calibrator]:
        """
        Access current list of :py:class:`~columnflow.calibration.Calibrator` instances.

        Loads the current :py:class:`~columnflow.calibration.Calibrator` *calibrator_insts* from
        the cache or initializes it.

        :return: Current list :py:class:`~columnflow.calibration.Calibrator` instances
        """
        if self._calibrator_insts is None:
            self._calibrator_insts = self.get_calibrator_insts(self.calibrators, {"task": self})
        return self._calibrator_insts

    @property
    def calibrators_repr(self) -> str:
        """
        Return a string representation of the calibrators.
        """
        calibs_repr = "none"
        if self.calibrators:
            calibs_repr = "__".join([str(calib) for calib in self.calibrator_insts[:5]])
            if len(self.calibrators) > 5:
                calibs_repr += f"__{law.util.create_hash([str(calib) for calib in self.calibrator_insts[5:]])}"
        return calibs_repr

    def store_parts(self):
        """
        Create parts to create the output path to store intermediary results
        for the current :py:class:`~law.task.base.Task`.

        Calls :py:meth:`store_parts` of the ``super`` class and inserts
        `{"calibrator": "calib__{HASH}"}` before keyword ``version``.
        Here, ``HASH`` is the joint string of the first five calibrator names
        + a hash created with :py:meth:`law.util.create_hash` based on
        the list of calibrators, starting at its 5th element (i.e. ``self.calibrators[5:]``)
        For more information, see e.g. :py:meth:`~columnflow.tasks.framework.base.ConfigTask.store_parts`.

        :return: Updated parts to create output path to store intermediary results.
        """
        parts = super().store_parts()
        parts.insert_before("version", "calibrators", f"calib__{self.calibrators_repr}")
        return parts

    def find_keep_columns(self: ConfigTask, collection: ColumnCollection) -> set[Route]:
        """
        Finds the columns to keep based on the *collection*.

        If the collection is ``ALL_FROM_CALIBRATORS``, it includes the columns produced by the calibrators.

        :param collection: The collection of columns.
        :return: Set of columns to keep.
        """
        columns: set[Route] = super().find_keep_columns(collection)

        if collection == ColumnCollection.ALL_FROM_CALIBRATORS:
            columns |= set.union(*(
                calibrator_inst.produced_columns
                for calibrator_inst in self.calibrator_insts
            ))

        return columns


class SelectorMixin(ConfigTask):
    """
    Mixin to include a single :py:class:`~columnflow.selection.Selector`
    instances into tasks.

    Inheriting from this mixin will allow a task to instantiate and access a
    :py:class:`~columnflow.selection.Selector` instance with name *selector*,
    which is an input parameter for this task.
    """
    selector = luigi.Parameter(
        default=RESOLVE_DEFAULT,
        description="the name of the selector to be applied; default: value of the "
        "'default_selector' config",
    )

    # decides whether the task itself runs the selector and implements its shifts
    register_selector_sandbox = False
    register_selector_shifts = False

    @classmethod
    def get_selector_inst(
        cls,
        selector: str,
        kwargs=None,
    ) -> Selector:
        """
        Get requested *selector*.

        :py:class:`~columnflow.selection.Selector` instance is either
        initalized or loaded from cache.

        :param selector: Name of :py:class:`~columnflow.selection.Selector` to load
        :param kwargs: Additional keyword arguments to forward to the
            :py:class:`~columnflow.selection.Selector` instance
        :return: :py:class:`~columnflow.selection.Selector` instance.
        """
        selector_cls = Selector.get_cls(selector)
        if not selector_cls.exposed:
            raise RuntimeError(f"cannot use unexposed selector '{selector}' in {cls.__name__}")

        inst_dict = cls.get_selector_kwargs(**kwargs) if kwargs else None
        return selector_cls(inst_dict=inst_dict)

    @classmethod
    def resolve_param_values(cls, params: dict[str, Any]) -> dict:
        """
        Resolve values *params* and check against possible default values and
        selector groups.

        Check the values in *params* against the default value ``"default_selector"``
        in the current config inst. For more information, see
        :py:meth:`~columnflow.tasks.framework.base.AnalysisTask.resolve_config_default`.

        :param params: Parameter values to resolve
        :return: Dictionary of parameters that contains the requested
            :py:class:`~columnflow.selection.Selector` instance under the
            keyword ``"selector_inst"``.
        """
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
    def get_known_shifts(
        cls,
        config_inst: od.Config,
        params: dict[str, Any],
    ) -> tuple[set[str], set[str]]:
        """
        Adds set of shifts that the current ``selector_inst`` registers to the
        set of known ``shifts`` and ``upstream_shifts``.

        First, the set of ``shifts`` and ``upstream_shifts`` are obtained from
        the *config_inst* and the current set of parameters *params* using the
        ``get_known_shifts`` methods of all classes that :py:class:`SelectorMixin`
        inherits from.
        Afterwards, check if the current ``selector_inst`` registers shifts.
        If :py:attr:`~SelectorMixin.register_selector_shifts` is ``True``,
        add them to the current set of ``shifts``. Otherwise, add the
        shifts obtained from the ``selector_inst`` to ``upstream_shifts``.

        :param config_inst: Config instance for the current task.
        :param params: Dictionary containing the current set of parameters provided
            by the user at commandline level
        :return: Tuple with updated sets of ``shifts`` and ``upstream_shifts``.
        """
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
    def req_params(cls, inst: law.Task, **kwargs) -> dict[str, Any]:
        """
        Get the required parameters for the task, preferring the ``--selector`` set on task-level via CLI.

        This method first checks if the --selector parameter is set at the task-level via the command line.
        If it is, this parameter is preferred and added to the '_prefer_cli' key in the kwargs dictionary.
        The method then calls the 'req_params' method of the superclass with the updated kwargs.

        :param inst: The current task instance.
        :param kwargs: Additional keyword arguments that may contain parameters for the task.
        :return: A dictionary of parameters required for the task.
        """
        # prefer --selector set on task-level via cli
        kwargs["_prefer_cli"] = law.util.make_set(kwargs.get("_prefer_cli", [])) | {"selector"}

        return super().req_params(inst, **kwargs)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # cache for selector inst
        self._selector_inst = None

    @property
    def selector_inst(self):
        """
        Access current :py:class:`~columnflow.selection.Selector` instance.

        Loads the current :py:class:`~columnflow.selection.Selector` *selector_inst* from
        the cache or initializes it.
        If the selector requests a specific ``sandbox``, set this sandbox as
        the environment for the current :py:class:`~law.task.base.Task`.

        :return: Current :py:class:`~columnflow.selection.Selector` instance
        """
        if self._selector_inst is None:
            self._selector_inst = self.get_selector_inst(self.selector, {"task": self})

            # overwrite the sandbox when set
            if self.register_selector_sandbox:
                sandbox = self._selector_inst.get_sandbox()
                if sandbox:
                    self.sandbox = sandbox
                    # rebuild the sandbox inst when already initialized
                    if self._sandbox_initialized:
                        self._initialize_sandbox(force=True)

        return self._selector_inst

    @property
    def selector_repr(self):
        """
        Return a string representation of the selector.
        """
        return str(self.selector_inst)

    def store_parts(self):
        """
        Create parts to create the output path to store intermediary results
        for the current :py:class:`~law.task.base.Task`.

        Calls :py:meth:`store_parts` of the ``super`` class and inserts
        `{"selector": "sel__{SELECTOR_NAME}"}` before keyword ``version``.
        Here, ``SELECTOR_NAME`` is the name of the current ``selector_inst``.

        :return: Updated parts to create output path to store intermediary results.
        """
        parts = super().store_parts()
        parts.insert_before("version", "selector", f"sel__{self.selector_repr}")
        return parts

    def find_keep_columns(self: ConfigTask, collection: ColumnCollection) -> set[Route]:
        columns = super().find_keep_columns(collection)

        if collection == ColumnCollection.ALL_FROM_SELECTOR:
            columns |= self.selector_inst.produced_columns

        return columns


class SelectorStepsMixin(SelectorMixin):
    """
    Mixin to include multiple selector steps into tasks.

    Inheriting from this mixin will allow a task to access selector steps, which can be a
    comma-separated list of selector step names and is an input parameter for this task.
    """

    selector_steps = law.CSVParameter(
        default=(),
        description="a subset of steps of the selector to apply; uses all steps when empty; "
        "default: empty",
        brace_expand=True,
        parse_empty=True,
    )

    exclude_params_repr_empty = {"selector_steps"}

    selector_steps_order_sensitive = False

    @classmethod
    def resolve_param_values(cls, params: dict[str, Any]) -> dict[str, Any]:
        """
        Resolve values *params* and check against possible default values and
        selector step groups.

        Check the values in *params* against the default value ``"default_selector_steps"``
        and the group ``"selector_step_groups"`` in the current config inst.
        For more information, see
        :py:meth:`~columnflow.tasks.framework.base.AnalysisTask.resolve_config_default`.
        If :py:attr:`SelectorStepsMixin.selector_steps_order_sensitive` is ``True``,
        :py:func:`sort <sorted>` the selector steps.

        :param params: Parameter values to resolve
        :return: Dictionary of parameters that contains the requested
            selector steps under the keyword ``"selector_steps"``.
        """
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
    def req_params(cls, inst: law.Task, **kwargs) -> dict[str, Any]:
        """
        Get the required parameters for the task, preferring the --selector-steps set on task-level via CLI.

        This method first checks if the --selector-steps parameter is set at the task-level via the command line.
        If it is, this parameter is preferred and added to the '_prefer_cli' key in the kwargs dictionary.
        The method then calls the 'req_params' method of the superclass with the updated kwargs.

        :param inst: The current task instance.
        :param kwargs: Additional keyword arguments that may contain parameters for the task.
        :return: A dictionary of parameters required for the task.
        """
        # prefer --selector-steps set on task-level via cli
        kwargs["_prefer_cli"] = law.util.make_set(kwargs.get("_prefer_cli", [])) | {"selector_steps"}

        return super().req_params(inst, **kwargs)

    def store_parts(self) -> law.util.InsertableDict:
        """
        Create parts to create the output path to store intermediary results
        for the current :py:class:`~law.task.base.Task`.

        Calls :py:meth:`store_parts` of the ``super`` class and inserts
        `{"selector": "__steps__LIST_OF_STEPS"}`, where ``LIST_OF_STEPS`` is the
        sorted list of selector steps.
        For more information, see e.g.
        :py:meth:`~columnflow.tasks.framework.base.ConfigTask.store_parts`.

        :return: Updated parts to create output path to store intermediary results.
        """
        parts = super().store_parts()

        steps = self.selector_steps
        if not self.selector_steps_order_sensitive:
            steps = sorted(steps)
        if steps:
            parts["selector"] += "__steps_" + "_".join(steps)

        return parts


class ProducerMixin(ConfigTask):
    """
    Mixin to include a single :py:class:`~columnflow.production.Producer` into tasks.

    Inheriting from this mixin will give access to instantiate and access a
    :py:class:`~columnflow.production.Producer` instance with name *producer*,
    which is an input parameter for this task.
    """

    producer = luigi.Parameter(
        default=RESOLVE_DEFAULT,
        description="the name of the producer to be applied; default: value of the "
        "'default_producer' config",
    )

    # decides whether the task itself runs the producer and implements its shifts
    register_producer_sandbox = False
    register_producer_shifts = False

    @classmethod
    def get_producer_inst(cls, producer: str, kwargs=None) -> Producer:
        """
        Initialize :py:class:`~columnflow.production.Producer` instance.

        Extracts relevant *kwargs* for this producer instance using the
        :py:meth:`~columnflow.tasks.framework.base.AnalaysisTask.get_producer_kwargs`
        method.
        After this process, the previously initialized instance of a
        :py:class:`~columnflow.production.Producer` with the name
        *producer* is initialized using the
        :py:meth:`~columnflow.util.DerivableMeta.get_cls` method with the
        relevant keyword arguments.

        :param producer: Name of the :py:class:`~columnflow.production.Producer`
            instance
        :param kwargs: Any set keyword argument that is potentially relevant for
            this :py:class:`~columnflow.production.Producer` instance
        :raises RuntimeError: if requested :py:class:`~columnflow.production.Producer` instance
            is not :py:attr:`~columnflow.production.Producer.exposed`
        :return: The initialized :py:class:`~columnflow.production.Producer`
            instance.
        """
        producer_cls: Producer = Producer.get_cls(producer)
        if not producer_cls.exposed:
            raise RuntimeError(f"cannot use unexposed producer '{producer}' in {cls.__name__}")

        inst_dict = cls.get_producer_kwargs(**kwargs) if kwargs else None
        return producer_cls(inst_dict=inst_dict)

    @classmethod
    def resolve_param_values(cls, params: dict[str, Any]) -> dict[str, Any]:
        """
        Resolve parameter values *params* relevant for the
        :py:class:`ProducerMixin` and all classes it inherits from.

        Loads the ``config_inst`` and loads the parameter ``"producer"``.
        In case the parameter is not found, defaults to ``"default_producer"``.
        Finally, this function adds the keyword ``"producer_inst"``, which
        contains the :py:class:`~columnflow.production.Producer` instance
        obtained using :py:meth:`~.ProducerMixin.get_producer_inst` method.

        :param params: Dictionary with parameters provided by the user at
            commandline level.
        :return: Dictionary of parameters that now includes new value for
            ``"producer_inst"``.
        """
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
    def get_known_shifts(cls, config_inst: od.Config, params: dict[str, Any]) -> tuple[set[str], set[str]]:
        """
        Adds set of shifts that the current ``producer_inst`` registers to the
        set of known ``shifts`` and ``upstream_shifts``.

        First, the set of ``shifts`` and ``upstream_shifts`` are obtained from
        the *config_inst* and the current set of parameters *params* using the
        ``get_known_shifts`` methods of all classes that :py:class:`ProducerMixin`
        inherits from.
        Afterwards, check if the current ``producer_inst`` registers shifts.
        If :py:attr:`~ProducerMixin.register_producer_shifts` is ``True``,
        add them to the current set of ``shifts``. Otherwise, add the
        shifts obtained from the ``producer_inst`` to ``upstream_shifts``.

        :param config_inst: Config instance for the current task.
        :param params: Dictionary containing the current set of parameters provided
            by the user at commandline level
        :return: Tuple with updated sets of ``shifts`` and ``upstream_shifts``.
        """
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
    def req_params(cls, inst: law.Task, **kwargs) -> dict[str, Any]:
        """
        Get the required parameters for the task, preferring the ``--producer`` set on task-level via CLI.

        This method first checks if the ``--producer`` parameter is set at the task-level via the command line.
        If it is, this parameter is preferred and added to the '_prefer_cli' key in the kwargs dictionary.
        The method then calls the 'req_params' method of the superclass with the updated kwargs.

        :param inst: The current task instance.
        :param kwargs: Additional keyword arguments that may contain parameters for the task.
        :return: A dictionary of parameters required for the task.
        """
        # prefer --producer set on task-level via cli
        kwargs["_prefer_cli"] = law.util.make_set(kwargs.get("_prefer_cli", [])) | {"producer"}

        return super().req_params(inst, **kwargs)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # cache for producer inst
        self._producer_inst = None

    @property
    def producer_inst(self) -> Producer:
        """
        Access current :py:class:`~columnflow.production.Producer` instance.

        Loads the current :py:class:`~columnflow.production.Producer` *producer_inst* from
        the cache or initializes it.
        If the producer requests a specific ``sandbox``, set this sandbox as
        the environment for the current :py:class:`~law.task.base.Task`.

        :return: Current :py:class:`~columnflow.production.Producer` instance
        """
        if self._producer_inst is None:
            self._producer_inst = self.get_producer_inst(self.producer, {"task": self})

            # overwrite the sandbox when set
            if self.register_producer_sandbox:
                sandbox = self._producer_inst.get_sandbox()
                if sandbox:
                    self.sandbox = sandbox
                    # rebuild the sandbox inst when already initialized
                    if self._sandbox_initialized:
                        self._initialize_sandbox(force=True)

        return self._producer_inst

    @property
    def producer_repr(self) -> str:
        """
        Return a string representation of the producer.
        """
        return str(self.producer) if self.producer != law.NO_STR else "none"

    def store_parts(self) -> law.util.InsertableDict[str, str]:
        """
        Create parts to create the output path to store intermediary results
        for the current :py:class:`~law.task.base.Task`.

        Calls :py:meth:`store_parts` of the ``super`` class and inserts
        `{"producer": "prod__{self.producer}"}` before keyword ``version``.
        For more information, see e.g. :py:meth:`~columnflow.tasks.framework.base.ConfigTask.store_parts`.

        :return: Updated parts to create output path to store intermediary results.
        """
        parts = super().store_parts()
        producer = f"prod__{self.producer_repr}"
        parts.insert_before("version", "producer", producer)
        return parts

    def find_keep_columns(self: ConfigTask, collection: ColumnCollection) -> set[Route]:
        """
        Finds the columns to keep based on the *collection*.

        This method first calls the 'find_keep_columns' method of the superclass with the given *collection*.
        If the *collection* is equal to ``ALL_FROM_PRODUCER``, it adds the
        columns produced by the producer instance to the set of columns.

        :param collection: The collection of columns.
        :return: A set of columns to keep.
        """
        columns = super().find_keep_columns(collection)

        if collection == ColumnCollection.ALL_FROM_PRODUCER:
            columns |= self.producer_inst.produced_columns

        return columns


class ProducersMixin(ConfigTask):
    """
    Mixin to include multiple :py:class:`~columnflow.production.Producer` instances into tasks.

    Inheriting from this mixin will allow a task to instantiate and access a set of
    :py:class:`~columnflow.production.Producer` instances with names *producers*,
    which is a comma-separated list of producer names and is an input parameter for this task.
    """

    producers = law.CSVParameter(
        default=(RESOLVE_DEFAULT,),
        description="comma-separated names of producers to be applied; default: value of the "
        "'default_producer' config",
        brace_expand=True,
        parse_empty=True,
    )

    # decides whether the task itself runs the producers and implements their shifts
    register_producers_shifts = False

    @classmethod
    def get_producer_insts(cls, producers: Iterable[str], kwargs=None) -> list[Producer]:
        """
        Get all requested *producers*.

        :py:class:`~columnflow.production.Producer` instances are either
        initalized or loaded from cache.

        :param producers: Names of :py:class:`~columnflow.production.Producer`
            instances to load
        :param kwargs: Additional keyword arguments to forward to individual
            :py:class:`~columnflow.production.Producer` instances
        :raises RuntimeError: if requested producers are not
            :py:attr:`~columnflow.production.Producer.exposed`
        :return: List of :py:class:`~columnflow.production.Producer` instances.
        """
        inst_dict = cls.get_producer_kwargs(**kwargs) if kwargs else None

        insts = []
        for producer in producers:
            producer_cls = Producer.get_cls(producer)
            if not producer_cls.exposed:
                raise RuntimeError(f"cannot use unexposed producer '{producer}' in {cls.__name__}")
            insts.append(producer_cls(inst_dict=inst_dict))

        return insts

    @classmethod
    def resolve_param_values(
        cls,
        params: law.util.InsertableDict[str, Any],
    ) -> law.util.InsertableDict[str, Any]:
        """
        Resolve values *params* and check against possible default values and
        producer groups.

        Check the values in *params* against the default value ``"default_producer"``
        and possible group definitions ``"producer_groups"`` in the current config inst.
        For more information, see
        :py:meth:`~columnflow.tasks.framework.base.ConfigTask.resolve_config_default_and_groups`.

        :param params: Parameter values to resolve
        :return: Dictionary of parameters that contains the list requested
            :py:class:`~columnflow.production.Producer` instances under the
            keyword ``"producer_insts"``. See :py:meth:`~.ProducersMixin.get_producer_insts`
            for more information.
        """
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
    def get_known_shifts(cls, config_inst: od.Config, params: dict[str, Any]) -> tuple[set[str], set[str]]:
        """
        Adds set of all shifts that the list of ``producer_insts`` register to the
        set of known ``shifts`` and ``upstream_shifts``.

        First, the set of ``shifts`` and ``upstream_shifts`` are obtained from
        the *config_inst* and the current set of parameters *params* using the
        ``get_known_shifts`` methods of all classes that :py:class:`ProducersMixin`
        inherits from.
        Afterwards, loop through the list of :py:class:`~columnflow.production.Producer`
        and check if they register shifts.
        If :py:attr:`~ProducersMixin.register_producers_shifts` is ``True``,
        add them to the current set of ``shifts``. Otherwise, add the
        shifts to ``upstream_shifts``.

        :param config_inst: Config instance for the current task.
        :param params: Dictionary containing the current set of parameters provided
            by the user at commandline level
        :return: Tuple with updated sets of ``shifts`` and ``upstream_shifts``.
        """
        shifts, upstream_shifts = super().get_known_shifts(config_inst, params)

        # get the producers, update them and add their shifts
        for producer_inst in params.get("producer_insts") or []:
            if cls.register_producers_shifts:
                shifts |= producer_inst.all_shifts
            else:
                upstream_shifts |= producer_inst.all_shifts

        return shifts, upstream_shifts

    @classmethod
    def req_params(cls, inst: law.Task, **kwargs) -> dict[str, Any]:
        """
        Get the required parameters for the task, preferring the --producers set on task-level via CLI.

        This method first checks if the --producers parameter is set at the task-level via the command line.
        If it is, this parameter is preferred and added to the '_prefer_cli' key in the kwargs dictionary.
        The method then calls the 'req_params' method of the superclass with the updated kwargs.

        :param inst: The current task instance.
        :param kwargs: Additional keyword arguments that may contain parameters for the task.
        :return: A dictionary of parameters required for the task.
        """
        # prefer --producers set on task-level via cli
        kwargs["_prefer_cli"] = law.util.make_set(kwargs.get("_prefer_cli", [])) | {"producers"}

        return super().req_params(inst, **kwargs)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # cache for producer insts
        self._producer_insts = None

    @property
    def producer_insts(self) -> list[Producer]:
        """
        Access current list of :py:class:`~columnflow.production.Producer` instances.

        Loads the current :py:class:`~columnflow.production.Producer` *producer_insts* from
        the cache or initializes it.

        :return: Current list :py:class:`~columnflow.production.Producer` instances
        """
        if self._producer_insts is None:
            self._producer_insts = self.get_producer_insts(self.producers, {"task": self})
        return self._producer_insts

    @property
    def producers_repr(self) -> str:
        """Return a string representation of the producers."""
        prods_repr = "none"
        if self.producers:
            prods_repr = "__".join([str(prod) for prod in self.producer_insts[:5]])
            if len(self.producers) > 5:
                prods_repr += f"__{law.util.create_hash([str(prod) for prod in self.producer_insts[5:]])}"
        return prods_repr

    def store_parts(self):
        """
        Create parts to create the output path to store intermediary results
        for the current :py:class:`~law.task.base.Task`.

        Calls :py:meth:`store_parts` of the ``super`` class and inserts
        `{"producers": "prod__{HASH}"}` before keyword ``version``.
        Here, ``HASH`` is the joint string of the first five producer names
        + a hash created with :py:meth:`law.util.create_hash` based on
        the list of producers, starting at its 5th element (i.e. ``self.producers[5:]``)
        For more information, see e.g. :py:meth:`~columnflow.tasks.framework.base.ConfigTask.store_parts`.

        :return: Updated parts to create output path to store intermediary results.
        """
        parts = super().store_parts()
        parts.insert_before("version", "producers", f"prod__{self.producers_repr}")

        return parts

    def find_keep_columns(self: ConfigTask, collection: ColumnCollection) -> set[Route]:
        """
        Finds the columns to keep based on the *collection*.

        This method first calls the 'find_keep_columns' method of the superclass with the given *collection*.
        If the *collection* is equal to ``ALL_FROM_PRODUCERS``, it adds the
        columns produced by all producer instances to the set of columns.

        :param collection: The collection of columns.
        :return: A set of columns to keep.
        """
        columns = super().find_keep_columns(collection)

        if collection == ColumnCollection.ALL_FROM_PRODUCERS:
            columns |= set.union(*(
                producer_inst.produced_columns
                for producer_inst in self.producer_insts
            ))

        return columns


class MLModelMixinBase(AnalysisTask):
    """
    Base Mixin to include a machine learning applications into tasks.

    Inheriting from this mixin will allow a task to instantiate and access a
    :py:class:`~columnflow.ml.MLModel` instance with name *ml_model*,
    which is an input parameter for this task.
    """

    ml_model = luigi.Parameter(
        description="the name of the ML model to be applied",
    )

    ml_model_settings = SettingsParameter(
        default=DotDict(),
        description="settings passed to the init function of the ML model",
    )

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
        Get the required parameters for the task, preferring the ``--ml-model`` set on task-level via CLI.

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


class MLModelTrainingMixin(MLModelMixinBase):
    """
    A mixin class for training machine learning models.

    This class provides parameters for configuring the training of machine learning models.
    """

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
        """
        Resolve the calibrators for the given ML model instance.

        This method retrieves the calibrators from the parameters *params* and
        broadcasts them to the configs if necessary.
        It also resolves `calibrator_groups` and `default_calibrator` from the config(s) associated
        with this ML model instance, and validates the number of sequences.
        Finally, it checks the retrieved calibrators against
        the training calibrators of the model using
        :py:meth:`~columnflow.ml.MLModel.training_calibrators` and instantiates them if necessary.

        :param ml_model_inst: The ML model instance.
        :param params: A dictionary of parameters that may contain the calibrators.
        :return: A tuple of tuples containing the resolved calibrators.
        :raises Exception: If the number of calibrator sequences does not match
            the number of configs used by the ML model.
        """
        calibrators: Union[tuple[str], tuple[tuple[str]]] = params.get("calibrators") or ((),)

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
        """
        Resolve the selectors for the given ML model instance.

        This method retrieves the selectors from the parameters *params* and
        broadcasts them to the configs if necessary.
        It also resolves `default_selector` from the config(s) associated
        with this ML model instance, validates the number of sequences.
        Finally, it checks the retrieved selectors against the training selectors
        of the model, using
        :py:meth:`~columnflow.ml.MLModel.training_selector`, and instantiates them.

        :param ml_model_inst: The ML model instance.
        :param params: A dictionary of parameters that may contain the selectors.
        :return: A tuple containing the resolved selectors.
        :raises Exception: If the number of selector sequences does not match
            the number of configs used by the ML model.
        """
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
        """
        Resolve the producers for the given ML model instance.

        This method retrieves the producers from the parameters *params* and
        broadcasts them to the configs if necessary.
        It also resolves `producer_groups` and `default_producer` from the config(s) associated
        with this ML model instance, validates the number of sequences.
        Finally, it checks the retrieved producers against the training producers
        of the model, using
        :py:meth:`~columnflow.ml.MLModel.training_producers`, and instantiates them.

        :param ml_model_inst: The ML model instance.
        :param params: A dictionary of parameters that may contain the producers.
        :return: A tuple of tuples containing the resolved producers.
        :raises Exception: If the number of producer sequences does not match
            the number of configs used by the ML model.
        """
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
        """
        Resolve the parameter values for the given parameters.

        This method retrieves the parameters and resolves the ML model instance, configs,
        calibrators, selectors, and producers. It also calls the model's setup hook.

        :param params: A dictionary of parameters that may contain the analysis instance and ML model.
        :return: A dictionary containing the resolved parameters.
        :raises Exception: If the ML model instance received configs to define training configs,
            but did not define any.
        """
        params = super().resolve_param_values(params)

        if "analysis_inst" in params and "ml_model" in params:
            analysis_inst = params["analysis_inst"]

            # NOTE: we could try to implement resolving the default ml_model here
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
            parameters=self.ml_model_settings,
        )

    def store_parts(self) -> law.util.InsertableDict[str, str]:
        """
        Generate a dictionary of store parts for the current instance.

        This method extends the base method to include additional parts related to machine learning
        model configurations, calibrators, selectors, producers (CSP), and the ML model instance itself.
        If the list of either of the CSPs is empty, the corresponding part is set to ``"none"``,
        otherwise, the first two elements of the list are joined with ``"__"``.
        If the list of either of the CSPs contains more than two elements, the part is extended
        with the number of elements and a hash of the remaining elements, which is
        created with :py:meth:`law.util.create_hash`.
        The parts are represented as strings and are used to create unique identifiers for the
        instance's output.

        :return: An InsertableDict containing the store parts.
        """
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
            parts.insert_before("version", "ml_model", f"ml__{self.ml_model_repr}")

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
                    parameters=params["ml_model_settings"],
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
                parameters=self.ml_model_settings,
            )

    def store_parts(self) -> law.util.InsertableDict:
        parts = super().store_parts()

        if self.ml_model_inst:
            parts.insert_before("version", "ml_model", f"ml__{self.ml_model_repr}")

        return parts

    def find_keep_columns(self: ConfigTask, collection: ColumnCollection) -> set[Route]:
        columns = super().find_keep_columns(collection)

        if collection == ColumnCollection.ALL_FROM_ML_EVALUATION and self.ml_model_inst:
            columns |= set.union(*self.ml_model_inst.produced_columns().values())

        return columns


class MLModelDataMixin(MLModelMixin):

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
    def ml_models_repr(self):
        """Returns a string representation of the ML models."""
        ml_models_repr = "__".join([str(model_inst) for model_inst in self.ml_model_insts])
        return ml_models_repr

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
            parts.insert_before("version", "ml_models", f"ml__{self.ml_models_repr}")

        return parts

    def find_keep_columns(self: ConfigTask, collection: ColumnCollection) -> set[Route]:
        columns = super().find_keep_columns(collection)

        if collection == ColumnCollection.ALL_FROM_ML_EVALUATION:
            columns |= set.union(*(
                set.union(*model_inst.produced_columns().values())
                for model_inst in self.ml_model_insts
            ))

        return columns


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

    @property
    def inference_model_repr(self):
        return str(self.inference_model)

    def store_parts(self) -> law.util.InsertableDict:
        parts = super().store_parts()
        if self.inference_model != law.NO_STR:
            parts.insert_before("version", "inf_model", f"inf__{self.inference_model_repr}")
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


class WeightProducerMixin(ConfigTask):

    weight_producer = luigi.Parameter(
        default=RESOLVE_DEFAULT,
        description="the name of the weight producer to be used; default: value of the "
        "'default_weight_producer' config",
    )

    # decides whether the task itself runs the weight producer and implements its shifts
    register_weight_producer_sandbox = False
    register_weight_producer_shifts = False

    @classmethod
    def get_weight_producer_inst(
        cls,
        weight_producer: str,
        kwargs: dict | None = None,
    ) -> WeightProducer:
        weight_producer_cls = WeightProducer.get_cls(weight_producer)
        if not weight_producer_cls.exposed:
            raise RuntimeError(
                f"cannot use unexposed weight producer '{weight_producer}' in {cls.__name__}",
            )

        inst_dict = cls.get_weight_producer_kwargs(**kwargs) if kwargs else None
        return weight_producer_cls(inst_dict=inst_dict)

    @classmethod
    def resolve_param_values(cls, params: dict[str, Any]) -> dict[str, Any]:
        params = super().resolve_param_values(params)

        config_inst = params.get("config_inst")
        if config_inst:
            # add the default weight producer when empty
            params["weight_producer"] = cls.resolve_config_default(
                params,
                params.get("weight_producer"),
                container=config_inst,
                default_str="default_weight_producer",
                multiple=False,
            )
            if params["weight_producer"] is None:
                raise Exception(f"no weight producer configured for task {cls.task_family}")
            params["weight_producer_inst"] = cls.get_weight_producer_inst(
                params["weight_producer"],
                params,
            )

        return params

    @classmethod
    def get_known_shifts(
        cls,
        config_inst: od.Config,
        params: dict[str, Any],
    ) -> tuple[set[str], set[str]]:
        shifts, upstream_shifts = super().get_known_shifts(config_inst, params)

        # get the weight producer, update it and add its shifts
        weight_producer_inst = params.get("weight_producer_inst")
        if weight_producer_inst:
            if cls.register_weight_producer_shifts:
                shifts |= weight_producer_inst.all_shifts
            else:
                upstream_shifts |= weight_producer_inst.all_shifts

        return shifts, upstream_shifts

    def __init__(self: WeightProducerMixin, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

        # cache for weight producer inst
        self._weight_producer_inst = None

    @property
    def weight_producer_inst(self: WeightProducerMixin) -> WeightProducer:
        if self._weight_producer_inst is None:
            self._weight_producer_inst = self.get_weight_producer_inst(
                self.weight_producer,
                {"task": self},
            )

            # overwrite the sandbox when set
            if self.register_weight_producer_sandbox:
                sandbox = self._weight_producer_inst.get_sandbox()
                if sandbox:
                    self.sandbox = sandbox
                    # rebuild the sandbox inst when already initialized
                    if self._sandbox_initialized:
                        self._initialize_sandbox(force=True)

        return self._weight_producer_inst

    @property
    def weight_producer_repr(self: WeightProducerMixin) -> str:
        return str(self.weight_producer_inst)

    def store_parts(self: WeightProducerMixin) -> law.util.InsertableDict[str, str]:
        parts = super().store_parts()
        parts.insert_before("version", "weightprod", f"weight__{self.weight_producer_repr}")
        return parts


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


class HistHookMixin(ConfigTask):

    hist_hook = luigi.Parameter(
        default=law.NO_STR,
        description="name of a function in the config's auxiliary dictionary 'hist_hooks' that is "
        "invoked before plotting to update a potentially nested dictionary of histograms; "
        "default: empty",
    )

    def invoke_hist_hook(self, hists: dict) -> dict:
        """
        Hook to update histograms before plotting.
        """
        if self.hist_hook in (None, "", law.NO_STR):
            return hists

        # get the hook from the config instance
        hooks = self.config_inst.x("hist_hooks", {})
        if self.hist_hook not in hooks:
            raise ValueError(
                f"hist hook '{self.hist_hook}' not found in 'hist_hooks' auxiliary entry of config",
            )
        func = hooks[self.hist_hook]
        if not callable(func):
            raise ValueError(f"hist hook '{self.hist_hook}' is not callable: {func}")

        # invoke it
        self.publish_message(f"invoking hist hook '{self.hist_hook}'")
        hists = func(self, hists)

        return hists
