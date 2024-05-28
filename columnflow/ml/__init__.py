# coding: utf-8

"""
Definition of basic objects for describing and creating ML models.
"""

from __future__ import annotations

import abc
from collections import OrderedDict

import law
import order as od

from columnflow.types import Any, Sequence
from columnflow.util import maybe_import, Derivable, DotDict, KeyValueMessage
from columnflow.columnar_util import Route

ak = maybe_import("awkward")


class MLModel(Derivable):
    """
    Minimal interface to ML models with connections to config objects (such as
    py:class:`order.Config` or a :py:class:`order.Dataset`) and, on an optional basis, to tasks.

    Inheriting classes need to overwrite eight methods:

        - :py:meth:`sandbox`
        - :py:meth:`datasets`
        - :py:meth:`uses`
        - :py:meth:`produces`
        - :py:meth:`output`
        - :py:meth:`open_model`
        - :py:meth:`train`
        - :py:meth:`evaluate`

    See their documentation below for more info.

    There are several optional hooks that allow for a custom setup after config objects were
    assigned (:py:meth:`setup`), a fine-grained configuration of additional training requirements
    (:py:meth:`requires`), diverging training and evaluation phase spaces
    (:py:meth:`training_configs`, :py:meth:`training_calibrators`, :py:meth:`training_selector`,
    :py:meth:`training_producers`), or how hyper-paramaters are string encoded for output
    declarations (:py:meth:`parameter_pairs`). The optional py:meth:`preparation_producer` allows
    setting a producer that is run during the initial preparation of ML columns.

    .. py:classattribute:: single_config

        type: bool

        The default flag that marks whether this model only accepts a single config object in case
        no value is passed in the constructor. Converted into an instance attribute upon
        instantiation.

    .. py:classattribute:: folds

        type: int

        The default number of folds for the k-fold cross-validation in case no value is passed in
        the constructor. Converted into an instance attribute upon instantiation.

    .. py:classattribute:: store_name

        type: str, None

        The default name for storing input data in case no value is passed in the constructor. When
        *None*, the name of the model class is used instead. Converted into an instance attribute
        upon instantiation.

    .. py:attribute:: analysis_inst

        type: order.Analysis

        Reference to the :py:class:`order.Analysis` object.

    .. py:attribute:: parameters

        type: OrderedDict

        A dictionary mapping parameter names to arbitrary values, such as
        ``{"layers": 5, "units": 128}``.

    .. py:attribute:: used_datasets

        type: dict
        read-only

        Sets of :py:class:`order.Dataset` instances that are used by the model training, mapped to
        their corresponding :py:class:`order.Config` instances.

    .. py:attribute:: used_columns

        type: set
        read-only

        Column names or :py:class:`Route`'s that are used by this model, mapped to
        :py:class:`order.Config` instances they belong to.

    .. py:attribute:: produced_columns

        type: set
        read-only

        Column names or :py:class:`Route`'s that are produces by this model, mapped to
        :py:class:`order.Config` instances they belong to.
    """

    # default setting mark whether this model accepts only a single config
    single_config: bool = False

    # default number of folds
    folds: int = 2

    # default name for storing e.g. input data
    # falls back to cls_name if None
    store_name: str | None = None

    # flag denoting whether the preparation_producer is invoked before evaluate()
    preparation_producer_in_ml_evaluation: bool = True

    # names of attributes that are automatically extracted from init kwargs and
    # fall back to classmembers in case they are missing
    init_attributes: list[str] = ["single_config", "folds", "store_name", "preparation_producer_in_ml_evaluation"]

    def __init__(
        self: MLModel,
        analysis_inst: od.Analysis,
        *,
        parameters: OrderedDict | None = None,
        **kwargs,
    ) -> None:
        super().__init__()

        # store attributes
        self.analysis_inst = analysis_inst
        self.parameters = OrderedDict(parameters or {})

        # set instance members based on registered init attributes
        for attr in self.init_attributes:
            # get the class-level attribute
            value = getattr(self, attr)
            # get the value from kwargs
            _value = kwargs.get(attr, law.no_value)
            if _value != law.no_value:
                value = _value
            # set the instance-level attribute
            setattr(self, attr, value)

        # list of config instances
        self.config_insts = []
        if "configs" in kwargs:
            self._setup(kwargs["configs"])

    def __str__(self):
        """
        Returns a string representation of this model instance. The string is composed of the class
        name and the string representation of all parameters.
        """
        model_str = f"{self.cls_name}"
        if self.parameters_repr:
            model_str += f"__{self.parameters_repr}"
        return model_str

    @property
    def config_inst(self: MLModel) -> od.Config:
        if self.single_config and len(self.config_insts) != 1:
            raise Exception(
                f"the config_inst property requires MLModel '{self.cls_name}' to have the "
                "single_config enabled to to contain exactly one config instance, but found "
                f"{len(self.config_insts)}",
            )

        return self.config_insts[0]

    def _assert_configs(self: MLModel, msg: str) -> None:
        """
        Raises an exception showing *msg* in case this model's :py:attr:`config_insts` is empty.
        """
        if not self.config_insts:
            raise Exception(f"MLModel '{self.cls_name}' has no config instances, {msg}")

    def _format_value(self: MLModel, value: Any) -> str:
        """
        Formats any paramter *value* to a readable string.
        """
        if isinstance(value, (list, tuple)):
            return "_".join(map(self._format_value, value))
        if isinstance(value, bool):
            return str(value).lower()
        if isinstance(value, float):
            # scientific notation when too small
            return f"{value}" if value >= 0.01 else f"{value:.2e}"

        # any other case
        return str(value)

    @property
    def parameters_repr(self: MLModel) -> str:
        """
        Returns a hash of string representation of all parameters. This is used to uniquely identify
        a model instance based on its parameters.

        :raises: Exception in case the parameters_repr changed after it was set.
        :returns: String representation of all parameters.
        """
        if not self.parameters:
            return ""
        parameters_repr = law.util.create_hash(self._join_parameter_pairs(only_significant=True))
        if hasattr(self, "_parameters_repr") and self._parameters_repr != parameters_repr:
            raise Exception(
                f"parameters_repr changed from {self._parameters_repr} to {parameters_repr};"
                "this should not happen",
            )
        self._parameters_repr = parameters_repr
        return self._parameters_repr

    def _join_parameter_pairs(self: MLModel, only_significant: bool = True) -> str:
        """
        Returns a joined string representation of all significant parameters. In this context,
        significant parameters are those that potentially lead to different results (e.g. network
        architecture parameters as opposed to some log level).
        """
        return "__".join(
            f"{name}_{self._format_value(value)}"
            for name, value in self.parameter_pairs(only_significant=True)
        )

    def parameter_pairs(self: MLModel, only_significant: bool = False) -> list[tuple[str, Any]]:
        """
        Returns a sorted list of all parameter name-value tuples. In this context, significant parameters
        are those that potentially lead to different results (e.g. network architecture parameters
        as opposed to some log level).
        """
        return sorted(self.parameters.items())

    @property
    def accepts_scheduler_messages(self: MLModel) -> bool:
        """
        Whether the training or evaluation loop expects and works with messages sent from a central
        luigi scheduler through the active worker to the underlying task. See
        :py:meth:`get_scheduler_messages` for more info.
        """
        return True

    def get_scheduler_messages(self: MLModel, task: law.Task) -> DotDict[str, KeyValueMessage]:
        """
        Checks if the *task* obtained messages from a central luigi scheduler, parses them expecting
        key - value pairs, and returns them in an ordered :py:class:`DotDict`. All values are
        :py:class:`KeyValueMessage` objects (with ``key``, ``value`` and ``respond()`` members).

        Scheduler messages are only sent while the task is actively running, so it most likely only
        makes sense to expect and react to messages during training and evaluation loops.
        """
        messages = DotDict()

        if task.accepts_messages and task.scheduler_messages:
            while not self.scheduler_messages.empty():
                msg = KeyValueMessage.from_message(self.scheduler_messages.get())
                if msg:
                    messages[msg.key] = msg

        return messages

    def _set_configs(self: MLModel, configs: list[str | od.Config]) -> None:
        # complain when only a single config is accepted
        if self.single_config and len(configs) > 1:
            raise Exception(
                f"MLModel '{self.cls_name}' only accepts a single config but received "
                f"{len(configs)}: {','.join(map(str, configs))}",
            )

        # remove existing config instances
        del self.config_insts[:]

        # add them one by one
        for config in configs:
            config_inst = (
                config
                if isinstance(config, od.Config)
                else self.analysis_inst.get_config(config)
            )
            self.config_insts.append(config_inst)

    def _setup(self: MLModel, configs: list[str | od.Config] | None = None) -> None:
        # setup configs
        if configs:
            self._set_configs(configs)

        # setup hook
        self.setup()

    @property
    def used_columns(self: MLModel) -> dict[od.Config, set[Route]]:
        self._assert_configs("cannot determined used columns")
        return {
            config_inst: set(map(Route, self.uses(config_inst)))
            for config_inst in self.config_insts
        }

    @property
    def produced_columns(self: MLModel) -> dict[od.Config, set[Route]]:
        """
        Helper function to resolve column names of produced with this MLModel instance.

        :returns: Set of column names
        """
        self._assert_configs("cannot determined produced columns")
        return {
            config_inst: set(map(Route, self.produces(config_inst)))
            for config_inst in self.config_insts
        }

    @property
    def used_datasets(self: MLModel) -> dict[od.Config, set[od.Dataset]]:
        self._assert_configs("cannot determined used datasets")
        return {
            config_inst: set(self.datasets(config_inst))
            for config_inst in self.config_insts
        }

    def setup(self: MLModel) -> None:
        """
        Hook that is called after the model has been setup and its :py:attr:`config_insts` were
        assigned.
        """
        return

    def requires(self: MLModel, task: law.Task) -> Any:
        """
        Returns tasks that are required for the training to run and whose outputs are needed.
        """
        return {}

    def training_configs(
        self: MLModel,
        requested_configs: Sequence[str],
    ) -> list[str]:
        """
        Given a sequence of names of requested :py:class:`order.Config` objects,
        *requested_configs*, this method can alter and/or replace them to define a different (set
        of) config(s) for the preprocessing and training pipeline. This can be helpful in cases
        where training and evaluation phase spaces, as well as the required input datasets and/or
        columns are intended to diverge.
        """
        return list(requested_configs)

    def training_calibrators(
        self: MLModel,
        config_inst: od.Config,
        requested_calibrators: Sequence[str],
    ) -> list[str]:
        """
        Given a sequence of *requested_calibrators* for a *config_inst*, this method can alter
        and/or replace them to define a different set of calibrators for the preprocessing and
        training pipeline. This can be helpful in cases where training and evaluation phase spaces,
        as well as the required input columns are intended to diverge.

        Example usage:

        .. literalinclude:: ../../user_guide/examples/ml_code.py
            :language: python
            :pyobject: TestModel.training_calibrators

        :param config_inst: Config instance to extract the *requested_calibrators* from
        :returns: Set with str of the *requested_calibrators*
        """
        return list(requested_calibrators)

    def training_selector(
        self: MLModel,
        config_inst: od.Config,
        requested_selector: str,
    ) -> str:
        """
        Given a *requested_selector* for a *config_inst*, this method can change it to define a
        different selector for the preprocessing and training pipeline. This can be helpful in cases
        where training and evaluation phase spaces, as well as the required input columns are
        intended to diverge.

        Example usage:

        .. literalinclude:: ../../user_guide/examples/ml_code.py
            :language: python
            :pyobject: TestModel.training_selector

        :param config_inst: Config instance to extract the *requested_selector* from
        :returns: Set with str of the *requested_selector*
        """
        return requested_selector

    def training_producers(
        self: MLModel,
        config_inst: od.Config,
        requested_producers: Sequence[str],
    ) -> list[str]:
        """
        Given a sequence of *requested_producers* for a *config_inst*, this method can alter and/or
        replace them to define a different set of producers for the preprocessing and training
        pipeline. This can be helpful in cases where training and evaluation phase spaces, as well
        as the required input columns are intended to diverge.

        Example usage:

        .. literalinclude:: ../../user_guide/examples/ml_code.py
            :language: python
            :pyobject: TestModel.training_producers

        :param config_inst: Config instance to extract the *requested_producers* from
        :returns: Set with str of the *requested_producers*
        """
        return list(requested_producers)

    def preparation_producer(
        self: MLModel,
        config_inst: od.Config,
    ) -> str | None:
        """
        This method allows setting a producer that can be called as part of the preparation
        of the ML input columns given a *config_inst*.

        :param config_inst: :py:class:`~order.Config` object for which the producer should run.
        :return: Name of a :py:class:`Producer` class or *None*.
        """
        return None

    @abc.abstractmethod
    def sandbox(self: MLModel, task: law.Task) -> str:
        """
        Given a *task*, returns the name of a sandbox that is needed to perform model training and
        evaluation.

        Example usage:

        .. literalinclude:: ../../user_guide/examples/ml_code.py
            :language: python
            :pyobject: TestModel.sandbox

        :param task: Task instance to extract the datasets from
        :returns: path to the requested sandbox, optinally prefixed by the executing shell command
            with trailing :: as separator
        """
        return

    @abc.abstractmethod
    def datasets(self: MLModel, config_inst: od.Config) -> set[od.Dataset]:
        """
        Returns a set of all required datasets for a certain *config_inst*. To be implemented in
        subclasses.

        Example usage:

        .. literalinclude:: ../../user_guide/examples/ml_code.py
            :language: python
            :pyobject: TestModel.datasets

        :param config_inst: Config instance to extract the datasets from
        :returns: Set with :py:class:`~order.dataset.Dataset` instances
        """
        return

    @abc.abstractmethod
    def uses(self: MLModel, config_inst: od.Config) -> set[Route]:
        """
        Returns a set of all required columns for a certain *config_inst*. To be implemented in
        subclasses.

        Example usage:

        .. literalinclude:: ../../user_guide/examples/ml_code.py
            :language: python
            :pyobject: TestModel.uses

        :param config_inst: Config instance to extract the datasets from
        :returns: Set with str of required columns
        """
        return

    @abc.abstractmethod
    def produces(self: MLModel, config_inst: od.Config) -> set[Route]:
        """
        Returns a set of all produced columns for a certain *config_inst*. To be implemented in
        subclasses.

        Example usage:

        .. literalinclude:: ../../user_guide/examples/ml_code.py
            :language: python
            :pyobject: TestModel.produces

        :param config_inst: Config instance to extract the datasets from
        :returns: Set with str of produced columns
        """
        return

    @abc.abstractmethod
    def output(self: MLModel, task: law.Task) -> Any:
        """
        Returns a structure of output targets. To be implemented in subclasses.

        Example usage:

        .. literalinclude:: ../../user_guide/examples/ml_code.py
            :language: python
            :pyobject: TestModel.output

        :param task: Task instance used extract task related information
        :returns: Instance of :py:class:`~law.DirectoryTarget`, containing the path to directory.
        """
        return

    @abc.abstractmethod
    def open_model(self: MLModel, target: Any) -> Any:
        """
        Implemenents the opening of a trained model from *target* (corresponding to the structure
        returned by :py:meth:`output`). To be implemented in subclasses.

        Example usage:

        .. literalinclude:: ../../user_guide/examples/ml_code.py
            :language: python
            :pyobject: TestModel.open_model

        :param target: Instance of :py:class:`~law.DirectoryTarget`,
            contains path to directory holding the machine learning model.
        :returns: Machine learning model instance
        """
        return

    @abc.abstractmethod
    def train(
        self: MLModel,
        task: law.Task,
        input: Any,
        output: Any,
    ) -> None:
        """
        Performs the creation and training of a model, being passed a *task* and its *input* and
        *output*. To be implemented in subclasses.

        Example usage:

        .. literalinclude:: ../../user_guide/examples/ml_code.py
            :language: python
            :pyobject: TestModel.train

        :param task: Task instance used extract task related information
        :param input: List of instances of :py:class:`~law.DirectoryTarget`, containing the paths
            of all required *input* files
        :param output: Instance of :py:class:`~law.DirectoryTarget`, contain path to *target*
            directory of the task
        :returns: None
        """
        return

    @abc.abstractmethod
    def evaluate(
        self: MLModel,
        task: law.Task,
        events: ak.Array,
        models: list[Any],
        fold_indices: ak.Array,
        events_used_in_training: bool = False,
    ) -> ak.Array:
        """
        Performs the model evaluation for a *task* on a chunk of *events* and returns them. The list
        of *models* corresponds to the number of folds generated by this model, and the already
        evaluated *fold_indices* for this event chunk that might used depending on
        *events_used_in_training*. To be implemented in subclasses.

        Example usage:

        .. literalinclude:: ../../user_guide/examples/ml_code.py
            :language: python
            :pyobject: TestModel.evaluate

        :param task: Task instance used to extract task related information
        :param events: Awkward Array containing the events to evaluate
        :param models: List containing trained models
        :param fold_indices: Awkward Array containing the indices of the folds used for training
        :param events_used_in_training: Boolean flag to indicate if events were used during training
        :returns: Awkward array containing events with additional columns
        """
        return
