# coding: utf-8

"""
Definition of basic objects for describing and creating ML models.
"""

# TODOs
# - documentation

from __future__ import annotations

import abc
from collections import OrderedDict
from typing import Any

import law
import order as od

from columnflow.util import maybe_import, Derivable, DotDict, KeyValueMessage
from columnflow.columnar_util import Route


ak = maybe_import("awkward")


class MLModelBase(Derivable):

    # default number of folds
    folds = 2

    # default name for storing e.g. input data
    # falls back to cls_name if None
    store_name = None

    def __init__(
        self,
        *,
        parameters: OrderedDict | None = None,
    ):
        super().__init__()

        # store attributes
        self.parameters = OrderedDict(parameters or {})

    def _format_value(self, value: Any) -> str:
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

    def _join_parameter_pairs(self, only_significant: bool = True) -> str:
        """
        Returns a joined string representation of all significant parameters. In this context,
        significant parameters are those that potentially lead to different results (e.g. network
        architecture parameters as opposed to some log level).
        """
        return "__".join(
            f"{name}_{self._format_value(value)}"
            for name, value in self.parameter_pairs(only_significant=True)
        )

    def parameter_pairs(self, only_significant: bool = False) -> list[tuple[str, Any]]:
        """
        Returns a list of all parameter name-value tuples. In this context, significant parameters
        are those that potentially lead to different results (e.g. network architecture parameters
        as opposed to some log level).
        """
        return list(self.parameters.items())

    @property
    def accepts_scheduler_messages(self) -> bool:
        """
        Whether the training or evaluation loop expects and works with messages sent from a central
        luigi scheduler through the active worker to the underlying task. See
        :py:meth:`get_scheduler_messages` for more info.
        """
        return True

    def get_scheduler_messages(self, task: law.Task) -> DotDict[str, KeyValueMessage]:
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

    def requires(self, task: law.Task) -> Any:
        """
        Returns tasks that are required for the training to run and whose outputs are needed.
        """
        return {}

    @property
    def used_columns(self) -> set[Route | str]:
        return set(self.uses())

    @property
    def produced_columns(self) -> set[Route | str]:
        return set(self.produces())

    @abc.abstractproperty
    def used_datasets(self):
        # default to be defined in soecialized MLModelBase interfaces, not by model definitions
        return

    @abc.abstractmethod
    def training_calibrators(self) -> list[str]:
        # default to be defined in soecialized MLModelBase interfaces, not by model definitions
        return

    @abc.abstractmethod
    def training_selector(self) -> str:
        # default to be defined in soecialized MLModelBase interfaces, not by model definitions
        return

    @abc.abstractmethod
    def training_producers(self) -> list[str]:
        # default to be defined in soecialized MLModelBase interfaces, not by model definitions
        return

    @abc.abstractmethod
    def sandbox(self, task: law.Task) -> str:
        """
        Given a *task*, teturns the name of a sandbox that is needed to perform model training and
        evaluation.
        """
        return

    @abc.abstractmethod
    def uses(self) -> set[Route | str]:
        """
        Returns a set of all required columns. To be implemented in subclasses.
        """
        return

    @abc.abstractmethod
    def produces(self) -> set[Route | str]:
        """
        Returns a set of all produced columns. To be implemented in subclasses.
        """
        return

    @abc.abstractmethod
    def output(self, task: law.Task) -> Any:
        """
        Returns a structure of output targets. To be implemented in subclasses.
        """
        return

    @abc.abstractmethod
    def open_model(self, target: Any) -> Any:
        """
        Implemenents the opening of a trained model from *target* (corresponding to the structure
        returned by :py:meth:`output`). To be implemented in subclasses.
        """
        return

    @abc.abstractmethod
    def train(
        self,
        task: law.Task,
        input: Any,
        output: Any,
    ) -> None:
        """
        Performs the creation and training of a model, being passed a *task* and its *input* and
        *output*. To be implemented in subclasses.
        """
        return

    @abc.abstractmethod
    def evaluate(
        self,
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
        """
        return


class MLModel(MLModelBase):
    """
    Minimal interface to ML models with connections to config objects (such as
    py:class:`order.Config` or :py:class:`order.Dataset`) and, on an optional basis, to tasks.

    Inheriting classes need to overwrite eight methods:

        - :py:meth:`sandbox`
        - :py:meth:`datasets`
        - :py:meth:`uses`,
        - :py:meth:`produces`
        - :py:meth:`requires`
        - :py:meth:`output`
        - :py:meth:`open_model`
        - :py:meth:`train`
        - :py:meth:`evaluate`

    See their documentation below for more info.

    There are several optional hooks that allow for a fine-grained configuration of additional
    training requirements (:py:meth:`requires`), diverging training and evaluation phase spaces
    (:py:meth:`training_calibrators`, :py:meth:`training_selector`, :py:meth:`training_producers`),
    or how hyper-paramaters are string encoded for output declarations (:py:meth:`parameter_pairs`).

    .. py:classattribute:: folds
       type: int

       The number of folds for the k-fold cross-validation.

    .. py:attribute:: config_inst
       type: order.Config

       Reference to the :py:class:`order.Config` object.

    .. py:attribute:: parameters
       type: OrderedDict

       A dictionary mapping parameter names to arbitrary values, such as
       ``{"layers": 5, "units": 128}``.

    .. py:attribute:: used_datasets
       type: set
       read-only

       :py:class:`order.Dataset` instances that are used by the model training.

    .. py:attribute:: used_columns
       type: set
       read-only

       Column names or :py:class:`Route`'s that are used by this model.

    .. py:attribute:: produced_columns
       type: set
       read-only

       Column names or :py:class:`Route`'s that are produces by this model.
    """

    def __init__(
        self,
        config_inst: od.Config,
        **kwargs,
    ):
        super().__init__(**kwargs)

        # store attributes
        self.config_inst = config_inst

    @property
    def used_datasets(self) -> set[od.Dataset]:
        return set(self.datasets())

    def training_calibrators(self, evaluation_calibrators: list[str]) -> list[str]:
        """
        Given a sequence of requested *evaluation_calibrators*, which are, as the name suggests,
        meant for model evaluation, this method can alter and/or replace them to define a different
        set of calibrators for the preprocessing and training pipeline. This can be helpful in cases
        where training and evaluation phase spaces, as well as the required input columns are
        intended to diverge.
        """
        return evaluation_calibrators

    def training_selector(self, evaluation_selector: str) -> str:
        """
        Given a requested *evaluation_selector*, which is, as the name suggests, meant for model
        evaluation, this method can change it to define a different selector for the preprocessing
        and training pipeline. This can be helpful in cases where training and evaluation phase
        spaces, as well as the required input columns are intended to diverge.
        """
        return evaluation_selector

    def training_producers(self, evaluation_producers: list[str]) -> list[str]:
        """
        Given a sequence of requested *evaluation_producers*, which are, as the name suggests,
        meant for model evaluation, this method can alter and/or replace them to define a different
        set of producers for the preprocessing and training pipeline. This can be helpful in cases
        where training and evaluation phase spaces, as well as the required input columns are
        intended to diverge.
        """
        return evaluation_producers

    @abc.abstractmethod
    def datasets(self) -> set[od.Dataset]:
        """
        Returns a set of all required datasets. To be implemented in subclasses.
        """
        return


class MLModelMultiConfig(MLModelBase):

    def __init__(
        self,
        config_insts: list[od.Config],
        **kwargs,
    ):
        super().__init__(**kwargs)

        # store attributes
        self.config_insts = config_insts

    @property
    def used_datasets(self) -> dict[str, set[od.Dataset]]:
        return {
            config_inst: set(self.datasets(config_inst))
            for config_inst in self.config_insts

        }

    def training_calibrators(
        self,
        config_inst: od.Config,
        evaluation_calibrators: list[str],
    ) -> list[str]:
        """
        Given a sequence of requested *evaluation_calibrators* for a *config_inst*, which are, as
        the name suggests, meant for model evaluation, this method can alter and/or replace them to
        define a different set of calibrators for the preprocessing and training pipeline. This can
        be helpful in cases where training and evaluation phase spaces, as well as the required
        input columns are intended to diverge.
        """
        return evaluation_calibrators

    def training_selector(
        self,
        config_inst: od.Config,
        evaluation_selector: str,
    ) -> str:
        """
        Given a requested *evaluation_selector* for a *config_inst*, which is, as the name suggests,
        meant for model evaluation, this method can change it to define a different selector for the
        preprocessing and training pipeline. This can be helpful in cases where training and
        evaluation phase spaces, as well as the required input columns are intended to diverge.
        """
        return evaluation_selector

    def training_producers(
        self,
        config_inst: od.Config,
        evaluation_producers: list[str],
    ) -> list[str]:
        """
        Given a sequence of requested *evaluation_producers* for a *config_inst*, which are, as the
        name suggests, meant for model evaluation, this method can alter and/or replace them to
        define a different set of producers for the preprocessing and training pipeline. This can be
        helpful in cases where training and evaluation phase spaces, as well as the required input
        columns are intended to diverge.
        """
        return evaluation_producers

    @abc.abstractmethod
    def datasets(self, config_inst: od.Config) -> set[od.Dataset]:
        """
        Returns a set of all required datasets for a certain *config_inst*. To be implemented in
        subclasses.
        """
        return
