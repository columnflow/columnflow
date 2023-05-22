# coding: utf-8

"""
Definition of basic objects for describing and creating BinOptimizer
"""

from __future__ import annotations

from collections import OrderedDict
from typing import Any

import law
import order as od

from columnflow.util import maybe_import, Derivable, DotDict, KeyValueMessage
from columnflow.columnar_util import Route


ak = maybe_import("awkward")


class BinOptimizer(Derivable):
    # NOTE: copied from MLModel, the might be some ml related leftovers
    # We need optional methods for required_events, required_histograms
    # We need exemplary methods for prepare_events, prepare_histograms  (but can be implemented by the user)
    # User implements a bin_optimizer function that takes the output of those functions
    """
    Minimal interface to BinOptimizer with connections to config objects (such as
    py:class:`order.Config` or :py:class:`order.Dataset`) and, on an optional basis, to tasks.

    Inheriting classes need to overwrite eight methods:

        - :py:meth:`sandbox`
        - :py:meth:`datasets`
        - :py:meth:`uses`
        - :py:meth:`produces`  # NOTE: remove?
        - :py:meth:`requires`
        - :py:meth:`output`
        - :py:meth:`open_model`
        - :py:meth:`train`
        - :py:meth:`evaluate`

    See their documentation below for more info.

    There are several optional hooks that allow for a fine-grained configuration of additional
    training requirements (:py:meth:`requires`), diverging optimization and evaluation phase spaces
    (:py:meth:`optimizing_calibrators`, :py:meth:`optimizing_selector`, :py:meth:`optimizing_producers`),
    or how paramaters are string encoded for output declarations (:py:meth:`parameter_pairs`).

    .. py:attribute:: config_inst
       type: order.Config

       Reference to the :py:class:`order.Config` object.

    .. py:attribute:: parameters
       type: OrderedDict

       A dictionary mapping parameter names to arbitrary values, such as
       ``{"n_bins": 15, "min_events_per_bin": 3}``.

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
        *,
        parameters: OrderedDict | None = None,
    ):
        super().__init__()

        # store attributes
        self.config_inst = config_inst
        self.parameters = OrderedDict(parameters or {})

        # cache for attributes that need to be defined in inheriting classes
        self._used_datasets = None
        self._used_columns = None
        self._produced_columns = None

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
    def used_datasets(self) -> set[od.Dataset]:
        if self._used_datasets is None:
            self._used_datasets = set(self.datasets())
        return self._used_datasets

    @property
    def used_columns(self) -> set[Route | str]:
        if self._used_columns is None:
            self._used_columns = set(self.uses())
        return self._used_columns

    @property
    def produced_columns(self) -> set[Route | str]:
        if self._produced_columns is None:
            self._produced_columns = set(self.produces())
        return self._produced_columns

    @property
    def accepts_scheduler_messages(self) -> bool:
        """
        Whether the bin_function call expects and works with messages sent from a central
        luigi scheduler through the active worker to the underlying task. See
        :py:meth:`get_scheduler_messages` for more info.
        """
        return True

    def optimizing_calibrators(self, evaluation_calibrators: list[str]) -> list[str]:
        """
        Given a sequence of requested *evaluation_calibrators*, which are, as the name suggests,
        meant for model evaluation, this method can alter and/or replace them to define a different
        set of calibrators for the preprocessing and optimizing pipeline. This can be helpful in cases
        where optimizing and evaluation phase spaces, as well as the required input columns are
        intended to diverge.
        """
        return evaluation_calibrators

    def optimizing_selector(self, evaluation_selector: str) -> str:
        """
        Given a requested *evaluation_selector*, which is, as the name suggests, meant for model
        evaluation, this method can change it to define a different selector for the preprocessing
        and optimizing pipeline. This can be helpful in cases where optimizing and evaluation phase
        spaces, as well as the required input columns are intended to diverge.
        """
        return evaluation_selector

    def optimizing_producers(self, evaluation_producers: list[str]) -> list[str]:
        """
        Given a sequence of requested *evaluation_producers*, which are, as the name suggests,
        meant for model evaluation, this method can alter and/or replace them to define a different
        set of producers for the preprocessing and optimizing pipeline. This can be helpful in cases
        where optimizing and evaluation phase spaces, as well as the required input columns are
        intended to diverge.
        """
        return evaluation_producers

    def requires(self, task: law.Task) -> Any:
        """
        Returns tasks that are required for the optimizing to run and whose outputs are needed.
        """
        return {}

    def sandbox(self, task: law.Task) -> str:
        """
        Given a *task*, teturns the name of a sandbox that is needed to perform model optimizing and
        evaluation.
        """
        raise NotImplementedError()

    def datasets(self) -> set[od.Dataset]:
        """
        Returns a set of all required datasets. To be implemented in subclasses.
        """
        raise NotImplementedError()

    def uses(self) -> set[Route | str]:
        """
        Returns a set of all required columns. To be implemented in subclasses.
        """
        raise NotImplementedError()

    def produces(self) -> set[Route | str]:
        """
        Returns a set of all produced columns. To be implemented in subclasses.
        """
        raise NotImplementedError()

    def output(self, task: law.Task) -> Any:
        """
        Returns a structure of output targets. To be implemented in subclasses.
        """
        raise NotImplementedError()

    def optimize(
        self,
        task: law.Task,
        input: Any,
        output: Any,
    ) -> None:
        """
        Performs the bin optimization, being passed a *task* and its *input* and
        *output*. To be implemented in subclasses.
        """
        raise NotImplementedError()

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
