# coding: utf-8

"""
Definition of basic objects for describing ML models.
"""

import copy as _copy
from typing import Optional, Union, Sequence, List, Tuple, Any, Set

import law
import order as od

from ap.util import maybe_import
from ap.columnar_util import Route

ak = maybe_import("awkward")


class MLModel(object):
    """
    Subclass of :py:class:`ArrayFunction` that provides access to ML model meta data.

    .. py:attribute:: backend
       type: type

       The backend class containing functions depending on the backend (e.g. opening models).

    .. py:attribute:: datasets
       type: list

       A list of :py:class:`order.Dataset` instances that are required by the model training.

    .. py:attribute:: folds
       type: int

       The number for folds to be used for k-fold cross-validation.

    .. py:attribute: update_func
       type: callable

       The registered function defining what to update, or *None*.

    .. py:attribute:: training_func
       type: callable

       The registered function performing the training, or *None*.
    """

    _instances = {}

    @classmethod
    def new(cls, name: str, *args, **kwargs) -> "MLModel":
        # check if the instance is already registered
        if name in cls._instances:
            raise ValueError(f"a model named '{name}' was already registered ({cls.get(name)})")

        # create it
        cls._instances[name] = cls(name, *args, **kwargs)

        return cls._instances[name]

    @classmethod
    def get(cls, name: str, copy: bool = False) -> "MLModel":
        if name not in cls._instances:
            raise ValueError(f"no model named named '{name}' found")

        return _copy.copy(cls._instances[name]) if copy else cls._instances[name]

    def __init__(
        self,
        name: str,
        *,
        config_inst: Optional[od.Config] = None,
        parameters: Optional[Sequence[Tuple[str, Any]]] = None,
        folds: int = 2,
    ):
        super().__init__()

        # store attributes
        self.name = name
        self.config_inst = None
        self.folds = folds
        self.parameters = parameters or []

        # cache for attributes that need to be defined in inheriting classes
        self._used_datasets = None
        self._used_columns = None
        self._produced_columns = None

        # set the config when given
        if config_inst:
            self.set_config(config_inst)

    def __str__(self):
        return f"{self.__class__.__name__}__{self.name}"

    def __repr__(self):
        return f"<{self.__class__.__name__} '{self.name}' at {hex(id(self))}>"

    def _format_value(self, value):
        if isinstance(value, (list, tuple)):
            return "_".join(map(self._format_value, value))
        if isinstance(value, bool):
            return str(value).lower()
        if isinstance(value, float):
            # scientific notation when too small
            return f"{value}" if value >= 0.01 else f"{value:.2e}"

        # any other case
        return str(value)

    def _join_parameter_pairs(self) -> str:
        return "__".join(
            f"{name}_{self._format_value(value)}"
            for name, value in self.parameter_pairs(only_significant=True)
        )

    def set_config(self, config_inst: od.Config) -> None:
        self.config_inst = config_inst

    def parameter_pairs(self, only_significant: bool = False) -> List[Tuple[str, Any]]:
        return self.parameters

    @property
    def used_datasets(self) -> Set[od.Dataset]:
        if self._used_datasets is None:
            self._used_datasets = set(self.datasets())
        return self._used_datasets

    @property
    def used_columns(self) -> Set[Union[Route, str]]:
        if self._used_columns is None:
            self._used_columns = set(self.uses())
        return self._used_columns

    @property
    def produced_columns(self) -> Set[Union[Route, str]]:
        if self._produced_columns is None:
            self._produced_columns = set(self.produces())
        return self._produced_columns

    def datasets(self) -> Set[od.Dataset]:
        raise NotImplementedError()

    def uses(self) -> Set[Union[Route, str]]:
        raise NotImplementedError()

    def produces(self) -> Set[Union[Route, str]]:
        raise NotImplementedError()

    def output(self, task: law.Task) -> Any:
        raise NotImplementedError()

    def open_model(self, output: Any) -> Any:
        raise NotImplementedError()

    def train(
        self,
        task: law.Task,
        input: Any,
        output: Any,
    ) -> None:
        raise NotImplementedError()

    def evaluate(
        self,
        task: law.Task,
        events: ak.Array,
        models: List[Any],
        fold_indices: ak.Array,
        events_used_in_training: bool = False,
    ) -> None:
        raise NotImplementedError()
