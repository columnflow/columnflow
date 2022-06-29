# coding: utf-8

"""
Definition of basic objects for describing ML models.
"""

import copy as _copy
from typing import Optional, Union, Sequence, List, Tuple, Any, Set

import law
import order as od

from ap.columnar_util import Route


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
        self.config_inst = config_inst
        self.folds = folds
        self.parameters = parameters or []

        # cache for attributes that need to be defined in inheriting classes
        self._datasets = None
        self._uses = None
        self._produces = None

    def __str__(self):
        return f"{self.__class__.__name__}__{self.name}"

    def __repr__(self):
        return f"<{self.__class__.__name__} '{self.name}' at {hex(id(self))}>"

    def _join_parameter_pairs(self) -> str:
        return "__".join(f"{name}_{value}" for name, value in self.parameter_pairs())

    def parameter_pairs(self) -> List[Tuple[str, Any]]:
        return self.parameters

    @property
    def datasets(self) -> Set[od.Dataset]:
        if self._datasets is None:
            self._datasets = set(self.define_datasets())
        return self._datasets

    @property
    def uses(self) -> Set[Union[Route, str]]:
        if self._uses is None:
            self._uses = set(self.define_used_columns())
        return self._uses

    @property
    def produces(self) -> Set[Union[Route, str]]:
        if self._produces is None:
            self._produces = set(self.define_produced_columns())
        return self._produces

    def define_datasets(self) -> Set[od.Dataset]:
        raise NotImplementedError()

    def define_used_columns(self) -> Set[Union[Route, str]]:
        raise NotImplementedError()

    def define_produced_columns(self) -> Set[Union[Route, str]]:
        raise NotImplementedError()

    def define_output(self, task: law.Task) -> Any:
        raise NotImplementedError()

    def open_model(self, output: Any) -> Any:
        raise NotImplementedError()

    def train(self, input: Any, output: Any) -> None:
        raise NotImplementedError()

    def evaluate(self) -> None:
        raise NotImplementedError()
