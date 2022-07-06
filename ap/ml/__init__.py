# coding: utf-8

"""
Definition of basic objects for describing and creating ML models.
"""

import copy as _copy
from collections import OrderedDict
from typing import Optional, Union, List, Tuple, Any, Set

import law
import order as od

from ap.util import maybe_import
from ap.columnar_util import Route

ak = maybe_import("awkward")


class MLModel(object):
    """
    Minimal interface to ML models with connections to config objects (such as
    py:class:`orderd.Config` or :py:class:`order.Dataset`) and, on an optional basis, to tasks.

    Inheriting classes need to overwrite seven methods: :py:meth:`datasets`, :py:meth:`uses`,
    :py:meth:`produces`, :py:meth:`output`, :py:meth:`open_model`, :py:meth:`train`, and
    :py:meth:`evaluate`.

    .. py:attribute:: name
       type: str

       The unique name of this model.

    .. py:attribute:: config_inst
       type: order.Config, None

       Reference to the :py:class:`order.Config` object.

    .. py:attribute:: folds
       type: int

       The number of folds for the k-fold cross-validation.

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

    _instances = {}

    @classmethod
    def new(cls, name: str, *args, **kwargs) -> "MLModel":
        """
        Creates a new instance with *name* and all *args* and *kwargs*, adds it to the instance
        cache and returns it. A *ValueError* is raised in case an instance with the same name was
        registered before.
        """
        # check if the instance is already registered
        if name in cls._instances:
            raise ValueError(f"a ML model named '{name}' was already registered ({cls.get(name)})")

        # create it
        cls._instances[name] = cls(name, *args, **kwargs)

        return cls._instances[name]

    @classmethod
    def get(cls, name: str, copy: bool = False) -> "MLModel":
        """
        Returns a previously registered instance named *name* and optionally copies it when *copy*
        is *True*. A *ValueError* is raised when no instance was found with that name.
        """
        if name not in cls._instances:
            raise ValueError(f"no ML model named '{name}' found")

        return _copy.copy(cls._instances[name]) if copy else cls._instances[name]

    def __init__(
        self,
        name: str,
        *,
        config_inst: Optional[od.Config] = None,
        parameters: Optional[OrderedDict] = None,
        folds: int = 2,
    ):
        super().__init__()

        # store attributes
        self.name = name
        self.config_inst = None
        self.folds = folds
        self.parameters = OrderedDict(parameters or {})

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

    def parameter_pairs(self, only_significant: bool = False) -> List[Tuple[str, Any]]:
        """
        Returns a list of all parameter name-value tuples. In this context, significant parameters
        are those that potentially lead to different results (e.g. network architecture parameters
        as opposed to some log level).
        """
        return list(self.parameters.items())

    def set_config(self, config_inst: od.Config) -> None:
        """
        Sets the :py:attr:`config_inst` attribute to *config_inst*.
        """
        self.config_inst = config_inst

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
        """
        Returns a set of all required datasets. To be implemented in subclasses.
        """
        raise NotImplementedError()

    def uses(self) -> Set[Union[Route, str]]:
        """
        Returns a set of all required columns. To be implemented in subclasses.
        """
        raise NotImplementedError()

    def produces(self) -> Set[Union[Route, str]]:
        """
        Returns a set of all produced columns. To be implemented in subclasses.
        """
        raise NotImplementedError()

    def output(self, task: law.Task) -> Any:
        """
        Returns a structure of :py:class:`ap.util.FunctionArgs` objects that can be used by *task*
        to create actual output target objects. To be implemented in subclasses.
        """
        raise NotImplementedError()

    def open_model(self, target: Any) -> Any:
        """
        Implemenents the opening of a trained model from *target* (corresponding to the structure
        returned by :py:meth:`output`). To be implemented in subclasses.
        """
        raise NotImplementedError()

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
        raise NotImplementedError()

    def evaluate(
        self,
        task: law.Task,
        events: ak.Array,
        models: List[Any],
        fold_indices: ak.Array,
        events_used_in_training: bool = False,
    ) -> None:
        """
        Performs the model evaluation for a *task* on a chunk of *events*. The list of *models*
        corresponds to the number of folds generated by this model, and the already evaluated
        *fold_indices* for this event chunk that might used depending on *events_used_in_training*.
        To be implemented in subclasses.
        """
        raise NotImplementedError()
