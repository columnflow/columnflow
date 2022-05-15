# coding: utf-8

"""
Object and event selection tools.
"""

from typing import Optional, Union, Callable

import law

from ap.util import import_module, DotDict, ColumnConsumer

ak = import_module("awkward")


class Selector(ColumnConsumer):
    """
    Wrapper class for functions performing object and event calibration on (most likely) coffea nano
    event arrays. The main purpose of wrappers is to store information about required columns next
    to the implementation. In addition, they have a unique name which allows for using it in a
    config file.

    The use of the :py:func:`selectpr` decorator function is recommended to create selector
    instances. Example:

    .. code-block:: python

        @selector(uses={"nJet", "Jet_pt"})
        def my_jet_selection(events):
            jet_mask = events.Jet.pt > 30
            jet_sel = ak.sum(jet_mask, axis=1) >= 4

            return SelectionResult(steps={"jet": jet_sel}, objects={"jet": jet_mask})

    This will register and return an instance named "my_jet_selection" that *uses* the "nJet" and
    "Jet_pt" columns of the array structure.

    The name defaults to the name of the function itself and can be altered by passing a custom
    *name*. It is used internally to store the instance in a cache from which it can be retrieved
    through the :py:meth:`get` class method.

    Knowledge of the columns to load is especially useful when opening files and selecting the
    content to deserialize. *uses* accepts not only strings but also previously registered instances
    to denote in inner dependence. Column names should always be given in the flat nano nomenclature
    (using underscores). The :py:attr:`used_columns` property will resolve this information and
    return a set of column names. Example:

    .. code-block:: python

        @selector(uses={my_jet_selection})
        def my_event_selection(events):
            result = my_jet_selection(events)
            return result

        print(my_event_selection.used_columns)
        # -> {"nJet", "Jet_pt"}

    .. py:attribute:: func
       type: callable

       The wrapped function.

    .. py:attribute:: name
       type: str

       The name of the selector in the instance cache.

    .. py:attribute:: uses
       type: set

       The set of column names or other selector instances to recursively resolve the names of
       required columns.

    .. py::attribute:: used_columns
       type: set
       read-only

       The resolved, flat set of used column names.
    """

    # create an own instance cache
    _instances = {}


def selector(
    func: Optional[Callable] = None,
    **kwargs,
) -> Union[Selector, Callable]:
    """
    Decorator for registering new selector functions. See :py:class:`Selector` for documentation.
    """
    def decorator(func):
        return Selector.new(func, **kwargs)

    return decorator(func) if func else decorator


class SelectionResult(object):
    """
    Lightweight class that wraps selection decisions (e.g. masks and new columns) and provides
    convenience methods to merge them or to dump them into an awkward array. The resulting structure
    looks like the example following:

    .. code-block:: python

        {
            # arbitrary, top-level main fields
            ...

            "steps": {
                # event selection decisions from certain steps
                "jet": array,
                "muon": array,
                ...
            },

            "objects": {
                # object selection decisions or indices
                "jet": array,
                "muon": array,
                ...
            },

            "columns": {
                # any structure of new columns to be added
                "my_new_column_a": array,
                "my_new_column_b": array,
                ...
            }
        }

    The fields can be configured through the *main*, *steps*, *objects* and *columns* keyword
    arguments. The following example creates the structure above.

    .. code-block:: python

        res = SelectionResult(
            main={...},
            steps={"jet": array, "muon": array, ...}
            objects={"jet": array, "muon": array, ...}
            columns={"my_new_column_a": array, "my_new_column_b": array, ...}
        )
        res.to_ak()
    """

    def __init__(
        self,
        main: Optional[Union[DotDict, dict]] = None,
        steps: Optional[Union[DotDict, dict]] = None,
        objects: Optional[Union[DotDict, dict]] = None,
        columns: Optional[Union[DotDict, dict]] = None,
    ):
        super(SelectionResult, self).__init__()

        # store fields
        self.main = DotDict(main or {})
        self.steps = DotDict(steps or {})
        self.objects = DotDict(objects or {})
        self.columns = DotDict(columns or {})

    def __iadd__(self, other: "SelectionResult") -> "SelectionResult":
        """
        Adds the field of a *other* instance in-place.
        """
        if not isinstance(other, SelectionResult):
            raise TypeError(f"cannot add '{other}' to {self.__class__.__name__} instance")

        # update fields in-place
        self.main.update(other.main)
        self.steps.update(other.steps)
        self.objects.update(other.objects)
        self.columns.update(other.columns)

        return self

    def __add__(self, other: "SelectionResult") -> "SelectionResult":
        """
        Returns a new instance with all fields of *this* and an *other* instance merged.
        """
        inst = self.__class__()

        inst += self
        inst += other

        return inst

    def to_ak(self) -> ak.Array:
        """
        Converts the contained fields into a nested awkward array and returns it.
        """
        return ak.zip(law.util.merge_dicts(
            self.main,
            {
                "steps": ak.zip(self.steps),
                "objects": ak.zip(self.objects, depth_limit=1),  # limit due to ragged first axis
                "columns": ak.zip(self.columns),
            }
        ))


# import all selection modules
for mod in law.config.get_expanded("analysis", "selection_modules", split_csv=True):
    import_module(mod.strip())
