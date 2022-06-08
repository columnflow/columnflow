# coding: utf-8

"""
Object and event selection tools.
"""

from typing import Optional, Union, Callable

import law

from ap.util import maybe_import, DotDict
from ap.columnar_util import TaskArrayFunction

ak = maybe_import("awkward")


class Selector(TaskArrayFunction):

    # dedicated instance cache
    _instances = {}


def selector(func: Optional[Callable] = None, **kwargs) -> Union[Selector, Callable]:
    """
    Decorator for registering new selector functions. All *kwargs* are forwarded to the
    :py:class:`Selector` constructor.
    """
    def decorator(func):
        return Selector.new(func, **kwargs)

    return decorator(func) if func else decorator


class SelectionResult(object):
    """
    Lightweight class that wraps selection decisions (e.g. masks and new columns) and provides
    convenience methods to merge them or to dump them into an awkward array. The resulting structure
    looks like the following example:

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
        super().__init__()

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
                "objects": ak.zip(self.objects, depth_limit=1),  # limit due to ragged axis 1
                "columns": ak.zip(self.columns),
            },
        ))


# import all selection modules
if law.config.has_option("analysis", "selection_modules"):
    for mod in law.config.get_expanded("analysis", "selection_modules", split_csv=True):
        maybe_import(mod.strip())
