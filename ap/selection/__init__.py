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

    def __init__(self, *args, exposed=False, **kwargs):
        super().__init__(*args, **kwargs)

        # flag denoting whether this selector is exposed, i.e., callable from tasks and returning
        # an actual SelectionResult
        self.exposed = False


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
    Lightweight class that wraps selection decisions (e.g. event and object selection steps) and
    provides convenience methods to merge them or to dump them into an awkward array. The resulting
    structure looks like the following example:

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
        }

    The fields can be configured through the *main*, *steps* and *objects* keyword arguments. The
    following example creates the structure above.

    .. code-block:: python

        res = SelectionResult(
            main={...},
            steps={"jet": array, "muon": array, ...}
            objects={"jet": array, "muon": array, ...}
        )
        res.to_ak()
    """

    def __init__(
        self,
        main: Optional[Union[DotDict, dict]] = None,
        steps: Optional[Union[DotDict, dict]] = None,
        objects: Optional[Union[DotDict, dict]] = None,
    ):
        super().__init__()

        # store fields
        self.main = DotDict.wrap(main or {})
        self.steps = DotDict.wrap(steps or {})
        self.objects = DotDict.wrap(objects or {})

    def __iadd__(self, other: Union["SelectionResult", None]) -> "SelectionResult":
        """
        Adds the field of an *other* instance in-place. When *None*, *this* instance is returned
        unchanged.
        """
        # do nothing if the other instance is none
        if other is None:
            return self

        # type check
        if not isinstance(other, SelectionResult):
            raise TypeError(f"cannot add '{other}' to {self.__class__.__name__} instance")

        # update fields in-place
        self.main.update(other.main)
        self.steps.update(other.steps)
        self.objects.update(other.objects)

        return self

    def __add__(self, other: Union["SelectionResult", None]) -> "SelectionResult":
        """
        Returns a new instance with all fields of *this* and an *other* instance merged. When
        *None*, a copy of *this* instance is returned.
        """
        inst = self.__class__()

        # add this instance
        inst += self

        # add the other instance if not none
        if other is not None:
            if not isinstance(other, SelectionResult):
                raise TypeError(f"cannot add '{other}' to {self.__class__.__name__} instance")
            inst += other

        return inst

    def to_ak(self) -> ak.Array:
        """
        Converts the contained fields into a nested awkward array and returns it.
        """
        to_merge = {}
        if self.steps:
            to_merge["steps"] = ak.zip(self.steps)
        if self.objects:
            to_merge["objects"] = ak.zip(self.objects, depth_limit=1)  # limit due to ragged axis 1

        return ak.zip(law.util.merge_dicts(self.main, to_merge))
