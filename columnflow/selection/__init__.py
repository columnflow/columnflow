# coding: utf-8

"""
Object and event selection tools.
"""

from typing import Optional, Union, Callable

import law

from columnflow.util import maybe_import, DotDict, DerivableMeta
from columnflow.columnar_util import TaskArrayFunction

ak = maybe_import("awkward")


class Selector(TaskArrayFunction):

    exposed = False

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # when not exposed and call_force is not specified,
        # set it to True which prevents calls from being cached
        if self.call_force is None and not self.exposed:
            self.call_force = True


def selector(
    func: Optional[Callable] = None,
    bases=(),
    **kwargs,
) -> Union[DerivableMeta, Callable]:
    """
    Decorator for creating a new :py:class:`Selector` subclass with additional, optional *bases* and
    attaching the decorated function to it as ``call_func``. All additional *kwargs* are added as
    class members of the new subclasses.
    """
    def decorator(func: Callable) -> DerivableMeta:
        # create the class dict
        cls_dict = {"call_func": func}
        cls_dict.update(kwargs)

        # create the subclass
        subclass = Selector.derive(func.__name__, bases=bases, cls_dict=cls_dict)

        return subclass

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
        # use deep merging for objects
        law.util.merge_dicts(self.objects, other.objects, inplace=True, deep=True)

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
            to_merge["objects"] = ak.zip({
                src_name: ak.zip(dst_dict, depth_limit=1)  # limit due to ragged axis 1
                for src_name, dst_dict in self.objects.items()
            })

        return ak.zip({**self.main, **to_merge})
