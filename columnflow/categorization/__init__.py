# coding: utf-8

"""
Event categorization tools.
"""

from __future__ import annotations

import inspect

from columnflow.types import Callable
from columnflow.util import DerivableMeta
from columnflow.columnar_util import TaskArrayFunction


class Categorizer(TaskArrayFunction):
    """
    Base class for all categorizers.
    """

    exposed = True

    @classmethod
    def categorizer(
        cls,
        func: Callable | None = None,
        bases: tuple = (),
        **kwargs,
    ) -> DerivableMeta | Callable:
        """
        Decorator for creating a new :py:class:`~.Categorizer` subclass with additional, optional
        *bases* and attaching the decorated function to it as ``call_func``.

        All additional *kwargs* are added as class members of the new subclasses.

        :param func: Function to be wrapped and integrated into new :py:class:`Categorizer` class.
        :param bases: Additional base classes for new :py:class:`Categorizer`.
        :return: The new :py:class:`Categorizer` subclass.
        """
        def decorator(func: Callable) -> DerivableMeta:
            # create the class dict
            cls_dict = {
                **kwargs,
                "call_func": func,
            }

            # get the module name
            frame = inspect.stack()[1]
            module = inspect.getmodule(frame[0])

            # get the categorizer name
            cls_name = cls_dict.pop("cls_name", func.__name__)

            # disable call caching since the current implementation does not work with multiple returns
            cls_dict["call_force"] = True

            # create the subclass
            subclass = cls.derive(cls_name, bases=bases, cls_dict=cls_dict, module=module)

            return subclass

        return decorator(func) if func else decorator


# shorthand
categorizer = Categorizer.categorizer
