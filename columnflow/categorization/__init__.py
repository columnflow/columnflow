# coding: utf-8

"""
Event categorization tools.
"""

from __future__ import annotations

import inspect
from typing import Callable

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

        :param func: Function to be wrapped and integrated into new :py:class:`Categorizer`
            instance.
        :param bases: Additional base classes for new :py:class:`Categorizer`.
        :return: The new :py:class:`Categorizer` instance.
        """
        def decorator(func: Callable) -> DerivableMeta:
            # create the class dict
            cls_dict = {
                "call_func": func,
            }
            cls_dict.update(kwargs)

            # get the module name
            frame = inspect.stack()[1]
            module = inspect.getmodule(frame[0])

            # get the categorizer name
            cls_name = cls_dict.pop("cls_name", func.__name__)

            # create the subclass
            subclass = cls.derive(cls_name, bases=bases, cls_dict=cls_dict, module=module)

            return subclass

        return decorator(func) if func else decorator


# shorthand
categorizer = Categorizer.categorizer
