# coding: utf-8

"""
Tools for producing new array columns (e.g. high-level variables).
"""

from typing import Optional, Union, Callable

from columnflow.util import DerivableMeta
from columnflow.columnar_util import TaskArrayFunction


Producer = TaskArrayFunction.derive("Producer")


def producer(
    func: Optional[Callable] = None,
    bases=(),
    **kwargs,
) -> Union[DerivableMeta, Callable]:
    """
    Decorator for creating a new :py:class:`Producer` subclass with additional, optional *bases* and
    attaching the decorated function to it as ``call_func``. All additional *kwargs* are added as
    class members of the new subclasses.
    """
    def decorator(func: Callable) -> DerivableMeta:
        # create the class dict
        cls_dict = {"call_func": func}
        cls_dict.update(kwargs)

        # create the subclass
        subclass = Producer.derive(func.__name__, bases=bases, cls_dict=cls_dict)

        return subclass

    return decorator(func) if func else decorator
