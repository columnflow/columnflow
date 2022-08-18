# coding: utf-8

"""
Object and event calibration tools.
"""

from typing import Optional, Union, Callable

from columnflow.util import DerivableMeta
from columnflow.columnar_util import TaskArrayFunction


Calibrator = TaskArrayFunction.derive("Calibrator")


def calibrator(
    func: Optional[Callable] = None,
    bases=(),
    **kwargs,
) -> Union[DerivableMeta, Callable]:
    """
    Decorator for creating a new :py:class:`Calibrator` subclass with additional, optional *bases*
    and attaching the decorated function to it as ``call_func``. All additional *kwargs* are added
    as class members of the new subclasses.
    """
    def decorator(func: Callable) -> DerivableMeta:
        # create the class dict
        cls_dict = {"call_func": func}
        cls_dict.update(kwargs)

        # create the subclass
        subclass = Calibrator.derive(func.__name__, bases=bases, cls_dict=cls_dict)

        return subclass

    return decorator(func) if func else decorator
