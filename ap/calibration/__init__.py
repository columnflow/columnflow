# coding: utf-8

"""
Object and event calibration tools.
"""

from typing import Optional, Union, Callable

from ap.columnar_util import TaskArrayFunction


class Calibrator(TaskArrayFunction):

    # dedicated instance cache
    _instances = {}


def calibrator(func: Optional[Callable] = None, **kwargs) -> Union[Calibrator, Callable]:
    """
    Decorator for registering new calibrator functions. All *kwargs* are forwarded to the
    :py:class:`Calibrator` constructor.
    """
    def decorator(func):
        return Calibrator.new(func, **kwargs)

    return decorator(func) if func else decorator
