# coding: utf-8

"""
Tools for producing new array columns (e.g. high-level variables).
"""

from typing import Optional, Union, Callable

from ap.columnar_util import TaskArrayFunction


class Producer(TaskArrayFunction):

    _instances = {}


def producer(func: Optional[Callable] = None, **kwargs) -> Union[Producer, Callable]:
    """
    Decorator for registering new producer functions. All *kwargs* are forwarded to the
    :py:class:`Producer` constructor.
    """
    def decorator(func):
        return Producer.new(func, **kwargs)

    return decorator(func) if func else decorator
