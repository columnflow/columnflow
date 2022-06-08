# coding: utf-8

"""
Tools for producing new array columns (e.g. high-level variables).
"""

from typing import Optional, Union, Callable

import law

from ap.util import maybe_import
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


# import all production modules
if law.config.has_option("analysis", "production_modules"):
    for mod in law.config.get_expanded("analysis", "production_modules", split_csv=True):
        maybe_import(mod.strip())
