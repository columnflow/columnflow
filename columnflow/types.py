# coding: utf-8

"""
Custom type definitions and shorthands to simplify imports of types that are spread across multiple
packages.
"""

from __future__ import annotations

__all__ = []

# warn when imported while _in_ this directory
import os
thisdir = os.path.dirname(os.path.abspath(__file__))
if os.path.realpath(thisdir) == os.path.realpath(os.getcwd()):
    msg = """
NOTE: you are running a python interpreter inside the columnflow source directory which
      is highly discouraged as it leads to unintended local imports in builtin packages
"""
    print(msg, flush=True)

from collections.abc import KeysView, ValuesView  # noqa
from types import ModuleType, GeneratorType, GenericAlias  # noqa
from typing import (  # noqa
    Any, Union, TypeVar, ClassVar, Sequence, Callable, Generator, TextIO, Iterable,
)

from typing_extensions import Annotated, _AnnotatedAlias as AnnotatedType  # noqa


#: Generic type variable, more stringent than Any.
T = TypeVar("T")
