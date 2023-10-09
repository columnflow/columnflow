# coding: utf-8

"""
Custom type definitions and shorthands to simplify imports of types that are spread across multiple
packages.
"""

from __future__ import annotations


__all__ = []


from collections.abc import KeysView, ValuesView  # noqa
from typing import (  # noqa
    Any, Union, TypeVar, ClassVar, List, Tuple, Sequence, Set, Dict, Callable, Generator, TextIO,
    Iterable,
)
from types import ModuleType, GeneratorType  # noqa

from typing_extensions import Annotated, _AnnotatedAlias as AnnotatedType  # noqa

#: Generic type variable, more stringent than Any.
T = TypeVar("T")
