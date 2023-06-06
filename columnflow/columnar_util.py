# coding: utf-8

"""
Helpers and utilities for working with columnar libraries.
"""

from __future__ import annotations

__all__ = [
    "mandatory_coffea_columns", "EMPTY_INT", "EMPTY_FLOAT",
    "Route", "RouteFilter", "ArrayFunction", "TaskArrayFunction", "ChunkedIOHandler",
    "eval_item", "get_ak_routes", "has_ak_column", "set_ak_column", "remove_ak_column",
    "add_ak_alias", "add_ak_aliases", "update_ak_array", "flatten_ak_array", "sort_ak_fields",
    "sorted_ak_to_parquet", "attach_behavior", "layout_ak_array", "flat_np_view",
]

import gc
import re
import math
import time
import copy
import enum
import threading
import multiprocessing
import multiprocessing.pool
from functools import partial
from collections import namedtuple, OrderedDict, defaultdict
from typing import Sequence, Callable, Any

import law

from columnflow.util import (
    UNSET, maybe_import, classproperty, DotDict, DerivableMeta, Derivable, pattern_matcher,
    get_source_code,
)


np = maybe_import("numpy")
ak = maybe_import("awkward")
dak = maybe_import("dask_awkward")
uproot = maybe_import("uproot")
coffea = maybe_import("coffea")
maybe_import("coffea.nanoevents")
maybe_import("coffea.nanoevents.methods.base")
maybe_import("coffea.nanoevents.methods.nanoaod")
pq = maybe_import("pyarrow.parquet")

logger = law.logger.get_logger(__name__)
logger_perf = law.logger.get_logger(f"{__name__}-perf")


#: Columns that are always required when opening a nano file with coffea.
mandatory_coffea_columns = {"run", "luminosityBlock", "event"}

#: Empty-value definition in places where an integer number is expected but not present.
EMPTY_INT = -99999

#: Empty-value definition in places where a float number is expected but not present.
EMPTY_FLOAT = -99999.0


class ItemEval(object):
    """
    Simple item evaluation helper, similar to NumPy's ``s_``. Example:

    .. code-block:: python

        ItemEval()[0:2, ...]
        # -> (slice(0, 2), Ellipsis)

        ItemEval()("[0:2, ...]")
        # -> (slice(0, 2), Ellipsis)
    """

    def __getitem__(self, item: Any) -> Any:
        return item

    def __call__(self, s: str) -> Any:
        return eval(f"self{s}")


#: ItemEval singleton mimicking a function.
eval_item = ItemEval()


class RouteMeta(type):
    """
    Meta class for :py:class:`Route` that prevents instances from being copied when passed to the
    constructor.
    """

    def __call__(cls, route: Route | Any | None = None):
        if isinstance(route, cls):
            return route

        return super().__call__(route=route)


class Route(object, metaclass=RouteMeta):
    """
    Route objects describe the location of columns in nested arrays and are basically wrappers
    around a sequence of nested fields. Additionally, they provide convenience methods for
    conversion into column names, either in dot or nano-style underscore format.

    The constructor takes another *route* instance, a sequence of strings, or a string in dot format
    to initialize the fields. Most operators are overwritten to work with routes in a tuple-like
    fashion. Examples:

    .. code-block:: python

        route = Route("Jet.pt")
        # same as Route(("Jet", "pt"))

        len(route)
        # -> 2

        route.fields
        # -> ("Jet", "pt")

        route.column
        # -> "Jet.pt"

        route.nano_column
        # -> "Jet_pt"

        route[-1]
        # -> "pt"

        route += "jec_up"
        route.fields
        # -> ("Jet", "pt", "jec_up")

        route[1:]
        # -> "pt.jec_up"

    .. py:attribute:: fields
       type: tuple
       read-only

       The fields of this route.

    .. py:attribute:: column
       type: string
       read-only

       The name of the corresponding column in dot format.

    .. py:attribute:: nano_column
       type: string
       read-only

       The name of the corresponding column in nano-style underscore format.

    .. py:attribute:: string_column
       type: string
       read-only

       The name of the corresponding column in dot format, but only consisting of string fields,
       i.e., without slicing or indexing fields.

    .. py:attribute:: string_nano_column
       type: string
       read-only

       The name of the corresponding column in nano-style underscore format, but only consisting of
       string fields, i.e., without slicing or indexing fields.
    """

    DOT_SEP = "."
    NANO_SEP = "_"

    @classmethod
    def slice_to_str(cls, s: slice) -> str:
        s_str = ("" if s.start is None else str(s.start)) + ":"
        s_str += "" if s.stop is None else str(s.stop)
        if s.step is not None:
            s_str += f":{s.step}"
        return s_str

    @classmethod
    def _join(
        cls,
        sep: str,
        fields: Sequence[str | int | slice | type(Ellipsis) | tuple | list],
        _outer: bool = True,
    ) -> str:
        """
        Joins a sequence of *fields* into a string with a certain separator *sep* and returns it.
        """
        s = ""
        for field in fields:
            if isinstance(field, str):
                s += (sep if s else "") + (field if _outer else f"'{field}'")
            elif isinstance(field, int):
                s += f"[{field}]" if _outer else str(field)
            elif isinstance(field, slice):
                field_str = cls.slice_to_str(field)
                s += f"[{field_str}]" if _outer else field_str
            elif isinstance(field, type(Ellipsis)):
                s += "[...]" if _outer else "..."
            elif isinstance(field, tuple):
                field_str = ",".join(cls._join(sep, [f], _outer=False) for f in field)
                s += f"[{field_str}]" if _outer else field_str
            elif isinstance(field, list):
                field_str = ",".join(cls._join(sep, [f], _outer=False) for f in field)
                field_str = f"[{field_str}]"
                s += f"[{field_str}]" if _outer else field_str
            else:
                raise TypeError(f"cannot interpret field '{field}' for joining")
        return s

    @classmethod
    def join(
        cls,
        fields: Sequence[str | int | slice | type(Ellipsis) | list | tuple],
    ) -> str:
        """
        Joins a sequence of strings into a string in dot format and returns it.
        """
        return cls._join(cls.DOT_SEP, fields)

    @classmethod
    def join_nano(
        cls,
        fields: Sequence[str | int | slice | type(Ellipsis) | list | tuple],
    ) -> str:
        """
        Joins a sequence of strings into a string in nano-style underscore format and returns it.
        """
        return cls._join(cls.NANO_SEP, fields)

    @classmethod
    def _split(
        cls,
        sep: str,
        column: str,
    ) -> tuple[str | int | slice | type(Ellipsis) | list | tuple]:
        """
        Splits a string at a separator *sep* and returns the fragments, potentially with selection,
        slice and advanced indexing expressions.
        """
        # first extract and replace possibly nested slices
        # note: a regexp would be far cleaner, but there are edge cases which possibly require
        #       sequential regexp evaluations which might be less maintainable
        slices = []
        repl = lambda i: f"__slice_{i}__"
        repl_cre = re.compile(r"^__slice_(\d+)__$")
        while True:
            depth = 0
            slice_start = -1
            for i, s in enumerate(column):
                if s == "[":
                    depth += 1
                    # remember the starting point when the slice started
                    if depth == 1:
                        slice_start = i
                elif s == "]":
                    if depth <= 0:
                        raise ValueError(f"invalid column format '{column}'")
                    depth -= 1
                    # when we are back at depth 0, the slice ended
                    if depth == 0:
                        # save the slice
                        slices.append(column[slice_start:i + 1])
                        # insert a temporary replacement
                        start = column[:slice_start]
                        tmp = repl(len(slices) - 1)
                        rest = column[i + 1:]
                        if rest and not rest.startswith((sep, "[")):
                            raise ValueError(f"invalid column format '{column}'")
                        column = start + (sep if start else "") + tmp + rest
                        # start over
                        break
            else:
                # when this point is reached all slices have been replaced
                break

        # evaluate all slices
        slices = [eval_item(s) for s in slices]

        # split parts and fill back evaluated slices
        parts = []
        for part in column.split(sep):
            m = repl_cre.match(part)
            parts.append(slices[int(m.group(1))] if m else part)

        return tuple(parts)

    @classmethod
    def split(cls, column: str) -> tuple[str | int | slice | type(Ellipsis) | list | tuple]:
        """
        Splits a string assumed to be in dot format and returns the fragments, potentially with
        selection, slice and advanced indexing expressions.
        """
        return cls._split(cls.DOT_SEP, column)

    @classmethod
    def split_nano(cls, column: str) -> tuple[str | int | slice | type(Ellipsis) | list | tuple]:
        """
        Splits a string assumed to be in nano-style underscore format and returns the fragments,
        potentially with selection, slice and advanced indexing expressions.
        """
        return cls._split(cls.NANO_SEP, column)

    def __init__(self, route=None):
        super().__init__()

        # initial storage of fields
        self._fields = []

        # use the add method to set the initial value
        if route:
            self.add(route)

    @property
    def fields(self) -> tuple:
        return tuple(self._fields)

    @property
    def column(self) -> str:
        return self.join(self._fields)

    @property
    def nano_column(self) -> str:
        return self.join_nano(self._fields)

    @property
    def string_column(self) -> str:
        return self.join(f for f in self._fields if isinstance(f, str))

    @property
    def string_nano_column(self) -> str:
        return self.join_nano(f for f in self._fields if isinstance(f, str))

    def __str__(self) -> str:
        return self.join(self._fields)

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__} '{self}' at {hex(id(self))}>"

    def __hash__(self) -> int:
        return hash(str(self.fields))

    def __len__(self) -> int:
        return len(self._fields)

    def __eq__(self, other: Route | Sequence[str] | str) -> bool:
        if isinstance(other, Route):
            return self.fields == other.fields
        elif isinstance(other, (list, tuple)):
            return self.fields == tuple(other)
        elif isinstance(other, str):
            return self.column == other
        return False

    def __bool__(self) -> bool:
        return len(self._fields) > 0

    def __nonzero__(self) -> bool:
        return self.__bool__()

    def __add__(
        self,
        other: Route | str | Sequence[str | int | slice | type(Ellipsis) | list | tuple],
    ) -> Route:
        route = self.copy()
        route.add(other)
        return route

    def __radd__(
        self,
        other: Route | str | Sequence[str | int | slice | type(Ellipsis) | list | tuple],
    ) -> Route:
        other = Route(other)
        other.add(self)
        return other

    def __iadd__(
        self,
        other: Route | str | Sequence[str | int | slice | type(Ellipsis) | list | tuple],
    ) -> Route:
        self.add(other)
        return self

    def __getitem__(
        self,
        index: Any,
    ) -> Route | str | int | slice | type(Ellipsis) | list | tuple:
        # detect slicing and return a new instance with the selected fields
        field = self._fields.__getitem__(index)
        return field if isinstance(index, int) else self.__class__(field)

    def __setitem__(
        self,
        index: Any,
        value: str | int | slice | type(Ellipsis) | list | tuple,
    ) -> None:
        self._fields.__setitem__(index, value)

    def add(
        self,
        other: Route | str | Sequence[str | int | slice | type(Ellipsis) | list | tuple],
    ) -> None:
        """
        Adds an *other* route instance, or the fields extracted from either a sequence of strings or
        a string in dot format to the fields if *this* instance. A *ValueError* is raised when
        *other* could not be interpreted.
        """
        if isinstance(other, Route):
            self._fields.extend(other._fields)
        elif isinstance(other, (list, tuple)):
            self._fields.extend(list(other))
        elif isinstance(other, str):
            self._fields.extend(self.split(other))
        else:
            raise ValueError(f"cannot add '{other}' to route '{self}'")

    def pop(self, index: int = -1) -> str:
        """
        Removes a field at *index* and returns it.
        """
        return self._fields.pop(index)

    def reverse(self) -> None:
        """
        Reverses the fields of this route in-place.
        """
        self._fields[:] = self._fields[::-1]

    def copy(self) -> Route:
        """
        Returns a copy if this instance.
        """
        return self.__class__(self._fields)

    def apply(
        self,
        ak_array: ak.Array,
        null_value: Any = UNSET,
    ) -> ak.Array:
        """
        Returns a selection of *ak_array* using the fields in this route. When the route is empty,
        *ak_array* is returned unchanged. When *null_value* is set, it is used to fill up missing
        elements in the selection corresponding to this route. Example:

        .. code-block:: python

            # select the 6th jet in each event
            Route("Jet.pt[:, 5]").apply(events)
            # -> might lead to "index out of range" errors for events with fewer jets

            Route("Jet.pt[:, 5]").apply(events, -999)
            # -> [
            #     34.15625,
            #     17.265625,
            #     -999.0,  # 6th jet was missing here
            #     19.40625,
            #     ...
            # ]
        """
        if not self:
            return ak_array

        pad = null_value is not UNSET

        # traverse fields and perform the lookup iteratively
        res = ak_array
        for i, f in enumerate(self._fields):
            # in most scenarios we can just look for the field except when
            # - padding is enabled, and
            # - f is the last field, and
            # - f is an integer (indexing), list (advanced indexing) or tuple (slicing)
            if not pad or not isinstance(f, (list, tuple, int)) or i < len(self) - 1:
                res = res[f]

            else:
                # at this point f is either an integer, a list or a tuple and padding is enabled,
                # so determine the pad size depending on f
                max_idx = -1
                pad_axis = 0
                if isinstance(f, int):
                    max_idx = f
                elif isinstance(f, list):
                    if all(isinstance(i, int) for i in f):
                        max_idx = max(f)
                else:  # tuple
                    last = f[-1]
                    if isinstance(last, int):
                        max_idx = last
                        pad_axis = len(f) - 1
                    elif isinstance(last, list) and all(isinstance(i, int) for i in last):
                        max_idx = max(last)
                        pad_axis = len(f) - 1

                # do the padding on the last axis
                if max_idx >= 0:
                    res = ak.pad_none(res, max_idx + 1, axis=pad_axis)

                # lookup the field
                res = res[f]

                # fill nones
                if max_idx >= 0 and null_value is not None:
                    # res can be an array or a value itself
                    # TODO: is there a better check than testing for the type attribute?
                    if getattr(res, "type", None) is None:
                        if res is None:
                            res = null_value
                    else:
                        res = ak.fill_none(res, null_value)

        return res


def get_ak_routes(
    ak_array: ak.Array,
    max_depth: int = 0,
) -> list[Route]:
    """
    Extracts all routes pointing to columns of a potentially deeply nested awkward array *ak_array*
    and returns them in a list of :py:class:`Route` instances. Example:

    .. code-block:: python

        # let arr be a nested array (e.g. from opening nano files via coffea)
        # (note that route objects serialize to strings using dot format)

        print(get_ak_routes(arr))
        # [
        #    "event",
        #    "luminosityBlock",
        #    "run",
        #    "Jet.pt",
        #    "Jet.mass",
        #    ...
        # ]

    When positive, *max_depth* controls the maximum size of returned route tuples. When negative,
    routes are shortened by the passed amount of elements. In both cases, only unique routes are
    returned.
    """
    routes = []

    # use recursive lookup pattern over (container, current route) pairs
    lookup = [(ak_array, ())]
    while lookup:
        arr, fields = lookup.pop(0)
        if getattr(arr, "fields", None) and (max_depth <= 0 or len(fields) < max_depth):
            # extend the lookup with nested fields
            lookup.extend([
                (getattr(arr, field), fields + (field,))
                for field in arr.fields
            ])
        else:
            # no sub fields found or positive max_depth reached, store the route
            # but check negative max_depth first
            if max_depth < 0:
                fields = fields[:max_depth]
            # create the route
            route = Route(fields)
            # add when not empty and unique
            if route and route not in routes:
                routes.append(route)

    return routes


def has_ak_column(
    ak_array: ak.Array,
    route: Route | Sequence[str] | str,
) -> bool:
    """
    Returns whether an awkward array *ak_array* contains a nested field identified by a *route*. A
    route can be a :py:class:`Route` instance, a tuple of strings where each string refers to a
    subfield, e.g. ``("Jet", "pt")``, or a string with dot format (e.g. ``"Jet.pt"``).
    """
    route = Route(route)

    # handle empty route
    if not route:
        return False

    try:
        route.apply(ak_array)
    except (ValueError, IndexError):
        return False

    return True


def set_ak_column(
    ak_array: ak.Array,
    route: Route | Sequence[str] | str,
    value: ak.Array,
    value_type: type | str | None = None,
) -> ak.Array:
    """
    Inserts a new column into awkward array *ak_array* and returns a new view with the column added
    or overwritten.

    The column can be defined through a route, i.e., a :py:class:`Route` instance, a tuple of
    strings where each string refers to a subfield, e.g. ``("Jet", "pt")``, or a string with dot
    format (e.g. ``"Jet.pt"``), and the column *value* itself. Intermediate, non-existing fields are
    automatically created. When a *value_type* is defined, *ak_array* is casted into this type
    before it is inserted.

    Example:

    .. code-block:: python

        arr = ak.zip({"Jet": {"pt": [30], "eta": [2.5]}})

        set_ak_column(arr, "Jet.mass", [40])
        set_ak_column(arr, "Muon.pt", [25])  # creates subfield "Muon" first

    .. note::

        Issues can arise in cases where the route to add already exists and has a different type
        than the newly added *value*. For this reason, existing columns are removed first, creating
        a view to operate on.
    """
    route = Route(route)

    # handle empty route
    if not route:
        raise ValueError("route must not be empty")

    # cast type
    if value_type:
        value = ak.values_astype(value, value_type)

    # force creating a view for consistent behavior
    orig_fields = ak_array.fields
    ak_array = ak.Array(ak_array)

    # when there is only one field, and route refers to that field, ak_array will be empty
    # after the removal in the next step and all shape information might get lost,
    # so in this case add the value first as a dummy column and remove it afterwards
    match_only_existing = len(orig_fields) == 1 and len(route) == 1 and orig_fields[0] == route[0]
    if match_only_existing:
        tmp_field = f"tmp_field_{id(object())}"
        ak_array = ak.with_field(ak_array, value, tmp_field)

    # try to remove the route first so that existing columns are not overwritten but re-inserted
    ak_array = remove_ak_column(ak_array, route, silent=True)

    # trivial case
    if len(route) == 1:
        ak_array = ak.with_field(ak_array, value, route.fields)

        # remove the tmp field if existing
        if match_only_existing:
            ak_array = remove_ak_column(ak_array, tmp_field, silent=True)

        return ak_array

    # identify existing parts of the subroute
    # example: route is ("a", "b", "c"), "a" exists, the sub field "b" does not, "c" should be set
    sub_route = route.copy()
    missing_sub_route = Route()
    while sub_route:
        if has_ak_column(ak_array, sub_route):
            break
        missing_sub_route += sub_route.pop()
    missing_sub_route.reverse()

    # add the first missing field to the sub route which will be used by __setitem__
    if missing_sub_route:
        sub_route += missing_sub_route.pop(0)

    # use the remaining missing sub route to wrap the value via ak.zip, generating new sub fields
    while missing_sub_route:
        value = ak.zip({missing_sub_route.pop(): value})

    # insert the value
    ak_array = ak.with_field(ak_array, value, sub_route.fields)

    return ak_array


def remove_ak_column(
    ak_array: ak.Array,
    route: Route | Sequence[str] | str,
    remove_empty: bool = True,
    silent: bool = False,
) -> ak.Array:
    """
    Removes a *route* from an awkward array *ak_array* and returns a new view with the corresponding
    column removed. When *route* points to a nested field that would be empty after removal, the
    parent field is removed completely unless *remove_empty* is *False*.

    Note that *route* can be a :py:class:`Route` instance, a sequence of strings where each string
    refers to a subfield, e.g. ``("Jet", "pt")``, or a string with dot format (e.g. ``"Jet.pt"``).
    Unless *silent* is *True*, a *ValueError* is raised when the route does not exist.
    """
    # force creating a view for consistent behavior
    ak_array = ak.Array(ak_array)

    # verify that the route is not empty
    route = Route(route)
    if not route:
        if silent:
            return ak_array
        raise ValueError("route must not be empty")

    # verify that the route exists
    if not has_ak_column(ak_array, route):
        if silent:
            return ak_array
        raise ValueError(f"no column found in array for route '{route}'")

    # remove it
    ak_array = ak.without_field(ak_array, route.fields)

    # remove empty parent fields
    if remove_empty and len(route) > 1:
        for i in range(len(route) - 1):
            parent_route = route[:-(i + 1)]
            if not parent_route.apply(ak_array).fields:
                ak_array = ak.without_field(ak_array, parent_route.fields)

    return ak_array


def add_ak_alias(
    ak_array: ak.Array,
    src_route: Route | Sequence[str] | str,
    dst_route: Route | Sequence[str] | str,
    remove_src: bool = False,
    missing_strategy: str = "remove",
) -> ak.Array:
    """
    Adds an alias to an awkward array *ak_array* pointing the array at *src_route* to *dst_route*
    and returns a new view with the alias applied.

    Note that existing columns referred to by *dst_route* might be overwritten. When *remove_src* is
    *True*, a view of the input array is returned with the column referred to by *src_route*
    missing. Both routes can be :py:class:`Route` instances, a tuple of strings where each string
    refers to a subfield, e.g. ``("Jet", "pt")``, or a string with dot format (e.g. ``"Jet.pt"``).

    In case *src_route* does not exist, *missing_strategy* is applied:

        - ``"original"``: If existing, *dst_route* remains unchanged.
        - ``"remove"``: If existing, *dst_route* is removed.
        - ``"raise"``: A *ValueError* is raised.
    """
    # check the strategy
    strategies = ("original", "remove", "raise")
    if missing_strategy not in strategies:
        raise ValueError(
            f"unknown missing_strategy '{missing_strategy}', valid values are {strategies}",
        )

    # convert to routes
    src_route = Route(src_route)
    dst_route = Route(dst_route)

    # check that the src exists
    if has_ak_column(ak_array, src_route):
        # add the alias, potentially overwriting existing columns
        ak_array = set_ak_column(ak_array, dst_route, src_route.apply(ak_array))
    else:
        if missing_strategy == "raise":
            raise ValueError(f"no column found in array for route '{src_route}'")
        if missing_strategy == "remove":
            ak_array = remove_ak_column(ak_array, dst_route)

    # remove the source column
    if remove_src:
        ak_array = remove_ak_column(ak_array, src_route)

    return ak_array


def add_ak_aliases(
    ak_array: ak.Array,
    aliases: dict[Route | Sequence[str] | str, Route | Sequence[str], str],
    **kwargs: dict[str, Any],
) -> ak.Array:
    """
    Adds multiple *aliases*, given in a dictionary mapping destination columns to source columns, to
    an awkward array *ak_array* and returns a new view with the aliases applied.

    Each column in this dictionary can be referred to by a :py:class:`Route` instance, a tuple of
    strings where each string refers to a subfield, e.g. ``("Jet", "pt")``, or a string with dot
    format (e.g. ``"Jet.pt"``).

    All additional *kwargs* are forwarded to :py:func:`add_ak_aliases`.
    """
    # add all aliases
    for dst_route, src_route in aliases.items():
        ak_array = add_ak_alias(ak_array, src_route, dst_route, **kwargs)

    return ak_array


def update_ak_array(
    ak_array: ak.Array,
    *others: ak.Array,
    overwrite_routes: bool | list[Route | Sequence[str], str] = True,
    add_routes: bool | list[Route | Sequence[str], str] = False,
    concat_routes: bool | list[Route | Sequence[str], str] = False,
) -> ak.Array:
    """
    Updates an awkward array *ak_array* with the content of multiple different arrays *others* and
    potentially (see below) returns a new view. Internally, :py:func:`get_ak_routes` is used to
    obtain the list of all routes pointing to potentially deeply nested arrays.

    If two columns overlap during this update process, four different cases can be configured to
    occur:

        1. If *concat_routes* is either *True* or a list of routes containing the route in question,
           the columns are concatenated along the last axis. This obviously implies that their
           shapes must be compatible.
        2. If case 1 does not apply and *add_routes* is either *True* or a list of routes containing
           the route in question, the columns are added using the plus operator, forwarding the
           actual implementation to awkward.
        3. If cases 1 and 2 do not apply and *overwrite_routes* is either *True* or a list of routes
           containing the route in question, new columns (right most in *others*) overwrite
           existing ones. A new view is returned in case this case occurs at least once.
        4. If none of the cases above apply, columns remain unchanged.
    """
    # force creating a view for consistent behavior
    ak_array = ak.Array(ak_array)

    # trivial case
    if not others:
        return ak_array

    # helpers to cache calls to has_ak_column for ak_array
    _has_column_cache = {Route(route): True for route in get_ak_routes(ak_array)}

    def has_ak_column_cached(route):
        if route not in _has_column_cache:
            _has_column_cache[route] = has_ak_column(ak_array, route)
        return _has_column_cache[route]

    # helpers to check if routes can be overwritten, added or concatenated
    def _match_route(matcher, cache, route):
        if route not in cache:
            cache[route] = matcher(route.column)
        return cache[route]

    # helper to create a matching function for routes
    def _create_matcher(routes):
        if isinstance(routes, (list, tuple, set)):
            return pattern_matcher([Route(route).column for route in routes])
        return lambda route: bool(routes)

    # decision helpers
    do_overwrite = partial(_match_route, _create_matcher(overwrite_routes), {})
    do_add = partial(_match_route, _create_matcher(add_routes), {})
    do_concat = partial(_match_route, _create_matcher(concat_routes), {})

    # go through all other arrays and merge their columns
    for other in others:
        for route in get_ak_routes(other):
            if has_ak_column_cached(route):
                if do_concat(route):
                    # concat and reassign
                    ak_array = set_ak_column(
                        ak_array,
                        route,
                        ak.concatenate((route.apply(ak_array), route.apply(other)), axis=-1),
                    )
                elif do_add(route):
                    # add and reassign
                    ak_array = set_ak_column(
                        ak_array,
                        route,
                        route.apply(ak_array) + route.apply(other),
                    )
                elif do_overwrite(route):
                    # delete the column first for type safety and then re-add it
                    ak_array = remove_ak_column(ak_array, route)
                    ak_array = set_ak_column(ak_array, route, route.apply(other))
            else:
                # the route is new, so add it and manually tell the cache
                ak_array = set_ak_column(ak_array, route, route.apply(other))
                _has_column_cache[route] = True

    return ak_array


def flatten_ak_array(
    ak_array: ak.Array,
    routes: Sequence | set | Callable[[str], bool] | None = None,
    nano_format: bool = False,
) -> OrderedDict:
    """
    Flattens a nested awkward array *ak_array* into a dictionary that maps joined column names to
    single awkward arrays. The returned dictionary might be used in conjuction with ``ak.Array`` to
    create a single array again.

    :py:func:`get_ak_routes` is used internally to determine the nested structure of the array.
    Names of flat columns in the returned dictionary follow the standard dot format by default, and
    nano-style underscore format if *nano_format* is *True*. The columns to save can be defined via
    *routes* which can be a sequence or set of column names or a function receiving a column name
    and returning a bool.
    """
    # use an ordered mapping to somewhat preserve row adjacency
    flat_array = OrderedDict()

    # helper to evaluate whether to keep a column
    keep_route = lambda column: True
    if isinstance(routes, (list, tuple, set)):
        keep_route = pattern_matcher([Route(route).column for route in routes])
    elif callable(routes):
        keep_route = routes

    # go through routes, create new names and store arrays
    for route in get_ak_routes(ak_array):
        if keep_route(route.column):
            flat_array[route.nano_column if nano_format else route.column] = route.apply(ak_array)

    return flat_array


def sort_ak_fields(
    ak_array: ak.Array,
    sort_fn: Callable[[str], int] | None = None,
) -> ak.Array:
    """
    Recursively sorts all fields of an awkward array *ak_array* and returns a new view. When a
    *sort_fn* is set, it is used internally for sorting field names.
    """
    # first identify all sub fields that need to be sorted (i.e. those that have fields themselves)
    fields_to_sort = []
    lookup = [(field,) for field in ak_array.fields]
    while lookup:
        fields = lookup.pop(0)
        arr = ak_array[fields]
        if arr.fields:
            fields_to_sort.append(fields)
            lookup.extend([fields + (field,) for field in arr.fields])

    # sort them, starting at highest depth
    for fields in reversed(fields_to_sort):
        arr = ak_array[fields]
        sorted_fields = sorted(arr.fields, key=sort_fn)
        if tuple(arr.fields) != tuple(sorted_fields):
            ak_array = set_ak_column(ak_array, fields, arr[sorted_fields])

    # sort the top level fields
    if len(ak_array.fields) > 1:
        sorted_fields = sorted(ak_array.fields, key=sort_fn)
        if tuple(ak_array.fields) != tuple(sorted_fields):
            ak_array = ak_array[sorted_fields]

    return ak_array


def sorted_ak_to_parquet(
    ak_array: ak.Array,
    *args,
    **kwargs,
) -> None:
    """
    Sorts the fields in an awkward array *ak_array* recursively with :py:func:`sort_ak_fields` and
    saves it as a parquet file using ``awkward.to_parquet`` which receives all additional *args* and
    *kwargs*.

    .. note::

        Since the order of fields in awkward arrays resulting from reading nano files might be
        arbitrary (depending on streamer info in the original root files), but formats such as
        parquet highly depend on the order for building internal table schemas, one should always
        make use of this function! Otherwise, operations like file merging might fail due to
        differently ordered schemas.
    """
    # sort fields
    ak_array = sort_ak_fields(ak_array)

    # workaround for https://github.com/dask-contrib/dask-awkward/issues/140
    # TODO: remove workaround
    kwargs["extensionarray"] = False
    # end workaround

    # TODO: empty fields cannot be saved to parquet, but for that we would need to identify them
    ak.to_parquet(ak_array, *args, **kwargs)


def attach_behavior(
    ak_array: ak.Array,
    type_name: str,
    behavior: dict | None = None,
    skip_fields: Sequence[str] | None = None,
) -> ak.Array:
    """
    Attaches behavior of type *type_name* to an awkward array *ak_array* and returns it. *type_name*
    must be a key of a *behavior* dictionary which defaults to the "behavior" attribute of
    *ak_array* when present. Otherwise, a *ValueError* is raised.

    By default, all subfields of *ak_array* are kept. For further control, *skip_fields* can contain
    names or name patterns of fields that are filtered.
    """

    if behavior is None:
        behavior = getattr(ak_array, "behavior", None) or coffea.nanoevents.methods.nanoaod.behavior
        if behavior is None:
            raise ValueError(
                f"behavior for type '{type_name}' is not set and not existing in input array",
            )

    # prepare field skipping
    keep_field = lambda field: True
    if skip_fields:
        skip_fields = law.util.make_list(skip_fields)
        skip_fields = {
            field for field in ak_array.fields
            if law.util.multi_match(field, skip_fields)
        }
        keep_field = lambda field: field not in skip_fields

    return ak.zip(
        {field: ak_array[field] for field in ak_array.fields if keep_field(field)},
        with_name=type_name,
        behavior=behavior,
    )


def layout_ak_array(data_array: np.array | ak.Array, layout_array: ak.Array) -> ak.Array:
    """
    Structures an input *data_array* by making at an awkward array with a layout inferred from
    *layout_array* and returns it. In particular, this function can be used to create new awkward
    arrays from existing numpy arrays and forcing a known, arbitrarily ragged shape to it. Example:

    .. code-block:: python

        a = np.array([1.0, 2.0, 3.0, 4.0, 5.0])
        b = ak.Array([[], [0, 0], [], [0, 0, 0]])

        c = layout_ak_array(a, b)
        # <Array [[], [1.0, 2.0], [], [3.0, 4.0, 5.0]] type='4 * var * float32'>
    """
    # flatten and convert to content
    if isinstance(data_array, np.ndarray):
        data = ak.contents.NumpyArray(np.reshape(data_array, (-1,)))
    elif isinstance(data_array, ak.Array):
        data = ak.flatten(data_array, axis=None)  # might create a copy
    else:
        raise TypeError(f"unhandled type of data array {data_array}")

    # infer the offsets
    if getattr(layout_array, "layout", None) is None:
        raise ValueError(f"layout array {layout_array} does not have a valid layout")
    if getattr(layout_array.layout, "offsets", None):
        offsets = layout_array.layout.offsets
    else:
        raise ValueError(f"offsets extraction not implemented for layout array {layout_array}")

    return ak.Array(ak.contents.ListOffsetArray(offsets, data))


def flat_np_view(ak_array: ak.Array, axis: int = 1) -> np.array:
    """
    Takes an *ak_array* and returns a fully flattened numpy view. The flattening is applied along
    *axis*. See *ak.flatten* for more info.

    .. note::
        Changes applied in-place to that view are transferred to the original *ak_array*, but
        **only** when the axis is not *None* but an integer value. For this reason, passing
        ``axis=None`` will cause an exception to be thrown.
    """
    if axis is None:
        raise ValueError(
            "axis must not be None as changes applied the returned view will not propagate to the ",
            "original awkward array",
        )

    return np.asarray(ak.flatten(ak_array, axis=axis))


def ak_copy(ak_array: ak.Array) -> ak.Array:
    """
    Workaround for ``ak.copy`` which currently fails to copy arrays with coffea nano-style behavior
    attached to them. As soon as this is functional again, this function should be deprecated and
    removed.
    """
    return layout_ak_array(np.array(ak.flatten(ak_array)), ak_array)


class RouteFilter(object):
    """
    Shallow helper class that handles removal of routes in an awkward array that do not match those
    in *keep_routes*. Each route can either be a :py:class:`Route` instance, or anything that is
    accepted by its constructor. Example:

    .. code-block:: python

        route_filter = RouteFilter(["Jet.pt", "Jet.eta"])
        events = route_filter(events)

        print(get_ak_routes(events))
        # [
        #    "Jet.pt",
        #    "Jet.eta",
        # ]

    .. py:attribute:: keep_routes
       type: list

       The routes to keep.

    .. py:attribute:: remove_routes
       type: None, set

       A set of :py:class:`Route` instances that are removed, defined after the first call to this
       instance.
    """

    def __init__(self, keep_routes: Sequence[Route | str]):
        super().__init__()

        self.keep_routes = list(keep_routes)
        self.remove_routes = None

    def __call__(self, ak_array: ak.Array) -> ak.Array:
        # manually remove colums that should not be kept
        if self.remove_routes is None:
            # convert routes to keep into string columns for pattern checks
            keep_columns = [Route(route).column for route in self.keep_routes]

            # determine routes to remove
            self.remove_routes = {
                route
                for route in get_ak_routes(ak_array)
                if not law.util.multi_match(route.column, keep_columns)
            }

        # apply the filtering
        for route in self.remove_routes:
            ak_array = remove_ak_column(ak_array, route)

        return ak_array


class ArrayFunction(Derivable):
    """
    Base class for function wrappers that act on arrays and keep track of used as well as produced
    columns, as close as possible to the actual implementation. ArrayFunction's can express the
    dependence between one another (either via used or produced columns) (1) and they can invoke one
    another for the purpose of modularity (2). Knowledge of the columns to load (save) is especially
    useful when opening (writing) files and selecting the content to deserialize (serialize).

    To understand the internals of both (1) and (2), it is imperative to distinguish between
    ArrayFunction subclasses and their instances, as shown in the example below.

    .. code-block:: python

        class my_func(ArrayFunction):
            uses = {"Jet.pt"}
            produces = {"Jet.pt2"}

            def call_func(self, events):
                events["Jet", "pt"] = events.Jet.pt ** 2

        class my_other_func(ArrayFunction):
            uses = {my_func}
            produces = {"Jet.pt4"}

            def call_func(self, events):
                # call the correct my_func instance
                events = self[my_func](events)

                events["Jet", "pt4"] = events.Jet.pt2 ** 2

        # call my_other_func on a chunk of events
        inst = my_other_func()
        inst(events)

    ArrayFunction's declare dependence between one another through class-level sets *uses* and
    *produces*. This allows for the construction of an internal callstack. Once an ArrayFunction is
    instantiated, all dependent objects in this callstack are instantiated as well and stored
    internally mapped to their class. This is strictly required as ArrayFunctions, and most likely
    their subclasses, can have a state (a set of instance-level members that are allowed to differ
    between instances). The instance of a dependency can be accessed via item syntax
    (``self[my_func]`` above).

    .. note::

        The above example uses explicit subclassing, but most certainly this might never be used in
        practice. Instead, please consider using a decorator to wrap the main callable as done by
        the :py:class:`Calibrator`, :py:class:`Selector` and :py:class:`Producer` interfaces.

    *uses* and *produces* should be strings denoting a column in dot format or a :py:class:`Route`
    instance, other :py:class:`ArrayFunction` instances, or a sequence or set of the two. On
    instance-level, the full sets of :py:attr:`used_columns` and :py:attr:`produced_columns` are
    simply resolvable through attributes.

    *call_func* defines the function being invoked when the instance is *called*. An additional
    initialization function can be wrapped through a decorator (similiar to ``property`` setters) as
    shown in the example below. They constitute a mechanism to update the :py:attr:`uses` and
    :py:attr:`produces` sets to declare dependencies in a more dynamic way.

    .. code-block:: python

        @my_other_func.init
        def my_other_func_init(self: ArrayFunction):
            self.uses.add(my_func)

        # the above adds an initialization function to the already created class my_other_func,
        # but the same could also be achived by just declaring an instance method *init_func* as
        # part of the class definition above

    Another function that can be dynamically assined (or simply implemented in the subclass) is
    *skip_func*. If set, it is assumed to be a function that returns a bool, deciding on whether to
    include this :py:class:`ArrayFunction` in the dependencies of other instances. The decorator

    In the example above, *my_other_func* declares a dependence on *my_func* by listing it in
    *uses*. This means that *my_other_func* uses the columns that *my_func* also uses. The same
    logic applies for *produces*. To make this dependence more explicit, the entry could also be
    changed to ``my_func.USES`` which results in the same behavior, or ``my_func.PRODUCES`` to
    reverse it - *my_other_func* would define that it is **using** the columns that *my_func*
    **produces**. Omitting these flags is identical to using (e.g.) ``my_func.AUTO``.

    .. py:classattribute:: uses
       type: set

       The set of used column names or other dependencies to recursively resolve the names of used
       columns.

    .. py:classattribute:: produces
       type: set

       The set of produced column names or other dependencies to recursively resolve the names of
       produced columns.

    .. py:classattribute:: AUTO
       type: ArrayFunction.IOFlag

       Flag that can be used in nested dependencies between array functions to denote automatic
       resolution of column names.

    .. py:classattribute:: USES
       type: ArrayFunction.IOFlag

       Flag that can be used in nested dependencies between array functions to denote columns names
       in the :py:attr:`uses` set.

    .. py:classattribute:: PRODUCES
       type: ArrayFunction.IOFlag

       Flag that can be used in nested dependencies between array functions to denote columns names
       in the :py:attr:`produces` set.

    .. py:attribute:: uses_instances
       type: set

       The set of used column names or instantiated dependencies to recursively resolve the names of
       used columns. Set during the deferred initialization.

    .. py:attribute:: produces_instances
       type: set

       The set of produces column names or instantiated dependencies to recursively resolve the
       names of produced columns. Set during the deferred initialization.

    .. py:attribute:: deps
       type: dict

       The callstack of dependencies, i.e., a dictionary mapping dependent classes to their
       instances as to be used by *this* instance. Item access on this instance is forwarded to this
       object.

    .. py:attribute:: deps_kwargs
       type: dict

       Optional keyword arguments mapped to dependent classes that are forwarded to their
       initialization.

    .. py::attribute:: used_columns
       type: set
       read-only

       The resolved, flat set of used column names.

    .. py::attribute:: produced_columns
       type: set
       read-only

       The resolved, flat set of produced column names.

    .. py:attribute:: call_func
       type: callable

       The wrapped function to be called on arrays.

    .. py:attribute: init_func
       type: callable

       The registered function defining what to update, or *None*.

    .. py:attribute: skip_func
       type: callable

       The registered function defining when to skip this instance while building dependencies of
       other instances.
    """

    # class-level attributes as defaults
    call_func = None
    init_func = None
    skip_func = None
    uses = set()
    produces = set()
    _dependency_sets = {"uses", "produces"}
    log_runtime = law.config.get_expanded_boolean("analysis", "log_array_function_runtime")

    # flags for declaring inputs (via uses) or outputs (via produces)
    class IOFlag(enum.Flag):
        AUTO = enum.auto()
        USES = enum.auto()
        PRODUCES = enum.auto()

    # shallow wrapper around an objects (a class or instance) and an IOFlag
    Flagged = namedtuple("Flagged", ["wrapped", "io_flag"])

    @classproperty
    def AUTO(cls) -> Flagged:
        """
        Returns a :py:attr:`Flagged` object, wrapping this class and the AUTO flag.
        """
        return cls.Flagged(cls, cls.IOFlag.AUTO)

    @classproperty
    def USES(cls) -> Flagged:
        """
        Returns a :py:attr:`Flagged` object, wrapping this class and the USES flag.
        """
        return cls.Flagged(cls, cls.IOFlag.USES)

    @classproperty
    def PRODUCES(cls) -> Flagged:
        """
        Returns a :py:attr:`Flagged` object, wrapping this class and the PRODUCES flag.
        """
        return cls.Flagged(cls, cls.IOFlag.PRODUCES)

    @classmethod
    def init(cls, func: Callable[[], None]) -> None:
        """
        Decorator to wrap a function *func* that should be registered as :py:meth:`init_func`
        which is used to initialize *this* instance dependent on specific task attributes. The
        function should not accept positional arguments.

        The decorator does not return the wrapped function.
        """
        cls.init_func = func

    @classmethod
    def skip(cls, func: Callable[[], bool]) -> None:
        """
        Decorator to wrap a function *func* that should be registered as :py:meth:`skip_func`
        which is used to decide whether *this* instance should be skipped during the building of
        dependency trees in other :py:class:`ArrayFunction`'s.

        The function should not accept positional arguments and return a boolean.
        """
        cls.skip_func = func

    def __init__(
        self,
        call_func: Callable | None = law.no_value,
        init_func: Callable | None = law.no_value,
        skip_func: Callable | None = law.no_value,
        deferred_init: bool | None = True,
        instance_cache: dict | None = None,
        log_runtime: bool | None = None,
        **kwargs,
    ):
        super().__init__()

        # add class-level attributes as defaults for unset arguments (no_value)
        if call_func == law.no_value:
            call_func = self.__class__.call_func
        if init_func == law.no_value:
            init_func = self.__class__.init_func
        if skip_func == law.no_value:
            skip_func = self.__class__.skip_func
        if log_runtime is not None:
            self.log_runtime = log_runtime

        # when a custom funcs are passed, bind them to this instance
        if call_func:
            self.call_func = call_func.__get__(self, self.__class__)
        if init_func:
            self.init_func = init_func.__get__(self, self.__class__)
        if skip_func:
            self.skip_func = skip_func.__get__(self, self.__class__)

        # create instance-level sets of dependent ArrayFunction classes,
        # optionally with priority to sets passed in keyword arguments
        for attr in self._dependency_sets:
            if attr in kwargs:
                try:
                    deps = set(law.util.make_list(kwargs.get(attr) or []))
                except TypeError as e:
                    e.args = (
                        f"cannot convert keyword argument '{attr}' passed to {self.__class__} "
                        f"to set: {e.args[0]}",
                    )
                    raise e
            else:
                try:
                    deps = set(law.util.make_list(getattr(self.__class__, attr)))
                except TypeError as e:
                    e.args = (
                        f"cannot convert class attribute '{attr}' of {self.__class__} "
                        f"to set: {e.args[0]}",
                    )
                    raise e
            setattr(self, attr, deps)

            # also register a set for storing instances, filled in create_dependencies
            setattr(self, f"{attr}_instances", set())

        # dictionary of dependency class to instance, set in create_dependencies
        self.deps = DotDict()

        # dictionary of keyword arguments mapped to dependenc classes to be forwarded to their init
        self.deps_kwargs = DotDict()

        # deferred part of the initialization
        if deferred_init:
            self.deferred_init(instance_cache=instance_cache)

    def __getitem__(self, dep_cls: DerivableMeta) -> ArrayFunction:
        """
        Item access to dependencies.
        """
        return self.deps[dep_cls]

    def deferred_init(self, instance_cache: dict | None = None) -> dict:
        """
        Controls the deferred part of the initialization process.
        """
        # create dependencies once
        instance_cache = instance_cache or {}
        self.create_dependencies(instance_cache)

        # run this instance's init function which might update dependent classes
        if callable(self.init_func):
            self.init_func()

        # instantiate dependencies again, but only perform updates
        self.create_dependencies(instance_cache, only_update=True)

        return instance_cache

    def create_dependencies(
        self,
        instance_cache: dict,
        only_update: bool = False,
    ) -> None:
        """
        Walks through all dependencies configured in the :py:attr:`_dependency_sets` and fills
        :py:attr:`deps` as well as separate sets, corresponding to the classes defined in
        :py:attr:`_dependency_sets` (e.g. :py:attr:`uses` -> :py:attr:`uses_instances`).

        *instance_cache* is a dictionary that is serves as a cache to prevent same classes being
        instantiated multiple times.
        """
        def add_dep(cls_or_inst):
            is_cls = ArrayFunction.derived_by(cls_or_inst)
            cls = cls_or_inst if is_cls else cls_or_inst.__class__
            if not only_update or cls not in self.deps:
                # create or get the instance
                if is_cls:
                    # use the cache
                    if cls not in instance_cache:
                        # create the instance first without its deps, then cache it but do not
                        # create its own deps yet within the deferred init
                        inst = self.instantiate_dependency(cls, deferred_init=False)
                        instance_cache[cls] = inst
                    inst = instance_cache[cls]
                else:
                    inst = cls_or_inst

                # optionally skip the instance
                if callable(inst.skip_func) and inst.skip_func():
                    self.deps.pop(cls, None)
                    return None

                # run the deferred init that creates its own deps
                inst.deferred_init(instance_cache)

                # store it
                self.deps[cls] = inst

            return self.deps[cls]

        # track dependent classes that are handled in the following
        added_deps = []

        for attr in self._dependency_sets:
            # get the current set of instances, potentially clearing existing ones
            instances = getattr(self, f"{attr}_instances")
            instances.clear()

            # go through all dependent objects and create instances of classes, considering caching
            for obj in getattr(self, attr):
                if ArrayFunction.derived_by(obj) or isinstance(obj, ArrayFunction):
                    obj = add_dep(obj)
                    if obj:
                        added_deps.append(obj.__class__)
                        instances.add(obj)
                elif isinstance(obj, self.Flagged):
                    obj = self.Flagged(add_dep(obj.wrapped), obj.io_flag)
                    if obj:
                        added_deps.append(obj.wrapped.__class__)
                        instances.add(obj)
                else:
                    obj = copy.deepcopy(obj)
                    if obj:
                        instances.add(obj)

        # synchronize dependencies
        # this might remove deps that were present in self.deps already before this method is called
        # but that were not added in the loop above
        if only_update:
            for cls in list(self.deps.keys()):
                if cls not in added_deps:
                    del self.deps[cls]

    def instantiate_dependency(self, cls: DerivableMeta, **kwargs: Any) -> ArrayFunction:
        """
        Controls the instantiation of a dependency given by its *cls* and arbitrary *kwargs*. The
        latter update optional keyword arguments in :py:attr:`self.deps_kwargs` and are then
        forwarded to the instantiation.
        """
        # merge kwargs with those in deps_kwargs
        deps_kwargs = self.deps_kwargs.get(cls) or {}
        if deps_kwargs:
            kwargs = law.util.merge_dicts(deps_kwargs, kwargs)

        return cls(**kwargs)

    def get_dependencies(self, include_others: bool = False) -> set[ArrayFunction | Any]:
        """
        Returns a set of instances of all dependencies. When *include_others* is *True*, also
        non-ArrayFunction types are returned.
        """
        deps = set()

        for attr in self._dependency_sets:
            for obj in getattr(self, f"{attr}_instances"):
                if isinstance(obj, self.Flagged):
                    obj = obj.wrapped
                if isinstance(obj, ArrayFunction):
                    deps.add(obj)
                elif include_others:
                    deps.add(obj)

        return deps

    def _get_columns(self, io_flag: IOFlag, call_cache: set | None = None, debug=False) -> set[str]:
        if io_flag == self.IOFlag.AUTO:
            raise ValueError("io_flag in internal _get_columns method must not be AUTO")

        # start with an empty set
        columns = set()

        # init the call cache
        if call_cache is None:
            call_cache = set()

        # declare _this_ call cached
        call_cache.add(self.Flagged(self, io_flag))

        # add columns of all dependent objects
        for obj in (self.uses_instances if io_flag == self.IOFlag.USES else self.produces_instances):
            if isinstance(obj, (ArrayFunction, self.Flagged)):
                flagged = obj
                if isinstance(obj, ArrayFunction):
                    flagged = self.Flagged(obj, io_flag)
                elif obj.io_flag == self.IOFlag.AUTO:
                    flagged = self.Flagged(obj.wrapped, io_flag)
                # skip when already cached
                if flagged in call_cache:
                    continue
                # add the columns
                columns |= flagged.wrapped._get_columns(flagged.io_flag, call_cache=call_cache)
            else:
                columns.add(obj)

        return columns

    def _get_used_columns(self, call_cache: set | None = None) -> set[str]:
        return self._get_columns(io_flag=self.IOFlag.USES, call_cache=call_cache)

    @property
    def used_columns(self) -> set[str]:
        return self._get_used_columns()

    def _get_produced_columns(self, call_cache: set | None = None) -> set[str]:
        return self._get_columns(io_flag=self.IOFlag.PRODUCES, call_cache=call_cache)

    @property
    def produced_columns(self) -> set[str]:
        return self._get_produced_columns()

    def __call__(self, *args, **kwargs):
        """
        Forwards the call to :py:attr:`call_func` with all *args* and *kwargs*. An exception is
        raised if :py:attr:`call_func` is not callable.
        """
        # check if the call_func is callable
        if not callable(self.call_func):
            raise Exception(f"call_func of {self} is not callable")

        # raise in case the call is actually being skipped
        if callable(self.skip_func) and self.skip_func():
            raise Exception(
                f"skip_func of {self} returned True, cannot invoke call_func; skip_func code: \n\n"
                f"{get_source_code(self.skip_func, indent=4)}",
            )

        t1 = time.perf_counter()
        try:
            return self.call_func(*args, **kwargs)
        finally:
            if self.log_runtime:
                duration = time.perf_counter() - t1
                logger_perf.info(
                    f"runtime of '{self.cls_name}': {law.util.human_duration(seconds=duration)}",
                )


class TaskArrayFunction(ArrayFunction):
    """
    Subclass of :py:class:`ArrayFunction` providing an interface to certain task features such as
    declaring dependent or produced shifts, task requirements, and defining a custom setup
    function. In addition, there is the option to update all these configurations based on task
    attributes.

    *shifts* can be defined similarly to columns to use and/or produce in the
    :py:class:`ArrayFunction` base class. It can be a sequence or set of shift names, or dependent
    TaskArrayFunction's. Similar to :py:attr:`used_columns` and :py:attr:`produced_columns`,
    the :py:attr:`all_shifts` property returns a flat set of all shifts, potentially resolving
    information from dependencies registered in `py:attr:`uses`, `py:attr:`produces` and
    `py:attr:`shifts` itself.

    As opposed to more basic :py:class:`ArrayFunction`'s, instances of *this* class have a direct
    interface to tasks and can influence their behavior - and vice-versa. For this purpose, custom
    task requirements, and a setup of objects resulting from these requirements can be defined in a
    similar, programmatic way. Also, they might define an optional *sandbox* that is required to run
    this array function.

    Exmple:

    .. code-block:: python

        class my_func(ArrayFunction):
            uses = {"Jet.pt"}
            produces = {"Jet.pt_weighted"}

            def call_func(self, events):
                # self.weights is defined below
                events["Jet", "pt_weighted"] = events.Jet.pt * self.weights

        # define requirements that (e.g.) compute the weights
        @my_func.requires
        def requires(self, reqs):
            # fill the requirements dict
            reqs["weights_task"] = SomeWeightsTask.req(self.task)

        # define the setup step that loads event weights from the required task
        @my_func.setup
        def setup(self, inputs):
            # load the weights once, inputs is corresponding to what we added to reqs above
            weights = inputs["weights_task"].load(formatter="json")

            # save them as an instance attribute
            self.weights = weights

        # call my_func on a chunk of events
        inst = my_func()
        inst(events)

    For a possible implementation, see :py:mod:`columnflow.production.pileup`.

    .. note::

        The above example uses explicit subclassing, mixed with decorator usage to extend the class.
        This is most certainly never used in practice. Instead, please either consider defining the
        class the normal way, or use a decorator to wrap the main callable first and by that
        creating the class as done by the :py:class:`Calibrator`, :py:class:`Selector` and
        :py:class:`Producer` interfaces.

    .. py:classattribute:: shifts
       type: set

       The set of dependent or produced shifts, or other dependencies to recursively resolve the
       names of shifts.

    .. py:attribute:: shifts_instances
       type: set

       The set of shift names or instantiated dependencies to recursively resolve the names of
       shifts. Set during the deferred initialization.

    .. py:attribute:: all_shifts
       type: set
       read-only

       The resolved, flat set of dependent or produced shifts.

    .. py:attribute: requires_func
       type: callable

       The registered function defining requirements, or *None*.

    .. py:attribute:: setup_func
       type: callable

       The registered function performing the custom setup step, or *None*.

    .. py:attribute:: sandbox
       type: str, None

       A optional string referring to a sandbox that is required to run this array function.

    .. py:attribute:: call_force
       type: None, bool

       When a bool, this flag decides whether calls of this instance are cached. However, note that
       when the *call_force* flag passed to :py:meth:`__call__` is specified, it has precedence over
       this attribute.
    """

    # class-level attributes as defaults
    requires_func = None
    setup_func = None
    sandbox = None
    call_force = None
    shifts = set()
    _dependency_sets = ArrayFunction._dependency_sets | {"shifts"}

    @classmethod
    def requires(cls, func: Callable[[dict], None]) -> None:
        """
        Decorator to wrap a function *func* that should be registered as :py:meth:`requires_func`
        which is used to define additional task requirements. The function should accept one
        positional argument:

            - *reqs*, a dictionary into which requirements should be inserted.

        The decorator does not return the wrapped function.

        .. note::

            When the task invoking the requirement is workflow, be aware that both the actual
            workflow instance as well as branch tasks might call the wrapped function. When the
            requirements should differ between them, make sure to use the
            :py:meth:`BaseWorkflow.is_workflow` and :py:meth:`BaseWorkflow.is_branch` methods to
            distinguish the cases.
        """
        cls.requires_func = func

    @classmethod
    def setup(cls, func: Callable[[dict], None]) -> None:
        """
        Decorator to wrap a function *func* that should be registered as :py:meth:`setup_func`
        which is used to perform a custom setup of objects. The function should accept two
        positional arguments:

            - *reqs*, a dictionary containing the required tasks as defined by the custom
              :py:meth:`requires_func`.
            - *inputs*, a dictionary containing the outputs created by the tasks in *reqs*.

        The decorator does not return the wrapped function.
        """
        cls.setup_func = func

    def __init__(
        self,
        *args,
        requires_func: Callable | None = law.no_value,
        setup_func: Callable | None = law.no_value,
        sandbox: str | None = law.no_value,
        call_force: bool | None = law.no_value,
        inst_dict: dict | None = None,
        **kwargs,
    ):
        # store the inst dict with arbitrary attributes that are forwarded to dependency creation
        self.inst_dict = dict(inst_dict or {})

        super().__init__(*args, **kwargs)

        # add class-level attributes as defaults for unset arguments (no_value)
        if requires_func == law.no_value:
            requires_func = self.__class__.requires_func
        if setup_func == law.no_value:
            setup_func = self.__class__.setup_func
        if sandbox == law.no_value:
            sandbox = self.__class__.sandbox
        if call_force == law.no_value:
            call_force = self.__class__.call_force

        # when custom funcs are passed, bind them to this instance
        if requires_func:
            self.requires_func = requires_func.__get__(self, self.__class__)
        if setup_func:
            self.setup_func = setup_func.__get__(self, self.__class__)

        # other attributes
        self.sandbox = sandbox
        self.call_force = call_force

    def __getattr__(self, attr: str) -> Any:
        """
        Attribute access to objects named *attr* in the :py:attr:`inst_dict`.
        """
        if attr in self.inst_dict:
            return self.inst_dict[attr]

        raise AttributeError(f"'{self.__class__.__name__}' object has no attribute '{attr}'")

    def instantiate_dependency(self, cls: DerivableMeta, **kwargs: Any) -> TaskArrayFunction:
        """
        Controls the instantiation of a dependency given by its *cls* and arbitrary *kwargs*,
        updated by *this* instances :py:attr:`inst_dict`.
        """
        # add inst_dict when cls is a TaskArrayFunction itself
        if TaskArrayFunction.derived_by(cls):
            kwargs.setdefault("inst_dict", self.inst_dict)

        return super().instantiate_dependency(cls, **kwargs)

    def _get_all_shifts(self, call_cache: set | None = None) -> set[str]:
        # init the call cache
        if call_cache is None:
            call_cache = set()

        # add shifts and consider _this_ call cached
        shifts = {
            shift
            for shift in self.shifts
            if not isinstance(shift, (ArrayFunction, self.Flagged))
        }
        call_cache.add(self)

        # add shifts of all dependent objects
        for obj in self.get_dependencies(include_others=False):
            if isinstance(obj, TaskArrayFunction):
                if obj not in call_cache:
                    call_cache.add(obj)
                    shifts |= obj._get_all_shifts(call_cache=call_cache)

        return shifts

    @property
    def all_shifts(self) -> set[str]:
        return self._get_all_shifts()

    def run_requires(
        self,
        reqs: dict | None = None,
        call_cache: set | None = None,
    ) -> dict:
        """
        Recursively runs the :py:meth:`requires_func` of this instance and all dependencies. *reqs*
        defaults to an empty dictionary which should be filled to store the requirements.
        """
        # default requirements
        if reqs is None:
            reqs = {}

        # create the call cache
        if call_cache is None:
            call_cache = set()

        # run this instance's requires function
        if callable(self.requires_func):
            self.requires_func(reqs)

        # run the requirements of all dependent objects
        for dep in self.get_dependencies():
            if dep not in call_cache:
                call_cache.add(dep)
                dep.run_requires(reqs=reqs, call_cache=call_cache)

        return reqs

    def run_setup(
        self,
        reqs: dict,
        inputs: dict,
        call_cache: set | None = None,
    ) -> None:
        """
        Recursively runs the :py:meth:`setup_func` of this instance and all dependencies. *reqs*
        corresponds to the requirements created by :py:func:`run_requires`, and *inputs* are their
        outputs.
        """
        # create the call cache
        if call_cache is None:
            call_cache = set()

        # run this instance's setup function
        if callable(self.setup_func):
            self.setup_func(reqs, inputs)

        # run the setup of all dependent objects
        for dep in self.get_dependencies():
            if dep not in call_cache:
                call_cache.add(dep)
                dep.run_setup(reqs, inputs, call_cache=call_cache)

    def __call__(
        self,
        *args,
        call_cache: bool | defaultdict | None = None,
        call_force: bool | None = None,
        n_return: int = 1,
        **kwargs,
    ) -> Any:
        """
        Calls the wrapped :py:meth:`call_func` with all *args* and *kwargs*. The latter is updated
        with :py:attr:`call_kwargs` when set, but giving priority to existing *kwargs*.

        Also, all calls are cached unless *call_cache* is *False*. In case caching is active and
        this instance was called before, it is not called again but the first *n_return* elements of
        *args* are returned unchanged. This check is bypassed when either *call_force* is *True*, or
        when it is *None* and the :py:attr:`call_force` attribute of this instance is *True*.
        """
        # call caching
        if call_cache is not False:
            # setup a new call cache when not present yet
            if not isinstance(call_cache, dict):
                call_cache = defaultdict(int)

            # check if the instance was called before or wether the call is forced
            if call_force is None:
                call_force = self.call_force
            if call_cache[self] > 0 and not call_force:
                return args[0] if n_return == 1 else args[:n_return]

            # increase the count and set kwargs for the call downstream
            call_cache[self] += 1

        # stack all kwargs
        kwargs = {"call_cache": call_cache, **kwargs}

        return super().__call__(*args, **kwargs)


class NoThreadPool(object):
    """
    Dummy implementation that mimics parts of the usual thread pool interface but instead of
    offloading to threads, functions are instantly invoked in the caller (main) thread.
    """

    class SyncResult(object):

        def __init__(self, return_value: Any):
            super().__init__()
            self.return_value = return_value

        def ready(self) -> bool:
            return True

        def get(self) -> Any:
            return self.return_value

    def __init__(self, processes):
        super().__init__()

        self.processes = processes
        self.opened = True

    def __enter__(self) -> NoThreadPool:
        if not self.opened:
            raise Exception(f"cannot enter closed {self.__class__.__name__}")

        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self.close()

    def close(self) -> None:
        self.opened = False

    def terminate(self) -> None:
        return

    def apply_async(self, func, args=(), kwargs=None) -> SyncResult:
        if not self.opened:
            raise Exception(f"cannot apply_async on closed {self.__class__.__name__}")

        return self.SyncResult(func(*args, **(kwargs or {})))


class TaskQueue(object):
    """
    Simple task queue that saves functions and arguments to call them with in multiple queues,
    separated by priority level, as used in the :py:class:`ChunkedIOHandler`.
    """

    # task object
    Task = namedtuple("Task", ["func", "args", "kwargs"])

    def __init__(self):
        super().__init__()

        self._tasks = {}

    def __bool__(self):
        return bool(self._tasks)

    def add(
        self,
        func: Callable,
        args: tuple = (),
        kwargs: dict | None = None,
        priority: int = 0,
    ) -> None:
        """
        Adds a callable *func* that will be executed with *args* and *kwargs* into the queue of
        tasks to process in multiple threads with a certain *priority*.
        """
        if priority not in self._tasks:
            self._tasks[priority] = []

        self._tasks[priority].append(self.Task(func, args, kwargs or {}))

    def get_next(self) -> tuple | None:
        """
        Returns the next task to be executed (see :py:meth:`add_task`), or *None* in case the task
        queue is empty.
        """
        if not self._tasks:
            return None

        # get the maximum existing priority
        prio = max(self._tasks)

        # get the first task
        task = self._tasks[prio].pop(0)

        # remove the list for that priority when empty
        if not self._tasks[prio]:
            del self._tasks[prio]

        return task


class DaskArrayReader(object):
    """
    Class that wraps a dask_awkward array and handles chunked reading via splitting and merging of
    materialized partitions. To allow memory efficient caching in case of overlaps between
    partitions on disk and chunks to be read (possibly with different sizes) this process is
    implemented as a one-time-only read operation. Hence, in situations where particular chunks need
    to be read more than once, another instance of this class should be used.
    """

    def __init__(self, path: str, open_options: dict | None = None):
        super().__init__()

        # open the file
        open_options = open_options or {}
        open_options["split_row_groups"] = False
        self.dak_array = dak.from_parquet(path, **open_options)
        self.path = path

        # fixed mapping of chunk to partition indices, created once in _materialize_via_partitions
        self.chunk_to_partitions = {}

        # temporary mapping of partition indices to cache information (chunks still to be handled
        # and a cached array) that changes during the read process in _materialize_via_partitions
        self.partition_cache = {
            p: DotDict(chunks=[], array=None)
            for p in range(self.dak_array.npartitions)
        }

        # locks to protect against RCs during read operations by different threads
        self.chunk_to_partitions_lock = threading.Lock()
        self.partition_locks = {p: threading.Lock() for p in range(self.dak_array.npartitions)}

    def __del__(self):
        self.close()

    def __len__(self) -> int:
        return len(self.dak_array)

    @property
    def closed(self) -> bool:
        return self.dak_array is None

    def close(self) -> None:
        # free memory and perform an eager, overly cautious gc round
        self.dak_array = None
        self.partition_cache.clear()
        gc.collect()

    def _materialize_via_partitions(
        self,
        chunk_index: int,
        entry_start: int,
        entry_stop: int,
        max_chunk_size: int,
    ) -> ak.array:
        """
        Strategy: read from disk with granularity given by partition divisions
            - use chunk info to determine which partitions need to be read
            - guard each read operation of a partition by locks
            - add materialized partitions that might overlap with another chunk in a temporary cache
            - remove cached partitions eagerly once it becomes clear that no chunk will need it
        """
        # fill the chunk -> partitions mapping once
        with self.chunk_to_partitions_lock:
            if not self.chunk_to_partitions:
                divs = self.dak_array.divisions
                # note: a hare-and-tortoise algorithm could be possible to get the mapping with less
                # than n^2 complexity, but for our case with ~30 chunks this should be ok (for now)
                n_chunks = int(math.ceil(len(self) / max_chunk_size))
                # in case there are no entries, ensure that at least one empty chunk is created
                for _chunk_index in range(max(n_chunks, 1)):
                    _entry_start = _chunk_index * max_chunk_size
                    _entry_stop = min(_entry_start + max_chunk_size, len(self))
                    partitions = []
                    for p, (p_start, p_stop) in enumerate(zip(divs[:-1], divs[1:])):
                        # note: check strict increase of chunk size to accommodate zero-length size
                        if p_stop <= _entry_start < _entry_stop:
                            continue
                        if p_start >= _entry_stop > _entry_start:
                            break
                        partitions.append(p)
                        self.partition_cache[p].chunks.append(_chunk_index)
                    self.chunk_to_partitions[_chunk_index] = partitions

        # read partitions one at a time and store parts that make up the chunk for concatenation
        parts = []
        for p in self.chunk_to_partitions[chunk_index]:
            # obtain the array
            with self.partition_locks[p]:
                # remove this chunk for the list of chunks to be handled
                self.partition_cache[p].chunks.remove(chunk_index)

                if self.partition_cache[p].array is None:
                    arr = self.dak_array.partitions[p].compute()
                    # add to cache when there is a chunk left that will need it
                    if self.partition_cache[p].chunks:
                        self.partition_cache[p].array = arr
                else:
                    arr = self.partition_cache[p].array
                    # remove from cache when there is no chunk left that would need it
                    if not self.partition_cache[p].chunks:
                        self.partition_cache[p].array = None

            # add part for concatenation using entry info
            div_start, div_stop = self.dak_array.divisions[p:p + 2]
            part_start = max(entry_start - div_start, 0)
            part_stop = min(entry_stop - div_start, div_stop - div_start)
            parts.append(arr[part_start:part_stop])
            # workaround for https://github.com/dask-contrib/dask-awkward/issues/140
            # make the array non-optional, assuming it is not meant to be optional
            # TODO: remove this workaround once this other workaround is removed as well:
            # https://github.com/uhh-cms/columnflow/blob/89f3429bbc4349ee2269fae497f3bf69a0b06ed3/columnflow/columnar_util.py#L957  # noqa
            # skip null-filling for completely empty arrays
            if not parts[-1].layout.contents:
                continue
            if getattr(parts[-1], "fields", None) and ak.any(ak.ravel(ak.is_none(parts[-1]))):
                logger.warning(
                    f"None values detected in chunk {chunk_index} of file {self.path} during "
                    "dask_awkward materialization that were filled with zeros while reading; "
                    "this is a workaround in columnflow to prevent optional types in arrays; "
                    "for now, please make sure that files are not written with optional types!",
                )
            parts[-1] = ak.fill_none(parts[-1], 0)
            # end workaround

        # construct the full array
        arr = parts[0] if len(parts) == 1 else ak.concatenate(parts, axis=0)

        del parts
        gc.collect()

        return arr

    def materialize(self, *args, **kwargs) -> ak.array:
        # for now, it seems like the only method for materializing slices of dak arrays to ak arrays
        # is through invoking the "compute()" operation on partitions, and in fact, slicing on dak
        # arrays is not supported at this time (arr[start:stop] raises DaskAwkwardNotImplemented);
        # therefore, the only way to perform parallel, chunked read operations is through partitions
        # and while potentially being to coarse on disk, there might even be two advantages:
        # 1. in cf, there are typically no parquet files from external sources, but they are all
        #    created within cf and thus, they typical partition sizes exactly match the desired
        #    chunk size (as they were created in a chunked way and eventually merged), leading to
        #    zero overhead and no caching / overlap issues
        # 2. it seems far more performant to read full partitions from disk rather than parts of
        #    them (if possible at all) since meta data might have to be read in any case
        return self._materialize_via_partitions(*args, **kwargs)


class ChunkedIOHandler(object):
    """
    Allows reading one or multiple files and iterating through chunks of their content with
    multi-threaded read operations. Chunks and their positions (denoted by start and stop markers,
    and the index of the chunk itself) are accessed by iterating through a handler instance. Also,
    this handler allows interactions with the internal queue handling tasks in multiple-threads to
    effectively write output chunks within the same pool of threads.

    The content to load is configurable through *source*, which can be a file path or an opened file
    object, and a *source_type*, which defines how the *source* should be opened and traversed for
    chunking. See the classmethods ``open_...`` and ``read_...`` below for implementation details
    and :py:meth:`get_source_handlers` for a list of currently supported sources.

    Example:

    .. code-block:: python

        # iterate through a single file
        # (creating the handler and iterating through it in the same line)
        for chunk, position in ChunkedIOHandler("data.root", source_type="coffea_root"):
            # chunk is a NanoAODEventsArray as returned by read_coffea_root
            jet_pts = chunk.Jet.pt
            print(f"jet pts of chunk {chunk.index}: {jet_pts}")

    .. code-block:: python

        # iterate through multiple files simultaneously
        # (also, now creating the handler first and then iterating through it)
        with ChunkedIOHandler(
            ("data.root", "masks.parquet"),
            source_type=("coffea_root", "awkward_parquet"),
        ) as handler:
            for (chunk, masks), position in handler:
                # chunk is a NanoAODEventsArray as returned by read_coffea_root
                # masks is an awkward array as returned by read_awkward_parquet
                selected_jet_pts = chunk[masks].Jet.pt
                print(f"selected jet pts of chunk {chunk.index}: {selected_jet_pts}")

                # add a callback to the task queue, e.g. for saving a column
                handler.queue(ak.to_parquet, (selected_jet_pts, "some/path.parquet"))

    The maximum size of the chunks and the number of threads to load them can be configured through
    *chunk_size* and *pool_size*. Chunks are fully loaded into memory before they are yielded to be
    used in the main thread.

    In addition, *open_options* and *read_options* are forwarded to internal open and read
    implementations to further control and optimize IO. For instance, they can be configured to
    select only a subset of (nested) columns to read from disk. However, since this is highly
    dependent on the specific source type, *read_columns* can be defined as a set (!) of strings (in
    nano-style dot format) or :py:class:`Route` objects and is added to either *open_options* or
    *read_options* internally (source type dependent).

    If *source* refers to a single object, *source_type*, *open_options*, *read_options* and
    *read_columns* should be single values as well. Otherwise, if *source* is a sequence of sources,
    the other arguments can be sequences as well with the same length.

    During iteration, before chunks are yielded, an optional message *iter_message* is printed when
    set, receiving the respective :py:class:`ChunkedIOHandler.ChunkPosition` as the field *pos* for
    formatting.
    """

    # source handler container
    SourceHandler = namedtuple(
        "SourceHandler",
        ["type", "open", "close", "read"],
    )

    # chunk position container
    ChunkPosition = namedtuple(
        "ChunkPosition",
        ["index", "entry_start", "entry_stop", "max_chunk_size"],
    )

    # read result container
    ReadResult = namedtuple(
        "ReadResult",
        ["chunk", "chunk_pos"],
    )

    def __init__(
        self,
        source: Any,
        source_type: str | Sequence[str] | None = None,
        chunk_size: int = law.config.get_expanded_int("analysis", "chunked_io_chunk_size", 50000),
        pool_size: int = law.config.get_expanded_int("analysis", "chunked_io_pool_size", 4),
        open_options: dict | Sequence[dict] | None = None,
        read_options: dict | Sequence[dict] | None = None,
        read_columns: set | Sequence[set] | None = None,
        iter_message: str = "handling chunk {pos.index}",
        debug: bool = law.config.get_expanded_boolean("analysis", "chunked_io_debug", False),
    ):
        super().__init__()

        # multiple inputs?
        is_multi = lambda obj: isinstance(obj, (list, tuple))
        self.is_multi = is_multi(source)
        self.n_sources = len(source) if self.is_multi else 1

        # check source
        if not self.n_sources:
            raise Exception("at least one source must be defined")

        # helper to check multiplicities of some args
        def _check_arg(name, value):
            if self.is_multi:
                if not is_multi(value):
                    value = self.n_sources * [value]
                if len(value) != self.n_sources:
                    if len(value) != 1:
                        raise Exception(
                            f"length of {name} should match length of source ({self.n_sources}), "
                            f"but got {len(value)}",
                        )
                    value *= self.n_sources
            elif is_multi(value):
                raise Exception(
                    f"when source is not a sequence, {name} should neither, but got '{value}'",
                )
            return value

        # check args
        source_type = _check_arg("source_type", source_type)
        open_options = _check_arg("open_options", open_options)
        read_options = _check_arg("read_options", read_options)
        read_columns = _check_arg("read_columns", read_columns)

        # store input attributes
        self.source_list = list(source) if self.is_multi else [source]
        self.source_type_list = list(source_type) if self.is_multi else [source_type]
        self.open_options_list = list(open_options) if self.is_multi else [open_options]
        self.read_options_list = list(read_options) if self.is_multi else [read_options]
        self.read_columns_list = list(read_columns) if self.is_multi else [read_columns]
        self.chunk_size = chunk_size
        self.pool_size = max(pool_size, 1)
        self.iter_message = iter_message

        # attributes that are set in open(), close() or __iter__()
        self.source_objects = []
        self.n_entries = None
        self.task_queue = TaskQueue()
        self.pool_cls = multiprocessing.pool.ThreadPool
        self.pool = None

        # debug settings
        if debug:
            logger.info(
                f"{self.__class__.__name__} set to debug mode, using in-thread pool with size 1",
            )
            self.pool_size = 1
            self.pool_cls = NoThreadPool

        # determine type, open and read functions per source
        self.source_handlers = [
            self.get_source_handler(source_type, source)
            for source_type, source in zip(self.source_type_list, self.source_list)
        ]

    @classmethod
    def create_chunk_position(
        cls,
        n_entries: int,
        chunk_size: int,
        chunk_index: int,
    ) -> ChunkPosition:
        """
        Creates and returns a *ChunkPosition* object based on the total number of entries
        *n_entries*, the maximum *chunk_size*, and the index of the chunk *chunk_index*.
        """
        # determine the start of stop of this chunk
        if n_entries == 0:
            entry_start = 0
            entry_stop = 0
        else:
            entry_start = chunk_index * chunk_size
            entry_stop = min((chunk_index + 1) * chunk_size, n_entries)

        return cls.ChunkPosition(chunk_index, entry_start, entry_stop, chunk_size)

    @classmethod
    def get_source_handler(
        cls,
        source_type: str | None,
        source: Any | None,
    ) -> SourceHandler:
        """
        Takes a *source_type* (see list below) and gathers information about how to open, read
        content from, and close a specific source, and returns that information in a named
        *SourceHandler* tuple.

        When *source_type* is *None* but an arbitrary *source* is set, the type is derived from that
        object, and an exception is raised in case no type can be inferred.

        Currently supported source types are:

            - "uproot_root"
            - "coffea_root"
            - "coffea_parquet" (currently unsupported)
            - "awkward_parquet"
        """
        if source_type is None:
            if isinstance(source, uproot.ReadOnlyDirectory):
                # uproot file
                source_type = "uproot_root"
            elif isinstance(source, str):
                # file path, guess based on extension
                if source.endswith(".root"):
                    # priotize coffea nano events
                    source_type = "coffea_root"
                elif source.endswith(".parquet"):
                    # priotize coffea nano events
                    source_type = "coffea_parquet"

            if not source_type:
                raise Exception(f"could not determine source_type from source '{source}'")

        if source_type == "uproot_root":
            return cls.SourceHandler(
                source_type,
                cls.open_uproot_root,
                cls.close_uproot_root,
                cls.read_uproot_root,
            )
        if source_type == "coffea_root":
            return cls.SourceHandler(
                source_type,
                cls.open_coffea_root,
                cls.close_coffea_root,
                cls.read_coffea_root,
            )
        # if source_type == "coffea_parquet":
        #     return cls.SourceHandler(
        #         source_type,
        #         cls.open_coffea_parquet,
        #         cls.close_coffea_parquet,
        #         cls.read_coffea_parquet,
        #     )
        if source_type == "awkward_parquet":
            return cls.SourceHandler(
                source_type,
                cls.open_awkward_parquet,
                cls.close_awkward_parquet,
                cls.read_awkward_parquet,
            )

        raise NotImplementedError(f"unknown source_type '{source_type}'")

    @classmethod
    def open_uproot_root(
        cls,
        source: (
            str |
            uproot.ReadOnlyDirectory |
            tuple[str, str] |
            tuple[uproot.ReadOnlyDirectory, str]
        ),
        open_options: dict | None = None,
        read_columns: set[str | Route] | None = None,
    ) -> tuple[uproot.TTree, int]:
        """
        Opens an uproot tree from a root file at *source* and returns a 2-tuple *(tree, entries)*.
        *source* can be the path of the file, an already opened, readable uproot file (assuming the
        tree is called "Events"), or a 2-tuple whose second item defines the name of the tree to be
        loaded. When a new file is opened, it receives *open_options*. Passing *read_columns* has no
        effect.
        """
        tree_name = "Events"
        if isinstance(source, tuple) and len(source) == 2:
            source, tree_name = source
        if isinstance(source, str):
            # default open options
            open_options = open_options or {}
            open_options["object_cache"] = None
            open_options["array_cache"] = None
            open_options.setdefault("decompression_executor", None)
            open_options.setdefault("interpretation_executor", None)
            f = uproot.open(source, **open_options)
            tree = f[tree_name]
        elif isinstance(source, uproot.ReadOnlyDirectory):
            tree = source[tree_name]
        else:
            raise Exception(f"'{source}' cannot be opened as uproot_root")

        return (tree, tree.num_entries)

    @classmethod
    def close_uproot_root(
        cls,
        source_object: uproot.TTree,
    ) -> None:
        """
        Closes the file that contains the TTree *source_object*.
        """
        f = getattr(source_object, "file", None)
        if f is not None:
            f.close()

    @classmethod
    def read_uproot_root(
        cls,
        source_object: uproot.TTree,
        chunk_pos: ChunkPosition,
        read_options: dict | None = None,
        read_columns: set[str | Route] | None = None,
    ) -> ak.Array:
        """
        Given an uproot TTree *source_object*, returns an awkward array chunk referred to by
        *chunk_pos*. *read_options* are passed to ``uproot.TTree.arrays``. *read_columns* are
        converted to strings and, if not already present, added as field ``filter_name`` to
        *read_options*.
        """
        # default read options
        read_options = read_options or {}
        read_options["array_cache"] = None

        # inject read_columns
        if read_columns and "filter_name" not in read_options:
            filter_name = [Route(s).string_nano_column for s in read_columns]
            read_options["filter_name"] = filter_name

        chunk = source_object.arrays(
            entry_start=chunk_pos.entry_start,
            entry_stop=chunk_pos.entry_stop,
            **read_options,
        )

        return chunk

    @classmethod
    def open_coffea_root(
        cls,
        source: (
            str |
            uproot.ReadOnlyDirectory |
            tuple[str, str] |
            tuple[uproot.ReadOnlyDirectory, str]
        ),
        open_options: dict | None = None,
        read_columns: set[str | Route] | None = None,
    ) -> tuple[uproot.ReadOnlyDirectory, int]:
        """
        Opens an uproot file at *source* for subsequent processing with coffea and returns a 2-tuple
        *(uproot file, tree entries)*. *source* can be the path of the file, an already opened,
        readable uproot file (assuming the tree is called "Events"), or a 2-tuple whose second item
        defines the name of the tree to be loaded. *open_options* are forwarded to ``uproot.open``
        if a new file is opened. Passing *read_columns* has no effect.
        """
        tree_name = "Events"
        if isinstance(source, tuple) and len(source) == 2:
            source, tree_name = source
        if isinstance(source, str):
            # default open options
            open_options = open_options or {}
            open_options["object_cache"] = None
            open_options["array_cache"] = None
            source = uproot.open(source, **open_options)
            tree = source[tree_name]
        elif isinstance(source, uproot.ReadOnlyDirectory):
            tree = source[tree_name]
        else:
            raise Exception(f"'{source}' cannot be opened as coffea_root")

        return (source, tree.num_entries)

    @classmethod
    def close_coffea_root(
        cls,
        source_object: str | uproot.ReadOnlyDirectory,
    ) -> None:
        """
        Closes a ROOT file referred to by *source_object* if it is a ``ReadOnlyDirectory``. In case
        a string is passed, this method does nothing.
        """
        close = None if isinstance(source_object, str) else getattr(source_object, "close", None)
        if callable(close):
            close()

    @classmethod
    def read_coffea_root(
        cls,
        source_object: str | uproot.ReadOnlyDirectory,
        chunk_pos: ChunkPosition,
        read_options: dict | None = None,
        read_columns: set[str | Route] | None = None,
    ) -> coffea.nanoevents.methods.base.NanoEventsArray:
        """
        Given a file location or opened uproot file *source_object*, returns an awkward array chunk
        referred to by *chunk_pos*, assuming nanoAOD structure. *read_options* are passed to
        ``coffea.nanoevents.NanoEventsFactory.from_root``. *read_columns* are converted to strings
        and, if not already present, added as nested field ``iteritems_options.filter_name`` to
        *read_options*.
        """
        # default read options
        read_options = read_options or {}
        read_options["runtime_cache"] = None
        read_options["persistent_cache"] = None

        # inject read_columns
        if read_columns and (
            "iteritems_options" not in read_options or
            "filter_name" not in read_options["iteritems_options"]
        ):
            filter_name = [Route(s).string_nano_column for s in read_columns]
            read_options.setdefault("iteritems_options", {})["filter_name"] = filter_name

        # read the events chunk into memory
        chunk = coffea.nanoevents.NanoEventsFactory.from_root(
            source_object,
            entry_start=chunk_pos.entry_start,
            entry_stop=chunk_pos.entry_stop,
            **read_options,
        ).events()

        return chunk

    # @classmethod
    # def open_coffea_parquet(
    #     cls,
    #     source: str,
    #     open_options: dict | None = None,
    #     read_columns: set[str | Route] | None = None,
    # ) -> tuple[str, int]:
    #     """
    #     Given a parquet file located at *source*, returns a 2-tuple *(source, entries)*. Passing
    #     *open_options* has no effect.
    #
    #     TODO: use read_columns?
    #     """
    #     return (source, pq.ParquetFile(source).metadata.num_rows)

    # @classmethod
    # def close_coffea_parquet(
    #     cls,
    #     source_object: str,
    # ) -> None:
    #     """
    #     This is a placeholder method and has no effect.
    #     """
    #     return

    # @classmethod
    # def read_coffea_parquet(
    #     cls,
    #     source_object: str,
    #     chunk_pos: ChunkPosition,
    #     read_options: dict | None = None,
    #     read_columns: set[str | Route] | None = None,
    # ) -> coffea.nanoevents.methods.base.NanoEventsArray:
    #     """
    #     Given a the location of a parquet file *source_object*, returns an awkward array chunk
    #     referred to by *chunk_pos*, assuming nanoAOD structure. *read_options* are passed to
    #     *coffea.nanoevents.NanoEventsFactory.from_parquet*.
    #
    #     TODO: use read_columns?
    #     """
    #     # TODO: default read options? go via dak and preloaded?
    #
    #     # read the events chunk into memory
    #     chunk = coffea.nanoevents.NanoEventsFactory.from_parquet(
    #         source_object,
    #         entry_start=chunk_pos.entry_start,
    #         entry_stop=chunk_pos.entry_stop,
    #         schemaclass=coffea.nanoevents.NanoAODSchema,
    #         **(read_options or {}),
    #     ).events()

    #     return chunk

    @classmethod
    def open_awkward_parquet(
        cls,
        source: str,
        open_options: dict | None = None,
        read_columns: set[str | Route] | None = None,
    ) -> tuple[ak.Array, int]:
        """
        Opens a parquet file saved at *source*, loads the content as an dask awkward array,
        wrapped by a :py:class:`DaskArrayReader`, and returns a 2-tuple *(array, length)*.
        *open_options* and *chunk_size* are forwarded to :py:class:`DaskArrayReader`. *read_columns*
        are converted to strings and, if not already present, added as field ``columns`` to
        *open_options*.
        """
        if not isinstance(source, str):
            raise Exception(f"'{source}' cannot be opened as awkward_parquet")

        # default open options
        open_options = open_options or {}

        # inject read_columns
        if read_columns and "columns" not in open_options:
            filter_name = [Route(s).string_column for s in read_columns]
            open_options["columns"] = filter_name

        # load the array wrapper
        arr = DaskArrayReader(source, open_options)

        return (arr, len(arr))

    @classmethod
    def close_awkward_parquet(
        cls,
        source_object: DaskArrayReader,
    ) -> None:
        """
        Closes the dask array wrapper referred to by *source_object*.
        """
        source_object.close()

    @classmethod
    def read_awkward_parquet(
        cls,
        source_object: DaskArrayReader,
        chunk_pos: ChunkPosition,
        read_options: dict | None = None,
        read_columns: set[str | Route] | None = None,
    ) -> ak.Array:
        """
        Given a :py:class:`DaskArrayReader` *source_object*, returns the chunk referred to by
        *chunk_pos* as a full copy loaded into memory. Passing neither *read_options* nor
        *read_columns* has an effect.
        """
        # get the materialized ak array for that chunk
        return source_object.materialize(
            chunk_pos.index,
            chunk_pos.entry_start,
            chunk_pos.entry_stop,
            chunk_pos.max_chunk_size,
        )

    @property
    def n_chunks(self) -> int:
        """
        Returns the number of chunks this instance will iterate over based on the number of entries
        :py:attr:`n_entries` and the configured :py:attr:`chunk_size`. In case :py:attr:`n_entries`
        was not initialzed yet (via :py:meth:`open`), an *AttributeError* is raised.
        """
        if self.n_entries is None:
            raise AttributeError("cannot determine number of chunks before open()")
        return int(math.ceil(self.n_entries / self.chunk_size))

    @property
    def closed(self) -> bool:
        """
        Returns whether the instance is closed for reading.
        """
        return len(self.source_objects) != self.n_sources

    def open(self) -> None:
        """
        Opens all previously registered sources and preloads all source objects to read content from
        later on. Nothing happens if this instance is already opened (i.e. not :py:attr:`closed`).
        """
        if not self.closed:
            # already open
            return

        # reset some attributes
        del self.source_objects[:]
        gc.collect()
        self.n_entries = None

        # open all sources and make sure they have the same number of entries
        for i, (source, source_handler, open_options, read_columns) in enumerate(zip(
            self.source_list,
            self.source_handlers,
            self.open_options_list,
            self.read_columns_list,
        )):
            # open the source
            obj, n = source_handler.open(source, open_options=open_options, read_columns=read_columns)
            # check entries
            if i == 0:
                self.n_entries = n
            elif n != self.n_entries:
                raise ValueError(
                    f"number of entries of source {i} '{source}' does not match first source",
                )
            # save the source object
            self.source_objects.append(obj)

            logger_perf.debug(f"opening {source} of type {source_handler.type} for reading")

    def close(self) -> None:
        """
        Closes all cached, opened files and deletes loaded source objects. Nothing happens if the
        instance is already :py:attr:`closed` for reading.
        """
        if self.closed:
            # already closed
            return

        # close all source objects
        for (obj, source_handler) in zip(self.source_objects, self.source_handlers):
            source_handler.close(obj)

        # just delete the file cache and reset some attributes
        del self.source_objects[:]
        gc.collect()

    def queue(self, *args, **kwargs) -> None:
        """
        Adds a new task to the internal task queue with all *args* and *kwargs* forwarded to
        :py:meth:`TaskQueue.add`.
        """
        return self.task_queue.add(*args, **kwargs)

    def _iter_impl(self):
        """
        Internal iterator implementation. Please refer to :py:meth:`__iter__` and the usual iterator
        interface.
        """
        if self.closed:
            raise Exception(f"cannot iterate through closed {self.__class__.__name__}")

        # create a list of read functions
        read_funcs = [
            partial(source_handler.read, obj, read_options=read_options, read_columns=read_columns)
            for obj, source_handler, read_options, read_columns in zip(
                self.source_objects,
                self.source_handlers,
                self.read_options_list,
                self.read_columns_list,
            )
        ]

        # lightweight callabe that wraps all read funcs and comines their return values
        def read(chunk_pos):
            chunks = []
            durations = []
            t1 = time.perf_counter()
            for read_func in read_funcs:
                chunks.append(read_func(chunk_pos))
                durations.append(time.perf_counter() - t1)

            duration = law.util.human_duration(seconds=sum(durations))
            durations = [f"{s:.1f}" for s in durations]
            logger_perf.debug(
                f"reading of chunk {chunk_pos.index} from {len(durations)} file(s) took {duration} "
                f"({'+'.join(durations)}s)",
            )

            return self.ReadResult((chunks if self.is_multi else chunks[0]), chunk_pos)

        # create a list of all chunk positions
        chunk_positions = [
            self.create_chunk_position(self.n_entries, self.chunk_size, chunk_index)
            for chunk_index in range(max(self.n_chunks, 1))
        ]

        # fill the list of tasks the pool has to work through
        for chunk_pos in chunk_positions:
            self.task_queue.add(read, (chunk_pos,), priority=-1)

        # strategy: setup the pool and manually keep it filled up to pool_size and do not insert all
        # chunks right away as this could swamp the memory if processing is slower than IO
        with self.pool_cls(self.pool_size) as self.pool:
            results = []
            no_result = object()

            try:
                while self.task_queue or results:
                    # find the first done result and remove it from the list
                    # this will do nothing in the first iteration
                    result_obj = no_result
                    for i, result in enumerate(list(results)):
                        if not result.ready():
                            continue

                        result_obj = result.get()
                        results.pop(i)
                        break

                    # if no result was ready, sleep and try again
                    if results and result_obj == no_result:
                        time.sleep(0.05)
                        continue

                    # immediately try to fill up the pool
                    while len(results) < self.pool_size and self.task_queue:
                        task = self.task_queue.get_next()
                        results.append(self.pool.apply_async(task.func, task.args, task.kwargs))

                    # if a result was ready and it returned a ReadResult, yield it
                    if isinstance(result_obj, self.ReadResult):
                        if self.iter_message:
                            print(self.iter_message.format(pos=result_obj.chunk_pos))

                        # probably overly-cautious, but run garbage collection before and after
                        gc.collect()
                        t1 = time.perf_counter()
                        try:
                            yield (result_obj.chunk, result_obj.chunk_pos)
                        finally:
                            duration = time.perf_counter() - t1
                            logger_perf.debug(
                                f"processing of chunk {result_obj.chunk_pos.index} took " +
                                law.util.human_duration(seconds=duration),
                            )
                        gc.collect()

            except:
                self.pool.close()
                self.pool.terminate()
                raise

            finally:
                self.pool = None

    def __del__(self):
        """
        Destructor that closes all cached, opened files.
        """
        try:
            self.close()
        except:
            pass

    def __enter__(self):
        """
        Context entry point that opens all sources.
        """
        self.open()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Context exit point that closes all cached, opened files.
        """
        self.close()

    def __iter__(self):
        """
        Iterator that yields chunks (and their positions) of all registered sources, fully
        preserving their order such that the same chunk is read and yielded per source. Internally,
        a multi-thread pool is used to load file content in a parallel fashion. During iteration,
        the so-called tasks to be processed by this pool can be extended via :py:meth:`add_task`.
        In case an exception is raised during the processing of chunks, or while content is loaded,
        the pool is closed and terminated.
        """
        # make sure all sources are opened using a context and yield the internal iterator
        with self:
            yield from self._iter_impl()
