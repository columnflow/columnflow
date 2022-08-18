# coding: utf-8

"""
Helpers and utilities for working with columnar libraries.
"""

__all__ = [
    "mandatory_coffea_columns", "EMPTY_INT", "EMPTY_FLOAT",
    "Route", "RouteFilter", "ArrayFunction", "TaskArrayFunction", "ChunkedReader",
    "PreloadedNanoEventsFactory",
    "eval_item", "get_ak_routes", "has_ak_column", "set_ak_column", "remove_ak_column",
    "add_ak_alias", "add_ak_aliases", "update_ak_array", "flatten_ak_array", "sort_ak_fields",
    "sorted_ak_to_parquet",
]


import re
import math
import time
import copy
import enum
import weakref
import multiprocessing
import multiprocessing.pool
from functools import partial
from collections import namedtuple, OrderedDict, defaultdict
from typing import Optional, Union, Sequence, Set, Tuple, List, Dict, Callable, Any

import law

from columnflow.util import UNSET, maybe_import, classproperty, DotDict, DerivableMeta, Derivable

np = maybe_import("numpy")
ak = maybe_import("awkward")
uproot = maybe_import("uproot")
coffea = maybe_import("coffea")
maybe_import("coffea.nanoevents")
maybe_import("coffea.nanoevents.methods.base")
pq = maybe_import("pyarrow.parquet")


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

    def __call__(cls, route: Optional[Union["Route", Any]] = None):
        if isinstance(route, cls):
            return route

        return super().__call__(route=route)


class Route(object, metaclass=RouteMeta):
    """
    Route objects describe the location of columns in nested arrays and are basically wrapper around
    a sequence of nested fields. Additionally, they provide convenience methods for conversions into
    column names, either in dot or nano-style underscore format.

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
        fields: Sequence[Union[str, int, slice, type(Ellipsis), tuple, list]],
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
        fields: Sequence[Union[str, int, slice, type(Ellipsis), list, tuple]],
    ) -> str:
        """
        Joins a sequence of strings into a string in dot format and returns it.
        """
        return cls._join(cls.DOT_SEP, fields)

    @classmethod
    def join_nano(
        cls,
        fields: Sequence[Union[str, int, slice, type(Ellipsis), list, tuple]],
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
    ) -> Tuple[Union[str, int, slice, type(Ellipsis), list, tuple]]:
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
    def split(cls, column: str) -> Tuple[Union[str, int, slice, type(Ellipsis), list, tuple]]:
        """
        Splits a string assumed to be in dot format and returns the fragments, potentially with
        selection, slice and advanced indexing expressions.
        """
        return cls._split(cls.DOT_SEP, column)

    @classmethod
    def split_nano(cls, column: str) -> Tuple[Union[str, int, slice, type(Ellipsis), list, tuple]]:
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
    def fields(self):
        return tuple(self._fields)

    @property
    def column(self):
        return self.join(self._fields)

    @property
    def nano_column(self):
        return self.join_nano(self._fields)

    def __str__(self) -> str:
        return self.join(self._fields)

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__} '{self}' at {hex(id(self))}>"

    def __hash__(self) -> int:
        return hash(self.fields)

    def __len__(self) -> int:
        return len(self._fields)

    def __eq__(self, other: Union["Route", Sequence[str], str]) -> bool:
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
        other: Union["Route", str, Sequence[Union[str, int, slice, type(Ellipsis), list, tuple]]],
    ) -> "Route":
        route = self.copy()
        route.add(other)
        return route

    def __radd__(
        self,
        other: Union["Route", str, Sequence[Union[str, int, slice, type(Ellipsis), list, tuple]]],
    ) -> "Route":
        return self.__add__(other)

    def __iadd__(
        self,
        other: Union["Route", str, Sequence[Union[str, int, slice, type(Ellipsis), list, tuple]]],
    ) -> "Route":
        self.add(other)
        return self

    def __getitem__(
        self,
        index: Any,
    ) -> Union["Route", str, int, slice, type(Ellipsis), list, tuple]:
        # detect slicing and return a new instance with the selected fields
        field = self._fields.__getitem__(index)
        return field if isinstance(index, int) else self.__class__(field)

    def __setitem__(
        self,
        index: Any,
        value: Union[str, int, slice, type(Ellipsis), list, tuple],
    ) -> None:
        self._fields.__setitem__(index, value)

    def add(
        self,
        other: Union["Route", str, Sequence[Union[str, int, slice, type(Ellipsis), list, tuple]]],
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

    def copy(self) -> "Route":
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
) -> List[Route]:
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
        if arr.fields and (max_depth <= 0 or len(fields) < max_depth):
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
    route: Union[Route, Sequence[str], str],
) -> bool:
    """
    Returns whether an awkward array *ak_array* contains a nested field identified by a *route*. A
    route can be a :py:class:`Route` instance, a tuple of strings where each string refers to a
    subfield, e.g. ``("Jet", "pt")``, or a string with dot format (e.g. ``"Jet.pt"``).
    """
    route = Route(route)

    try:
        route.apply(ak_array)
    except ValueError:
        return False

    return True


def set_ak_column(
    ak_array: ak.Array,
    route: Union[Route, Sequence[str], str],
    value: ak.Array,
) -> ak.Array:
    """
    Inserts a new column into awkward array *ak_array* in-place and returns it. The column can be
    defined through a route, i.e., a :py:class:`Route` instance, a tuple of strings where each
    string refers to a subfield, e.g. ``("Jet", "pt")``, or a string with dot format (e.g.
    ``"Jet.pt"``), and the column *value* itself. Intermediate, non-existing fields are
    automatically created. Example:

    .. code-block:: python

        arr = ak.zip({"Jet": {"pt": [30], "eta": [2.5]}})

        set_ak_column(arr, "Jet.mass", [40])
        set_ak_column(arr, "Muon.pt", [25])  # creates subfield "Muon" first

    .. note::

        Issues can arise in cases where the route to add already exists and has a different type
        than the newly added *value*. If this is the case, you should consider remove the column
        first with :py:func:`remove_ak_column`. As this one might return a new view, it is not
        automatically handled in *this* function which is meant to perform an in-place operation.
    """
    route = Route(route)

    # try to remove the route first so that existing columns are not overwritten but replaces
    ak_array = remove_ak_column(ak_array, route, silent=True)

    # trivial case
    if len(route) == 1:
        ak_array[route.fields] = value
        return ak_array

    # identify the existing part of the subroute
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
    ak_array[sub_route.fields] = value

    return ak_array


def remove_ak_column(
    ak_array: ak.Array,
    route: Union[Route, Sequence[str], str],
    silent: bool = False,
) -> ak.Array:
    """
    Removes a *route* from an awkward array *ak_array* and returns the array. The (sub)array in
    which the column is to be removed is replaced by a view which misses the corresponding column.
    Therefore, if the column to be removed is found in a top-level field, the returned array itself
    is a view. Otherwise, one of its subarrays will be a view.

    Note that only *route* can be a :py:class:`Route` instance, a tuple of strings where each
    string refers to a subfield, e.g. ``("Jet", "pt")``, or a string with dot format (e.g.
    ``"Jet.pt"``). Unless *silent* is *True*, a *ValueError* is raised when the route does not
    exist.
    """
    # verify that the route exists
    route = Route(route)
    if not route:
        if silent:
            return ak_array
        raise ValueError("route must not be empty")
    if not has_ak_column(ak_array, route):
        if silent:
            return ak_array
        raise ValueError(f"no column found in array for route '{route}'")

    if len(route) == 1:
        # trivial case: remove a top level column
        ak_array = ak_array[[f for f in ak_array.fields if f != route[0]]]
    else:
        # nested case: given the route ("a", "b", "c"), set ak_array["a", "b"] to a view of "b"
        # with all fields but "c" using __setitem__ syntax (and in particular not __setattr__!)
        sub_route, remove_field = route[:-1], route[-1]
        sub_array = ak_array[sub_route.fields]
        # determine remaining fields
        remaining_fields = [f for f in sub_array.fields if f != remove_field]
        # if no fields are left, remove the entire sub_view
        if not remaining_fields:
            return remove_ak_column(ak_array, route[:-1])
        # set the reduced view
        ak_array[sub_route.fields] = sub_array[remaining_fields]

    return ak_array


def add_ak_alias(
    ak_array: ak.Array,
    src_route: Union[Route, Sequence[str], str],
    dst_route: Union[Route, Sequence[str], str],
    remove_src: bool = False,
) -> ak.Array:
    """
    Adds an alias to an awkward array *ak_array* in-place (depending on *remove_src*, see below),
    pointing the array at *src_route* to *dst_route*. Both routes can be a :py:class:`Route`
    instance, a tuple of strings where each string refers to a subfield, e.g. ``("Jet", "pt")``, or
    a string with dot format (e.g. ``"Jet.pt"``). A *ValueError* is raised when *src_route* does not
    exist.

    Note that existing columns referred to by *dst_route* might be overwritten. When *remove_src* is
    *True*, a view of the input array is returned with the column referred to by *src_route*
    missing. Otherwise, the input array is returned with all columns.
    """
    src_route = Route(src_route)
    dst_route = Route(dst_route)

    # check that the src exists
    if not has_ak_column(ak_array, src_route):
        raise ValueError(f"no column found in array for route '{src_route}'")

    # add the alias, potentially overwriting existing columns
    ak_array = set_ak_column(ak_array, dst_route, src_route.apply(ak_array))

    # create a view without the source if requested
    if remove_src:
        ak_array = remove_ak_column(ak_array, src_route)

    return ak_array


def add_ak_aliases(
    ak_array: ak.Array,
    aliases: Dict[Union[Route, Sequence[str], str], Union[Route, Sequence[str], str]],
    remove_src: bool = False,
) -> ak.Array:
    """
    Adds multiple *aliases*, given in a dictionary mapping destination columns to source columns, to
    an awkward array *ak_array* in-place (depending on *remove_src*, see below). Each column in this
    dictionary can be referred to by a :py:class:`Route` instance, a tuple of strings where each
    string refers to a subfield, e.g. ``("Jet", "pt")``, or a string with dot format (e.g.
    ``"Jet.pt"``). See :py:func:`add_ak_aliases` for more info.

    When *remove_src* is *True*, a view of the input array is returned with all source columns
    missing. Otherwise, the input array is returned with all columns.
    """
    # add all aliases
    for dst_route, src_route in aliases.items():
        ak_array = add_ak_alias(ak_array, src_route, dst_route, remove_src=remove_src)

    return ak_array


def update_ak_array(
    ak_array: ak.Array,
    *others: ak.Array,
    overwrite_routes: Union[bool, List[Union[Route, Sequence[str], str]]] = True,
    add_routes: Union[bool, List[Union[Route, Sequence[str], str]]] = False,
    concat_routes: Union[bool, List[Union[Route, Sequence[str], str]]] = False,
) -> ak.Array:
    """
    Updates an awkward array *ak_array* with the content of multiple different arrays *others* and
    potentially (see below) returns a new view. Internally, :py:func:`get_ak_routes` is used to
    obtain the list of all routes pointing to potentially deeply nested arrays. The input array is
    also returned.

    If two columns overlap during this update process, four different cases can be configured to
    occur:

        1. If *concat_routes* is either *True* or a list of routes containing the route in question,
           the columns are concatenated along axis 1. This obviously implies that their shapes must
           be compatible.
        2. If case 1 does not apply and *add_routes* is either *True* or a list of routes containing
           the route in question, the columns are added using the plus operator, forwarding the
           actual implementation to awkward.
        3. If cases 1 and 2 do not apply and *overwrite_routes* is either *True* or a list of routes
           containing the route in question, new columns (right most in *others*) overwrite
           existing ones. A new view is returned in case this case occurs at least once.
        4. If none of the cases above apply, an exception is raised.
    """
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
    def _match_route(bool_or_list, cache, route):
        if route not in cache:
            cache[route] = route in bool_or_list if isinstance(bool_or_list, list) else bool_or_list
        return cache[route]

    do_overwrite = partial(_match_route, overwrite_routes, {})
    do_add = partial(_match_route, add_routes, {})
    do_concat = partial(_match_route, concat_routes, {})

    # go through all other arrays and merge their columns
    for other in others:
        for route in get_ak_routes(other):
            if has_ak_column_cached(route):
                if do_concat(route):
                    # concat and reassign
                    ak_array = set_ak_column(
                        ak_array,
                        route,
                        ak.concatenate((
                            route.apply(ak_array)[..., None],
                            route.apply(other)[..., None],
                        ), axis=-1),
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
                    raise Exception(f"cannot update already existing array column '{route}'")
            else:
                # the route is new, so add it and manually tell the cache
                ak_array = set_ak_column(ak_array, route, route.apply(other))
                _has_column_cache[route] = True

    return ak_array


def flatten_ak_array(
    ak_array: ak.Array,
    routes: Optional[Union[Sequence, set, Callable[[str], bool]]] = None,
) -> OrderedDict:
    """
    Flattens a nested awkward array *ak_array* into a dictionary that maps joined column names to
    single awkward arrays. The returned dictionary might be used in conjuction with ``ak.Array`` to
    create a single array again.

    :py:func:`get_ak_routes` is used internally to determine the nested structure of the array. The
    name of flat columns in the returned dictionary follows the standard dot format. The columns to
    save can be defined via *routes* which can be a sequence or set of column names or a function
    receiving a column name and returning a bool.
    """
    # use an ordered mapping to somewhat preserve row adjacency
    flat_array = OrderedDict()

    # helper to evaluate whether to keep a column
    keep_route = lambda column: True
    if isinstance(routes, (list, tuple, set)):
        keep_route = lambda column: column in routes
    elif callable(routes):
        keep_route = routes

    # go through routes, create new names and store arrays
    for route in get_ak_routes(ak_array):
        if keep_route(route.column):
            flat_array[route.column] = route.apply(ak_array)

    return flat_array


def sort_ak_fields(
    ak_array: ak.Array,
    sort_fn: Optional[Callable[[str], int]] = None,
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
    ak.to_parquet(sort_ak_fields(ak_array), *args, **kwargs)


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

    def __init__(self, keep_routes):
        super().__init__()

        self.keep_routes = list(keep_routes)
        self.remove_routes = None

    def __call__(self, ak_array):
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
                self[my_func](events)

                events["Jet", "pt4"] = events.Jet.pt2 ** 2

        # call my_other_func on a chunk of events
        inst = my_other_func()
        inst(events)

    ArrayFunction's declare dependence between one another through class-level sets *uses* and
    *produces*. This allows for the construction of an internal callstack. Once an ArrayFunction is
    instantiated, all dependent objects in this callstack are instantiated as well and stored
    internally *once per class*. This is strictly required as ArrayFunctions, and most likely their
    subclasses, can have a state (a set of instance-level members that are allowed to differ between
    instances). The instance of a dependency can be accessed via item syntax (``self[my_func]``
    above).

    .. note::

        The above example uses explicit subclassing, but most certainly this might never be used in
        practice. Instead, please consider using a decorator to wrap the main callable as done by
        the :py:class:`Calibrator`, :py:class:`Selector` and :py:class:`Producer` interfaces.

    *call_func* defines the function being invoked when the instance is *called*. *uses* and
    *produces* should be strings denoting a column in dot format or a :py:class:`Route` instance,
    other :py:class:`ArrayFunction` instances, or a sequence or set of the two. On instance-level,
    the full sets of :py:attr:`used_columns` and :py:attr:`produced_columns` are simply resolvable
    through attributes.

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

       The callstack of dependencies, i.e., a dictionary mapping depenent classes to their instances
       as to be used by *this* instance. Item access on this instance is forwarded to this object.

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
    """

    # class-level attributes as defaults
    call_func = None
    uses = set()
    produces = set()
    dependency_sets = {"uses", "produces"}

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

    def __init__(
        self,
        call_func: Optional[Callable] = None,
        deferred_init: Optional[bool] = True,
        instance_cache: Optional[dict] = None,
        **kwargs,
    ):
        super().__init__()

        # when a custom call_func is passed, bind it to this instance
        if call_func:
            self.call_func = call_func.__get__(self, self.__class__)

        # create instance-level sets of dependent ArrayFunction classes,
        # optionally extended by sets passed in keyword arguments
        for attr in self.dependency_sets:
            deps = getattr(self.__class__, attr) | set(law.util.make_list(kwargs.get(attr) or []))
            setattr(self, attr, deps)

        # dictionary of dependency class to instance, set in create_dependencies
        self.deps = DotDict()

        # deferred part of the initialization
        if deferred_init:
            self.deferred_init(instance_cache or {})

    def __getitem__(self, dep_cls: DerivableMeta) -> "ArrayFunction":
        """
        Item access to dependencies.
        """
        return self.deps[dep_cls]

    def deferred_init(self, instance_cache: dict) -> None:
        """
        Controls the deferred part of the initialization process.
        """
        self.create_dependencies(instance_cache)

    def create_dependencies(
        self,
        instance_cache: dict,
    ) -> None:
        # create instance-level sets to store instances of dependent ArrayFunction classes
        # that are defined in class-level sets
        def instantiate(cls):
            if cls not in instance_cache:
                # create the instance first without its deps, then cache it and
                # finally create its deps
                inst = self.instantiate_dependency(cls, deferred_init=False)
                instance_cache[cls] = inst
                inst.deferred_init(instance_cache)
            return instance_cache[cls]

        def add_dep(inst):
            self.deps[inst.__class__] = inst
            return inst

        for attr in self.dependency_sets:
            # get the current set of instances
            deps = set()

            # go through all dependent objects and create instances of classes, considering caching
            for obj in getattr(self, attr):
                if ArrayFunction.derived_by(obj):
                    obj = add_dep(instantiate(obj))
                elif isinstance(obj, ArrayFunction):
                    add_dep(obj)
                elif isinstance(obj, self.Flagged):
                    if ArrayFunction.derived_by(obj.wrapped):
                        obj = self.Flagged(instantiate(obj.wrapped), obj.io_flag)
                    add_dep(obj.wrapped)
                else:
                    obj = copy.deepcopy(obj)
                deps.add(obj)

            # save the updated set of dependencies
            setattr(self, f"{attr}_instances", deps)

    def instantiate_dependency(self, cls: DerivableMeta, **kwargs: Any) -> "ArrayFunction":
        """
        Controls the instantiation of a dependency given by its *cls* and arbitrary *kwargs*.
        """
        return cls(**kwargs)

    def get_dependencies(self, include_others: bool = False) -> Set[Union["ArrayFunction", Any]]:
        """
        Returns a set of instances of all dependencies. When *include_others* is *True*, also
        non-ArrayFunction types are returned.
        """
        deps = set()

        for attr in self.dependency_sets:
            for obj in getattr(self, f"{attr}_instances"):
                if isinstance(obj, ArrayFunction):
                    deps.add(obj)
                elif isinstance(obj, self.Flagged):
                    deps.add(obj.wrapped)
                elif include_others:
                    deps.add(obj)

        return deps

    def _get_columns(self, io_flag: IOFlag, call_cache: Optional[set] = None) -> Set[str]:
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

    def _get_used_columns(self, call_cache: Optional[set] = None) -> Set[str]:
        return self._get_columns(io_flag=self.IOFlag.USES, call_cache=call_cache)

    @property
    def used_columns(self) -> Set[str]:
        return self._get_used_columns()

    def _get_produced_columns(self, call_cache: Optional[set] = None) -> Set[str]:
        return self._get_columns(io_flag=self.IOFlag.PRODUCES, call_cache=call_cache)

    @property
    def produced_columns(self) -> Set[str]:
        return self._get_produced_columns()

    def __call__(self, *args, **kwargs):
        """
        Forwards the call to :py:attr:`call_func` with all *args* and *kwargs*. An exception is
        raised if :py:attr:`call_func` is not callable.
        """
        if not callable(self.call_func):
            raise Exception(f"call_func of {self} is not callable")

        return self.call_func(*args, **kwargs)


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
    interface to tasks and can influence their behavior - and vice-versa. For this purpose, an
    initialization function can be wrapped through a decorator (similiar to ``property`` setters) as
    shown in the example below. Custom task requirements, and a setup of objects resulting from
    these requirements can be defined in a similar, programmatic way. Exmple:

    .. code-block:: python

        class my_func(ArrayFunction):
            uses = {"Jet.pt"}
            produces = {"Jet.pt_weighted"}

            def call_func(self, events):
                # self.weights is defined below
                events["Jet", "pt_weighted"] = events.Jet.pt * self.weights

        # define config-dependent updates (e.g. extending shifts, or used and produced columns)
        @my_func.init
        def update(self):
            self.shifts |= {"some_shift_up", "some_shift_down"}

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

    .. py:attribute: init_func
       type: callable

       The registered function defining what to update, or *None*.

    .. py:attribute: requires_func
       type: callable

       The registered function defining requirements, or *None*.

    .. py:attribute:: setup_func
       type: callable

       The registered function performing the custom setup step, or *None*.

    .. py:attribute:: call_force
       type: None, bool

       When a bool, this flag decides whether calls of this instance are cached. However, note that
       when the *call_force* flag passed to :py:meth:`__call__` is specified, it has precedence over
       this attribute.
    """

    # class-level attributes as defaults
    init_func = None
    requires_func = None
    setup_func = None
    shifts = set()
    dependency_sets = ArrayFunction.dependency_sets | {"shifts"}

    @classmethod
    def init(cls, func: Callable[[], None]) -> None:
        """
        Decorator to wrap a function *func* that should be registered as :py:meth:`init_func`
        which is used to initialize *this* instance dependent on specific task attributes. The
        function should not accept positional arguments.

        The decorator does not return the wrapped function.

        .. note::

            When the task invoking the requirement is workflow, be aware that both the actual
            workflow instance as well as branch tasks might call the wrapped function. When the
            requirements should differ between them, make sure to use the
            :py:meth:`BaseWorkflow.is_workflow` and :py:meth:`BaseWorkflow.is_branch` methods to
            distinguish the cases.
        """
        cls.init_func = func

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
        which is used to perform a custom setup of objects. The function is invoked while bound to
        this instance should accept three arguments,

            - *task*, the :py:class:`law.Task` instance,
            - *inputs*, a dictionary with input targets corresponding to the requirements created by
              :py:meth:`run_requires`, and
            - *call_kwargs*, a dictionary into which arguments should be inserted that are later
              on passed to the wrapped function.

        The decorator does not return the wrapped function.
        """
        cls.setup_func = func

    def __init__(
        self,
        *args,
        init_func: Optional[Callable] = None,
        requires_func: Optional[Callable] = None,
        setup_func: Optional[Callable] = None,
        call_force: Optional[bool] = None,
        inst_dict: Optional[dict] = None,
        **kwargs,
    ):
        # store the inst dict with arbitrary attributes that are forwarded to dependency creation
        self.inst_dict = dict(inst_dict or {})

        super().__init__(*args, **kwargs)

        # when custom funcs are passed, bind them to this instance
        if init_func:
            self.init_func = init_func.__get__(self, self.__class__)
        if requires_func:
            self.requires_func = requires_func.__get__(self, self.__class__)
        if setup_func:
            self.setup_func = setup_func.__get__(self, self.__class__)

        # other attributes
        self.call_force = call_force

    def __getattr__(self, attr: str) -> Any:
        """
        Attribute access to objects named *attr* in the :py:attr:`inst_dict`.
        """
        if attr in self.inst_dict:
            return self.inst_dict[attr]

        raise AttributeError(f"'{self.__class__.__name__}' object has no attribute '{attr}'")

    def deferred_init(
        self,
        instance_cache: dict,
    ) -> None:
        """
        Controls the deferred part of the initialization process, first calling this instances
        :py:meth:`init_func` and then setting up dependencies.
        """
        # run this instance's init function which might update dependent classes
        if callable(self.init_func):
            self.init_func()

        # call super, which instantiates the dependencies
        super().deferred_init(instance_cache)

    def instantiate_dependency(self, cls: DerivableMeta, **kwargs: Any) -> "TaskArrayFunction":
        """
        Controls the instantiation of a dependency given by its *cls* and arbitrary *kwargs*,
        updated by *this* instances :py:attr:`inst_dict`.
        """
        # add inst_dict when cls is a TaskArrayFunction itself
        if TaskArrayFunction.derived_by(cls):
            kwargs.setdefault("inst_dict", self.inst_dict)

        return super().instantiate_dependency(cls, **kwargs)

    def _get_all_shifts(self, call_cache: Optional[set] = None) -> Set[str]:
        shifts = set()

        # init the call cache
        if call_cache is None:
            call_cache = set()

        # consider _this_ call cached
        call_cache.add(self)

        # add shifts of all dependent objects
        for obj in self.get_dependencies(include_others=True):
            if isinstance(obj, TaskArrayFunction):
                if obj not in call_cache:
                    call_cache.add(obj)
                    shifts |= obj._get_all_shifts(call_cache=call_cache)
            elif isinstance(obj, str):
                shifts.add(obj)

        return shifts

    @property
    def all_shifts(self) -> Set[str]:
        return self._get_all_shifts()

    def run_requires(
        self,
        reqs: Optional[dict] = None,
        call_cache: Optional[set] = None,
    ) -> dict:
        """
        Recursively runs the :py:meth:`requires_func` of this instance and all dependencies. *reqs*
        defaults to an empty dictionary which should be filled to store the requirements.
        """
        # default requirements
        if reqs is None:
            reqs = {}

        # init the call cache
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
        inputs: dict,
        call_cache: Optional[set] = None,
    ) -> None:
        """
        Recursively runs the :py:meth:`setup_func` of this instance and all dependencies. *inputs*
        corresponds to the requirements created by :py:func:`run_requires`.
        """
        # init the call cache
        if call_cache is None:
            call_cache = set()

        # run this instance's setup function
        if callable(self.setup_func):
            self.setup_func(inputs)

        # run the setup of all dependent objects
        for dep in self.get_dependencies():
            if dep not in call_cache:
                call_cache.add(dep)
                dep.run_setup(inputs, call_cache=call_cache)

    def __call__(
        self,
        *args,
        call_cache: Optional[Union[bool, defaultdict]] = None,
        call_force: Optional[bool] = None,
        **kwargs,
    ) -> Any:
        """
        Calls the wrapped :py:meth:`call_func` with all *args* and *kwargs*. The latter is updated
        with :py:attr:`call_kwargs` when set, but giving priority to existing *kwargs*.

        Also, all calls are cached unless *call_cache* is *False*. In case caching is active and
        this instance was called before, it is not called again but *None* is returned. This check
        is bypassed when either *call_force* is *True*, or when it is *None* and the
        :py:attr:`call_force` attribute of this instance is *True*.
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
                return

            # increase the count and set kwargs for the call downstream
            call_cache[self] += 1

        # stack all kwargs
        kwargs = {"call_cache": call_cache, **kwargs}

        return super().__call__(*args, **kwargs)


class PreloadedNanoEventsFactory(coffea.nanoevents.NanoEventsFactory or object):
    """
    Custom NanoEventsFactory that re-implements the ``events()`` method that immediately loads
    an event chunk into memory.
    """

    def events(self):
        """
        Builds and returns the eager (non-lazy) awkward array describing the event content of the
        wrapped mapping.
        """
        events = self._events()
        if events is None:
            behavior = dict(self._schema.behavior)
            behavior["__events_factory__"] = self
            key_format = partial(coffea.nanoevents.factory._key_formatter, self._partition_key)
            events = ak.from_buffers(
                self._schema.form,
                len(self),
                self._mapping,
                key_format=key_format,
                lazy=False,
                lazy_cache="new" if self._cache is None else self._cache,
                behavior=behavior,
            )
            self._events = weakref.ref(events)
        return events


class ChunkedReader(object):
    """
    Allows reading one or multiple files and iterating through chunks of their content with
    multi-threaded read operations. Chunks and their positions (denoted by start and stop markers,
    and the index of the chunk itself) are accessed by iterating through the reader.

    The content to load is configurable through *source*, which can be a file path or an opened file
    object, and a *source_type*, which defines how the *source* should be opened and traversed for
    chunking. See the classmethods ``open_...`` and ``read_...`` below for implementation details
    and :py:meth:`get_source_handlers` for a list of currently supported sources.

    Example:

    .. code-block:: python

        # iterate through a single file
        # (creating the reader and iterating through it in the same line)
        for chunk, position in ChunkedReader("data.root", source_type="coffea_root"):
            # chunk is a NanoAODEventsArray as returned by read_coffea_root
            jet_pts = chunk.Jet.pt
            print(f"jet pts of chunk {chunk.index}: {jet_pts}")

    .. code-block:: python

        # iterate through multiple files simultaneously
        # (also, now creating the reader first and then iterating through it)
        with ChunkedReader(
            ("data.root", "masks.parquet"),
            source_type=("coffea_root", "awkward_parquet"),
        ) as reader:
            for (chunk, masks), position in reader:
                # chunk is a NanoAODEventsArray as returned by read_coffea_root
                # masks is a awkward array as returned by read_awkward_parquet
                selected_jet_pts = chunk[masks].Jet.pt
                print(f"selected jet pts of chunk {chunk.index}: {selected_jet_pts}")

    The maximum size of the chunks and the number of threads to load them can be configured through
    *chunk_size* and *pool_size*. Unless *lazy* is *True*, chunks are fully loaded into memory
    before they are yielded to be used in the main thread. In addition, *open_options* and
    *read_options* are forwarded to internal open and read implementations to further control and
    optimize IO.

    If *source* refers to a single object, *source_type*, *lazy*, *open_options* and *read_options*
    should be single values as well. Otherwise, if *source* is a sequence of sources, the other
    arguments can be sequences as well with the same length.

    During iteration, before chunks are yielded, an optional message *iter_message* is printed when
    set, receiving the chunk position as the field *pos* for formatting.
    """

    # chunk position container
    ChunkPosition = namedtuple("ChunkPosition", ["index", "entry_start", "entry_stop"])

    # read result containers
    ReadResult = namedtuple("ReadResult", ["chunk", "chunk_pos"])

    def __init__(
        self,
        source: Any,
        source_type: Optional[Union[str, List[str]]] = None,
        chunk_size: int = 50000,
        pool_size: int = 4,
        lazy: Union[bool, List[bool]] = False,
        open_options: Optional[Union[dict, List[dict]]] = None,
        read_options: Optional[Union[dict, List[dict]]] = None,
        iter_message: str = "handling chunk {pos.index}",
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
        lazy = _check_arg("lazy", lazy)

        # store input attributes
        self.source_list = list(source) if self.is_multi else [source]
        self.source_type_list = list(source_type) if self.is_multi else [source_type]
        self.open_options_list = list(open_options) if self.is_multi else [open_options]
        self.read_options_list = list(read_options) if self.is_multi else [read_options]
        self.lazy_list = list(lazy) if self.is_multi else [lazy]
        self.chunk_size = chunk_size
        self.pool_size = pool_size
        self.iter_message = iter_message

        # attributes that are set in open(), close() or __iter__()
        self.file_cache = []
        self.source_objects = []
        self.n_entries = None
        self.tasks = []
        self.pool = None

        # determine type, open and read functions per source
        self.source_handlers = [
            self.get_source_handlers(source_type, source)
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
        entry_start = chunk_index * chunk_size
        entry_stop = min((chunk_index + 1) * chunk_size, n_entries)

        return cls.ChunkPosition(chunk_index, entry_start, entry_stop)

    @classmethod
    def open_awkward_parquet(
        cls,
        source: str,
        open_options: Optional[dict] = None,
        file_cache: Optional[list] = None,
    ) -> Tuple[ak.Array, int]:
        """
        Opens a parquet file saved at *source*, loads the content as an awkward array and returns a
        2-tuple *(array, length)*. *open_options* are forwarded to ``awkward.from_parquet``. Passing
        *file_cache* has no effect.
        """
        if not isinstance(source, str):
            raise Exception(f"'{source}' cannot be opend awkward_parquet")

        # prepare open options
        open_options = open_options or {}
        open_options.setdefault("lazy", True)

        # load the array
        arr = ak.from_parquet(source, **open_options)

        return (arr, len(arr))

    @classmethod
    def open_uproot_root(
        cls,
        source: Union[
            str,
            uproot.ReadOnlyDirectory,
            Tuple[str, str],
            Tuple[uproot.ReadOnlyDirectory, str],
        ],
        open_options: Optional[dict] = None,
        file_cache: Optional[list] = None,
    ) -> Tuple[uproot.TTree, int]:
        """
        Opens an uproot tree from a root file at *source* and returns a 2-tuple *(tree, entries)*.
        *source* can be the path of the file, an already opened, readable uproot file (assuming the
        tree is called "Events"), or a 2-tuple whose second item defines the name of the tree to be
        loaded. When a new file is opened, it receives *open_options* and is put into the
        *file_cache* when set.
        """
        tree_name = "Events"
        if isinstance(source, tuple) and len(source) == 2:
            source, tree_name = source
        if isinstance(source, str):
            source = uproot.open(source, **(open_options or {}))
            if isinstance(file_cache, list):
                file_cache.append(source)
            tree = source[tree_name]
        elif isinstance(source, uproot.ReadOnlyDirectory):
            tree = source[tree_name]
        else:
            raise Exception(f"'{source}' cannot be opend as uproot_root")

        return (tree, tree.num_entries)

    @classmethod
    def open_coffea_root(
        cls,
        source: Union[
            str,
            uproot.ReadOnlyDirectory,
            Tuple[str, str],
            Tuple[uproot.ReadOnlyDirectory, str],
        ],
        open_options: Optional[dict] = None,
        file_cache: Optional[list] = None,
    ) -> Tuple[uproot.ReadOnlyDirectory, int]:
        """
        Opens an uproot file at *source* for subsequent processing with coffea and returns a 2-tuple
        *(uproot file, tree entries)*. *source* can be the path of the file, an already opened,
        readable uproot file (assuming the tree is called "Events"), or a 2-tuple whose second item
        defines the name of the tree to be loaded. *open_options* are forwarded to ``uproot.open``
        if a new file is opened, which is then put into the *file_cache* when set.
        """
        tree_name = "Events"
        if isinstance(source, tuple) and len(source) == 2:
            source, tree_name = source
        if isinstance(source, str):
            source = uproot.open(source, **(open_options or {}))
            if isinstance(file_cache, list):
                file_cache.append(source)
            tree = source[tree_name]
        elif isinstance(source, uproot.ReadOnlyDirectory):
            tree = source[tree_name]
        else:
            raise Exception(f"'{source}' cannot be opend as coffea_root")

        return (source, tree.num_entries)

    @classmethod
    def open_coffea_parquet(
        cls,
        source: str,
        open_options: Optional[dict] = None,
        file_cache: Optional[list] = None,
    ) -> Tuple[str, int]:
        """
        Given a parquet file located at *source*, returns a 2-tuple *(source, entries)*. Passing
        *open_options* and or *file_cache* has no effect.
        """
        return (source, pq.ParquetFile(source).metadata.num_rows)

    @classmethod
    def read_awkward_parquet(
        cls,
        source_object: ak.Array,
        chunk_pos: ChunkPosition,
        lazy: bool = False,
        read_options: Optional[dict] = None,
    ):
        """
        Given an awkward array *source_object*, returns the chunk referred to by *chunk_pos* either
        as a lazy slice if *lazy* is *True*, or as a full copy loaded into memory otherwise. Passing
        *read_options* has no effect.
        """
        chunk = source_object[chunk_pos.entry_start:chunk_pos.entry_stop]
        return chunk if lazy else ak.copy(chunk)

    @classmethod
    def read_uproot_root(
        cls,
        source_object: uproot.TTree,
        chunk_pos: ChunkPosition,
        lazy: bool = False,
        read_options: Optional[dict] = None,
    ) -> ak.Array:
        """
        Given an uproot TTree *source_object*, returns an awkward array chunk referred to by
        *chunk_pos* as a lazy slice if *lazy* is *True*, or as a full copy loaded into memory
        otherwise. *read_options* are passed to either ``uproot.TTree.arrays`` or ``uproot.lazy``.
        """
        if lazy:
            view = uproot.lazy(source_object, **(read_options or {}))
            chunk = view[chunk_pos.entry_start:chunk_pos.entry_stop]
        else:
            chunk = source_object.arrays(
                entry_start=chunk_pos.entry_start,
                entry_stop=chunk_pos.entry_stop,
                **(read_options or {}),
            )

        return chunk

    @classmethod
    def read_coffea_root(
        cls,
        source_object: Union[str, uproot.ReadOnlyDirectory],
        chunk_pos: ChunkPosition,
        lazy: bool = False,
        read_options: Optional[dict] = None,
    ) -> coffea.nanoevents.methods.base.NanoEventsArray:
        """
        Given a file location or opened uproot file *source_object*, returns an awkward array chunk
        referred to by *chunk_pos*, assuming nanoAOD structure. The array is a lazy slice if *lazy*
        is *True*, and fully loaded into memory otherwise. *read_options* are passed to
        ``coffea.nanoevents.NanoEventsFactory.from_root``.
        """
        # define the factory to use
        factory = coffea.nanoevents.NanoEventsFactory if lazy else PreloadedNanoEventsFactory

        # read the events chunk into memory
        chunk = factory.from_root(
            source_object,
            entry_start=chunk_pos.entry_start,
            entry_stop=chunk_pos.entry_stop,
            schemaclass=coffea.nanoevents.NanoAODSchema,
            **(read_options or {}),
        ).events()

        return chunk

    @classmethod
    def read_coffea_parquet(
        cls,
        source_object: str,
        chunk_pos: ChunkPosition,
        lazy: bool = False,
        read_options: Optional[dict] = None,
    ) -> coffea.nanoevents.methods.base.NanoEventsArray:
        """
        Given a the location of a parquet file *source_object*, returns an awkward array chunk
        referred to by *chunk_pos*, assuming nanoAOD structure. The array is a lazy slice if *lazy*
        is *True*, and fully loaded into memory otherwise. *read_options* are passed to
        *coffea.nanoevents.NanoEventsFactory.from_parquet*.
        """
        # define the factory to use
        factory = coffea.nanoevents.NanoEventsFactory if lazy else PreloadedNanoEventsFactory

        # read the events chunk into memory
        chunk = factory.from_parquet(
            source_object,
            entry_start=chunk_pos.entry_start,
            entry_stop=chunk_pos.entry_stop,
            schemaclass=coffea.nanoevents.NanoAODSchema,
            **(read_options or {}),
        ).events()

        return chunk

    @classmethod
    def get_source_handlers(
        cls,
        source_type: Optional[str],
        source: Optional[Any],
    ) -> Tuple[str, Callable, Callable]:
        """
        Takes a *source_type* (see list below) and gathers information about how to open and read
        content from a specific source. A 3-tuple *(source type, open function, read function)* is
        returned.

        When *source_type* is *None* but an arbitrary *source* is set, the type is derived from that
        object, and an exception is raised in case no type can be inferred.

        Currently supported source types are:

            - "awkward_parquet"
            - "uproot_root"
            - "coffea_root"
            - "coffea_parquet"
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

        if source_type == "awkward_parquet":
            return (source_type, cls.open_awkward_parquet, cls.read_awkward_parquet)
        if source_type == "uproot_root":
            return (source_type, cls.open_uproot_root, cls.read_uproot_root)
        if source_type == "coffea_root":
            return (source_type, cls.open_coffea_root, cls.read_coffea_root)
        if source_type == "coffea_parquet":
            return (source_type, cls.open_coffea_parquet, cls.read_coffea_parquet)

        raise NotImplementedError(f"unknown source_type '{source_type}'")

    @property
    def n_chunks(self) -> int:
        """
        Returns the number of chunks this reader will iterate over based on the number of entries
        :py:attr:`n_entries` and the configured :py:attr:`chunk_size`. In case :py:attr:`n_entries`
        was not initialzed yet (via :py:meth:`open`), an *AttributeError* is raised.
        """
        if self.n_entries is None:
            raise AttributeError("cannot determine number of chunks before open()")
        return int(math.ceil(self.n_entries / self.chunk_size))

    @property
    def closed(self) -> bool:
        """
        Returns whether the reader is closed.
        """
        return len(self.source_objects) != self.n_sources

    def open(self) -> None:
        """
        Opens all previously registered sources and preloads all source objects to read content from
        later on. Nothing happens if the reader is already opened (i.e. not :py:attr:`closed`).
        """
        if not self.closed:
            # already open
            return

        # reset some attributes
        del self.file_cache[:]
        del self.source_objects[:]
        self.n_entries = None

        # open all sources and make sure they have the same number of entries
        for i, (source, (_, open_source, _), open_options) in enumerate(zip(
            self.source_list,
            self.source_handlers,
            self.open_options_list,
        )):
            # open the source
            obj, n = open_source(source, open_options=open_options, file_cache=self.file_cache)
            # check entries
            if i == 0:
                self.n_entries = n
            elif n != self.n_entries:
                raise ValueError(
                    f"number of entries of source {i} '{source}' does not match first source",
                )
            # save the source object
            self.source_objects.append(obj)

    def close(self) -> None:
        """
        Closes all cached, opened files and deletes loaded source objects. Nothing happens if the
        reader is already :py:attr:`closed`.
        """
        if self.closed:
            # already closed
            return

        # just delete the file cache and reset some attributes
        del self.source_objects[:]
        del self.file_cache[:]

    def add_task(
        self,
        func: Callable,
        args: tuple = (),
        kwargs: Optional[dict] = None,
        where: int = 0,
    ) -> None:
        """
        Adds a callable *func* that will be executed with *args* and *kwargs* into the list of tasks
        to process in multiple threads at the position denoted by *where*. The processing commences
        upon iteration through this reader instance.
        """
        self.tasks.insert(where, (func, args, kwargs or {}))

    def _iter_impl(self):
        """
        Internal iterator implementation. Please refer to :py:meth:`__iter__` and the usual iterator
        interface.
        """
        if self.closed:
            raise Exception("cannot iterate trough closed reader")

        # create a list of read functions
        read_funcs = [
            partial(read_source, obj, lazy=lazy, read_options=read_options)
            for obj, (_, _, read_source), read_options, lazy in zip(
                self.source_objects,
                self.source_handlers,
                self.read_options_list,
                self.lazy_list,
            )
        ]

        # lightweight callabe that wraps all read funcs and comines their return values
        def read(chunk_pos):
            chunks = [read_func(chunk_pos) for read_func in read_funcs]
            return self.ReadResult((chunks if self.is_multi else chunks[0]), chunk_pos)

        # create a list of all chunk positions
        chunk_positions = [
            self.create_chunk_position(self.n_entries, self.chunk_size, chunk_index)
            for chunk_index in range(self.n_chunks)
        ]

        # fill the list of tasks the pool has to work through
        self.tasks.extend((read, (chunk_pos,)) for chunk_pos in chunk_positions)

        # strategy: setup the pool and manually keep it filled up to pool_size and do not insert all
        # chunks right away as this could swamp the memory if processing is slower than IO
        with multiprocessing.pool.ThreadPool(self.pool_size) as self.pool:
            results = []
            no_result = object()

            try:
                while self.tasks or results:
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
                        time.sleep(0.02)
                        continue

                    # immediately try to fill up the pool
                    while len(results) < self.pool_size and self.tasks:
                        results.append(self.pool.apply_async(*self.tasks.pop(0)))

                    # if a result was ready and it returned a ReadResult, yield it
                    if isinstance(result_obj, self.ReadResult):
                        if self.iter_message:
                            print(self.iter_message.format(pos=result_obj.chunk_pos))
                        yield (result_obj.chunk, result_obj.chunk_pos)

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
