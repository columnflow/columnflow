# coding: utf-8

"""
Helpers and utilities for working with columnar libraries.
"""

__all__ = [
    "mandatory_coffea_columns",
    "ArrayFunction", "ChunkedReader", "PreloadedNanoEventsFactory",
    "get_ak_routes", "find_nano_route", "remove_nano_column", "add_nano_alias", "add_nano_aliases",
    "update_ak_array", "sort_nano_fields", "sorted_ak_to_parquet", "flatten_ak_array",
]


import math
import time
import weakref
import multiprocessing
import multiprocessing.pool
from functools import partial
from collections import namedtuple, OrderedDict
from typing import Optional, Union, Sequence, Set, Tuple, List, Dict, Callable, Any

import law

from ap.util import maybe_import

ak = maybe_import("awkward")
uproot = maybe_import("uproot")
coffea = maybe_import("coffea")
maybe_import("coffea.nanoevents")
maybe_import("coffea.nanoevents.methods.base")
pq = maybe_import("pyarrow.parquet")


#: columns that are always required when opening a nano file with coffea
mandatory_coffea_columns = {"run", "luminosityBlock", "event"}


def get_ak_routes(
    ak_array: ak.Array,
    max_depth: int = 0,
) -> List[Tuple[str]]:
    """
    Extracts the list of all routes pointing to columns of a potentially deeply nested akward array
    *ak_array* and returns it. Example:

    .. code-block:: python

        # let arr be a nested array (e.g. from opening nano files via coffea)

        print(get_ak_routes(arr))
        # [
        #    ("event",),
        #    ("luminosityBlock,"),
        #    ("run",),
        #    ("Jet", "pt"),
        #    ("Jet", "mass"),
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
        arr, route = lookup.pop(0)
        if arr.fields and (max_depth <= 0 or len(route) < max_depth):
            # extend the lookup with nested fields
            lookup.extend([
                (getattr(arr, field), route + (field,))
                for field in arr.fields
            ])
        else:
            # no sub fields found or positive max_depth reached, store the route
            # but check negative max_depth first
            if max_depth < 0:
                route = route[:max_depth]
            # add when not empty and unique
            if route and route not in routes:
                routes.append(route)

    return routes


def find_nano_route(
    nano_array: coffea.nanoevents.methods.base.NanoEventsArray,
    nano_column: str,
) -> tuple:
    """
    For a given name of a column in NanoAOD underscore format *nano_column* (e.g. "Jet_pt"), returns
    a tuple containing field names that can be used to access the column in a coffea NanoEventsArray
    *nano_array* (e.g. ``("Jet", "pt")``). Example:

    .. code-block:: python

        route = find_nano_route(events, "Jet_pt_jec_up")
        print(route)  # -> ("Jet", "pt_jec_up")

        print(events[route])  # -> <Array [[25.123, ...], ...] type=`50000 * var * float32`>
    """
    # split into parts
    parts = nano_column.split("_")

    # traverse parts and check if there is a deeper substructure
    route = []
    obj = nano_array
    for i, part in enumerate(parts):
        if part in obj.fields:
            # substructure found, try the next part
            route.append(part)
            obj = getattr(obj, part)
        else:
            # final object, store or attach to the last route
            rest = "_".join(parts[i:])
            if route:
                route[-1] += f"_{rest}"
            else:
                route.append(rest)
            break

    return tuple(route)


def remove_nano_column(
    nano_array: coffea.nanoevents.methods.base.NanoEventsArray,
    nano_column: Union[str, Tuple[str]],
) -> coffea.nanoevents.methods.base.NanoEventsArray:
    """
    Removes a column either in NanoAOD underscore format *nano_column* (e.g. "Jet_pt") or given as a
    route (e.g. *("Jet", "pt")*) from a coffea NanoEventsArray *nano_array* and returns a new view.
    """
    # find the route
    if isinstance(nano_column, str):
        route = find_nano_route(nano_array, nano_column)
    elif isinstance(nano_column, tuple):
        route = nano_column
    else:
        raise ValueError(f"cannot interpret '{nano_column}' as nano column")

    if len(route) == 1:
        # trivial case: remove a top level column
        nano_array = nano_array[[f for f in nano_array.fields if f != route[0]]]
    else:
        # nested case: given the route ("a", "b", "c"), set nano_array["a", "b"] to a view of "b"
        # with all fields but "c" using __setitem__ syntax (and in particular not __setattr__!)
        sub_route, remove_field = route[:-1], route[-1]
        sub_array = nano_array[sub_route]
        # determine remaining fields
        remaining_fields = [f for f in sub_array.fields if f != remove_field]
        # if no fields are left, remove the entire sub_view
        if not remaining_fields:
            return remove_nano_column(nano_array, route[:-1])
        # set the reduced view
        nano_array.__setitem__(sub_route, sub_array[remaining_fields])

    return nano_array


def add_nano_alias(
    nano_array: coffea.nanoevents.methods.base.NanoEventsArray,
    src_column: str,
    dst_column: str,
    remove_src: bool = False,
) -> coffea.nanoevents.methods.base.NanoEventsArray:
    """
    Adds an alias to a coffea NanoEventsArray *nano_array*, pointing the array at *src_column* to
    *dst_column*, which both column names being in NanoAOD underscore format (e.g. "Jet_pt").
    Existing columns referred to by *dst_column* might be overwritten. When *remove_src* is *True*,
    a view of the input array is returned with *src_column* missing. Otherwise, the input array is
    returned with all columns.
    """
    # find source and destination routes for simple access
    src_route = find_nano_route(nano_array, src_column)
    dst_route = find_nano_route(nano_array, dst_column)

    # add the alias, potentially overwriting existing columns
    nano_array[dst_route] = nano_array[src_route]

    # create a view without the source if requested
    if remove_src:
        nano_array = remove_nano_column(nano_array, src_route)

    return nano_array


def add_nano_aliases(
    nano_array: coffea.nanoevents.methods.base.NanoEventsArray,
    aliases: Dict[str, str],
    remove_src: bool = False,
) -> coffea.nanoevents.methods.base.NanoEventsArray:
    """
    Adds multiple *aliases*, given in a dictionary mapping destination columns to source columns, to
    a coffea NanoEventsArray *nano_array*. See :py:func:`add_nano_aliases` for more info. When
    *remove_src* is *True*, a view of the input array is returned with all source columns missing.
    Otherwise, the input array is returned with all columns.
    """
    # add all aliases
    for dst_column, src_column in aliases.items():
        nano_array = add_nano_alias(
            nano_array,
            src_column,
            dst_column,
            remove_src=remove_src,
        )

    return nano_array


def update_ak_array(
    ak_array: ak.Array,
    *others: ak.Array,
) -> ak.Array:
    """
    Updates an awkward array *ak_array* in-place with the content of multiple different arrays
    *others*. Internally, :py:func:`get_ak_routes` is used to obtain the list of all routes pointing
    to potentially deeply nested arrays. The input array is also returned.
    """
    for other in others:
        for route in get_ak_routes(other):
            ak_array[route] = other[route]

    return ak_array


def sort_nano_fields(
    ak_array: ak.Array,
    sort_fn: Optional[Callable] = None,
) -> ak.Array:
    """
    Recursively sorts all fields of an awkward array *ak_array* and returns a new view. When a
    *sort_fn* is set, it is used internally for sorting field names.`
    """
    # sort the top level fields
    ak_array = ak_array[sorted(ak_array.fields, key=sort_fn)]

    # identify fields with nested structure, then sort and reassign them
    nested_fields = [field for field in ak_array.fields if ak_array[field].fields]
    for field in nested_fields:
        ak_array[field] = sort_nano_fields(ak_array[field], sort_fn=sort_fn)

    return ak_array


def sorted_ak_to_parquet(
    ak_array: ak.Array,
    *args,
    **kwargs,
) -> None:
    """
    Sorts the fields in an awkward array *ak_array* resurvively with :py:func:`sort_nano_fields` and
    saves it as a parquet file using ``awkward.to_parquet`` which receives all additional *args* and
    *kwargs*.

    .. note::

        Since the order of fields in awkward arrays resulting from reading nano files might be
        arbitrary (depending on streamer info in the original root files), but formats such as
        parquet highly depend on the order for building internal table schemes, one should always
        make use of this function! Otherwise, operations like file merging might fail due to
        differently ordered schemas.
    """
    ak.to_parquet(sort_nano_fields(ak_array), *args, **kwargs)


def flatten_ak_array(
    ak_array: ak.Array,
    name_fn: Optional[Callable] = None,
    columns: Optional[Union[list, tuple, set, Callable]] = None,
) -> OrderedDict:
    """
    Flattens a nested awkward array *ak_array* into a dictionary that maps joined column names to
    single awkward arrays. The returned dictionary might be used in conjuction with ``ak.Array`` to
    create a single array again.

    :py:func:`get_ak_routes` is used internally to determine the nested structure of the array. The
        name of flat columns in the returned dictionary is build via *name_fn*. When not set, nested
        routes are joined via underscore. The columns to save can be defined via *columns* which can
        be a sequence of column names or a function receiving a column name and returning a bool.
    """
    # use an ordered mapping to somewhat preserve row adjacency
    flat_array = OrderedDict()

    # default name_fn
    if not name_fn:
        name_fn = lambda route: "_".join(route)

    # helper to evaluate whether to keep a column
    keep_column = lambda name: True
    if isinstance(columns, (list, tuple, set)):
        keep_column = lambda name: name in columns
    elif callable(columns):
        keep_column = columns

    # go through routes, create new names and store arrays
    for route in get_ak_routes(ak_array):
        column_name = name_fn(route)
        if keep_column(column_name):
            flat_array[column_name] = ak_array[route]

    return flat_array


class ArrayFunction(object):
    """
    Base class for function wrappers that act on arrays and keep track of certain columns. Also,
    instances are named and put into a class-level cache when created via :py:meth:`new`
    (recommended).

    For possible implementations, see :py:class:`calibration.Calibrator` or
    :py:class:`selection.Selector`.

    .. py:attribute:: func
       type: callable

       The wrapped function.

    .. py:attribute:: name
       type: str

       The name of the instance in the cache dictionary.

    .. py:attribute:: uses
       type: set

       The set of column names or other instances to recursively resolve the names of columns.

    .. py::attribute:: used_columns
       type: set
       read-only

       The resolved, flat set of used column names.
    """

    _instances = {}

    @classmethod
    def new(cls, *args, **kwargs) -> "ArrayFunction":
        """
        Creates a new instance with all *args* and *kwargs*, adds it to the instance cache using its
        name attribute, and returns it. An exception is raised if an instance with the same name was
        already reigstered.
        """
        inst = cls(*args, **kwargs)

        if inst.name in cls._instances:
            raise ValueError(f"{cls.__name__} named '{inst.name}' was already registered")

        cls._instances[inst.name] = inst

        return inst

    @classmethod
    def get(cls, name: str) -> "ArrayFunction":
        """
        Returns a previously registered instance named *name* from the cache.
        """
        if name not in cls._instances:
            raise ValueError(f"no {cls.__name__} named '{name}' registered")

        return cls._instances[name]

    @classmethod
    def create_subclass(cls, name: str, attributes: Optional[dict] = None) -> "ArrayFunction":
        """
        Creates a new class named *name* inheriting from *this* class. *attributes* are used to add
        custom class-level members to the newly generated class. By default, the new subclass has a
        separate instance cache dictionary.

        In general, it would be trivial to create a subclass the usual way, but since the base
        :py:class:`ArrayFunction` and its features might be used *as is* with a different type and
        instance cache, this method could be useful. Example:

        .. code-block:: python

            Categorizer = ArrayFunction.create_subclass("Categorizer")
        """
        if not attributes:
            attributes = {}

        # enforce a separate instance cache attribute
        attributes["_instances"] = {}

        # create and return the subclass
        return type(name, (cls,), attributes)

    def __init__(
        self,
        func: Callable,
        name: Optional[str] = None,
        uses: Optional[Union[
            str, "ArrayFunction", Sequence[str], Sequence["ArrayFunction"], Set[str],
            Set["ArrayFunction"],
        ]] = None,
    ):
        super().__init__()

        self.func = func
        self.name = name or self.func.__name__
        self.uses = law.util.make_list(uses) if uses else []

    @property
    def used_columns(self) -> Set[str]:
        columns = set()
        for obj in self.uses:
            if isinstance(obj, ArrayFunction):
                columns |= obj.used_columns
            else:
                columns.add(obj)
        return columns

    def __call__(self, *args, **kwargs):
        """
        Invokes the wrapped :py:attr:`func`, forwarding all *args* and *kwargs*.
        """
        return self.func(*args, **kwargs)


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
                behavior=behavior,
            )
            self._events = weakref.ref(events)
        return events


class ChunkedReader(object):
    """
    Allows reading one or multiple files and iterating through chunks of their content with
    multi-threaded read operations. Chunks and their positions (denoted by start and stop markers,
    and the index of the chunk itself) are accessed by iterating through the reader.

    The content to load can be configured through *source*, which can be a file path or an opened
    file object, and a *source_type*, which defines how the *source* should be opened and traversed
    for chunking. See the classmethods ``open_...`` and ``read_...`` below for implementation
    details and :py:meth:`get_source_handlers` for a list of currently supported sources.

    Example:

    .. code-block:: python

        # iterate through a single file
        with ChunkedReader("data.root", source_type="coffea_root") as reader:
            for chunk, position in reader:
                # chunk is a NanoAODEventsArray as returned by read_coffea_root
                jet_pts = chunk.Jet.pt
                print(f"jet pts of chunk {chunk.index}: {jet_pts}")

    .. code-block:: python

        # iterate through multiple files simultaneously
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
        # make sure all sources are opened
        self.open()

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
