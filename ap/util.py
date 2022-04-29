# coding: utf-8

"""
Helpers and utilities.
"""

__all__ = []


import os
import math
import time
import uuid
import queue
import weakref
import threading
import subprocess
import multiprocessing
import multiprocessing.pool
from functools import partial
from collections import namedtuple
from typing import Tuple, Callable, Any, Optional
from types import ModuleType

import law

try:
    import awkward as ak
    HAS_AWKWARD = True
except ImportError:
    HAS_AWKWARD = False

try:
    import uproot
    ReadOnlyDirectory = uproot.ReadOnlyDirectory
    HAS_UPROOT = True
except ImportError:
    HAS_UPROOT = False
    ReadOnlyDirectory = None

try:
    import coffea.nanoevents
    import coffea.nanoevents.methods.base
    NanoEventsArray = coffea.nanoevents.methods.base.NanoEventsArray
    HAS_COFFEA = True
except ImportError:
    HAS_COFFEA = False
    NanoEventsArray = None


# modules and objects from lazy imports
_plt = None
_ROOT = None


def import_plt() -> ModuleType:
    """
    Lazily imports and configures matplotlib pyplot.
    """
    global _plt

    if not _plt:
        import matplotlib

        matplotlib.use("Agg")
        matplotlib.rc("text", usetex=True)
        matplotlib.rcParams["text.latex.preamble"] = [r"\usepackage{amsmath}"]
        matplotlib.rcParams["legend.edgecolor"] = "white"
        import matplotlib.pyplot as plt

        _plt = plt

    return _plt


def import_ROOT() -> ModuleType:
    """
    Lazily imports and configures ROOT.
    """
    global _ROOT

    if not _ROOT:
        import ROOT

        ROOT.PyConfig.IgnoreCommandLineOptions = True
        ROOT.gROOT.SetBatch()

        _ROOT = ROOT

    return _ROOT


def create_random_name() -> str:
    """
    Returns a random string based on UUID v4.
    """
    return str(uuid.uuid4())


def expand_path(*path: str) -> str:
    """
    Takes *path* fragments, joins them and recursively expands all contained environment variables.
    """
    path = os.path.join(*map(str, path))
    while "$" in path or "~" in path:
        path = os.path.expandvars(os.path.expanduser(path))

    return path


def real_path(*path: str) -> str:
    """
    Takes *path* fragments and returns the joined,  real and absolute location with all variables
    expanded.
    """
    return os.path.realpath(expand_path(*path))


def wget(src: str, dst: str, force: bool = False) -> str:
    """
    Downloads a file from a remote *src* to a local destination *dst*, creating intermediate
    directories when needed. When *dst* refers to an existing file, an exception is raised unless
    *force* is *True*.

    The full, normalized destination path is returned.
    """
    # check if the target directory exists
    dst = real_path(dst)
    if os.path.isdir(dst):
        dst = os.path.join(dst, os.path.basename(src))
    else:
        dst_dir = os.path.dirname(dst)
        if not os.path.exists(dst_dir):
            raise IOError(f"target directory '{dst_dir}' does not exist")

    # remove existing dst or complain
    if os.path.exists(dst):
        if force:
            os.remove(dst)
        else:
            raise IOError(f"target '{dst}' already exists")

    # actual download
    cmd = ["wget", src, "-O", dst]
    code, _, error = law.util.interruptable_popen(law.util.quote_cmd(cmd), shell=True,
        executable="/bin/bash", stderr=subprocess.PIPE)
    if code != 0:
        raise Exception(f"wget failed: {error}")

    return dst


def call_thread(func: Callable, args: tuple = (), kwargs: Optional[dict] = None,
        timeout: Optional[float] = None) -> Tuple[bool, Any, Optional[str]]:
    """
    Execute a function *func* in a thread and aborts the call when *timeout* is reached. *args* and
    *kwargs* are forwarded to the function.

    The return value is a 3-tuple ``(finsihed_in_time, func(), err)``.
    """
    def wrapper(q, *args, **kwargs):
        try:
            ret = (func(*args, **kwargs), None)
        except Exception as e:
            ret = (None, str(e))
        q.put(ret)

    q = queue.Queue(1)

    thread = threading.Thread(target=wrapper, args=(q,) + args, kwargs=kwargs or {})
    thread.start()
    thread.join(timeout)

    if thread.is_alive():
        return (False, None, None)
    else:
        return (True,) + q.get()


def call_proc(func: Callable, args: tuple = (), kwargs: Optional[dict] = None,
        timeout: Optional[float] = None) -> Tuple[bool, Any, Optional[str]]:
    """
    Execute a function *func* in a process and aborts the call when *timeout* is reached. *args* and
    *kwargs* are forwarded to the function.

    The return value is a 3-tuple ``(finsihed_in_time, func(), err)``.
    """
    def wrapper(q, *args, **kwargs):
        try:
            ret = (func(*args, **kwargs), None)
        except Exception as e:
            ret = (None, str(e))
        q.put(ret)

    q = multiprocessing.Queue(1)

    proc = multiprocessing.Process(target=wrapper, args=(q,) + args, kwargs=kwargs or {})
    proc.start()
    proc.join(timeout)

    if proc.is_alive():
        proc.terminate()
        return (False, None, None)
    else:
        return (True,) + q.get()


@law.decorator.factory(accept_generator=True)
def ensure_proxy(fn: Callable, opts: dict, task: law.Task, *args, **kwargs):
    """
    Law task decorator that checks whether either a voms or arc proxy is existing before calling
    the decorated method.
    """
    def before_call():
        # do nothing for grid jobs
        if os.getenv("AP_ON_GRID") == "1":
            return None

        # check the proxy validity
        if not law.wlcg.check_voms_proxy_validity() and not law.arc.check_arc_proxy_validity():
            raise Exception("neither voms nor arc proxy valid")

    def call(state):
        return fn(task, *args, **kwargs)

    def after_call(state):
        return

    return before_call, call, after_call


def process_nano_events(
    uproot_file: ReadOnlyDirectory,
    chunk_size: int = 40000,
    pool_size: int = 4,
    pool_insert: bool = False,
    **kwargs,
) -> None:
    """
    Generator that loops through an *uproot_file* and yields chunks of events of size *chunk_size*
    in a multi-threaded fashion using a pool of size *pool_size*.

    As soon as a chunk is read, a 2-tuple (chunk position, NanoEventsArray) is yielded, with the
    latter being already fully pre-loaded into memory. A chunk position is a named tuple with fields
    *index*, *entry_start* and *entry_stop*.

    When *pool_insert* is *True*, the yielded tuple will have a third item, which is a function
    (accepting a callable as well as arguments and keyword arguments) that allows to insert new
    tasks into the pool.

    All additional *kwargs* are forwarded to *NanoEventsFactory.from_root* as *iteritems_options*.

    Example:

    .. code-block:: python

        uproot_file = uproot.open("ZMuMu.root")

        # loop through certain branches
        branches = ["run", "luminosityBlock", "event", "nJet", "Jet_pt"]
        for pos, events in process_nano_events(uproot_file, filter_name=branches):
            print(pos.index, events.Jet.pt)

        # insert new tasks to the same pool used internally for multi-threading
        for pos, events, pool_insert in process_nano_events(uproot_file, pool_insert=True):
            print(pos.index, events.Jet.pt)

            pool_insert((lambda events: ak.to_parquet(events, "/some/path")), (events,))
    """
    assert HAS_AWKWARD and HAS_UPROOT and HAS_COFFEA

    class PreloadedNanoEventsFactory(coffea.nanoevents.NanoEventsFactory):
        """
        Custom NanoEventsFactory that re-implements the ``events()`` method that immediately loads
        an event chunk into memory.
        """

        def events(self):
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

    # get the events tree and some stats
    tree = uproot_file["Events"]
    n_entries = tree.num_entries
    n_chunks = int(math.ceil(n_entries / chunk_size))

    # container describing the chunk position
    ChunkPosition = namedtuple("ChunkPosition", ["index", "entry_start", "entry_stop"])

    # container describing the return value of read operations below
    ReadResult = namedtuple("ReadResult", ["chunk_pos", "events"])

    # the read operation that is executed per thread, parameterized by the chunk index
    def read(chunk_idx):
        # determine the start of stop of this chunk
        entry_start = chunk_idx * chunk_size
        entry_stop = min((chunk_idx + 1) * chunk_size, n_entries)

        # read the events chunk into memory
        events = PreloadedNanoEventsFactory.from_root(
            uproot_file,
            entry_start=entry_start,
            entry_stop=entry_stop,
            schemaclass=coffea.nanoevents.NanoAODSchema,
            iteritems_options=kwargs,
        ).events()

        return ReadResult(ChunkPosition(chunk_idx, entry_start, entry_stop), events)

    # setup the pool with the strategy to manually keep it filled up to pool_size and do not use
    # insert all chunks right away as this could swamp the memory if processing is slower than IO
    with multiprocessing.pool.ThreadPool(pool_size) as pool:
        tasks = [(read, (chunk_idx,)) for chunk_idx in range(n_chunks)]
        results = []
        no_result = object()

        # insert function that is yielded when pool_insert is set
        def _pool_insert(func, args=(), kwargs=None, where=0):
            tasks.insert(where, (func, args, kwargs or {}))

        while tasks or results:
            # find the first done result and remove it from the list
            # this will do nothing in the first iteration
            result_obj = no_result
            for i, result in enumerate(list(results)):
                if not result.ready():
                    continue

                try:
                    result_obj = result.get()
                except:
                    pool.close()
                    pool.terminate()
                    raise

                results.pop(i)
                break

            # if no result was ready, sleep and try again
            if results and result_obj == no_result:
                time.sleep(0.02)
                continue

            # immediately try to fill up the pool
            while len(results) < pool_size and tasks:
                results.append(pool.apply_async(*tasks.pop(0)))

            # if a result was ready and it returned a ReadResult, yield it, otherwise sleep
            if isinstance(result_obj, ReadResult):
                try:
                    if pool_insert:
                        yield (result_obj.chunk_pos, result_obj.events, _pool_insert)
                    else:
                        yield (result_obj.chunk_pos, result_obj.events)
                except:
                    pool.close()
                    pool.terminate()
                    raise
