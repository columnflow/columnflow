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
import threading
import subprocess
import multiprocessing
import multiprocessing.pool
from typing import Tuple, Callable, Any, Optional, Union
from types import ModuleType

import law

try:
    import coffea.nanoevents
    import coffea.nanoevents.methods.base
    NanoEventsArray = coffea.nanoevents.methods.base.NanoEventsArray
    HAS_COFFEA = True
except ImportError:
    HAS_COFFEA = False
    NanoEventsArray = None

try:
    import uproot
    TTree = uproot.TTree
    ReadOnlyDirectory = uproot.ReadOnlyDirectory
    HAS_UPROOT = True
except ImportError:
    HAS_UPROOT = False
    TTree = None
    ReadOnlyDirectory = None


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


class PreloadedChunk(dict):
    """
    Class that is compatible to coffea's interface for preloaded array sources. See
    https://coffeateam.github.io/coffea/api/coffea.nanoevents.NanoEventsFactory.html#coffea.nanoevents.NanoEventsFactory.from_preloaded
    for more info.

    A chunk can be created from an uproot *tree* that is preloaded between *entry_start* and
    *entry_stop* via *tree.arrays()*, receiving all additional *kwargs*.
    """

    def __init__(
            self,
            tree: TTree,
            entry_start: Optional[int] = None,
            entry_stop: Optional[int] = None,
            **kwargs):
        # normalize edges
        n = tree.num_entries
        entry_start = 0 if not entry_start else max(entry_start, 0)
        entry_stop = n if not entry_stop else min(max(entry_stop, entry_start), n)

        # preload arrays
        chunk = tree.arrays(entry_start=entry_start, entry_stop=entry_stop, how=dict, **kwargs)

        # init the dict
        super(PreloadedChunk, self).__init__(chunk)

        # save attributes
        self._tree = tree
        self._entry_start = entry_start
        self._entry_stop = entry_stop

    @property
    def metadata(self) -> dict:
        return {
            "num_rows": self._entry_stop - self._entry_start,
            "uuid": self._tree.file.uuid,
            "object_path": self._tree.object_path,
        }


def process_nano_events(
        uproot_input: Union[ReadOnlyDirectory, TTree],
        pool_size: int = 4,
        chunk_size: int = 40000,
        **kwargs) -> None:
    """
    Generator to loop through chunks of an *uproot_input* (either an opened file or a tree instance)
    in a multi-threaded fashion using a pool of size *pool_size*. As soon as a chunk of size
    *chunk_size* is read from disk, the generator yields a 4-tuple of (chunk index, NanoEventsArray,
    start entry, stop entry). The array is already fully pre-loaded into memory. All additional
    *kwargs* are forwarded to :py:class:`PreloadedChunk` which is used for the preloading.

    Example:

    .. code-block:: python

        uproot_file = uproot.open("ZMuMu.root")
        expressions = ["run", "luminosityBlock", "event", "nJet", "Jet_pt"]
        for i, events, *_ in process_nano_events(uproot_file, expressions=expressions):
            print(i, events.Jet.pt)
    """
    assert HAS_UPROOT
    assert HAS_COFFEA

    # get the events tree
    tree = uproot_input if isinstance(uproot_input, TTree) else uproot_input["Events"]
    n_entries = tree.num_entries
    n_chunks = int(math.ceil(n_entries / chunk_size))

    def read(chunk_idx):
        # determine the start of stop of this chunk
        entry_start = chunk_idx * chunk_size
        entry_stop = min((chunk_idx + 1) * chunk_size, n_entries)

        # preload array chunk
        chunk = PreloadedChunk(tree, entry_start, entry_stop, **kwargs)

        # wrap by nano factory
        events = coffea.nanoevents.NanoEventsFactory.from_preloaded(chunk,
            schemaclass=coffea.nanoevents.NanoAODSchema).events()

        return (chunk_idx, events, entry_start, entry_stop)

    # setup the pool
    with multiprocessing.pool.ThreadPool(pool_size) as pool:
        # fill it one by one up to pool_size as we should not use map or apply_async on all chunks
        # right away as this would fill up the entire memory in case processing is slower than IO
        chunks = list(range(n_chunks))
        results = []

        while chunks or results:
            # first, fill up results
            while len(results) < pool_size and chunks:
                results.append(pool.apply_async(read, (chunks.pop(0),)))

            # then, query results with a fast polling and try to clear them
            remove_indices = []
            for i, result in enumerate(list(results)):
                if not result.ready():
                    continue

                # the result is ready, try to yield its payload
                try:
                    yield result.get()
                except:
                    pool.close()
                    pool.terminate()
                    raise

                # mark the result as "to be removed"
                remove_indices.append(i)

            # remove results, or sleep when none was ready
            if remove_indices:
                results = [result for i, result in enumerate(results) if i not in remove_indices]
            else:
                time.sleep(0.05)
