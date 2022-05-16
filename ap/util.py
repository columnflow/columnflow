# coding: utf-8

"""
Collection of general helpers and utilities.
"""

__all__ = [
    "DotDict", "MockModule",
    "maybe_import", "import_plt", "import_ROOT", "create_random_name", "expand_path", "real_path",
    "wget", "call_thread", "call_proc", "ensure_proxy",
]


import os
import uuid
import queue
import threading
import subprocess
import importlib
import multiprocessing
import multiprocessing.pool
from typing import Tuple, Callable, Any, Optional, Union
from types import ModuleType

import law


class DotDict(dict):
    """
    Subclass of *dict* that provides read access for items via attributes by implementing
    ``__getattr__``. In case a item is accessed via attribute and it does not exist, an
    *AttriuteError* is raised rather than a *KeyError*. Example:

    .. code-block:: python

       d = DotDict()
       d["foo"] = 1

       print(d["foo"])
       # => 1

       print(d.foo)
       # => 1

       print(d["bar"])
       # => KeyError

       print(d.bar)
       # => AttributeError
    """

    def __getattr__(self, attr):
        try:
            return self[attr]
        except KeyError:
            raise AttributeError("'{}' object has no attribute '{}'".format(
                self.__class__.__name__, attr))

    def copy(self):
        """"""
        return self.__class__(self)


class MockModule(object):
    """
    Mockup object that resembles a module with arbitrarily deep structure such that, e.g.,

    .. code-block:: python

        coffea = MockModule("coffea")
        print(coffea.nanoevents.NanoEventsArray)
        # -> "<MockupModule 'coffea' at 0x981jald1>"

    will always succeed at declaration, but most likely fail at execution time. In fact, each
    attribute access will return the mock object again. This might only be useful in places where
    a module is potentially not existing (e.g. due to sandboxing) but one wants to import it either
    way a) to perform only one top-level import as opposed to imports in all functions of a package,
    or b) to provide type hints for documentation purposes.

    .. py:attribute:: _name
       type: str

       The name of the mock module.
    """

    def __init__(self, name):
        super(MockModule, self).__init__()

        self._name = name

    def __getattr__(self, attr):
        return self

    def __repr__(self):
        return f"<{self.__class__.__name__} '{self._name}' at {hex(id(self))}>"

    def __call__(self, *args, **kwargs):
        raise Exception(f"{self._name} is a mock module and cannot be called")

    def __nonzero__(self):
        return False

    def __bool__(self):
        return False


def maybe_import(name: str, package: Optional[str] = None) -> Union[ModuleType, MockModule]:
    """
    Calls *importlib.import_module* internally and returns the module if it exists, or otherwise a
    :py:class:`MockModule` instance with the same name.
    """
    try:
        return importlib.import_module(name, package)
    except ImportError:
        if package:
            name = package + name
        return MockModule(name)


_plt = None


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


_ROOT = None


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
