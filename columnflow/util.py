# coding: utf-8

"""
Collection of general helpers and utilities.
"""

from __future__ import annotations

__all__ = [
    "UNSET", "env_is_remote", "env_is_dev", "primes",
    "maybe_import", "import_plt", "import_ROOT", "import_file", "create_random_name", "expand_path",
    "real_path", "ensure_dir", "wget", "call_thread", "call_proc", "ensure_proxy", "dev_sandbox",
    "safe_div", "test_float", "is_pattern", "is_regex", "pattern_matcher",
    "dict_add_strict", "get_source_code",
    "DotDict", "MockModule", "FunctionArgs", "ClassPropertyDescriptor", "classproperty",
    "DerivableMeta", "Derivable",
]

import os
import uuid
import queue
import threading
import subprocess
import importlib
import fnmatch
import re
import inspect
import multiprocessing
import multiprocessing.pool
from functools import wraps
from collections import OrderedDict
from typing import Callable, Any, Sequence, Union
from types import ModuleType

import law


#: Placeholder for an unset value.
UNSET = object()

#: Boolean denoting whether the environment is in a remote job (based on ``CF_REMOTE_JOB``).
env_is_remote = law.util.flag_to_bool(os.getenv("CF_REMOTE_JOB", "0"))

#: Boolean denoting whether the environment is in a remote job on the WLCG (based on ``CF_ON_GRID``).
env_is_grid = law.util.flag_to_bool(os.getenv("CF_ON_GRID", "0"))

#: Boolean denoting whether the environment is used for development (based on ``CF_DEV``).
env_is_dev = not env_is_remote and law.util.flag_to_bool(os.getenv("CF_DEV", "0"))

#: List of the first 100 primes.
primes = [
    2, 3, 5, 7, 11, 13, 17, 19, 23, 29, 31, 37, 41, 43, 47, 53, 59, 61, 67, 71, 73, 79, 83, 89, 97,
    101, 103, 107, 109, 113, 127, 131, 137, 139, 149, 151, 157, 163, 167, 173, 179, 181, 191, 193,
    197, 199, 211, 223, 227, 229, 233, 239, 241, 251, 257, 263, 269, 271, 277, 281, 283, 293, 307,
    311, 313, 317, 331, 337, 347, 349, 353, 359, 367, 373, 379, 383, 389, 397, 401, 409, 419, 421,
    431, 433, 439, 443, 449, 457, 461, 463, 467, 479, 487, 491, 499, 503, 509, 521, 523, 541,
]


def maybe_import(
    name: str,
    package: str | None = None,
    force: bool = False,
) -> ModuleType | MockModule:
    """
    Calls *importlib.import_module* internally and returns the module if it exists, or otherwise a
    :py:class:`MockModule` instance with the same name. When *force* is *True* and the import fails,
    an *ImportError* is raised.
    """
    try:
        return importlib.import_module(name, package)
    except ImportError as e:
        # raise in case force is set, or an other package than the requested one was not found
        m = re.match(r"^No\smodule\snamed\s\'(.+)\'$", str(e))
        if force or not m or not name.startswith(m.group(1)):
            raise
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


def import_file(path: str, attr: str | None = None):
    """
    Loads the content of a python file located at *path* and returns its package content as a
    dictionary. When *attr* is set, only the attribute with that name is returned.

    The file is not required to be importable as its content is loaded directly into the
    interpreter. While this approach is not necessarily clean, it can be useful in places where
    custom code must be loaded.
    """
    # load the package contents (do not try this at home)
    path = expand_path(path)
    pkg = DotDict()
    with open(path, "r") as f:
        exec(f.read(), pkg)

    # extract a particular attribute
    if attr:
        if attr not in pkg:
            raise AttributeError(f"no local member '{attr}' found in file {path}")
        return pkg[attr]

    return pkg


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


def ensure_dir(path: str) -> str:
    """
    Ensures that a directory at *path* (and its subdirectories) exists and returns the full,
    expanded path.
    """
    path = real_path(path)
    if not os.path.exists(path):
        os.makedirs(path)
    return path


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


def call_thread(
    func: Callable,
    args: tuple = (),
    kwargs: dict | None = None,
    timeout: float | None = None,
) -> tuple[bool, Any, str | None]:
    """
    Execute a function *func* in a thread and aborts the call when *timeout* is reached. *args* and
    *kwargs* are forwarded to the function.

    The return value is a 3-tuple (finsihed_in_time, func(), err).
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


def call_proc(
    func: Callable,
    args: tuple = (),
    kwargs: dict | None = None,
    timeout: float | None = None,
) -> tuple[bool, Any, str | None]:
    """
    Execute a function *func* in a process and aborts the call when *timeout* is reached. *args* and
    *kwargs* are forwarded to the function.

    The return value is a 3-tuple (finsihed_in_time, func(), err).
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
def ensure_proxy(
    fn: Callable,
    opts: dict,
    task: law.Task,
    *args,
    **kwargs,
) -> tuple[Callable, Callable, Callable]:
    """
    Law task decorator that checks whether either a voms or arc proxy is existing before calling
    the decorated method.
    """
    def before_call():
        # do nothing in remote jobs
        if env_is_remote:
            return None

        # do nothing when explicitly skipped by the law config
        if law.config.get_expanded_boolean("analysis", "skip_ensure_proxy", default=False):
            return None

        # check the proxy validity
        if not law.wlcg.check_voms_proxy_validity() and not law.arc.check_arc_proxy_validity():
            raise Exception("neither voms nor arc proxy valid")

    def call(state):
        return fn(task, *args, **kwargs)

    def after_call(state):
        return

    return before_call, call, after_call


def dev_sandbox(sandbox: str) -> str:
    """
    Takes a sandbox key *sandbox* and injects the subtring "_dev" right before the file extension
    (if any) in case the current environment is used for development (see :py:attr:`env_is_dev`) and
    the corresponding sandbox exists. Otherwise *sandbox* is returned unchanged.

    .. code-block:: python

        # if env_is_dev and /path/to/script_dev.sh exists
        dev_sandbox("bash::/path/to/script.sh")
        # -> "bash::/path/to/script_dev.sh"

        # otherwise
        dev_sandbox("bash::/path/to/script.sh")
        # -> "bash::/path/to/script.sh"
    """
    # do nothing when not in dev env
    if not env_is_dev:
        return sandbox

    # only take into account venv and bash sandboxes
    _type, path = law.Sandbox.split_key(sandbox)
    if _type not in ["venv", "bash"]:
        return sandbox

    # create the dev path and check if it exists, with special treatment of the cf_{prod,dev} envs
    if path == "$CF_BASE/sandboxes/cf_prod.sh":
        dev_path = "$CF_BASE/sandboxes/cf_dev.sh"
    else:
        dev_path = "{}_dev{}".format(*os.path.splitext(path))
    if not os.path.exists(real_path(dev_path)):
        return sandbox

    # all checks passed
    return law.Sandbox.join_key(_type, dev_path)


def freeze(cont: Any) -> Any:
    """Constructs an immutable version of a native Python container.

    Recursively replaces all mutable containers (``dict``, ``list``, ``set``) encountered within
    *cont* by an immutable equivalent: Lists are converted to tuples, sets to ``frozenset``
    objects, and dictionaries to tuples of (*key*, *value*) pairs.
    """

    if isinstance(cont, dict):
        return tuple((k, freeze(v)) for k, v in cont.items())
    elif isinstance(cont, (list, tuple)):
        return tuple(freeze(v) for v in cont)
    elif isinstance(cont, set):
        return frozenset(freeze(v) for v in cont)
    return cont


def memoize(f: Callable) -> Callable:
    """
    Function decorator that implements memoization. Function results are cached on
    first call and returned from cache on every subsequent call with the same arguments.
    """
    _cache = {}

    @wraps(f)
    def wrapper(*args, **kwargs):
        frozen_args = freeze(dict(args=args, kwargs=kwargs))
        if frozen_args not in _cache:
            _cache[frozen_args] = f(*args, **kwargs)
        return _cache[frozen_args]

    return wrapper


def safe_div(a: int | float, b: int | float) -> float:
    """
    Returns *a* divided by *b* if *b* is not zero, and zero otherwise.
    """
    return (a / b) if b else 0.0


def test_float(f: Any) -> bool:
    """
    Tests whether a value *i* can be converted to a float.
    """
    try:
        float(f)
        return True
    except (ValueError, TypeError):
        return False


def is_pattern(s: str) -> bool:
    """
    Returns *True* if a string *s* contains pattern characters such as "*" or "?", and *False*
    otherwise.
    """
    return "*" in s or "?" in s


def is_regex(s: str) -> bool:
    """
    Returns *True* if a string *s* is a regular expression starting with "^" and ending with "$",
    and *False* otherwise.
    """
    return s.startswith("^") and s.endswith("$")


def pattern_matcher(pattern: Sequence[str] | str, mode: Callable = any) -> Callable[[str], bool]:
    r"""
    Takes a string *pattern* which might be an actual pattern for fnmatching, a regular expressions
    or just a plain string and returns a function that can be used to test of a string matches that
    pattern.

    When *pattern* is a sequence, all its patterns are compared the same way and the result is the
    combination given a *mode* which typically should be *any* or *all*.

    Example:

    .. code-block:: python

        matcher = pattern_matcher("foo*")
        matcher("foo123")  # -> True
        matcher("bar123")  # -> False

        matcher = pattern_matcher(r"^foo\d+.*$")
        matcher("foox")  # -> False
        matcher("foo1")  # -> True

        matcher = pattern_matcher(("foo*", "*bar"), mode=any)
        matcher("foo123")  # -> True
        matcher("123bar")  # -> True

        matcher = pattern_matcher(("foo*", "*bar"), mode=all)
        matcher("foo123")     # -> False
        matcher("123bar")     # -> False
        matcher("foo123bar")  # -> True
    """
    if isinstance(pattern, (list, tuple, set)):
        matchers = [pattern_matcher(p) for p in pattern]
        return lambda s: mode(matcher(s) for matcher in matchers)

    # special cases
    if pattern in ["*", "^.*$"]:
        return lambda s: True

    # identify fnmatch patterns
    if is_pattern(pattern):
        return lambda s: fnmatch.fnmatch(s, pattern)

    # identify regular expressions
    if is_regex(pattern):
        cre = re.compile(pattern)
        return lambda s: cre.match(s) is not None

    # fallback to string comparison
    return lambda s: s == pattern


def dict_add_strict(d: dict, key: str, value: Any) -> None:
    """
    Adds key-value pair to dictionary, but only if it does not change an existing value;
    Raises KeyError otherwise.
    """
    if key in d.keys() and d[key] != value:
        raise KeyError(f"'{d.__class__.__name__}' object already has key {key}")
    d[key] = value


def get_source_code(obj: Any, indent: str | int = None) -> str:
    """
    Returns the source code of any object *obj* as a string. When *indent* is not *None*, the code
    indentation is first removed and then re-applied with *indent* if it is a string, or by that
    many spaces in case it is an integer.
    """
    code = inspect.getsource(obj)

    if indent is not None:
        code = code.replace("\t", "    ")
        lines = code.split("\n")
        n_old_indent = len(lines[0]) - len(lines[0].lstrip(" "))
        new_indent = (" " * indent) if isinstance(indent, int) else indent
        code = "\n".join(
            (new_indent + line[n_old_indent:]) if line.strip() else ""
            for line in lines
        )

    return code


class DotDict(OrderedDict):
    """
    Subclass of *OrderedDict* that provides read and write access to items via attributes by
    implementing ``__getattr__`` and ``__setattr__``. In case a item is accessed via attribute and
    it does not exist, an *AttriuteError* is raised rather than a *KeyError*. Example:

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

        d.bar = 123
        print(d.bar)
        # => 123

        # use wrap() to convert a nested dict
        d = DotDict({"foo": {"bar": 1}})
        print(d.foo.bar)
        # => 1
    """

    def __getattr__(self, attr: str) -> Any:
        try:
            return self[attr]
        except KeyError:
            raise AttributeError(f"'{self.__class__.__name__}' object has no attribute '{attr}'")

    def __setattr__(self, attr: str, value: Any) -> None:
        self[attr] = value

    def copy(self) -> DotDict:
        """"""
        return self.__class__(self)

    @classmethod
    def wrap(cls, *args, **kwargs) -> DotDict:
        """
        Takes a dictionary *d* and recursively replaces it and all other nested dictionary types
        with :py:class:`DotDict`'s for deep attribute-style access.
        """
        wrap = lambda d: cls((k, wrap(v)) for k, v in d.items()) if isinstance(d, dict) else d
        return wrap(OrderedDict(*args, **kwargs))


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

    def __init__(self, name: str):
        super().__init__()

        self._name = name

    def __getattr__(self, attr: str) -> MockModule:
        return self

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__} '{self._name}' at {hex(id(self))}>"

    def __call__(self, *args, **kwargs) -> None:
        raise Exception(f"{self._name} is a mock module and cannot be called")

    def __nonzero__(self) -> bool:
        return False

    def __bool__(self) -> bool:
        return False

    def __or__(self, other) -> Any:
        # forward union type hints
        return Union[type(self), other]


class FunctionArgs(object):
    """
    Leight-weight utility class that wraps all passed *args* and *kwargs* and allows to invoke
    different functions with them.
    """

    def __init__(self, *args, **kwargs):
        super().__init__()

        # store attributes
        self.args = args
        self.kwargs = kwargs

    def __call__(self, func: Callable) -> Any:
        return func(*self.args, **self.kwargs)


class ClassPropertyDescriptor(object):
    """
    Generic descriptor class that is used by :py:func:`classproperty`.
    """

    def __init__(self, fget: Callable, fset: Callable | None = None):
        super().__init__()

        self.fget = fget
        self.fset = fset

    def __get__(self, obj: type, cls: type | None = None) -> Any:
        if cls is None:
            cls = type(obj)

        return self.fget.__get__(obj, cls)()

    def __set__(self, obj: type, value: Any) -> None:
        # fset must exist
        if not self.fset:
            raise AttributeError("can't set attribute")

        return self.fset.__get__(obj, type(obj))(value)


def classproperty(func: Callable) -> ClassPropertyDescriptor:
    """
    Propety decorator for class-level methods.
    """
    if not isinstance(func, (classmethod, staticmethod)):
        func = classmethod(func)

    return ClassPropertyDescriptor(func)


class DerivableMeta(type):
    """
    Meta class for :py:class:`Derivable` objects providing class-level features such as improved
    tracing and lookup of subclasses, and single-line subclassing for partial-like overwriting of
    class-level attributes.
    """

    _classes = {}

    def __new__(
        metacls,
        cls_name: str,
        bases: tuple,
        cls_dict: dict,
    ) -> DerivableMeta:
        """
        Class creation.
        """
        # default attributes, irrespective of inheritance
        cls_dict.setdefault("_subclasses", {})

        # create the class
        cls = super().__new__(metacls, cls_name, bases, cls_dict)

        # save the class in the meta class dict
        metacls._classes[cls_name] = cls

        # save the class in the class cache of all subclassable base classes
        for base in bases:
            if isinstance(base, metacls):
                # TODO: is it safe to overwrite here? probably yes
                base._subclasses[cls_name] = cls

        return cls

    def has_cls(
        cls,
        cls_name: str,
        deep: bool = True,
    ) -> bool:
        """
        Returns *True* if this class has a subclass named *cls_name* and *False* otherwise. When
        *deep* is *True*, the lookup is recursive through all levels of subclasses.
        """
        return cls.get_cls(cls_name, deep=deep, silent=True) is not None

    def get_cls(
        cls,
        cls_name: str,
        deep: bool = True,
        silent: bool = False,
    ) -> DerivableMeta | None:
        """
        Returns a previously created subclass named *cls_name*. When *deep* is *True*, the lookup is
        recursive through all levels of subclasses. When no such subclass was found an exception is
        raised, unless *silent* is *True* in which case *None* is returned.
        """
        if not deep:
            if cls_name not in cls._subclasses:
                if silent:
                    return None
                raise ValueError(f"no subclass named '{cls_name}' found")

        # recursive implementation
        lookup = [cls._subclasses]
        while lookup:
            subclasses = lookup.pop(0)
            if cls_name in subclasses:
                return subclasses[cls_name]
            lookup.extend(
                subcls._subclasses
                for subcls in subclasses.values()
                if issubclass(subcls, cls)
            )

        # lookup empty
        if silent:
            return None
        raise ValueError(f"no subclass named '{cls_name}' found")

    def derive(
        cls,
        cls_name: str,
        bases: tuple = (),
        cls_dict: dict | None = None,
        module: str | None = None,
    ) -> DerivableMeta:
        """
        Creates a subclass named *cls_name* inheriting from *this* class an additional, optional
        *bases*. *cls_dict* will be attached as class-level attributes.
        """
        # prepare bases
        bases = tuple(bases) if isinstance(bases, (list, tuple)) else (bases,)

        # create the subclass
        subcls = cls.__class__(cls_name, (cls,) + bases, cls_dict or {})

        # overwrite __module__ to point to the module of the calling stack
        if not module:
            frame = inspect.stack()[1]
            module = inspect.getmodule(frame[0])
        if module:
            subcls.__module__ = module.__name__

        return subcls

    def derived_by(cls, other: DerivableMeta) -> bool:
        """
        Returns if a class *other* is either *this* or derived from *this* class, and *False*
        otherwise.
        """
        return isinstance(other, DerivableMeta) and issubclass(other, cls)


class Derivable(object, metaclass=DerivableMeta):
    """
    Derivable base class with features provided by the meta :py:class:`DerivableMeta`.

    .. py:classattribute:: cls_name
       type: str
       read-only

       A shorthand to access the name of the class.
    """

    @classproperty
    def cls_name(cls) -> str:
        # shorthand to the class name
        return cls.__name__
