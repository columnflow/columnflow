# coding: utf-8

"""
Generic tools and base tasks that are defined along typical objects in an analysis.
"""

import os
import enum
import importlib
import itertools
import inspect
import functools
from typing import Optional, Sequence, List, Union, Set, Dict

import luigi
import law
import order as od


default_analysis = law.config.get_expanded("analysis", "default_analysis")
default_config = law.config.get_expanded("analysis", "default_config")
default_dataset = law.config.get_expanded("analysis", "default_dataset")


class OutputLocation(enum.Enum):
    """
    Output location flag.
    """

    config = "config"
    local = "local"
    wlcg = "wlcg"


class BaseTask(law.Task):

    task_namespace = os.getenv("CF_TASK_NAMESPACE")


class AnalysisTask(BaseTask, law.SandboxTask):

    analysis = luigi.Parameter(
        default=default_analysis,
        description=f"name of the analysis; default: '{default_analysis}'",
    )
    version = luigi.Parameter(description="mandatory version that is encoded into output paths")

    allow_empty_sandbox = True
    sandbox = None

    local_workflow_require_branches = False
    output_collection_cls = law.SiblingFileCollection

    # defaults for targets
    default_store = "$CF_STORE_LOCAL"
    default_wlcg_fs = "wlcg_fs"
    default_output_location = "local"

    @classmethod
    def modify_param_values(cls, params):
        params = super().modify_param_values(params)

        # store a reference to the analysis inst
        if "analysis" in params:
            params["analysis_inst"] = cls.get_analysis_inst(params["analysis"])

        return params

    @classmethod
    def get_analysis_inst(cls, analysis):
        # prepare names
        if "." not in analysis:
            raise ValueError(f"invalid analysis format: {analysis}")
        module_id, name = analysis.rsplit(".", 1)

        # import the module
        try:
            mod = importlib.import_module(module_id)
        except ImportError as e:
            raise ImportError(f"cannot import analysis module {module_id}: {e}")

        # get the analysis instance
        analysis_inst = getattr(mod, name, None)
        if analysis_inst is None:
            raise Exception(f"module {module_id} does not contain analysis instance {name}")

        return analysis_inst

    @classmethod
    def req_params(cls, inst, **kwargs):
        """
        Returns parameters that are jointly defined in this class and another task instance of some
        other class. The parameters are used when calling ``Task.req(self)``.
        """
        # always prefer certain parameters given as task family parameters (--TaskFamily-parameter)
        _prefer_cli = set(law.util.make_list(kwargs.get("_prefer_cli", [])))
        _prefer_cli.add("version")
        kwargs["_prefer_cli"] = _prefer_cli

        # when cls accepts a version, but non was actively requested, use the version map to assign it
        version_map = None
        if isinstance(getattr(cls, "version", None), luigi.Parameter) and "version" not in kwargs:
            version_map = cls.get_version_map(inst)

        # build the params
        params = super().req_params(inst, **kwargs)

        # when the version map is set, lookup the key with which to find a version in the map and overwrite it
        if version_map and cls.task_family in version_map:
            version_params = list(cls.get_version_params())
            version = version_map[cls.task_family]
            while isinstance(version, dict) and version_params:
                param = version_params.pop(0)
                if param not in params:
                    break
                value = params[param]
                if value not in version:
                    if None not in version:
                        break
                    value = None
                version = version[value]
            params["version"] = version

        return params

    @classmethod
    def get_version_map(cls, task):
        return task.analysis_inst.get_aux("versions", {})

    @classmethod
    def get_version_params(cls):
        return ()

    @classmethod
    def determine_allowed_shifts(cls, config_inst, params):
        # implemented only for simplified mro control
        return set()

    @classmethod
    def get_array_function_kwargs(cls, task=None, **params):
        return {
            "task": task,
            "analysis_inst": task.analysis_inst if task else cls.get_analysis_inst(params["analysis"]),
        }

    @classmethod
    def get_calibrator_kwargs(cls, task=None, **params):
        # implemented here only for simplified mro control
        return cls.get_array_function_kwargs(task=task, **params)

    @classmethod
    def get_selector_kwargs(cls, task=None, **params):
        # implemented here only for simplified mro control
        return cls.get_array_function_kwargs(task=task, **params)

    @classmethod
    def get_producer_kwargs(cls, task=None, **params):
        # implemented here only for simplified mro control
        return cls.get_array_function_kwargs(task=task, **params)

    @classmethod
    def find_config_objects(
        cls,
        names: Union[str, Sequence[str], Set[str]],
        container: od.UniqueObject,
        object_cls: od.UniqueObjectMeta,
        object_groups: Optional[Dict[str, list]] = None,
        accept_patterns: bool = True,
        deep: bool = False,
    ) -> List[str]:
        """
        Returns all names of objects of type *object_cls* known to a *container* (e.g.
        :py:class:`od.Analysis` or :py:class:`od.Config`) that match *names*. A name can also be a
        pattern to match if *accept_patterns* is *True*, or, when given, the key of a mapping
        *object_group* that matches group names to object names. When *deep* is *True* the lookup of
        objects in the *container* is recursive. Example:

        .. code-block:: python

            find_config_objects(["st_tchannel_*"], config_inst, od.Dataset)
            # -> ["st_tchannel_t", "st_tchannel_tbar"]
        """
        singular = object_cls.cls_name_singular
        plural = object_cls.cls_name_plural
        _cache = {}

        def get_all_object_names():
            if "all_object_names" not in _cache:
                if deep:
                    _cache["all_object_names"] = {
                        obj.name
                        for obj, _, _ in
                        getattr(container, "walk_{}".format(plural))()
                    }
                else:
                    _cache["all_object_names"] = set(getattr(container, plural).names())
            return _cache["all_object_names"]

        def has_obj(name):
            if "has_obj_func" not in _cache:
                kwargs = {}
                if object_cls in container._deep_child_classes:
                    kwargs["deep"] = deep
                _cache["has_obj_func"] = functools.partial(
                    getattr(container, "has_{}".format(singular)),
                    **kwargs,
                )
            return _cache["has_obj_func"](name)

        object_names = []
        patterns = []
        lookup = list(names)
        while lookup:
            name = lookup.pop(0)
            if has_obj(name):
                # known object
                object_names.append(name)
            elif object_groups and name in object_groups:
                # a key in the object group dict
                lookup.extend(list(object_groups[name]))
            elif accept_patterns:
                # must eventually be a pattern, store it for object traversal
                # special case
                if name == "*":
                    object_names = list(get_all_object_names())
                    del patterns[:]
                    break
                patterns.append(name)

        # if patterns are found, loop through them to preserve the order of patterns
        # and compare to all existing names
        for pattern in patterns:
            for name in get_all_object_names():
                if law.util.multi_match(name, pattern):
                    object_names.append(name)

        return law.util.make_unique(object_names)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # store the analysis instance
        self.analysis_inst = self.get_analysis_inst(self.analysis)

    def store_parts(self):
        """
        Returns a :py:class:`law.util.InsertableDict` whose values are used to create a store path.
        For instance, the parts ``{"keyA": "a", "keyB": "b", 2: "c"}`` lead to the path "a/b/c". The
        keys can be used by subclassing tasks to overwrite values.
        """
        parts = law.util.InsertableDict()

        # add the analysis name
        parts["analysis"] = self.analysis_inst.name

        # in this base class, just add the task class name
        parts["task_family"] = self.task_family

        # add the version when set
        if self.version is not None:
            parts["version"] = self.version

        return parts

    def local_path(self, *path, **kwargs):
        """ local_path(*path, store=None)
        Joins path fragments from *store* (defaulting to :py:attr:`default_store`), *store_parts()*
        and *path* and returns the joined path.
        """
        # determine the main store directory
        store = kwargs.get("store") or self.default_store

        # concatenate all parts that make up the path and join them
        parts = tuple(self.store_parts().values()) + path
        path = os.path.join(store, *(str(p) for p in parts))

        return path

    def local_target(self, *path, **kwargs):
        """ local_target(*path, dir=False, store=None, **kwargs)
        Creates either a local file or directory target, depending on *dir*, forwarding all *path*
        fragments and *store* to :py:meth:`local_path` and all *kwargs* the respective target class.
        """
        # select the target class
        cls = law.LocalDirectoryTarget if kwargs.pop("dir", False) else law.LocalFileTarget

        # create the local path
        path = self.local_path(*path, store=kwargs.pop("store", None))

        # create the target instance and return it
        return cls(path, **kwargs)

    def wlcg_path(self, *path):
        """
        Joins path fragments from *store_parts()* and *path* and returns the joined path.

        The full URI to the target is not considered as it is usually defined in ``[wlcg_fs]``
        sections in the law config and hence subject to :py:func:`wlcg_target`.
        """
        # concatenate all parts that make up the path and join them
        parts = tuple(self.store_parts().values()) + path
        path = os.path.join(*(str(p) for p in parts))

        return path

    def wlcg_target(self, *path, **kwargs):
        """ wlcg_target(*path, dir=False, fs=default_wlcg_fs, **kwargs)
        Creates either a remote WLCG file or directory target, depending on *dir*, forwarding all
        *path* fragments to :py:meth:`wlcg_path` and all *kwargs* the respective target class. When
        *None*, *fs* defaults to the *default_wlcg_fs* class level attribute.
        """
        if not kwargs.get("fs"):
            kwargs["fs"] = self.default_wlcg_fs

        # select the target class
        cls = law.wlcg.WLCGDirectoryTarget if kwargs.pop("dir", False) else law.wlcg.WLCGFileTarget

        # create the local path
        path = self.wlcg_path(*path)

        # create the target instance and return it
        return cls(path, **kwargs)

    def target(self, *path, **kwargs):
        """ target(*path, location=None, **kwargs)
        """
        # get the default location
        location = kwargs.pop("location", self.default_output_location)

        # parse it and obtain config values if necessary
        if isinstance(location, str):
            location = OutputLocation[location]
        if location == OutputLocation.config:
            location = law.config.get_expanded("outputs", self.task_family, split_csv=True)
            if not location:
                self.logger.info(
                    f"no option 'outputs.{self.task_family}' found in law.cfg to obtain target "
                    "location, falling back to 'local'",
                )
                location = ["local"]
            location[0] = OutputLocation[location[0]]
        location = law.util.make_list(location)

        # forward to correct function
        if location[0] == OutputLocation.local:
            # get other options
            (store,) = (location[1:] + [None])[:1]
            kwargs.setdefault("store", store)
            return self.local_target(*path, **kwargs)

        elif location[0] == OutputLocation.wlcg:
            # get other options
            (fs,) = (location[1:] + [None])[:1]
            kwargs.setdefault("fs", fs)
            return self.wlcg_target(*path, **kwargs)

        raise Exception(f"cannot determine output location based on '{location}'")


class ConfigTask(AnalysisTask):

    config = luigi.Parameter(
        default=default_config,
        description=f"name of the analysis config to use; default: '{default_config}'",
    )

    @classmethod
    def modify_param_values(cls, params):
        params = super().modify_param_values(params)

        # store a reference to the config inst
        if "analysis_inst" in params and "config" in params:
            params["config_inst"] = params["analysis_inst"].get_config(params["config"])

        return params

    @classmethod
    def get_version_map(cls, task):
        if isinstance(task, ConfigTask):
            return task.config_inst.get_aux("versions", {})

        return super().get_version_map(task)

    @classmethod
    def get_array_function_kwargs(cls, task=None, **params):
        kwargs = super().get_array_function_kwargs(task=task, **params)
        if task:
            kwargs["config_inst"] = task.config_inst
        elif "config" in params and "analysis_inst" in kwargs:
            kwargs["config_inst"] = kwargs["analysis_inst"].get_config(params["config"])
        return kwargs

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # store a reference to the config instance
        self.config_inst = self.analysis_inst.get_config(self.config)

    def store_parts(self):
        parts = super().store_parts()

        # add the config name
        parts.insert_after("task_family", "config", self.config_inst.name)

        return parts


class ShiftTask(ConfigTask):

    shift = luigi.Parameter(
        default="nominal",
        description="name of a systematic shift to apply; must fulfill order.Shift naming rules; "
        "default: 'nominal'",
    )
    effective_shift = luigi.Parameter(default=law.NO_STR)

    # shifts implemented by this task or one of its requirements
    shifts = set()

    # skip passing effective_shift to cli completion, req params and sandboxing
    exclude_params_index = {"effective_shift"}
    exclude_params_req = {"effective_shift"}
    exclude_params_sandbox = {"effective_shift"}

    allow_empty_shift = False

    @classmethod
    def get_version_params(cls):
        params = super().get_version_params()
        if cls.shift:
            params += ("shift",)
        return params

    @classmethod
    def modify_param_values(cls, params):
        """
        When "config" and "shift" are set, this method evaluates them to set the effecitve shift.
        For that, it takes the shifts stored in the config instance and compares it with those
        defined by this class.
        """
        # the modify_param_values super method must not necessarily be set
        super_func = super().modify_param_values
        if callable(super_func):
            params = super_func(params)

        # get params
        config_inst = params.get("config_inst")
        requested_shift = params.get("shift")
        requested_effective_shift = params.get("effective_shift")

        # initialize the effective shift when not set
        no_values = (None, law.NO_STR)
        if requested_effective_shift in no_values:
            params["effective_shift"] = "nominal"

        # do nothing when the effective shift is already set and no config is defined
        if requested_effective_shift not in no_values or config_inst in no_values:
            return params

        # shift must be set
        if requested_shift in no_values:
            if cls.allow_empty_shift:
                return params
            raise Exception(f"no shift found in params: {params}")

        # complain when the requested shift is not known
        if requested_shift not in config_inst.shifts:
            raise ValueError(f"unknown shift: {requested_shift}")

        # determine the allowed shifts for this class
        allowed_shifts = cls.determine_allowed_shifts(config_inst, params)

        # when allowed, add it to the task parameters
        if requested_shift in allowed_shifts:
            params["effective_shift"] = requested_shift

        # store references
        params["shift_inst"] = config_inst.get_shift(requested_shift)
        params["effective_shift_inst"] = config_inst.get_shift(params["effective_shift"])

        return params

    @classmethod
    def determine_allowed_shifts(cls, config_inst, params):
        # for the basic shift task, only the shifts implemented by this task class are allowed
        # still call super for simplified mro control
        shifts = super().determine_allowed_shifts(config_inst, params)

        # add class level shifts
        shifts |= cls.shifts

        return shifts

    @classmethod
    def get_array_function_kwargs(cls, task=None, **params):
        kwargs = super().get_array_function_kwargs(task=task, **params)
        if task:
            if task.shift_inst:
                kwargs["shift_inst"] = task.shift_inst
        elif "effective_shift" in params and "config_inst" in kwargs:
            if params["effective_shift"] != law.NO_STR:
                kwargs["shift_inst"] = kwargs["config_inst"].get_shift(params["effective_shift"])
        return kwargs

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # store a reference to the effective shift instance
        self.shift_inst = None
        if self.effective_shift and self.effective_shift != law.NO_STR:
            self.shift_inst = self.config_inst.get_shift(self.effective_shift)

    def store_parts(self):
        parts = super().store_parts()

        # add the shift name
        if self.shift_inst:
            parts.insert_after("config", "shift", self.shift_inst.name)

        return parts


class DatasetTask(ShiftTask):

    dataset = luigi.Parameter(
        default=default_dataset,
        description=f"name of the dataset to process; default: '{default_dataset}'",
    )

    file_merging = None

    @classmethod
    def modify_param_values(cls, params):
        params = super().modify_param_values(params)

        # store a reference to the dataset inst
        if "config_inst" in params and "dataset" in params:
            params["dataset_inst"] = params["config_inst"].get_dataset(params["dataset"])

        return params

    @classmethod
    def get_version_params(cls):
        params = super().get_version_params()
        if cls.shift:
            params = params[:-1] + ("dataset", "shift")
        else:
            params += ("dataset",)
        return params

    @classmethod
    def determine_allowed_shifts(cls, config_inst, params):
        # dataset can have shifts, so extend the set of allowed shifts
        allowed_shifts = super().determine_allowed_shifts(config_inst, params)

        # dataset must be set
        if "dataset" in params:
            requested_dataset = params.get("dataset")
            if requested_dataset not in (None, law.NO_STR):
                dataset_inst = config_inst.get_dataset(requested_dataset)
                # clear shifts for data and extend with dataset variations for mc
                if dataset_inst.is_data:
                    allowed_shifts.clear()
                else:
                    allowed_shifts |= set(dataset_inst.info.keys())

        return allowed_shifts

    @classmethod
    def get_array_function_kwargs(cls, task=None, **params):
        kwargs = super().get_array_function_kwargs(task=task, **params)
        if task:
            kwargs["dataset_inst"] = task.dataset_inst
        elif "dataset" in params and "config_inst" in kwargs:
            kwargs["dataset_inst"] = kwargs["config_inst"].get_dataset(params["dataset"])
        return kwargs

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # store references to the dataset instance
        self.dataset_inst = self.config_inst.get_dataset(self.dataset)

        # store dataset info for the effective shift
        key = self.effective_shift if self.effective_shift in self.dataset_inst.info else "nominal"
        self.dataset_info_inst = self.dataset_inst.get_info(key)

    def store_parts(self):
        parts = super().store_parts()

        # insert the dataset
        parts.insert_after("config", "dataset", self.dataset_inst.name)

        return parts

    @property
    def file_merging_factor(self):
        """
        Returns the number of files that are handled in one branch. Consecutive merging steps are
        not handled yet.
        """
        n_files = self.dataset_info_inst.n_files

        if isinstance(self.file_merging, int):
            # interpret the file_merging attribute as the merging factor itself
            # non-positive numbers mean "merge all in one"
            n_merge = self.file_merging if self.file_merging > 0 else n_files
        else:
            # no merging at all
            n_merge = 1

        return n_merge

    def create_branch_map(self):
        """
        Define the branch map for when this task is used as a workflow. By default, use the merging
        information provided by :py:attr:`file_merging_factor` to return a dictionary which maps
        branches to one or more input file indices. E.g. `1 -> [3, 4, 5]` would mean that branch 1
        is simultaneously handling input file indices 3, 4 and 5.
        """
        n_merge = self.file_merging_factor
        n_files = self.dataset_info_inst.n_files

        # use iter_chunks which splits a list of length n_files into chunks of maximum size n_merge
        chunks = law.util.iter_chunks(n_files, n_merge)

        # use enumerate for simply indexing
        return dict(enumerate(chunks))

    def htcondor_destination_info(self, info):
        """
        Hook to modify the additional info printed along logs of the htcondor workflow.
        """
        info.append(self.dataset_inst.name)
        if self.shift_inst and self.shift_inst.name != "nominal":
            info.append(self.shift_inst.name)
        return info


class CommandTask(AnalysisTask):
    """
    A task that provides convenience methods to work with shell commands, i.e., printing them on the
    command line and executing them with error handling.
    """

    print_command = law.CSVParameter(
        default=(),
        significant=False,
        description="print the command that this task would execute but do not run any task; this "
        "CSV parameter accepts a single integer value which sets the task recursion depth to also "
        "print the commands of required tasks (0 means non-recursive)",
    )
    custom_args = luigi.Parameter(
        default="",
        description="custom arguments that are forwarded to the underlying command; they might not "
        "be encoded into output file paths; empty default",
    )

    exclude_index = True
    exclude_params_req = {"custom_args"}
    interactive_params = AnalysisTask.interactive_params + ["print_command"]

    run_command_in_tmp = False

    def _print_command(self, args):
        max_depth = int(args[0])

        print(f"print task commands with max_depth {max_depth}")

        for dep, _, depth in self.walk_deps(max_depth=max_depth, order="pre"):
            offset = depth * ("|" + law.task.interactive.ind)
            print(offset)

            print("{}> {}".format(offset, dep.repr(color=True)))
            offset += "|" + law.task.interactive.ind

            if isinstance(dep, CommandTask):
                # when dep is a workflow, take the first branch
                text = law.util.colored("command", style="bright")
                if isinstance(dep, law.BaseWorkflow) and dep.is_workflow():
                    dep = dep.as_branch(0)
                    text += " (from branch {})".format(law.util.colored("0", "red"))
                text += ": "

                cmd = dep.build_command()
                if cmd:
                    cmd = law.util.quote_cmd(cmd) if isinstance(cmd, (list, tuple)) else cmd
                    text += law.util.colored(cmd, "cyan")
                else:
                    text += law.util.colored("empty", "red")
                print(offset + text)
            else:
                print(offset + law.util.colored("not a CommandTask", "yellow"))

    def build_command(self):
        # this method should build and return the command to run
        raise NotImplementedError

    def touch_output_dirs(self):
        # keep track of created uris so we can avoid creating them twice
        handled_parent_uris = set()

        for outp in law.util.flatten(self.output()):
            # get the parent directory target
            parent = None
            if isinstance(outp, law.SiblingFileCollection):
                parent = outp.dir
            elif isinstance(outp, law.FileSystemFileTarget):
                parent = outp.parent

            # create it
            if parent and parent.uri() not in handled_parent_uris:
                parent.touch()
                handled_parent_uris.add(parent.uri())

    def run_command(self, cmd, optional=False, **kwargs):
        # proper command encoding
        cmd = (law.util.quote_cmd(cmd) if isinstance(cmd, (list, tuple)) else cmd).strip()

        # when no cwd was set and run_command_in_tmp is True, create a tmp dir
        if "cwd" not in kwargs and self.run_command_in_tmp:
            tmp_dir = law.LocalDirectoryTarget(is_tmp=True)
            tmp_dir.touch()
            kwargs["cwd"] = tmp_dir.path
        self.publish_message("cwd: {}".format(kwargs.get("cwd", os.getcwd())))

        # call it
        with self.publish_step("running '{}' ...".format(law.util.colored(cmd, "cyan"))):
            p, lines = law.util.readable_popen(cmd, shell=True, executable="/bin/bash", **kwargs)
            for line in lines:
                print(line)

        # raise an exception when the call failed and optional is not True
        if p.returncode != 0 and not optional:
            raise Exception(f"command failed with exit code {p.returncode}: {cmd}")

        return p

    @law.decorator.log
    @law.decorator.notify
    @law.decorator.safe_output
    def run(self, **kwargs):
        self.pre_run_command()

        # default run implementation
        # first, create all output directories
        self.touch_output_dirs()

        # build the command
        cmd = self.build_command()

        # run it
        self.run_command(cmd, **kwargs)

        self.post_run_command()

    def pre_run_command(self):
        return

    def post_run_command(self):
        return


def wrapper_factory(
    base_cls: law.Task,
    require_cls: AnalysisTask,
    enable: Sequence[str],
    cls_name: Optional[str] = None,
    attributes: Optional[dict] = None,
) -> law.task.base.Register:
    """
    Factory function creating wrapper task classes, inheriting from *base_cls* and
    ``law.WrapperTask``, that do nothing but require multiple instances of *require_cls*. Unless
    *cls_name* is defined, the name of the created class defaults to the name of *require_cls* plus
    "Wrapper". Additional *attributes* are added as class-level members when given.

    The instances of *require_cls* to be required in the ``requires()`` method can be controlled by
    task parameters. These parameters can be enabled through the string sequence *enable*, which
    currently accepts:

        - ``configs``, ``skip_configs``
        - ``shifts``, ``skip_shifts``
        - ``datasets``, ``skip_datasets``

    This allows to easily build wrapper tasks that loop over (combinations of) parameters that are
    either defined in the analysis or config, which would otherwise lead to mostly redundant code.
    Example:

    .. code-block:: python

        class MyTask(DatasetTask):
            ...

        MyTaskWrapper = wrapper_factory(
            base_cls=ConfigTask,
            require_cls=MyTask,
            enable=["datasets", "skip_datasets"],
        )

        # this allows to run (e.g.)
        # law run MyTaskWrapper --datasets st_* --skip-datasets *_tbar

    When building the requirements, the full combinatorics of parameters is considered. However,
    certain conditions apply depending on enabled features. For instance, in order to use the
    "configs" feature (adding a parameter "--configs" to the created class, allowing to loop over a
    list of config instances known to an analysis), *require_cls* must be at least a
    :py:class:`ConfigTask` accepting "--config" (mind the singular form), whereas *base_cls* must
    explicitly not.
    """
    # check known features
    known_features = [
        "configs", "skip_configs",
        "shifts", "skip_shifts",
        "datasets", "skip_datasets",
    ]
    for feature in enable:
        if feature not in known_features:
            raise ValueError(
                f"unknown enabled feature '{feature}', known features are "
                f"'{','.join(known_features)}'",
            )

    # define wrapper feature flags
    has_configs = "configs" in enable
    has_skip_configs = has_configs and "skip_configs" in enable
    has_shifts = "shifts" in enable
    has_skip_shifts = has_shifts and "skip_shifts" in enable
    has_datasets = "datasets" in enable
    has_skip_datasets = has_datasets and "skip_datasets" in enable

    # helper to check if enabled features are compatible with required and base class
    def check_class_compatibility(name, min_require_cls, max_base_cls):
        if not issubclass(require_cls, min_require_cls):
            raise TypeError(
                f"when the '{name}' feature is enabled, require_cls must inherit from "
                f"{min_require_cls}, but {require_cls} does not",
            )
        if issubclass(base_cls, min_require_cls):
            raise TypeError(
                f"when the '{name}' feature is enabled, base_cls must not inherit from "
                f"{min_require_cls}, but {base_cls} does",
            )
        if not issubclass(max_base_cls, base_cls):
            raise TypeError(
                f"when the '{name}' feature is enabled, base_cls must be a super class of "
                f"{max_base_cls}, but {base_cls} is not",
            )

    # check classes
    if has_configs:
        check_class_compatibility("configs", ConfigTask, AnalysisTask)
    if has_shifts:
        check_class_compatibility("shifts", ShiftTask, ConfigTask)
    if has_datasets:
        check_class_compatibility("datasets", DatasetTask, ShiftTask)

    # create the class
    class Wrapper(base_cls, law.WrapperTask):

        if has_configs:
            configs = law.CSVParameter(
                default=("*",),
                description="names or name patterns of configs to use; can also be the key of a "
                "mapping defined in the 'config_groups' auxiliary data of the analysis; default: "
                "('*',)",
            )
        if has_skip_configs:
            skip_configs = law.CSVParameter(
                default=(),
                description="names or name patterns of configs to skip after evaluating --configs; "
                "can also be the key of a mapping defined in the 'config_groups' auxiliary data "
                "of the analysis; empty default",
            )
        if has_datasets:
            datasets = law.CSVParameter(
                default=("*",),
                description="names or name patterns of datasets to use; can also be the key of a "
                "mapping defined in the 'dataset_groups' auxiliary data of the corresponding "
                "config; default: ('*',)",
            )
        if has_skip_datasets:
            skip_datasets = law.CSVParameter(
                default=(),
                description="names or name patterns of datasets to skip after evaluating "
                "--datasets; can also be the key of a mapping defined in the 'dataset_groups' "
                "auxiliary data of the corresponding config; empty default",
            )
        if has_shifts:
            shifts = law.CSVParameter(
                default=("nominal",),
                description="names or name patterns of shifts to use; can also be the key of a "
                "mapping defined in the 'shift_groups' auxiliary data of the corresponding "
                "config; default: ('nominal',)",
            )
        if has_skip_shifts:
            skip_shifts = law.CSVParameter(
                default=(),
                description="names or name patterns of shifts to skip after evaluating --shifts; "
                "can also be the key of a mapping defined in the 'shift_groups' auxiliary data "
                "of the corresponding config; empty default",
            )

        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)

            # store wrapper flags
            self.wrapper_require_cls = require_cls
            self.wrapper_has_configs = has_configs and self.configs
            self.wrapper_has_skip_configs = has_skip_configs
            self.wrapper_has_shifts = has_shifts and self.shifts
            self.wrapper_has_skip_shifts = has_skip_shifts
            self.wrapper_has_datasets = has_datasets and self.datasets
            self.wrapper_has_skip_datasets = has_skip_datasets

            # store wrapped fields
            self.wrapper_fields = [
                field for field in ["config", "shift", "dataset"]
                if getattr(self, f"wrapper_has_{field}s")
            ]

            # build the parameter space
            self.wrapper_parameters = self._build_wrapper_parameters()

        def _build_wrapper_parameters(self):
            # collect a list of tuples whose order corresponds to wrapper_fields
            params = []

            # get the target config instances
            if self.wrapper_has_configs:
                configs = self.find_config_objects(
                    self.configs,
                    self.analysis_inst,
                    od.Config,
                    self.analysis_inst.x("config_groups", {}),
                )
                if not configs:
                    raise ValueError(
                        f"no configs found in analysis {self.analysis_inst} matching {self.configs}",
                    )
                if self.wrapper_has_skip_configs:
                    skip_configs = self.find_config_objects(
                        self.skip_configs,
                        self.analysis_inst,
                        od.Config,
                        self.analysis_inst.x("config_groups", {}),
                    )
                    configs = [c for c in configs if c not in skip_configs]
                    if not configs:
                        raise ValueError(
                            f"no configs found in analysis {self.analysis_inst} after skipping "
                            f"{self.skip_configs}",
                        )
                config_insts = list(map(self.analysis_inst.get_config, sorted(configs)))
            else:
                config_insts = [self.config_inst]

            # for the remaining fields, build the full combinatorics per config_inst
            for config_inst in config_insts:
                # sequences for building combinatorics
                prod_sequences = []
                if self.wrapper_has_configs:
                    prod_sequences.append([config_inst.name])

                # find all shifts
                if self.wrapper_has_shifts:
                    shifts = self.find_config_objects(
                        self.shifts,
                        config_inst,
                        od.Shift,
                        config_inst.x("shift_groups", {}),
                    )
                    if not shifts:
                        raise ValueError(
                            f"no shifts found in config {config_inst} matching {self.shifts}",
                        )
                    if self.wrapper_has_skip_shifts:
                        skip_shifts = self.find_config_objects(
                            self.skip_shifts,
                            config_inst,
                            od.Shift,
                            config_inst.x("shift_groups", {}),
                        )
                        shifts = [s for s in shifts if s not in skip_shifts]
                    if not shifts:
                        raise ValueError(
                            f"no shifts found in config {config_inst} after skipping "
                            f"{self.skip_shifts}",
                        )
                    # move "nominal" to the front if present
                    shifts = sorted(shifts)
                    if "nominal" in shifts:
                        shifts.insert(0, shifts.pop(shifts.index("nominal")))
                    prod_sequences.append(shifts)

                # find all datasets
                if self.wrapper_has_datasets:
                    datasets = self.find_config_objects(
                        self.datasets,
                        config_inst,
                        od.Dataset,
                        config_inst.x("dataset_groups", {}),
                    )
                    if not datasets:
                        raise ValueError(
                            f"no datasets found in config {self.config_inst} matching "
                            f"{self.datasets}",
                        )
                    if self.wrapper_has_skip_datasets:
                        skip_datasets = self.find_config_objects(
                            self.skip_datasets,
                            config_inst,
                            od.Dataset,
                            config_inst.x("dataset_groups", {}),
                        )
                        datasets = [d for d in datasets if d not in skip_datasets]
                        if not datasets:
                            raise ValueError(
                                f"no datasets found in config {self.config_inst} after skipping "
                                f"{self.skip_datasets}",
                            )
                    prod_sequences.append(sorted(datasets))

                # add the full combinatorics
                params.extend(itertools.product(*prod_sequences))

            return params

        def requires(self):
            # build all requirements based on the parameter space
            reqs = {}

            for values in self.wrapper_parameters:
                params = dict(zip(self.wrapper_fields, values))

                # allow custom checks and updates
                params = self.update_wrapper_params(params)
                if not params:
                    continue

                # add the requirement if not present yet
                req = self.wrapper_require_cls.req(self, **params)
                if req not in reqs.values():
                    reqs[values] = req

            return reqs

        def update_wrapper_params(self, params):
            return params

        # add additional class-level members
        if attributes:
            locals().update(attributes)

    # overwrite __module__ to point to the module of the calling stack
    frame = inspect.stack()[1]
    module = inspect.getmodule(frame[0])
    Wrapper.__module__ = module.__name__

    # overwrite __name__
    Wrapper.__name__ = cls_name or require_cls.__name__ + "Wrapper"

    return Wrapper
