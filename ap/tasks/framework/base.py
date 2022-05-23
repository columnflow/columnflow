# coding: utf-8

"""
Generic tools and base tasks that are defined along typical objects in an analysis.
"""

import os

import luigi
import law
import six


class BaseTask(law.Task):

    task_namespace = os.getenv("AP_TASK_NAMESPACE")


class AnalysisTask(BaseTask, law.SandboxTask):

    version = luigi.Parameter(description="mandatory version that is encoded into output paths")

    allow_empty_sandbox = True
    sandbox = None

    output_collection_cls = law.SiblingFileCollection

    # hard-coded analysis name, could be changed to a parameter
    analysis = "analysis_st"

    # defaults for targets
    default_store = "$AP_STORE_LOCAL"
    default_wlcg_fs = "wlcg_fs"

    @classmethod
    def get_analysis_inst(cls, analysis):
        if analysis == "analysis_st":
            from ap.config.analysis_st import analysis_st
            return analysis_st
        else:
            raise ValueError(f"unknown analysis {analysis}")

    @classmethod
    def modify_param_values(cls, params):
        """
        Hook to modify command line arguments before instances of this class are created.
        """
        return params

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

        # default to the version of the requested task class in the version map accessible through
        # the task instance
        if isinstance(getattr(cls, "version", None), luigi.Parameter) and "version" not in kwargs:
            version_map = cls.get_version_map(inst)
            if cls.__name__ in version_map:
                kwargs["version"] = version_map[cls.__name__]

        return super(AnalysisTask, cls).req_params(inst, **kwargs)

    @classmethod
    def get_version_map(cls, task):
        return task.analysis_inst.get_aux("versions", {})

    def __init__(self, *args, **kwargs):
        super(AnalysisTask, self).__init__(*args, **kwargs)

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
        parts["task_class"] = self.task_family

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


class ConfigTask(AnalysisTask):

    config = luigi.Parameter(
        default="run2_pp_2018",
        description="name of the analysis config to use; default: run2_pp_2018",
    )

    @classmethod
    def get_version_map(cls, task):
        return task.config_inst.get_aux("versions", {})

    def __init__(self, *args, **kwargs):
        super(ConfigTask, self).__init__(*args, **kwargs)

        # store a reference to the config instance
        self.config_inst = self.analysis_inst.get_config(self.config)

    def store_parts(self):
        parts = super(ConfigTask, self).store_parts()

        # add the config name
        parts.insert_after("task_class", "config", self.config_inst.name)

        return parts


class ShiftTask(ConfigTask):

    shift = luigi.Parameter(
        default="nominal",
        significant=False,
        description="name of a systematic shift to apply; must fulfill order.Shift naming rules; "
        "default: nominal",
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
    def modify_param_values(cls, params):
        """
        When "config" and "shift" are set, this method evlauates them to set the effecitve shift.
        For that, it takes the shifts stored in the config instance and compares it with those
        defined by this class.
        """
        # the modify_param_values super method must not necessarily be set
        super_func = super(ShiftTask, cls).modify_param_values
        if callable(super_func):
            params = super_func(params)

        # get params
        requested_config = params.get("config")
        requested_shift = params.get("shift")
        requested_effective_shift = params.get("effective_shift")

        # initialize the effective shift when not set
        no_values = (None, law.NO_STR)
        if requested_effective_shift in no_values:
            params["effective_shift"] = "nominal"

        # do nothing when the effective shift is already set and no config is defined
        if requested_effective_shift not in no_values or requested_config in no_values:
            return params

        # shift must be set
        if requested_shift in no_values:
            if cls.allow_empty_shift:
                return params
            raise Exception(f"no shift found in params: {params}")

        # get the config instance
        config_inst = cls.get_analysis_inst(cls.analysis).get_config(requested_config)

        # complain when the requested shift is not known
        if requested_shift not in config_inst.shifts:
            raise ValueError(f"unknown shift: {requested_shift}")

        # determine the allowed shifts for this class
        allowed_shifts = cls.determine_allowed_shifts(config_inst, params)

        # when allowed, add it to the task parameters
        if requested_shift in allowed_shifts:
            params["effective_shift"] = requested_shift

        return params

    @classmethod
    def determine_allowed_shifts(cls, config_inst, params):
        # for the basic shift task, only the shifts implemented by this task class are allowed
        return set(cls.shifts)

    def __init__(self, *args, **kwargs):
        super(ShiftTask, self).__init__(*args, **kwargs)

        # store a reference to the effective shift instance
        self.shift_inst = None
        if self.effective_shift and self.effective_shift != law.NO_STR:
            self.shift_inst = self.config_inst.get_shift(self.effective_shift)

    def store_parts(self):
        parts = super(ShiftTask, self).store_parts()

        # add the shift name
        if self.shift_inst:
            parts.insert_after("config", "shift", self.shift_inst.name)

        return parts


class DatasetTask(ShiftTask):

    dataset = luigi.Parameter(
        default="st_tchannel_t",
        description="name of the dataset to process; default: st_tchannel_t",
    )

    file_merging = None

    @classmethod
    def determine_allowed_shifts(cls, config_inst, params):
        # dataset can have shifts, so extend the set of allowed shifts
        allowed_shifts = super(DatasetTask, cls).determine_allowed_shifts(config_inst, params)

        # dataset must be set
        if "dataset" in params:
            requested_dataset = params.get("dataset")
            if requested_dataset not in (None, law.NO_STR):
                dataset_inst = config_inst.get_dataset(requested_dataset)
                allowed_shifts |= set(dataset_inst.info.keys())

        return allowed_shifts

    def __init__(self, *args, **kwargs):
        super(DatasetTask, self).__init__(*args, **kwargs)

        # store references to the dataset instance
        self.dataset_inst = self.config_inst.get_dataset(self.dataset)

        # store dataset info for the effective shift
        key = self.effective_shift if self.effective_shift in self.dataset_inst.info else "nominal"
        self.dataset_info_inst = self.dataset_inst.get_info(key)

    def store_parts(self):
        parts = super(DatasetTask, self).store_parts()

        # insert the dataset
        parts.insert_after("config", "dataset", self.dataset_inst.name)

        return parts

    @property
    def file_merging_factor(self):
        """
        Returns the number of files that are handled in one branch. Consecutive merging steps are
        not handled yet.
        """
        merging_info = self.config_inst.get_aux("file_merging")
        n_files = self.dataset_info_inst.n_files

        if isinstance(self.file_merging, six.integer_types):
            # interpret the file_merging attribute as the merging factor itself
            # non-positive numbers mean "merge all in one"
            n_merge = self.file_merging if self.file_merging > 0 else n_files
        elif self.file_merging in merging_info:
            # file_merging refers to an entry in merging_info which can be nested as
            # dataset -> shift -> version
            n_merge = merging_info[self.file_merging]

            # mapped to dataset?
            if isinstance(n_merge, dict):
                n_merge = n_merge.get(self.dataset_inst.name, n_files)

            # mapped to shift?
            if self.shift_inst and isinstance(n_merge, dict):
                n_merge = n_merge.get(self.shift_inst.name, n_merge.get("nominal", n_files))

            # mapped to version?
            if self.version and isinstance(n_merge, dict):
                n_merge = n_merge.get(self.version, n_files)

            if not isinstance(n_merge, int):
                raise TypeError(
                    "the merging factor in the file_merging config must be an integer, but got "
                    f"'{n_merge}' for dataset {self.dataset_inst}, shift {self.shift_inst} and "
                    f"version {self.version}",
                )

            n_merge = n_merge or n_files
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
        "be encoded into output file paths; no default",
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
