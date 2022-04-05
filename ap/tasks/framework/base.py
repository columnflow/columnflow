# coding: utf-8

"""
Generic tools and base tasks that are defined along typical objects in an analysis.
"""

import os
from collections import OrderedDict

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
        _prefer_cli = law.util.make_list(kwargs.get("_prefer_cli", []))
        if "version" not in _prefer_cli:
            _prefer_cli.append("version")
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
        if self.analysis == "analysis_st":
            from ap.config.analysis_st import analysis_st
            self.analysis_inst = analysis_st
        else:
            raise ValueError("unknown analysis {}".format(self.analysis))

    def store_parts(self):
        """
        Returns an ordered dictionary whose values are used to create a store path. For instance,
        the parts ``{"keyA": "a", "keyB": "b", 2: "c"}`` lead to the path "a/b/c". The keys can be
        used by subclassing tasks to overwrite values.
        """
        parts = OrderedDict()

        # add the analysis name
        parts["analysis"] = self.analysis_inst.name

        # in this base class, just add the task class name
        parts["task_class"] = self.__class__.__name__

        return parts

    def store_parts_ext(self):
        """
        Similar to :py:meth:`store_parts` but these external parts are appended to the end of paths
        created with (e.g.) :py:meth:`local_path`.
        """
        parts = OrderedDict()

        # add the version when set
        if self.version is not None:
            parts["version"] = self.version

        return parts

    def local_path(self, *path, **kwargs):
        """ local_path(*path, store=None)
        Joins several path fragments for use on local targets in the following order:

            - *store* (defaulting to :py:attr:`default_store`)
            - Values of :py:meth:`store_parts` (in that order)
            - Values of :py:meth:`store_parts_ext` (in that order)
            - *path* fragments passed to this method
        """
        # determine the main store directory
        store = kwargs.get("store") or self.default_store

        # concatenate all parts that make up the path and join them
        parts = tuple(self.store_parts().values()) + tuple(self.store_parts_ext().values()) + path
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
        Joins several path fragments for use in remote targets in the following order:

            - Values of :py:meth:`store_parts` (in that order)
            - Values of :py:meth:`store_parts_ext` (in that order)
            - *path* fragments passed to this method

        The full URI to the target is not considered as it is usually defined in ``[wlcg_fs]``
        sections in the law config and hence subject to :py:func:`wlcg_target`.
        """
        # concatenate all parts that make up the path and join them
        parts = tuple(self.store_parts().values()) + tuple(self.store_parts_ext().values()) + path
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
        parts["config"] = self.config_inst.name

        return parts


class DatasetTask(ConfigTask):

    dataset = luigi.Parameter(
        default="st_tchannel_t",
        description="name of the dataset to process; default: st_tchannel_t",
    )

    file_merging = None

    def __init__(self, *args, **kwargs):
        super(DatasetTask, self).__init__(*args, **kwargs)

        # store references to the dataset instance
        self.dataset_inst = self.config_inst.get_dataset(self.dataset)

        # for the moment, store the nominal dataset info
        # this will change once there is the intermediate ShiftTask that models uncertainties
        self.dataset_info_inst = self.dataset_inst.get_info("nominal")

    def store_parts(self):
        parts = super(DatasetTask, self).store_parts()

        # insert the dataset
        parts["dataset"] = self.dataset_inst.name

        return parts

    def modify_polling_status_line(self, status_line):
        """
        Hook to modify the line printed by remote workflows when polling for jobs.
        """
        return "{}, {}".format(status_line, self.dataset_inst.name)

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
            # the file_merging attributes refers to a dict in the merging_info
            n_merge = merging_info[self.file_merging].get(self.dataset_inst.name, n_files)
        else:
            # no merging at all
            n_merge = 1

        return n_merge

    def create_branch_map(self):
        n_merge = self.file_merging_factor
        n_files = self.dataset_info_inst.n_files

        return dict(enumerate(law.util.iter_chunks(n_files, n_merge)))


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

        print("print task commands with max_depth {}".format(max_depth))

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
            raise Exception("command failed with exit code {}: {}".format(p.returncode, cmd))

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
