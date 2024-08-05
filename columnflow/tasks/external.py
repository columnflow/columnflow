# coding: utf-8

"""
Tasks dealing with external data.
"""

from __future__ import annotations

import os
import time
import shutil
import subprocess

import luigi
import law
import order as od

from columnflow.types import Sequence
from columnflow.tasks.framework.base import AnalysisTask, ConfigTask, DatasetTask, wrapper_factory
from columnflow.tasks.framework.parameters import user_parameter_inst
from columnflow.util import wget, DotDict


logger = law.logger.get_logger(__name__)


class GetDatasetLFNs(DatasetTask, law.tasks.TransferLocalFile):
    """
    Task to get list of logical file names (LFNs).
    """

    replicas = luigi.IntParameter(
        default=5,
        description="number of replicas to generate; default: 5",
    )

    validate = law.OptionalBoolParameter(
        default=None,
        significant=False,
        description="when True, complains if the number of obtained LFNs does not match the value "
        "expected from the dataset info; default: obtained from 'validate_dataset_lfns' auxiliary "
        "entry in config",
    )

    version = None
    """Version parameter - deactivated for :py:class:`~columnflow.tasks.external.GetDatasetLFNs`
    """

    @classmethod
    def resolve_param_values(cls, params: DotDict) -> DotDict:
        """
        Resolve parameter values *params* from command line and propagate them to this set of
        parameters.

        :param params: Parameters provided at command line level.
        :return: Updated list of parameter values.
        """
        params = super().resolve_param_values(params)

        # add the default calibrator when empty
        if "config_inst" in params and params.get("validate") is None:
            config_inst = params["config_inst"]
            params["validate"] = config_inst.x("validate_dataset_lfns", False)

        return params

    @property
    def sandbox(self) -> str:
        """
        Defines sandbox for this task.

        :return: Path to shell script that sets up the requested sandbox.
        """
        sandbox = self.config_inst.x("get_dataset_lfns_sandbox", None)
        if sandbox is None:
            sandbox = "bash::/cvmfs/cms.cern.ch/cmsset_default.sh"
        return sandbox if sandbox and sandbox != law.NO_STR else None

    def single_output(self) -> law.target.file.FileSystemFileTarget:
        """
        Creates a remote target file for the final .json file containing the list of LFNs.

        :return: Law remote target with the initialized output name
        """
        # required by law.tasks.TransferLocalFile
        h = law.util.create_hash(list(sorted(self.dataset_info_inst.keys)))
        return self.target(f"lfns_{h}.json")

    @law.decorator.log
    def run(self):
        """
        Run function for this task.

        :raises ValueError: If number of loaded LFNs does not correspond to number of LFNs specified
            in this ``dataset_info_inst``.
        """
        # prepare the lfn getter
        get_dataset_lfns = self.config_inst.x("get_dataset_lfns", None)
        msg = "via custom config function"
        if not callable(get_dataset_lfns):
            get_dataset_lfns = self.get_dataset_lfns_dasgoclient
            msg = "via dasgoclient"

        lfns = []
        for key in sorted(self.dataset_info_inst.keys):
            self.logger.info(f"get lfns for dataset key {key} {msg}")
            lfns.extend(get_dataset_lfns(self.dataset_inst, self.global_shift_inst, key))

        if self.validate and len(lfns) != self.dataset_info_inst.n_files:
            raise ValueError(
                f"number of obtained lfns ({len(lfns)}) does not match number of files "
                f"for dataset {self.dataset_inst.name} ({self.dataset_info_inst.n_files})",
            )

        self.logger.info(f"found {len(lfns)} lfn(s) for dataset {self.dataset}")

        tmp = law.LocalFileTarget(is_tmp=True)
        tmp.dump(lfns, indent=4, formatter="json")
        self.transfer(tmp)

    def get_dataset_lfns_dasgoclient(
        self,
        dataset_inst: od.Dataset,
        shift_inst: od.Shift,
        dataset_key: str,
    ) -> list[str]:
        """
        Get the LNF information with the ``dasgoclient``.

        :param dataset_inst: Current dataset instance, currently not used.
        :param shift_inst: Current shift instance, currently not used.
        :param dataset_key: DAS key identifier for the current dataset.
        :raises Exception: If query with ``dasgoclient`` fails.
        :return: The list of LFNs corresponding to the dataset with the identifier *dataset_key*.
        """
        code, out, _ = law.util.interruptable_popen(
            f"dasgoclient --query='file dataset={dataset_key}' --limit=0",
            shell=True,
            stdout=subprocess.PIPE,
            executable="/bin/bash",
        )
        if code != 0:
            raise Exception(f"dasgoclient query failed:\n{out}")

        return [
            line.strip()
            for line in out.strip().split("\n")
            if line.strip().endswith(".root")
        ]

    def iter_nano_files(
        self,
        task: AnalysisTask | DatasetTask,
        fs: str | Sequence[str] | None = None,
        lfn_indices: list[int] | None = None,
        eager_lookup: bool | int = 1,
    ) -> None:
        """
        Generator function that reduces the boilerplate code for looping over files referred to by
        *lfn_indices* given the lfns obtained by *this* task which needs to be complete for this
        function to succeed.

        When *lfn_indices* are not given, *task* must be a branch of a :py:class:`DatasetTask`
        workflow whose branch value is used instead.

        :param task: Current task that needs to access the nanoAOD files
        :param fs: Name of the local or remote file system where the LFNs are located, defaults to None
        :param lfn_indices: List of indices of LFNs that are processed by this *task* instance, defaults to None
        :param eager_lookup: Look at the next fs if stat takes too long, defaults to 1
        :raises TypeError: If *task* is not of type :external+law:py:class:`~law.workflow.base.BaseWorkflow` or not
            a task analyzing a single branch in the task tree
        :raises Exception: If current task is not complete as indicated with ``self.complete()``
        :raises ValueError: If no fs is provided at call and none can be found in either the config instance or the law
            config.
        :raises Exception: If a given LFN cannot be found at any fs
        :yield: a file target that points to a LFN
        """
        # input checks
        if not lfn_indices:
            if not isinstance(task, law.BaseWorkflow) or not task.is_branch():
                raise TypeError(f"task must be a workflow branch, but got {task}")
            lfn_indices = task.branch_data
        if not self.complete():
            raise Exception(f"{self} is required to be complete")

        # prepare fs names to resolve lfns with
        if not fs:
            # use an optional hook in the config
            get_fs = self.config_inst.x("get_dataset_lfns_remote_fs", None)
            if callable(get_fs):
                fs = get_fs(self.dataset_inst)
        if not fs:
            # use the law config
            fs = law.config.get_expanded("outputs", "lfn_sources", [], split_csv=True)
        if not fs:
            raise ValueError("no fs given or found to resolve lfns")
        fs = law.util.make_list(fs)

        # get all lfns
        output = self.output()
        target = (output.random_target() if isinstance(output, law.TargetCollection) else output)
        lfns = target.load(formatter="json")

        # loop
        for lfn_index in lfn_indices:
            task.publish_message(f"handling file {lfn_index}")

            # get the lfn of the file referenced by this file index
            lfn = str(lfns[lfn_index])

            # get the input file
            i = 0
            last_working = None
            while i < len(fs):
                selected_fs = fs[i]
                logger.debug(f"checking fs {selected_fs} for lfn {lfn}")

                # check if the fs is really remote or local
                fs_base = law.config.get_expanded(selected_fs, "base")
                is_local = law.target.file.get_scheme(fs_base) in (None, "file")
                logger.debug(f"fs {selected_fs} is {'local' if is_local else 'remote'}")
                target_cls = law.LocalFileTarget if is_local else law.wlcg.WLCGFileTarget

                # measure the time required to perform the stat query
                logger.debug(f"checking fs {selected_fs} for lfn {lfn}")
                input_file = target_cls(lfn.lstrip(os.sep) if is_local else lfn, fs=selected_fs)
                t1 = time.perf_counter()
                input_stat = input_file.exists(stat=True)
                duration = time.perf_counter() - t1
                i += 1
                logger.info(f"file {lfn} does{'' if input_stat else ' not'} exist at fs {selected_fs}")

                # when the stat query took longer than some duration, eagerly try the next fs
                # and check if it responds faster and if so, take it instead
                latency = 4.0  # s
                if input_stat and eager_lookup:
                    if (
                        isinstance(eager_lookup, int) and
                        not isinstance(eager_lookup, bool) and
                        i <= eager_lookup
                    ):
                        logger.debug(f"eager fs lookup skipped for fs {selected_fs} at index {i}")
                    else:
                        if input_stat and not last_working and duration > latency and i < len(fs):
                            last_working = selected_fs, input_file, input_stat, duration
                            logger.debug(
                                f"duration exceeded {latency}s, checking next fs for comparison",
                            )
                            continue
                        if last_working and (not input_stat or last_working[3] < duration):
                            logger.debug("previously checked fs responded faster")
                            selected_fs, input_file, input_stat, duration = last_working

                # stop when the stat was successful at this point
                if input_stat:
                    task.publish_message(
                        f"using fs {selected_fs}, stat responded in "
                        f"{law.util.human_duration(seconds=duration)}",
                    )
                    break
            else:
                raise Exception(f"LFN {lfn} not found at any remote fs {fs}")

            # log the file size
            input_size = law.util.human_bytes(input_stat.st_size, fmt=True)
            task.publish_message(f"lfn {lfn}, size is {input_size}")

            yield (lfn_index, input_file)


GetDatasetLFNsWrapper = wrapper_factory(
    base_cls=AnalysisTask,
    require_cls=GetDatasetLFNs,
    enable=["configs", "skip_configs", "datasets", "skip_datasets", "shifts", "skip_shifts"],
    attributes={"version": None},
    docs="""
Wrapper task to get LFNs for multiple datasets.

:enables: ["configs", "skip_configs", "datasets", "skip_datasets", "shifts", "skip_shifts"]
:overwrites: attribute ``version`` with None
""",
)


class BundleExternalFiles(ConfigTask, law.tasks.TransferLocalFile):
    """
    Task to collect external files.

    This task is intended to download source files for other tasks, such as files containing
    corrections for objects, the "golden" json files, source files for the calculation of pileup
    weights, and others.

    All information about the relevant external files is extracted from the given ``config_inst``,
    which must contain the keyword ``external_files`` in the auxiliary information. This can look
    like this:

    .. code-block:: python

        # cfg is the current config instance
        cfg.x.external_files = DotDict.wrap({
        # The following assumes that the zip files are reachable under the
        # url ``SOURCE_URL``
        # jet energy correction
        "jet_jerc": (f"{SOURCE_URL}/POG/JME/{year}{corr_postfix}_UL/jet_jerc.json.gz", "v1"),

        # tau energy correction and scale factors
        "tau_sf": (f"{SOURCE_URL}/POG/TAU/{year}{corr_postfix}_UL/tau.json.gz", "v1"),

        # electron scale factors
        "electron_sf": (f"{SOURCE_URL}/POG/EGM/{year}{corr_postfix}_UL/electron.json.gz", "v1"),

    The entries in this DotDict can either be simply the path to the source files or can be a tuple
    of the format ``(path/or/url/to/source/file, VERSION)`` to introduce a versioning mechanism for
    external files.
    """

    replicas = luigi.IntParameter(
        default=5,
        description="number of replicas to generate; default: 5",
    )
    user = user_parameter_inst
    version = None

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # cached hash
        self._files_hash = None

        # cached dictionary with the same structure as external files, mapping to unique basenames
        self._file_names = None

        # cached dict for lazy access to files in fetched bundle
        self.files_dir = None
        self._files = None

    @classmethod
    def create_unique_basename(cls, path: tuple[str] | str) -> str:
        """
        Create a unique basename.

        :param path: path to create a unique basename for
        :return: Unique basename
        """
        h = law.util.create_hash(path)
        basename = os.path.basename(path[0] if isinstance(path, tuple) else path)
        return f"{h}_{basename}"

    @property
    def files_hash(self) -> str:
        """
        Create a hash based on all external files.

        :return: Hash based on the flattened list of external files in the current config instance.
        """
        if self._files_hash is None:
            # take the external files and flatten them into a deterministic order, then hash
            def deterministic_flatten(d):
                return [
                    (key, (deterministic_flatten(d[key]) if isinstance(d[key], dict) else d[key]))
                    for key in sorted(d)
                ]
            flat_files = deterministic_flatten(self.config_inst.x.external_files)
            self._files_hash = law.util.create_hash(flat_files)

        return self._files_hash

    @property
    def file_names(self) -> DotDict:
        """
        Create a unique basename for each external file.

        :return: DotDict of same shape as ``external_files`` DotDict with unique basenames.
        """
        if self._file_names is None:
            self._file_names = law.util.map_struct(
                self.create_unique_basename,
                self.config_inst.x.external_files,
            )

        return self._file_names

    def get_files(self, output=None):
        if self._files is None:
            # get the output
            if not output:
                output = self.output()
            if not output.exists():
                raise Exception(
                    f"accessing external files from the bundle requires the output of {self} to "
                    "exist, but it appears to be missing",
                )
            if isinstance(output, law.FileCollection):
                output = output.random_target()
            self.files_dir = law.LocalDirectoryTarget(is_tmp=True)
            output.load(self.files_dir, formatter="tar")

            # resolve basenames in the bundle directory and map to file targets
            def resolve_basename(unique_basename):
                return self.files_dir.child(unique_basename, type="f")

            self._files = law.util.map_struct(resolve_basename, self.file_names)

        return self._files

    @property
    def files(self):
        return self.get_files()

    def single_output(self):
        # required by law.tasks.TransferLocalFile
        return self.target(f"externals_{self.files_hash}.tgz")

    @law.decorator.log
    @law.decorator.safe_output
    def run(self):
        # create a tmp dir to work in
        tmp_dir = law.LocalDirectoryTarget(is_tmp=True)
        tmp_dir.touch()

        # progress callback
        n_files = len(law.util.flatten(self.config_inst.x.external_files))
        progress = self.create_progress_callback(n_files)

        # helper function to fetch generic files
        def fetch_file(src, counter=[0]):
            dst = os.path.join(tmp_dir.path, self.create_unique_basename(src))
            src = src[0] if isinstance(src, tuple) else src
            if src.startswith(("http://", "https://")):
                # download via wget
                wget(src, dst)
            else:
                # must be a local file
                shutil.copy2(src, dst)
            # log
            self.publish_message(f"fetched {src}")
            progress(counter[0])
            counter[0] += 1

        # fetch all files
        law.util.map_struct(fetch_file, self.config_inst.x.external_files)

        # create the bundle
        tmp = law.LocalFileTarget(is_tmp="tgz")
        tmp.dump(tmp_dir, formatter="tar")

        # log the file size
        bundle_size = law.util.human_bytes(tmp.stat().st_size, fmt=True)
        self.publish_message(f"bundle size is {bundle_size}")

        # transfer the result
        self.transfer(tmp)
