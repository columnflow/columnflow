# coding: utf-8

"""
Tasks dealing with external data.
"""

from __future__ import annotations

import os
import time
import shutil
import subprocess
from typing import Sequence

import luigi
import law
import order as od

from columnflow.tasks.framework.base import AnalysisTask, ConfigTask, DatasetTask, wrapper_factory
from columnflow.util import wget


logger = law.logger.get_logger(__name__)


class GetDatasetLFNs(DatasetTask, law.tasks.TransferLocalFile):

    replicas = luigi.IntParameter(
        default=5,
        description="number of replicas to generate; default: 5",
    )
    validate = law.OptionalBoolParameter(
        default=None,  # set to True as long as we are doing tests with reduced n_files in datasets
        significant=False,
        description="when True, complains if the number of obtained LFNs does not match the value "
        "expected from the dataset info; default: obtained from 'validate_dataset_lfns' auxiliary "
        "entry in config",
    )
    version = None

    @classmethod
    def resolve_param_values(cls, params):
        params = super().resolve_param_values(params)

        # add the default calibrator when empty
        if "config_inst" in params and params.get("validate") is None:
            config_inst = params["config_inst"]
            params["validate"] = config_inst.x("validate_dataset_lfns", False)

        return params

    @property
    def sandbox(self):
        sandbox = self.config_inst.x("get_dataset_lfns_sandbox", None)
        return sandbox or "bash::/cvmfs/cms.cern.ch/cmsset_default.sh"

    def single_output(self):
        # required by law.tasks.TransferLocalFile
        h = law.util.create_hash(list(sorted(self.dataset_info_inst.keys)))
        return self.target(f"lfns_{h}.json")

    @law.decorator.log
    def run(self):
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
        remote_fs: str | Sequence[str] | None = None,
        lfn_indices: list[int] | None = None,
        eager_lookup: bool | int = 1,
    ) -> None:
        """
        Generator function that reduces the boilerplate code for looping over files referred to by
        *lfn_indices* given the lfns obtained by *this* task which needs to be complete for this
        function to succeed.

        When *lfn_indices* are not given, *task* must be a branch of a :py:class:`DatasetTask`
        workflow whose branch value is used instead.

        Iterating yields a 2-tuple (file index, input file) where the latter is either a
        :py:class:`law.LocalFileTarget` or a :py:class:`law.wlcg.WLCGFileTarget` with its fs set to
        *remote_fs*. When a sequence is passed, the fs names are evaluated in that order and the
        first existing one is generally used. However, if *eager_lookup* is *True*, in case the stat
        request to a fs was successful but took longer than two seconds, the next fs is eagerly
        checked and used in case it responded with less delay. In case *eager_lookup* is an integer,
        this check is only performed after the *eager_lookup*th fs.
        """
        # input checks
        if not lfn_indices:
            if not isinstance(task, law.BaseWorkflow) or not task.is_branch():
                raise TypeError(f"task must be a workflow branch, but got {task}")
            lfn_indices = task.branch_data
        if not self.complete():
            raise Exception(f"{self} is required to be complete")

        # prepare the remote fs names to resolve lfns with
        if not remote_fs:
            # use an optional hook in the config
            get_remote_fs = self.config_inst.x("get_dataset_lfns_remote_fs", None)
            if callable(get_remote_fs):
                remote_fs = get_remote_fs(task.dataset_inst)
        if not remote_fs:
            # use the law config
            remote_fs = law.config.get_expanded("outputs", "lfn_sources", split_csv=True)
        if not remote_fs:
            raise ValueError("no remote_fs given or found to resolve lfns")
        remote_fs = law.util.make_list(remote_fs)

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
            while i < len(remote_fs):
                fs = remote_fs[i]
                logger.debug(f"checking fs {fs} for lfn {lfn}")

                # check if the fs is really remote or local
                fs_base = law.config.get_expanded(fs, "base")
                is_local = law.target.file.get_scheme(fs_base) in (None, "file")
                logger.debug(f"fs {fs} is {'local' if is_local else 'remote'}")
                target_cls = law.LocalFileTarget if is_local else law.wlcg.WLCGFileTarget

                # measure the time required to perform the stat query
                logger.debug(f"checking fs {fs} for lfn {lfn}")
                input_file = target_cls(lfn.lstrip(os.sep) if is_local else lfn, fs=fs)
                t1 = time.perf_counter()
                input_stat = input_file.exists(stat=True)
                duration = time.perf_counter() - t1
                i += 1
                logger.info(f"file {lfn} does{'' if input_stat else ' not'} exist at fs {fs}")

                # when the stat query took longer than 2 seconds, eagerly try the next fs
                # and check if it responds faster and if so, take it instead
                if input_stat and eager_lookup:
                    if (
                        isinstance(eager_lookup, int) and
                        not isinstance(eager_lookup, bool) and
                        i <= eager_lookup
                    ):
                        logger.debug(f"eager fs lookup skipped for fs {fs} at index {i}")
                    else:
                        if input_stat and not last_working and duration > 2.0 and i < len(remote_fs):
                            last_working = fs, input_file, input_stat, duration
                            logger.debug("duration exceeded 2s, checking next fs for comparison")
                            continue
                        if last_working and (not input_stat or last_working[3] < duration):
                            logger.debug("previously checked fs responded faster")
                            fs, input_file, input_stat, duration = last_working

                # stop when the stat was successful at this point
                if input_stat:
                    task.publish_message(
                        f"using fs {fs}, stat responded in "
                        f"{law.util.human_duration(seconds=duration)}",
                    )
                    break
            else:
                raise Exception(f"LFN {lfn} not found at any remote fs {remote_fs}")

            # log the file size
            input_size = law.util.human_bytes(input_stat.st_size, fmt=True)
            task.publish_message(f"lfn {lfn}, size is {input_size}")

            yield (lfn_index, input_file)


GetDatasetLFNsWrapper = wrapper_factory(
    base_cls=AnalysisTask,
    require_cls=GetDatasetLFNs,
    enable=["configs", "skip_configs", "datasets", "skip_datasets", "shifts", "skip_shifts"],
    attributes={"version": None},
)


class BundleExternalFiles(ConfigTask, law.tasks.TransferLocalFile):

    replicas = luigi.IntParameter(
        default=5,
        description="number of replicas to generate; default: 5",
    )
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
    def create_unique_basename(cls, path):
        h = law.util.create_hash(path)
        basename = os.path.basename(path[0] if isinstance(path, tuple) else path)
        return f"{h}_{basename}"

    @property
    def files_hash(self):
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
    def file_names(self):
        if self._file_names is None:
            self._file_names = law.util.map_struct(
                self.create_unique_basename,
                self.config_inst.x.external_files,
            )

        return self._file_names

    @property
    def files(self):
        if self._files is None:
            # get the output
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
