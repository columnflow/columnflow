# coding: utf-8

"""
Tasks dealing with external data.
"""

__all__ = []


import os
import shutil
import subprocess
from typing import Union, Tuple, List

import luigi
import law

from ap.tasks.framework import AnalysisTask, ConfigTask, DatasetTask, wrapper_factory
from ap.util import ensure_proxy, wget


class GetDatasetLFNs(DatasetTask, law.tasks.TransferLocalFile):

    sandbox = "bash::/cvmfs/cms.cern.ch/cmsset_default.sh"

    replicas = luigi.IntParameter(
        default=5,
        description="number of replicas to generate; default: 5",
    )
    skip_check = luigi.BoolParameter(
        default=False,
        significant=False,
        description="whether to skip the check of the number of obtained LFNs vs. expected ones; "
        "default: False",
    )
    version = None

    def single_output(self):
        # required by law.tasks.TransferLocalFile
        h = law.util.create_hash(list(sorted(self.dataset_info_inst.keys)))
        return self.local_target(f"lfns_{h}.json")

    @law.decorator.safe_output
    @ensure_proxy
    def run(self):
        lfns = []
        for key in sorted(self.dataset_info_inst.keys):
            self.logger.info("get lfns for key {}".format(key))
            cmd = f"dasgoclient --query='file dataset={key}' --limit=0"
            code, out, _ = law.util.interruptable_popen(cmd, shell=True, stdout=subprocess.PIPE,
                executable="/bin/bash")
            if code != 0:
                raise Exception(f"dasgoclient query failed:\n{out}")
            lfns.extend(out.strip().split("\n"))

        if not self.skip_check and len(lfns) != self.dataset_info_inst.n_files:
            raise ValueError("number of lfns does not match number of files "
                f"for dataset {self.dataset_inst.name}")

        self.logger.info(f"found {len(lfns)} lfns for dataset {self.dataset}")

        tmp = law.LocalFileTarget(is_tmp=True)
        tmp.dump(lfns, formatter="json")
        self.transfer(tmp)

    def iter_nano_files(
        self,
        branch_task: DatasetTask,
        remote_fs: Union[str, List[str], Tuple[str]] = (
            "wlcg_fs_desy_store",
            "wlcg_fs_infn_redirector",
            "wlcg_fs_global_redirector",
        ),
    ) -> None:
        """
        Generator function that reduces the boilerplate code for looping over files referred to by
        the branch data of a law *branch_task*, given the lfns obtained by *this* task which needs
        to be complete for this function to succeed. Iterating yields a 2-tuple (file index,
        input file) where the latter is a :py:class:`law.wlcg.WLCGFileTarget` with its fs set to
        *remote_fs*. When a sequence is passed, the fs names are evaluated in that order and the
        first existing one is used.
        """
        # input checks
        if not branch_task.is_branch():
            raise TypeError(f"branch_task must be a workflow branch, but got {branch_task}")
        if not self.complete():
            raise Exception(f"{self:r} is required to be complete")
        remote_fs = law.util.make_list(remote_fs)

        # get all lfns
        output = self.output()
        target = (output.random_target() if isinstance(output, law.TargetCollection) else output)
        lfns = target.load(formatter="json")

        # loop
        for lfn_index in branch_task.branch_data:
            branch_task.publish_message(f"handling file {lfn_index}")

            # get the lfn of the file referenced by this file index
            lfn = str(lfns[lfn_index])

            # get the input file
            for fs in remote_fs:
                input_file = law.wlcg.WLCGFileTarget(lfn, fs=fs)
                input_stat = input_file.exists(stat=True)
                if input_stat:
                    branch_task.publish_message(f"using fs {fs}")
                    break
            else:
                raise Exception(f"LFN {lfn} not found at any remote fs {remote_fs}")

            # log the file size
            input_size = law.util.human_bytes(input_stat.st_size, fmt=True)
            branch_task.publish_message(f"lfn {lfn}, size is {input_size}")

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
        self._files_dir = None
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
            self._files_dir = law.LocalDirectoryTarget(is_tmp=True)
            output.load(self._files_dir, formatter="tar")

            # resolve basenames in the bundle directory and map to file targets
            def resolve_basename(unique_basename):
                return self._files_dir.child(unique_basename, type="f")

            self._files = law.util.map_struct(resolve_basename, self.file_names)

        return self._files

    def single_output(self):
        # required by law.tasks.TransferLocalFile
        return self.local_target(f"externals_{self.files_hash}.tgz")

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
