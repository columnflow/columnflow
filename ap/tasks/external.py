# coding: utf-8

"""
Tasks dealing with external data.
"""

__all__ = []


import subprocess
from typing import Union, Tuple, List

import luigi
import law

from ap.tasks.framework import DatasetTask
from ap.util import ensure_proxy


class GetDatasetLFNs(DatasetTask, law.tasks.TransferLocalFile):

    sandbox = "bash::/cvmfs/cms.cern.ch/cmsset_default.sh"

    replicas = luigi.IntParameter(
        default=5,
        description="number of replicas to generate; default: 5",
    )
    version = None

    def single_output(self):
        # required by law.tasks.TransferLocalFile
        h = law.util.create_hash(list(sorted(self.dataset_info_inst.keys)))
        return self.local_target(f"lfns_{h}.json")

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

        if len(lfns) != self.dataset_info_inst.n_files:
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
        for file_index in branch_task.branch_data:
            branch_task.publish_message(f"handling file {file_index}")

            # get the lfn of the file referenced by this file index
            lfn = str(lfns[file_index])

            # get the input file
            for fs in remote_fs:
                input_file = law.wlcg.WLCGFileTarget(lfn, fs=fs)
                input_stat = input_file.exists(stat=True)
                if input_stat:
                    break
            else:
                raise Exception(f"LFN {lfn} not found at any remote fs {remote_fs}")

            # log the file size
            input_size = law.util.human_bytes(input_stat.st_size, fmt=True)
            branch_task.publish_message(f"lfn {lfn}, size is {input_size}")

            yield (file_index, input_file)
