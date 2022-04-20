# coding: utf-8

"""
Tasks dealing with external data.
"""

__all__ = []


import subprocess

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
