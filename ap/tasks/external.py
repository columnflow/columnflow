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
        h = law.util.create_hash(sorted(self.dataset_info_inst.keys))
        return self.wlcg_target("lfns_{}.json".format(h))

    @ensure_proxy
    def run(self):
        lfns = []
        for key in self.dataset_info_inst.keys:
            self.logger.info("get lfns for key {}".format(key))
            cmd = "dasgoclient --query='file dataset={}' --limit=0".format(key)
            code, out, _ = law.util.interruptable_popen(cmd, shell=True, stdout=subprocess.PIPE,
                executable="/bin/bash")
            if code != 0:
                raise Exception("dasgoclient query failed:\n{}".format(out))
            lfns.extend(out.strip().split("\n"))

        if len(lfns) != self.dataset_info_inst.n_files:
            raise ValueError("number of lfns does not match number of files "
                "for dataset {}".format(self.dataset_inst.name))

        self.logger.info("found {} lfns for dataset {}".format(len(lfns), self.dataset))

        tmp = law.LocalFileTarget(is_tmp="json")
        tmp.dump(lfns)
        self.transfer(tmp)
