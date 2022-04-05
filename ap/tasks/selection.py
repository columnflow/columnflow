# coding: utf-8

"""
Tasks related to obtaining, preprocessing and selecting events.
"""

import law

from ap.tasks.framework import DatasetTask, HTCondorWorkflow
from ap.util import ensure_proxy


class SelectEvents(DatasetTask, law.LocalWorkflow, HTCondorWorkflow):

    sandbox = "bash::$AP_BASE/sandboxes/cmssw_default.sh"

    def workflow_requires(self):
        # workflow super classes might already define requirements, so extend them
        reqs = super(SelectEvents, self).workflow_requires()
        reqs["lfns"] = GetDatasetLFNs.req(self)
        return reqs

    def requires(self):
        # workflow branches are normal tasks, so define requirements the normal way
        return {"lfns": GetDatasetLFNs.req(self)}

    def output(self):
        return self.wlcg_target(f"data_{self.branch}.npz")

    @law.decorator.safe_output
    @ensure_proxy
    def run(self):
        import numpy as np

        # get the lfn of the file referenced by this branch
        lfn = str(self.input()["lfns"].random_target().load(formatter="json")[self.branch_data[0]])
        self.publish_message(f"found LFN {lfn}")

        # describe the input file by a target and open it right away
        input_file = law.wlcg.WLCGFileTarget(lfn, fs="wlcg_fs_infn")

        # open with uproot
        with self.publish_step("loading content with uproot ..."):
            data = input_file.load(formatter="uproot")
            events = data["Events"]
            self.publish_message(f"found {events.num_entries} events")

        # dummy task: get all jet 1 pt values
        jet1_pt = []
        with self.publish_step("load jet pts ..."):
            for batch in events.iterate(["nJet", "Jet_pt"], step_size=1000):
                print("batch")
                mask = batch["nJet"] >= 1
                jet1_pt.append(batch["Jet_pt"][mask][:, 0])

        jet1_pt = np.concatenate(jet1_pt, axis=0)
        self.output().dump(data=jet1_pt, formatter="numpy")


# trailing imports
from ap.tasks.external import GetDatasetLFNs
