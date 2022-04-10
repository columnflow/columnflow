# coding: utf-8

"""
Tasks related to obtaining, preprocessing and selecting events.
"""

import math

import law

from ap.tasks.framework import DatasetTask, HTCondorWorkflow
from ap.util import ensure_proxy


class SelectEvents(DatasetTask, law.LocalWorkflow, HTCondorWorkflow):

    sandbox = "bash::$AP_BASE/sandboxes/cmssw_default.sh"

    shifts = {"jec_up", "jec_down"}

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

        # get all lfns
        lfns = self.input()["lfns"].random_target().load(formatter="json")

        # prepare output arrays to be concated
        output_arrays = []

        # loop over all input file indices requested by this branch
        for file_index in self.branch_data:
            # get the lfn of the file referenced by this file index
            lfn = str(lfns[file_index])
            self.publish_message(f"file {file_index}: fround {lfn}")

            # always use the INFN redirector for now
            input_file = law.wlcg.WLCGFileTarget(lfn, fs="wlcg_fs_infn")

            # open with uproot
            with self.publish_step("loading content with uproot ..."):
                data = input_file.load(formatter="uproot")
                events = data["Events"]
                self.publish_message(f"file {file_index}: found {events.num_entries} events")

            # dummy task: get all jet 1 pt values
            step_size = 1000
            steps = int(math.ceil(events.num_entries / step_size))
            events = events.iterate(["nJet", "Jet_pt"], step_size=step_size)
            for batch in self.iter_progress(events, steps, msg=f"file {file_index}: select ..."):
                print("batch")
                mask = batch["nJet"] >= 1
                jet_pt = batch["Jet_pt"][mask][:, 0]

                # emulate jec
                jec_factor = {"jec_up": 1.05, "jec_down": 0.95}.get(self.effective_shift, 1.0)
                jet_pt = jet_pt * jec_factor

                # store the jet pt
                output_arrays.append(jet_pt)

        data = np.concatenate(output_arrays, axis=0)
        self.output().dump(data=data, formatter="numpy")


# trailing imports
from ap.tasks.external import GetDatasetLFNs
