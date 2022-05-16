# coding: utf-8

"""
Task to define objects such as electrons, muons and jets and selections, returns masks for each object and each selection
"""

import math

import law

from ap.tasks.framework import DatasetTask, HTCondorWorkflow
from ap.util import ensure_proxy
from collections import defaultdict

import ap.config.functions as fcts


class SelectEvents(DatasetTask, law.LocalWorkflow, HTCondorWorkflow):

    sandbox = "bash::$AP_BASE/sandboxes/cmssw_default.sh"


    def requires(self):
        # workflow branches are normal tasks, so define requirements the normal way
        return {"lfns": GetDatasetLFNs.req(self)}

    def output(self):
        return {
            "mask": self.local_target(f"masks_{self.branch}.pickle"),
            "stats": self.local_target(f"stats_{self.branch}.json"),
            "data": self.local_target(f"data_{self.branch}.json"), # remove later
        }
    @law.decorator.safe_output
    @ensure_proxy
    def run(self):
        import numpy as np
        import awkward as ak
        c = self.config_inst

        # get all lfns
        lfns = self.input()["lfns"].random_target().load(formatter="json")

        # prepare output arrays to be concated
        output_arrays = []
        data_output = []
        stats = defaultdict(float)

        # loop over all input file indices requested by this branch
        for file_index in self.branch_data:
            # get the lfn of the file referenced by this file index
            lfn = str(lfns[file_index])
            self.publish_message(f"file {file_index}: fround {lfn}")

            # always use the INFN redirector for now
            input_file = law.wlcg.WLCGFileTarget(lfn, fs="wlcg_fs_infn")

            # open with uproot
            with self.publish_step("loading content with uproot ..."):
                data = input_file.load(formatter="uproot") # 52 seconds
                events = data["Events"]
                self.publish_message(f"file {file_index}: found {events.num_entries} events")

            # readout all fields of interest, define electrons, muons, jets
            step_size = 100000 # 1000 -> 31 seconds; 1000000 -> 4.3 seconds; 100000 -> 5.2 seconds
            steps = int(math.ceil(events.num_entries / step_size))
            events = events.iterate(["Muon_pt", "Muon_eta", "Muon_tightId", "Electron_pt", "Electron_eta", "Electron_cutBased", "Jet_pt", "Jet_eta", "Jet_btagDeepFlavB", "HLT_IsoMu27", "HLT_Ele27_WPTight_Gsf", "LHEWeight_originalXWGTUP"], step_size=step_size)
            for batch in self.iter_progress(events, steps, msg=f"file {file_index}: select ..."):
                print("batch")

                obj_indices = {}
                for obj in c.x.objects: 
                    obj_indices[obj] = getattr(fcts, c.x.objects[obj])(batch)

                masks_selections = {}
                event_sel = True
                for sel in c.x.selections:
                    mask_sel = getattr(fcts, sel)(batch)
                    masks_selections[sel] = mask_sel
                    event_sel = (event_sel) & (mask_sel)

                # also include categories here?
                
                stats["n_events"] += len(batch)
                stats["n_events_selected"] += ak.sum(event_sel, axis=0)
                stats["sum_mc_weight"] += ak.sum(batch.LHEWeight_originalXWGTUP)
                stats["sum_mc_weight_selected"] += ak.sum(batch.LHEWeight_originalXWGTUP[event_sel])

                output = ak.zip({
                    "event": event_sel,
                    "eventweights": batch.LHEWeight_originalXWGTUP,
                    "object": ak.zip(obj_indices, depth_limit=1),
                    "step": ak.zip(masks_selections),
                })
                print(type(output))
                output_arrays.append(output) # list of awkward.highlevel.Arrays
                data_output.append(batch) # save data just to simplify implementation for now

        print("----------------------")
        print(type(output_arrays))
        print(len(output_arrays))
        final_output = np.concatenate(output_arrays, axis=0) # awkward.highlevel.Array
        print(type(data))
        self.output()["mask"].dump(final_output, formatter="pickle")
        self.output()["stats"].dump(stats, formatter="json")
        self.output()["data"].dump(np.concatenate(data_output, axis=0), formatter="pickle")



# trailing imports
from ap.tasks.external import GetDatasetLFNs
