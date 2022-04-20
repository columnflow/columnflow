# coding: utf-8

"""
Task to define objects such as electrons, muons and jets
"""

import math

import law

from ap.tasks.framework import DatasetTask, HTCondorWorkflow
from ap.util import ensure_proxy


class ObjectDefinition(DatasetTask, law.LocalWorkflow, HTCondorWorkflow):

    sandbox = "bash::$AP_BASE/sandboxes/cmssw_default.sh"

    shifts = {"jec_up", "jec_down"}

    #def workflow_requires(self):
        # workflow super classes might already define requirements, so extend them
    #    reqs = super(ObjectDefinition, self).workflow_requires()
    #    reqs["lfns"] = GetDatasetLFNs.req(self)
    #    return reqs

    def requires(self):
        # workflow branches are normal tasks, so define requirements the normal way
        return {"lfns": GetDatasetLFNs.req(self)}

    def output(self):
        return self.local_target(f"data_{self.branch}.pickle")
        #return self.wlcg_target(f"data_{self.branch}.pickle")

    @law.decorator.safe_output
    @ensure_proxy
    def run(self):
        import numpy as np
        import awkward as ak

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
                data = input_file.load(formatter="uproot") # 52 seconds
                events = data["Events"]
                self.publish_message(f"file {file_index}: found {events.num_entries} events")

            # readout all fields of interest, define electrons, muons, jets
            step_size = 1000000 # 1000 -> 31 seconds; 1000000 -> 4.3 seconds
            steps = int(math.ceil(events.num_entries / step_size))
            events = events.iterate(["nMuon", "nElectron", "Muon_pt", "Muon_eta", "Muon_tightId", "Electron_pt", "Electron_eta", "Electron_cutBased", "nJet", "Jet_pt", "Jet_eta", "Jet_btagDeepFlavB", "HLT_IsoMu27", "HLT_Ele27_WPTight_Gsf"], step_size=step_size)
            for batch in self.iter_progress(events, steps, msg=f"file {file_index}: select ..."):
                print("batch")

                # Electrons
                pt_e = batch["Electron_pt"]
                eta_e = batch["Electron_eta"]
                Id_e = (batch["Electron_cutBased"] == 4) #(tight Id)

                mask_e = (pt_e > 30) & (eta_e < 2.4) & (Id_e)
                

                batch["Electron_pt"] = pt_e[mask_e]
                batch["Electron_eta"] = eta_e[mask_e]
                batch["Electron_cutBased"] = Id_e[mask_e]
                batch["nElectron"] = ak.num(pt_e[mask_e], axis=1)
                
                ## Muons
                pt_mu = batch["Muon_pt"]
                eta_mu = batch["Muon_eta"]
                Id_mu = batch["Muon_tightId"] #(tight Id)

                mask_mu = (pt_mu > 30) & (eta_mu < 2.4) & (Id_mu)
                #print(mask_mu)
                
                batch["Muon_pt"] = pt_mu[mask_mu]
                batch["Muon_eta"] = eta_mu[mask_mu]
                batch["Muon_tightId"] = Id_mu[mask_mu]
                batch["nMuon"] = ak.num(pt_mu[mask_mu], axis=1)

                ## Jets
                pt_j = batch["Jet_pt"]
                eta_j = batch["Jet_eta"]

                mask_j = (pt_j > 30) & (eta_j < 2.4)
                #print(mask_j)
                
                batch["Jet_pt"] = pt_j[mask_j]
                batch["Jet_eta"] = eta_j[mask_j]
                batch["Jet_btagDeepFlavB"] = batch["Jet_btagDeepFlavB"][mask_j]
                #need to apply the 'jet mask' on all Jet branches, sth like: 
                #batch[Jet_*] = batch[Jet_*][mask_j]
                batch["nJet"] = ak.num(pt_j[mask_j], axis=1)
                #print(batch["nJet"])

                ## BJets
                b_score = batch["Jet_btagDeepFlavB"]
                #print(batch["Jet_btagDeepFlavB"])
                mask_b = (batch["Jet_btagDeepFlavB"] > 0.3) # random value
                batch["nDeepjet"] = ak.num(batch["Jet_pt"][mask_b])
                # we could also build other branches for the Deepjet category, but I suppose that's not really necessary right now


                output_arrays.append(batch) # list of awkward.highlevel.Arrays

        print("-------")
        print(type(output_arrays))
        print(len(output_arrays))
        data = np.concatenate(output_arrays, axis=0) # awkward.highlevel.Array
        print(type(data))
        self.output().dump(data, formatter="pickle")




# trailing imports
from ap.tasks.external import GetDatasetLFNs
