# coding: utf-8

"""
Task to plot the first histograms
"""

import math

import law

from ap.tasks.framework import DatasetTask, HTCondorWorkflow
from ap.util import ensure_proxy

class FillHistograms(DatasetTask, law.LocalWorkflow, HTCondorWorkflow):

    sandbox = "bash::$AP_BASE/sandboxes/cmssw_default.sh"

    #def workflow_requires(self):
    #    reqs = super(FillHistograms, self).workflow_requires()
    #    reqs["data"] = DefineObjects.req(self)
    #    reqs["selection"] = DefineSelection.req(self)
    #    return reqs

    def requires(self):
        return {
            "data": DefineObjects.req(self),
            "selection": DefineSelection.req(self)
        }
        
    def output(self):
        return self.local_target(f"histograms_{self.branch}.pickle")
        #return self.wlcg_target(f"data_{self.branch}.pickle")

    @law.decorator.safe_output
    @ensure_proxy
    def run(self):
        import numpy as np
        import awkward as ak
        import hist

        events = self.input()["data"].load(formatter="pickle")
        selection = self.input()["selection"].load(formatter="pickle")
        print(type(events))
        print(type(selection))

        # how to mask without changing the size of 'events':
        #events.mask[selection.trigger_sel]
        
        # for now, just apply all selections
        events = events[(selection.trigger_sel) & (selection.lep_sel) & (selection.jet_sel) & (selection.bjet_sel)]

        # define variables of interest
        HT = ak.sum(events.Jet_pt, axis=1)

        lepton_pt = np.where(events["nMuon"]==1, events["Muon_pt"], events["Electron_pt"]) # this works only if there's either only electrons or only muons

        # define histogram
        h_eventVars = (
            hist.Hist.new
            #.StrCat(["st", "tt", "data"], name="sample", label="Sample")
            .Reg(8, -0.5, 7.5, name ="N_jet", label="$N_{jet}$")
            .Reg(40, 0, 200, name="pt_j1", label="$p_{T, j1}$")
            #.Reg(40, 0, 200, name="pt_j2", label="$p_{T, j2}$")
            #.Reg(40, 0, 200, name="pt_j3", label="$p_{T, j3}$")
            .Reg(40, 0, 800, name="HT", label="HT")
            .Reg(5, -0.5, 4.5, name ="N_lep", label="$N_{lep}$")
            .Reg(40, 0, 200, name="pt_lep", label="$p_{T, lep}$")
            .Weight()
        )

        # h_jets
        # h_jet1, h_jet2, ...
        # h_lep?
        
        h_eventVars.fill(
            #sample="st", # sample type should be read out here using Order
            N_jet=events["nJet"],
            N_lep=events["nElectron"]+events["nMuon"],
            pt_j1=events["Jet_pt"][:,0],
            #pt_j2=ak.mask(events, ak.num(events.Jet_pt, axis=1) > 1).Jet_pt[:,1], # allow_missing parameter needs to be set true: but how?
            #pt_j3=ak.mask(events, ak.num(events.Jet_pt, axis=1) > 2).Jet_pt[:,2],
            HT=HT,
            pt_lep=lepton_pt[:,0],
            weight=1 # dummy, should be read out here using Order
        )
    
        self.output().dump(h_eventVars, formatter="pickle")



# trailing imports
from ap.tasks.defineObjects import DefineObjects
from ap.tasks.defineSelection import DefineSelection

