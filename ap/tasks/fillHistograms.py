# coding: utf-8

"""
Task to plot the first histograms
"""

import math

import law

from ap.tasks.framework import DatasetTask, HTCondorWorkflow
from ap.util import ensure_proxy

import ap.config.analysis_st as an

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

        print("test: readout e-channel from config")
        print(an.config_2018.get_channel("e"))

        
        # declare output: list of histograms?
        output = []

        # how to mask without changing the size of 'events':
        #events.mask[selection.trigger_sel]
        
        # for now, just apply all selections
        events = events[(selection.trigger_sel) & (selection.lep_sel) & (selection.jet_sel) & (selection.bjet_sel)]

        # variables of interest: needs to be defined in a previous task?
        HT = ak.sum(events.Jet_pt, axis=1)

        for var in an.config_2018.variables:
            print(var.get_mpl_hist_data())
        for var in an.config_2018.variables:
            print(var.get_full_title())

        lepton_pt = np.where(events["nMuon"]==1, events["Muon_pt"], events["Electron_pt"]) # this works only if there's either only electrons or only muons
        lepton_eta = np.where(events["nMuon"]==1, events["Muon_eta"], events["Electron_eta"]) # this works only if there's either only electrons or only muons

        # define histograms
        h_nJet = (
            hist.Hist.new
            .Reg(8, -.5, 7.5, name="nJet", label="$N_{jet}$")
            .Weight()
        )
        h_Jet1_pt = (
            hist.Hist.new
            .Reg(40, 0, 200, name="pt_j1", label="$p_{T, j1}$")
            .Weight()
        )
        h_Jet2_pt = (
            hist.Hist.new
            .Reg(40, 0, 200, name="pt_j2", label="$p_{T, j2}$")
            .Weight()
        )
        '''
        h_Jet1_eta = (
            hist.Hist.new
            .Reg(40, 0, 200, name="eta_j1", label="$\eta_{j1}$")
            .Weight()
        )
        h_Jet2_eta = (
            hist.Hist.new
            .Reg(40, 0, 200, name="eta_j2", label="$\eta_{j2}$")
            .Weight()
        )
        '''
        h_nLep = (
            hist.Hist.new
            .Reg(8, -.5, 7.5, name="nLep", label="$N_{lep}$")
            .Weight()
        )
        h_Lep_pt = (
            hist.Hist.new
            .Reg(40, 0, 200, name="pt_lep", label="$p_{T, lep}$")
            .Weight()
        )
        h_Lep_eta = (
            hist.Hist.new
            .Reg(40, 0, 200, name="eta_lep", label="$\eta_{lep}$")
            .Weight()
        )
        h_HT = (
            hist.Hist.new
            .Reg(40,0,800, name="HT", label="HT")
            .Weight()
        )


        # manually fill histograms
        h_nJet.fill(
            nJet=events["nJet"],
            weight=1
        )
        h_Jet1_pt.fill(
            pt_j1=events["Jet_pt"][:,0], # nJet>0 is given by selection
            weight=1
        )
        h_Jet2_pt.fill(
            pt_j2=events["Jet_pt"][(events.nJet>1)][:,1], # nJet>1 required to fill this histogram
            weight=1
        )

        h_nLep.fill(
            nLep=events["nElectron"]+events["nMuon"],
            weight=1
        )
        h_Lep_pt.fill(
            pt_lep=lepton_pt[:,0],
            weight=1
        )
        h_Lep_eta.fill(
            eta_lep=lepton_eta[:,0],
            weight=1
        )
        h_HT.fill(
            HT=HT,
            weight=1
        )

        output.append(h_nJet)
        output.append(h_Jet1_pt)
        output.append(h_Jet2_pt)
        output.append(h_nLep)
        output.append(h_Lep_pt)
        output.append(h_Lep_eta)
        output.append(h_HT)

        '''
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
        '''

        self.output().dump(output, formatter="pickle")



# trailing imports
from ap.tasks.defineObjects import DefineObjects
from ap.tasks.defineSelection import DefineSelection

