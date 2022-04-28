# coding: utf-8

"""
Task to produce the first histograms
"""

import math

import law

from ap.tasks.framework import DatasetTask, HTCondorWorkflow
from ap.util import ensure_proxy


class FillHistograms(DatasetTask, law.LocalWorkflow, HTCondorWorkflow):

    sandbox = "bash::$AP_BASE/sandboxes/cmssw_default.sh"
    #variables = law.CSVParameter(description="List of variables to define hists for")


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

        # stuff that can be used from order
        config = self.config_inst
        analysis = self.get_analysis_inst(self.analysis)
        categories = config.categories #analysis.get_categories(config)
        leaf_categories = config.get_leaf_categories()
        print(type(leaf_categories))
        processes = config.processes #analysis.get_processes(config)
        variables = config.variables #analysis.get_variables(config)


        events = self.input()["data"].load(formatter="pickle")
        selection = self.input()["selection"].load(formatter="pickle")
        print(type(events))
        print(type(selection))
        
        #events[cat1.selection]

        # declare output: dict of histograms
        output = {}

        mask = selection["trigger_sel"] # how to properly initialize mask?
        print(type(mask))
        # for now, just apply all selections
        for sel in ak.fields(selection):
            print(sel)
            mask = (mask) & (selection[sel])
        
        #events = events[(selection.trigger_sel) & (selection.lep_sel) & (selection.jet_sel) & (selection.bjet_sel)]
        events = events[mask]


        # variables of interest: needs to be defined in a previous task?
        HT = ak.sum(events.Jet_pt, axis=1)

        lepton_pt = np.where(events["nMuon"]==1, events["Muon_pt"], events["Electron_pt"]) # this works only if there's either only electrons or only muons
        lepton_eta = np.where(events["nMuon"]==1, events["Muon_eta"], events["Electron_eta"]) # this works only if there's either only electrons or only muons

        for var in self.config_inst.variables:
            print(var.get_full_title())
            print(var.get_mpl_hist_data())

        
        # determine which event belongs in which category
        # this should probably be included in an earlier task... (part of defineSelection?)

        # I'm looping over all categories twice... a bit inefficient?
        cat_dict = {}
        for cat in leaf_categories:
            sel = cat.selection
            print(sel)
            mask = eval(sel)(events)
            cat_array = np.where(mask)
            cat_dict[cat.name] = mask

        # translate dict of booleans into array of strings (assuming that each event is part of max. 1 category, should be checked somewhere)
        cat_array = "no_cat"
        for k in cat_dict.keys():
            cat_array = np.where(cat_dict[k], k, cat_array)
        print(cat_array)
        print(cat_dict)
        print([k for k in cat_dict.keys()])
        print("-----")
        print(type(events["nJet"]))
        print(type(cat_array))
              
        # define histograms
        h_nJet = (
            hist.Hist.new
            #.StrCategory(["st","tt","data"], name="process")
            .StrCategory([k for k in cat_dict.keys()], name="category")
            .Reg(8, -.5, 7.5, name="nJet", label="$N_{jet}$")
            .Weight()
        )
        h_Jet1_pt = (
            hist.Hist.new
            .StrCategory([k for k in cat_dict.keys()], name="category")
            .Reg(40, 0, 200, name="pt_j1", label="$p_{T, j1}$")
            .Weight()
        )
        h_Jet2_pt = (
            hist.Hist.new
            .StrCategory([k for k in cat_dict.keys()], name="category")
            .Reg(40, 0, 200, name="pt_j2", label="$p_{T, j2}$")
            .Weight()
        )
        h_nLep = (
            hist.Hist.new
            .StrCategory([k for k in cat_dict.keys()], name="category")
            .Reg(8, -.5, 7.5, name="nLep", label="$N_{lep}$")
            .Weight()
        )
        h_Lep_pt = (
            hist.Hist.new
            .StrCategory([k for k in cat_dict.keys()], name="category")
            .Reg(40, 0, 200, name="pt_lep", label="$p_{T, lep}$")
            .Weight()
        )
        h_Lep_eta = (
            hist.Hist.new
            .StrCategory([k for k in cat_dict.keys()], name="category")
            .Reg(40, 0, 200, name="eta_lep", label="$\eta_{lep}$")
            .Weight()
        )
        h_HT = (
            hist.Hist.new
            .StrCategory([k for k in cat_dict.keys()], name="category")
            .Reg(40,0,800, name="HT", label="HT")
            .Weight()
        )

        print("-----")
        # weight should be read out from config with order
        # sampleweight = Lumi / Lumi_sim = Lumi * xsec / Number_MC
        # weight = sampleweight * eventweight ,(eventweight=genWeight? Generator_weight? LHEWeight_originalXWGTUP?)
        weight = 1


        # manually fill histograms
        h_nJet.fill(
            category=cat_array,
            nJet=events["nJet"],
            weight=weight
        )
        h_Jet1_pt.fill(
            category=cat_array,
            pt_j1=events["Jet_pt"][:,0], # nJet>0 is given by selection
            weight=weight
        )
        h_Jet2_pt.fill(
            category=cat_array,
            pt_j2=events["Jet_pt"][(events.nJet>1)][:,1], # nJet>1 required to fill this histogram
            weight=weight
        )
        h_nLep.fill(
            category=cat_array,
            nLep=events["nElectron"]+events["nMuon"],
            weight=weight
        )
        h_Lep_pt.fill(
            category=cat_array,
            pt_lep=lepton_pt[:,0],
            weight=weight
        )
        h_Lep_eta.fill(
            category=cat_array,
            eta_lep=lepton_eta[:,0],
            weight=weight
        )
        h_HT.fill(
            category=cat_array,
            HT=HT,
            weight=weight
        )

        output["nJet"] = h_nJet
        output["Jet1_pt"] = h_Jet1_pt
        output["Jet2_pt"] = h_Jet2_pt
        output["nLep"] = h_nLep
        output["Lep_pt"] = h_Lep_pt
        output["Lep_eta"] = h_Lep_eta
        output["HT"] = h_HT

        print("-----")

        self.output().dump(output, formatter="pickle")



# trailing imports
from ap.tasks.defineObjects import DefineObjects
from ap.tasks.defineSelection import DefineSelection

