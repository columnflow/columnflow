# coding: utf-8

"""
Task to produce the first histograms
"""

import math

import law

from ap.tasks.framework import DatasetTask, HTCondorWorkflow
from ap.util import ensure_proxy

import ap.config.functions as fcts


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


        events = self.input()["data"].load(formatter="pickle")
        selection = self.input()["selection"].load(formatter="pickle")
        print(type(events))
        print(type(selection))
        

        mask = selection["trigger_sel"] # how to properly initialize mask?
        print(type(mask))
        # for now, just apply all selections
        for sel in ak.fields(selection):
            print(sel)
            mask = (mask) & (selection[sel])
        
        #events = events[(selection.trigger_sel) & (selection.lep_sel) & (selection.jet_sel) & (selection.bjet_sel)]
        events = events[mask]

        # determine which event belongs in which category
        # this could be included in an earlier task... (part of defineSelection? Returning a mask for each selection? Or an array of strings (requires the categories to be orthogonal))
        cat_titles = []
        cat_array = "no_cat"
        for cat in leaf_categories:
            sel = cat.selection
            cat_titles.append(sel)
            #mask = eval(sel)(events)
            mask = getattr(fcts, sel)(events)
            cat_array = np.where(mask, sel, cat_array)

        

        # declare output: dict of histograms
        output = {}

        # weight should be read out from config with order
        # sampleweight = Lumi / Lumi_sim = Lumi * xsec / Number_MC
        # weight = sampleweight * eventweight ,(eventweight=genWeight? Generator_weight? LHEWeight_originalXWGTUP?)
        weight = 1 # dummy

        for var in config.variables:
            print(var.name)
            h_var = (
                hist.Hist.new
                .StrCategory(cat_titles, name="category")
                .Reg(*var.binning, name=var.name, label=var.get_full_x_title())
                .Weight()
            )
            #print("fill histograms:")
            fill_kwargs = {
                "category": cat_array,
                var.name: getattr(fcts, var.expression)(events),
                "weight": weight,
            }
            h_var.fill(**fill_kwargs)
            output[var.name] = h_var


        self.output().dump(output, formatter="pickle")



# trailing imports
from ap.tasks.defineObjects import DefineObjects
from ap.tasks.defineSelection import DefineSelection

