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
    #variables = law.CSVParameter(
        #default = self.config_inst.variables.names, # can I read out config here?
    #    description="List of variables to define hists for"
    #)



    def requires(self):
        return {
            "data": DefineObjects.req(self),
            "selection": DefineSelection.req(self)
        }
        
    def output(self):
        return self.local_target(f"histograms_{self.branch}.pickle")

    @law.decorator.safe_output
    @ensure_proxy
    def run(self):
        import numpy as np
        import awkward as ak
        import hist

        # stuff that can be used from order
        config = self.config_inst
        analysis = self.get_analysis_inst(self.analysis)
        #categories = config.categories
        print(type(leaf_categories))
        #processes = config.processes


        print("dataset: %s" % self.dataset)
        # need process to read out xsec
        process = config.get_process(self.dataset)
        # need dataset to read out number of MC events
        dataset = config.get_dataset(self.dataset)
        dataset_names = analysis.get_datasets(config).names()

        events = self.input()["data"].load(formatter="pickle")
        selection = self.input()["selection"].load(formatter="pickle")
        #print(type(events))
        #print(type(selection))
        

        mask = True
        print(type(mask))
        # for now, just apply all selections
        for sel in ak.fields(selection):
            print(sel)
            mask = (mask) & (selection[sel])
        print(mask)
        #events = events[(selection.trigger_sel) & (selection.lep_sel) & (selection.jet_sel) & (selection.bjet_sel)]
        events = events[mask]

        # determine which event belongs in which category
        # this could be included in an earlier task... (part of defineSelection? Returning a mask for each category? Or an array of strings (requires the categories to be orthogonal))
        cat_titles = []
        cat_array = "no_cat"
        mask_int = 0
        for cat in self.config_inst.get_leaf_categories():
            sel = cat.selection
            cat_titles.append(sel)
            mask = getattr(fcts, sel)(events)
            cat_array = np.where(mask, sel, cat_array)
            mask_int = mask_int + np.where(mask,1,0) # to check orthogonality of categories
        print(mask_int)
        if not ak.all(mask_int==1):
            if len(mask_int[mask_int==0])>0:
                raise ValueError('Some events are without leaf category')
            else:
                raise ValueError('Leaf categories are supposed to be fully orthogonal')
        

        # declare output: dict of histograms
        output = {}

        # weight should be read out from config with order
        # weight = sampleweight * eventweight ,(eventweight=genWeight? Generator_weight? LHEWeight_originalXWGTUP?)
        # lumi where?
        sampleweight = 59740 * process.get_xsec(config.campaign.ecm).get() / dataset.n_events # get_xsec() gives not a number but number+-uncert
        eventweight = 1 # dummy
        weight = sampleweight * eventweight
        print("weight = %f" % weight)
        for var in config.variables:
            print(var.name)
            h_var = (
                hist.Hist.new
                .StrCategory(dataset_names, name="dataset")
                .StrCategory(cat_titles, name="category")
                .Reg(*var.binning, name=var.name, label=var.get_full_x_title())
                .Weight()
            )
            #print("fill histograms:")
            fill_kwargs = {
                "category": cat_array,
                "dataset": self.dataset, 
                var.name: getattr(fcts, var.expression)(events),
                "weight": weight,
            }
            h_var.fill(**fill_kwargs)
            output[var.name] = h_var


        self.output().dump(output, formatter="pickle")



# trailing imports
from ap.tasks.defineObjects import DefineObjects
from ap.tasks.defineSelection import DefineSelection

