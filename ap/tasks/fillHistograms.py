# coding: utf-8

"""
Task to produce the first histograms
"""

import math

import luigi
import law

from ap.tasks.framework import DatasetTask, HTCondorWorkflow
from ap.util import ensure_proxy

import ap.config.functions as fcts


class FillHistograms(DatasetTask, law.LocalWorkflow, HTCondorWorkflow):

    sandbox = "bash::$AP_BASE/sandboxes/cmssw_default.sh"
    #variables = law.CSVParameter(
    #    default = None, # self.config_inst.variables.names, # can I read out config here?
    #    description="List of variables to define hists for"
    #)
    '''
    selections = law.CSVParameter(
        default = law.NO_STR,
        description = "List of selections to consider for plotting"
    )
    do_cutflow = luigi.BoolParameter(
        default = False,
        description = "Boolean whether to produce plots after each individual selection or only after applying all selections"
    )
    '''
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

        sampleweight = config.get_aux("luminosity") * process.get_xsec(config.campaign.ecm).get() / dataset.n_events # get_xsec() gives not a number but number+-uncert
        eventweight = events["LHEWeight_originalXWGTUP"] / process.get_xsec(config.campaign.ecm).get()
        print("sampleweight = %f" %sampleweight)
        print("eventweight = ", eventweight)
        weight = sampleweight * eventweight
        print("weight = ", weight)
        
        #print(ak.all(ak.isclose(events["LHEWeight_originalXWGTUP"],events["Generator_weight"])))
        #print(ak.all(ak.isclose(events["LHEWeight_originalXWGTUP"],events["genWeight"])))

        
        #if(self.variables==None):
        #    self.variables = config.variables.names()
        variables = config.variables.names()
        for var_name in variables:
            print(var_name)
            var = config.variables.get(var_name)
            h_var = (
                hist.Hist.new
                .StrCategory(dataset_names, name="dataset")
                .StrCategory(cat_titles, name="category")
                .Reg(*var.binning, name=var_name, label=var.get_full_x_title())
                .Weight()
            )
            #print("fill histograms:")
            fill_kwargs = {
                "category": cat_array,
                "dataset": self.dataset, 
                var_name: getattr(fcts, var.expression)(events),
                "weight": weight,
            }
            h_var.fill(**fill_kwargs)
            output[var_name] = h_var


        self.output().dump(output, formatter="pickle")



# trailing imports
from ap.tasks.defineObjects import DefineObjects
from ap.tasks.defineSelection import DefineSelection

