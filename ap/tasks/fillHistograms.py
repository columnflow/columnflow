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
    #    default = law.NO_STR,
    #    description="List of variables to define hists for"
    #)

    '''
    def workflow_requires(self):
        #workflow super classes might already define requirements, so extend them
        reqs = super(FillHistograms, self).workflow_requires()
        reqs["data"] = DefineObjects.req(self)
        reqs["selection"] = DefineSelection.req(self)
        return reqs
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
        with self.publish_step("FillHistograms run ..."):

            import numpy as np
            import awkward as ak
            import hist

            # stuff that can be used from order
            config = self.config_inst
            analysis = self.get_analysis_inst(self.analysis)
            #categories = config.categories
            #processes = config.processes
        
            # need process to read out xsec
            process = config.get_process(self.dataset)
            # need dataset to read out number of MC events
            dataset = config.get_dataset(self.dataset)

            events = self.input()["data"].load(formatter="pickle")
            selection = self.input()["selection"].load(formatter="pickle")


            mask = True
            # just apply all selections (to fill workflow histograms, create a parallel task)
            for sel in ak.fields(selection):
                mask = (mask) & (selection[sel])
            events = events[mask]
            

            # determine which event belongs in which category
            # this could be included in an earlier task... (part of defineSelection? Returning a mask for each category? Or an array of strings (requires the categories to be orthogonal))
            cat_titles = []
            cat_array = "no_cat"
            mask_int = 0
            for cat in self.config_inst.get_leaf_categories():
                sel = cat.selection
                cat_titles.append(cat.name)
                mask = getattr(fcts, sel)(events)
                cat_array = np.where(mask, cat.name, cat_array)
                mask_int = mask_int + np.where(mask,1,0) # to check orthogonality of categories
            if not ak.all(mask_int==1):
                if ak.any(mask_int>=2):
                    raise ValueError('Leaf categories are supposed to be fully orthogonal')
                else:
                    raise ValueError('Some events are without leaf category')
                    #print('Some events are without leaf category')
        

            # declare output: dict of histograms
            output = {}
        
            sampleweight = config.x.luminosity * process.get_xsec(config.campaign.ecm).get() / dataset.n_events # get_xsec() gives not a number but number+-uncert
            eventweight = events["LHEWeight_originalXWGTUP"] / process.get_xsec(config.campaign.ecm).get()
            print("sampleweight = %f" %sampleweight)
            print("eventweight = ", eventweight)
            weight = sampleweight * eventweight
            print("weight = ", weight)
        
            #print(ak.all(ak.isclose(events["LHEWeight_originalXWGTUP"],events["Generator_weight"])))
            #print(ak.all(ak.isclose(events["LHEWeight_originalXWGTUP"],events["genWeight"])))


            variables = config.variables.names()
            with self.publish_step("looping over all variables in config ...."):
                for var_name in variables:
                    with self.publish_step("var: ", var_name):
                        var = config.variables.get(var_name)
                        h_var = (
                            hist.Hist.new
                            .StrCategory(cat_titles, name="category")
                            .Reg(*var.binning, name=var_name, label=var.get_full_x_title())
                            .Weight()
                        )
                        fill_kwargs = {
                            "category": cat_array,
                            var_name: getattr(fcts, var.expression)(events),
                            "weight": weight,
                        }
                        h_var.fill(**fill_kwargs)
                        output[var_name] = h_var
                
                
            self.output().dump(output, formatter="pickle")
            self.publish_message(f"FillHistograms run method done")


# trailing imports
from ap.tasks.defineObjects import DefineObjects
from ap.tasks.defineSelection import DefineSelection

