# coding: utf-8

"""
Tasks to define selectios and return the corresponding masks, which can later be applied on data
"""

import math

import law

from ap.tasks.framework import DatasetTask, HTCondorWorkflow
from ap.util import ensure_proxy

import ap.config.analysis_st as an
import ap.config.functions as fcts

class DefineSelection(DatasetTask, law.LocalWorkflow, HTCondorWorkflow):

    sandbox = "bash::$AP_BASE/sandboxes/cmssw_default.sh"
    '''
    def workflow_requires(self):
        #workflow super classes might already define requirements, so extend them
        reqs = super(DefineSelection, self).workflow_requires()
        reqs["data"] = DefineObjects.req(self)
        return reqs
    '''
    def requires(self):
        return DefineObjects.req(self)
        
    def output(self):
        return self.local_target(f"selection_{self.branch}.pickle")

    @law.decorator.safe_output
    @ensure_proxy
    def run(self):        
        with self.publish_step("Hello from DefineSelection run ..."):

            import numpy as np
            import awkward as ak

            events = self.input().load(formatter="pickle")
            print(type(events))
            print(type(an.selections))
        
        
            selection = {} # dict of numpy array
            for sel in an.selections:
                print(sel)
                print(type(sel))
                selection[sel] = an.selections[sel](events)

            selection = ak.zip(selection) # convert dict to awkward array
            print(type(selection))
            self.output().dump(selection, formatter="pickle") # numpy formatter should also work



class Categorization(DatasetTask, law.LocalWorkflow, HTCondorWorkflow):

    sandbox = "bash::$AP_BASE/sandboxes/cmssw_default.sh"

    def requires(self):
        return DefineObjects.req(self)
        
    def output(self):
        return self.local_target(f"categories_{self.branch}.pickle")

    @law.decorator.safe_output
    @ensure_proxy
    def run(self):
        with self.publish_step("Hello from Categorization run ..."):
            import numpy as np
            import awkward as ak

            events = self.input().load(formatter="pickle")

            # determine which event belongs in which category
            cat_titles = []
            cat_array = "no_cat"
            mask_int = 0
            for cat in self.config_inst.get_leaf_categories():
                sel = cat.selection
                cat_titles.append(sel)
                mask = getattr(fcts, sel)(events)
                cat_array = np.where(mask, sel, cat_array)
                mask_int = mask_int + np.where(mask,1,0) # to check orthogonality of categories
            #print(mask_int)
            if not ak.all(mask_int==1):
                if len(mask_int[mask_int==0])>0:
                    if ak.any(mask_int>=2):
                        raise ValueError('Leaf categories are supposed to be fully orthogonal')
                    else:
                        #raise ValueError('Some events are without leaf category')
                        print('Some events are without leaf category')
                        # some events are without leaf category since selections are expected to be applied...
            
            self.output().dump(cat_array, formatter="pickle") # numpy formatter should also work


# trailing imports
from ap.tasks.defineObjects import DefineObjects
