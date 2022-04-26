# coding: utf-8

"""
Tasks to define selectios and return the corresponding masks, which can later be applied on data
"""

import math

import law

from ap.tasks.framework import DatasetTask, HTCondorWorkflow
from ap.util import ensure_proxy

import ap.config.analysis_st as an

class DefineSelection(DatasetTask, law.LocalWorkflow, HTCondorWorkflow):

    sandbox = "bash::$AP_BASE/sandboxes/cmssw_default.sh"

    #def workflow_requires(self):
    #    reqs = super(DefineSelection, self).workflow_requires()
    #    reqs["lfns"] = DefineObjects.req(self)
    #    return reqs

    def requires(self):
        return DefineObjects.req(self)
        
    def output(self):
        return self.local_target(f"selection_{self.branch}.pickle")
        #return self.wlcg_target(f"selection_{self.branch}.pickle")

    @law.decorator.safe_output
    @ensure_proxy
    def run(self):
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



# trailing imports
from ap.tasks.defineObjects import DefineObjects
