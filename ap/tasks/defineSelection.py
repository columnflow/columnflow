# coding: utf-8

"""
Tasks to define selectios and return the corresponding masks, which can later be applied on data
"""

import math

import law

from ap.tasks.framework import DatasetTask, HTCondorWorkflow
from ap.util import ensure_proxy

class DefineSelection(DatasetTask, law.LocalWorkflow, HTCondorWorkflow):

    sandbox = "bash::$AP_BASE/sandboxes/cmssw_default.sh"

    #def workflow_requires(self):
    #    reqs = super(DefineSelection, self).workflow_requires()
    #    reqs["lfns"] = DefineObjects.req(self)
    #    return reqs

    def requires(self):
        return DefineObjects.req(self)
        
    def output(self):
        return self.local_target(f"data_{self.branch}.pickle")
        #return self.wlcg_target(f"data_{self.branch}.pickle")

    @law.decorator.safe_output
    @ensure_proxy
    def run(self):
        import numpy as np
        import awkward as ak

        events = self.input().load(formatter="pickle")
        print(type(events))
        # how do I initialize a new awkward array?
        selection = events
        selection["trigger_sel"] = (events.HLT_IsoMu27) | (events.HLT_Ele27_WPTight_Gsf)
        selection["lep_sel"] = (events.nMuon + events.nElectron == 1)
        selection["jet_sel"] = (events.nJet >= 1)
        selection["bjet_sel"] = (events.nDeepjet >= 1)
        print(type(selection))
        self.output().dump(selection, formatter="pickle")



# trailing imports
from ap.tasks.defineObjects import DefineObjects
