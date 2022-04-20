# coding: utf-8

"""
Tasks to define selectios and return the corresponding masks, which can later be applied on data
"""

import math

import law

from ap.tasks.framework import DatasetTask, HTCondorWorkflow
from ap.util import ensure_proxy

class ApplySelection(DatasetTask, law.LocalWorkflow, HTCondorWorkflow):

    sandbox = "bash::$AP_BASE/sandboxes/cmssw_default.sh"

    def workflow_requires(self):
        reqs = super(ApplySelection, self).workflow_requires()
        reqs["data"] = DefineObjects.req(self)
        reqs["selection"] = DefineSelection.req(self)
        return reqs

    def requires(self):
        return {
            "data": DefineObjects.req(self),
            "selection": DefineSelection.req(self)
        }
        
    def output(self):
        return self.local_target(f"data_{self.branch}.pickle")
        #return self.wlcg_target(f"data_{self.branch}.pickle")

    @law.decorator.safe_output
    @ensure_proxy
    def run(self):
        import numpy as np
        import awkward as ak

        events = self.input()["data"].load(formatter="pickle")
        selection = self.input()["selection"].load(formatter="pickle")
        mask = (selection["trigger_sel"] & selection["lep_sel"] & selection["jet_sel"] & selection["bjet_sel"])

        events = events[mask]
        self.output().dump(events, formatter="pickle")



# trailing imports
from ap.tasks.defineObjects import DefineObjects
from ap.tasks.defineSelection import DefineSelection
