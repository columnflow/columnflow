# coding: utf-8

"""
Task to plot the first histograms
"""

import math

import law

from ap.tasks.framework import DatasetTask, HTCondorWorkflow
from ap.util import ensure_proxy

import ap.config.analysis_st as an

class Plotting(DatasetTask, law.LocalWorkflow, HTCondorWorkflow):

    sandbox = "bash::$AP_BASE/sandboxes/cmssw_default.sh"

    #def workflow_requires(self):
    #    reqs = super(FillHistograms, self).workflow_requires()
    #    reqs["data"] = DefineObjects.req(self)
    #    reqs["selection"] = DefineSelection.req(self)
    #    return reqs

    def requires(self):
        return {
            "histograms": FillHistograms.req(self),
        }
        
    def output(self):
        return self.local_target(f"plots_{self.branch}.pdf")
        #return self.wlcg_target(f"data_{self.branch}.pickle")

    @law.decorator.safe_output
    @ensure_proxy
    def run(self):
        #import numpy as np
        #import awkward as ak
        import hist
        import matplotlib.pyplot as plt
        import mplhep
        plt.style.use(mplhep.style.CMS)

        histograms = self.input()["histograms"].load(formatter="pickle")
        
        plots = []
        '''
        for hist in histograms:
            fix,ax = plt.subplots()
            hist.plot1d(
                ax=ax,
                stack=true,
                histtype="fill",
                alpha=0.5,
                edgecolor=(0, 0, 0, 0.3),
            )
            ax.legend(title="Counts")
        '''
            
        fix,ax = plt.subplots()
        histograms[0].plot1d(
            ax=ax,
            stack=True,
            histtype="fill",
            alpha=0.5,
            edgecolor=(0, 0, 0, 0.3),
            )
        ax.legend(title="Counts")
        
        #plt.savefig('plot.pdf')

        self.output().dump(plt.savefig('plot.pdf'))

# trailing imports
from ap.tasks.fillHistograms import FillHistograms
