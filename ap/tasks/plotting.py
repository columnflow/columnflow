# coding: utf-8

"""
Task to plot the first histograms
"""

import math

import law
import luigi

from ap.tasks.framework import ConfigTask, HTCondorWorkflow
from ap.util import ensure_proxy

class Plotting(ConfigTask, law.LocalWorkflow, HTCondorWorkflow):

    sandbox = "bash::$AP_BASE/sandboxes/cmssw_default.sh"

    #processes = law.CSVParameter(description="List of processes to plot")
    variables = law.CSVParameter(description="List of variables to plot")
    

    def create_branch_map(self):
        return {i: {"variable": var} for i, var in enumerate(self.variables)}



    def requires(self):
        return MergeHistograms.req(self)
        
    def output(self):
        return self.local_target(f"plots_{self.branch_data['variable']}.pdf")

    @law.decorator.safe_output
    @ensure_proxy
    def run(self):
        import hist
        import matplotlib.pyplot as plt
        import mplhep
        plt.style.use(mplhep.style.CMS)

    
        histograms = []
        colors = []

        histogram = self.input().load(formatter="pickle")[self.branch_data['variable']]
        print(type(histogram))


        fix,ax = plt.subplots()
        histogram.plot(
            ax=ax,
            stack=True,
            overlay="category",
            histtype="fill",
            edgecolor=(0, 0, 0, 0.3),
            #label=self.processes,
            #color=colors,
        )
        
        ax.set_ylabel("Counts")
        ax.legend(title="Category")

        plt.savefig(self.output().path)

# trailing imports
from ap.tasks.mergeHistograms import MergeHistograms
