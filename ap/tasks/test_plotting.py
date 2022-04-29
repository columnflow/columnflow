# coding: utf-8

"""
Task to quickly plot data from branch 0
"""

import math

import law
import luigi

from ap.tasks.framework import ConfigTask, HTCondorWorkflow
from ap.util import ensure_proxy

class TestPlotting(ConfigTask, law.LocalWorkflow, HTCondorWorkflow):

    sandbox = "bash::$AP_BASE/sandboxes/cmssw_default.sh"

    #processes = law.CSVParameter(description="List of processes to plot")
    variables = law.CSVParameter(description="List of variables to plot")
    

    def create_branch_map(self):
        return {i: {"variable": var} for i, var in enumerate(self.variables)}



    def requires(self):
        return FillHistograms.req(self, branch=0)
        
    def output(self):
        return self.local_target(f"plot_{self.branch_data['variable']}.pdf")

    @law.decorator.safe_output
    @ensure_proxy
    def run(self):
        import hist
        import matplotlib.pyplot as plt
        import mplhep
        plt.style.use(mplhep.style.CMS)

        histograms = self.input().load(formatter="pickle")
        print(type(histograms))
        histogram = self.input().load(formatter="pickle")[self.branch_data['variable']]

        fix,ax = plt.subplots()
        histogram.plot(
            ax=ax,
            stack=True,
            histtype="fill",
            #edgecolor=(0, 0, 0, 0.3),
            #label="dummy",
        )
        
        ax.set_ylabel("Counts")
        ax.legend(title="Processes")

        plt.savefig(self.output().path)

# trailing imports
from ap.tasks.fillHistograms import FillHistograms
