# coding: utf-8

"""
Task to quickly plot data from branch 0 (skips MergeHistograms)
"""

import math

import law
import luigi

from ap.tasks.framework import ConfigTask, HTCondorWorkflow
from ap.util import ensure_proxy

class TestPlotting(ConfigTask, law.LocalWorkflow, HTCondorWorkflow):

    sandbox = "bash::$AP_BASE/sandboxes/cmssw_default.sh"

    #processes = law.CSVParameter(description="List of processes to plot")
    datasets = law.CSVParameter(description="List of datasets to plot")
    variables = law.CSVParameter(description="List of variables to plot")
    

    def create_branch_map(self):
        return {i: {"variable": var} for i, var in enumerate(self.variables)}



    #def requires(self):
    #    return FillHistograms.req(self, branch=0)
    
    def requires(self):
        return {d: FillHistograms.req(self, branch=0, dataset=d) for d in self.datasets}
   
    def output(self):
        return self.local_target(f"plot_{self.branch_data['variable']}.pdf")

    @law.decorator.safe_output
    @ensure_proxy
    def run(self):
        import hist
        import matplotlib.pyplot as plt
        import mplhep
        plt.style.use(mplhep.style.CMS)

        #histograms = self.input().load(formatter="pickle")
        #print(type(histograms))
        h_out = None
        for d in self.datasets:
            h_in = self.input()[d].load(formatter="pickle")[self.branch_data['variable']]
            h_in = h_in[:,::sum,:] # for now: summing over categories
            if(h_out == None):
                h_out = h_in
            h_out+= h_in

        
        fix,ax = plt.subplots()

        colors = [p.color for p in self.get_analysis_inst(self.analysis).get_processes(self.config_inst)]

        h_out.plot(
            ax=ax,
            stack=True,
            histtype="fill",
            #edgecolor=(0, 0, 0, 0.3),
            #label="dummy",
            color = colors,
        )
        
        ax.set_ylabel("Counts")
        ax.legend(title="Processes")

        plt.savefig(self.output().path)

# trailing imports
from ap.tasks.fillHistograms import FillHistograms
