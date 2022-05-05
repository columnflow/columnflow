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

    datasets = law.CSVParameter(
        default = law.NO_STR,
        description="List of datasets to plot"
    )
    variables = law.CSVParameter(
        default = law.NO_STR,
        description="List of variables to plot"
    )

    def create_branch_map(self):
        print('Hello from create_branch_map')
        print(self.variables)
        print(len(self.variables))
        if(self.variables[0]==law.NO_STR):
            self.variables = self.config_inst.variables.names()
        return {i: {"variable": var} for i, var in enumerate(self.variables)}
    
    def requires(self):
        print('Hello from requires')
        print(self.datasets)
        if(self.datasets==law.NO_STR):
            self.datasets = self.get_analysis_inst(self.analysis).get_datasets(self.config_inst).names()
        print(self.datasets)
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


        analysis = self.get_analysis_inst(self.analysis)

        h_out = None
        print("datasets: %s" % self.datasets)

        for d in self.datasets:
            h_in = self.input()[d].load(formatter="pickle")[self.branch_data['variable']]
            h_in = h_in[:,::sum,:] # for now: summing over categories
            if(h_out == None):
                h_out = h_in
            h_out+= h_in

        
        fix,ax = plt.subplots()

        colors = [p.color for p in self.get_analysis_inst(self.analysis).get_processes(self.config_inst)] # only the colors for the parent processes st and tt

        h_out.plot(
            ax=ax,
            stack=True,
            histtype="fill",
            color = colors,
        )
        
        lumi = mplhep.cms.label(ax=ax, lumi=59740, label="WiP")


        ax.set_ylabel("Counts")
        ax.legend(title="Category")

        plt.savefig(self.output().path)

# trailing imports
from ap.tasks.mergeHistograms import MergeHistograms
