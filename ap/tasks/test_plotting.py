# coding: utf-8

"""
Task to quickly plot data from branch 0 (skips MergeHistograms)
"""

import math

import law
import luigi

from ap.tasks.functions import functions_general as gfcts

from ap.tasks.framework import ConfigTask, HTCondorWorkflow
from ap.util import ensure_proxy

class TestPlotting(ConfigTask, law.LocalWorkflow, HTCondorWorkflow):

    sandbox = "bash::$AP_BASE/sandboxes/cmssw_default.sh"

    processes = law.CSVParameter(
        default = (),
        description="List of processes to plot"
    )

    variables = law.CSVParameter(
        #default = "nJet,HT",
        default = (),# law.NO_STR,
        description="List of variables to plot"
    )
    # for now, only consider the leaf categories
    category = luigi.Parameter( # to categories (CSV)
        default = law.NO_STR,
        description="String of leaf category to create plots for (takes all categories combined if no parameter is given)"
    )
    '''
    def store_parts(self):
        parts = super(ConfigTask, self).store_parts()

        # add the config name
        parts.insert_after("task_class", "config", self.config_inst.name)

        return parts
    '''
    def create_branch_map(self):
        print('Hello from create_branch_map')
        print(self.variables)
        print(len(self.variables))
        if not self.variables:
            self.variables = self.config_inst.variables.names()
        return {i: {"variable": var} for i, var in enumerate(self.variables)}
    
    def requires(self):
        print('Hello from requires')
        c = self.config_inst
        # check if given leaf category exists
        if not ((self.category==law.NO_STR) | (self.category in c.get_leaf_categories())):
            raise ValueError("This leaf category does not exist.")

        # determine which datasets to require
        print(self.processes)
        '''
        if self.processes:
            procs = [c.get_process(p) for p in self.processes]
        else: # take all processes
            procs = c.analysis.get_processes(c)
        #datasets = []
        '''
        self.datasets = gfcts.getDatasetNamesFromProcesses(c, self.processes)
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

        c = self.config_inst

        #h_out = None
        print("datasets: %s" % self.datasets)

        histograms = []
        colors = []
        
        print("Adding histograms together ....")
        for p in self.processes:
            # define one histogram per process and add all histograms corresponding to this process
            h_proc = None
            print("process: ", p)
            for d in gfcts.getDatasetNamesFromProcess(c, p):
                print("dataset: ", d)
                h_in = self.input()[d].load(formatter="pickle")[self.branch_data['variable']]
                if(self.category==law.NO_STR):
                    h_in = h_in[::sum,:] # summing over categories
                else:
                    h_in = h_in[self.category,:] # take the given category
                
                if(h_proc==None):
                    h_proc = h_in
                else:
                    h_proc += h_in

            histograms.append(h_proc)
            colors.append(c.get_process(p).color)

        h_final = hist.Stack(*histograms)
        fix,ax = plt.subplots()

        h_final.plot(
            ax=ax,
            stack=True,
            histtype="fill",
            label=self.processes,
            color = colors,
        )
        
        lumi = mplhep.cms.label(ax=ax, lumi=c.x.luminosity / 1000, label="WiP")

        ax.set_ylabel("Counts")
        ax.legend(title="Processes")

        self.output().dump(plt, formatter="mpl")
        #plt.savefig(self.output().path)

# trailing imports
from ap.tasks.fillHistograms import FillHistograms
