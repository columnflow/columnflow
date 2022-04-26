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

    #process = luigi.ChoiceParameter(choices=self.get_analysis_inst(self.analysis).get_processes(an.config_2018).names())
    processes = law.CSVParameter(description="List of processes to plot")
    variables = law.CSVParameter(description="List of variables to plot")
    

    def create_branch_map(self):
        return {i: {"variable": var} for i, var in enumerate(self.variables)}



    #def workflow_requires(self):
    #    reqs = super(FillHistograms, self).workflow_requires()
    #    reqs["data"] = DefineObjects.req(self)
    #    reqs["selection"] = DefineSelection.req(self)
    #    return reqs

    def requires(self):
        return {
            "histograms": {
                "st": FillHistograms.req(self, branch=0, dataset="st_tchannel_t"),
                "tt": FillHistograms.req(self, branch=0, dataset="tt_sl"),
            }
        }
        
    def output(self):
        return self.local_target(f"plots_{self.branch_data['variable']}.pdf")
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

        #processes = [self.get_analysis_inst(self.analysis).get_processes(self.config_inst).get(p) for p in self.processes]

       # histograms = {p: self.input()["histograms"][p].load(formatter="pickle") for p in self.processes}

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

        histograms = []

        colors = []


        for p in self.processes:
            process = self.get_analysis_inst(self.analysis).get_processes(self.config_inst).get(p)
            histogram = self.input()["histograms"][p].load(formatter="pickle")[self.branch_data['variable']]
            colors.append(process.color)
            print(type(histogram))
            #histograms[p] = histogram
            histograms.append(histogram)


        h_final = hist.Stack(*histograms)
        #h_final = hist.Stack.from_dict(histograms)

        h_final.plot(
            ax=ax,
            stack=True,
            histtype="fill",
            #alpha=0.5,
            edgecolor=(0, 0, 0, 0.3),
            label=self.processes,
            color=colors,
        )
        
        ax.set_ylabel("Counts")
        ax.legend(title="Processes")

        plt.savefig(self.output().path)

# trailing imports
from ap.tasks.fillHistograms import FillHistograms
