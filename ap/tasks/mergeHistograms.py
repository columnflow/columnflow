# coding: utf-8

"""
Task to merge histogram files
"""

import math

import law
import luigi

from functools import reduce
from law.contrib.tasks import ForestMerge

from ap.tasks.framework import DatasetTask, HTCondorWorkflow
from ap.util import ensure_proxy

class MergeHistograms(ForestMerge, DatasetTask, law.LocalWorkflow, HTCondorWorkflow):

    sandbox = "bash::$AP_BASE/sandboxes/cmssw_default.sh"

    #processes = law.CSVParameter(description="List of processes")
    #variables = law.CSVParameter(description="List of variables to plot")
    

    merge_factor = 16

    def merge_workflow_requires(self):
        return FillHistograms.req(self)

    def merge_requires(self, start_leaf, end_leaf):
        return [FillHistograms.req(self, branch=i) for i in range(start_leaf, end_leaf)]
        #return{
        #    [process: [FillHistograms.req(self, branch=i, dataset=process.datasets()) for i in range(start_leaf, end_leaf)] for process in processes]
        #}

    def merge_output(self):
        return self.local_target(f"histograms_merged.pickle")
        #return self.local_target(f"histograms_{self.branch_data['process']}")


    def merge(self, inputs, output):
        #import awkward as ak
        import hist
        
        merged = {}

        # config
        #config = self.config_inst

        print(type(inputs))
        inputs_list = [i.load(formatter="pickle") for i in inputs]

        inputs_dict = {k:[el[k] for el in inputs_list] for k in inputs_list[0].keys()}
        
        for k in inputs_dict.keys():
            print(k)
            print(type(inputs_dict[k]))
            h_out = inputs_dict[k][0]
            for i,h_in in enumerate(inputs_dict[k]):
                if(i==0):
                    continue
                print(type(h_in))
                h_out += h_in
            merged[k] = h_out

        for k in merged.keys():
            print(k)
            print(type(merged[k]))


        output.dump(merged, formatter="pickle")
    



# trailing imports
from ap.tasks.fillHistograms import FillHistograms
