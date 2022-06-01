# coding: utf-8

"""
Task to merge histogram files
"""

import law
import luigi

from ap.tasks.framework import ConfigTask, DatasetTask, HTCondorWorkflow

from ap.tasks.histograms import CreateHistograms


class MergeHistograms(DatasetTask, law.tasks.ForestMerge, HTCondorWorkflow):

    # sandbox = "bash::$AP_BASE/sandboxes/cmssw_default.sh"
    sandbox = "bash::$AP_BASE/sandboxes/venv_columnar.sh"

    merge_factor = 10

    shifts = CreateHistograms.shifts

    @classmethod
    def modify_param_values(cls, params):
        params = cls._call_super_cls_method(DatasetTask.modify_param_values, params)
        params = cls._call_super_cls_method(law.tasks.ForestMerge.modify_param_values, params)
        return params

    def create_branch_map(self):
        # DatasetTask implements a custom branch map, but we want to use the one in ForestMerge
        return law.tasks.ForestMerge.create_branch_map(self)

    def merge_workflow_requires(self):
        # return CreateHistograms.req(self, _exclude=['branches'])
        return CreateHistograms.req(self, _exclude=['branches'], branches=[23])

    def merge_requires(self, start_leaf, end_leaf):
        return [CreateHistograms.req(self, branch=i) for i in range(start_leaf, end_leaf)]

    def merge_output(self):
        return self.local_target(f"histograms_{self.dataset}.pickle")

    def merge(self, inputs, output):
        with self.publish_step("Hello from MergeHistograms"):
            merged = {}
            inputs_list = [i.load(formatter="pickle") for i in inputs]
            inputs_dict = {k: [el[k] for el in inputs_list] for k in inputs_list[0].keys()}

            for k in inputs_dict.keys():
                h_out = inputs_dict[k][0]
                for i, h_in in enumerate(inputs_dict[k]):
                    if(i == 0):
                        continue
                    h_out += h_in
                merged[k] = h_out

            output.dump(merged, formatter="pickle")


class MergeShiftograms(ConfigTask, law.LocalWorkflow, HTCondorWorkflow):

    # sandbox = "bash::$AP_BASE/sandboxes/cmssw_default.sh"
    sandbox = "bash::$AP_BASE/sandboxes/venv_columnar.sh"

    dataset = luigi.Parameter(
        default="st_tchannel_t",
        description="dataset"
    )
    systematics = law.CSVParameter(
        default=("jec"),
        description="List of systematic uncertainties to consider"
    )

    def workflow_requires(self):
        syst_map = super(MergeShiftograms, self).workflow_requires()
        syst_map["nominal"] = MergeHistograms.req(self, shift="nominal")
        for s in self.systematics:
            print(s)
            syst_map[s + "_up"] = MergeHistograms.req(self, shift=s + "_up")
            syst_map[s + "_down"] = MergeHistograms.req(self, shift=s + "_down")
        print(syst_map)
        return syst_map

    def requires(self):
        syst_map = {}
        syst_map["nominal"] = MergeHistograms.req(self, shift="nominal")
        for s in self.systematics:
            syst_map[s + "up"] = MergeHistograms.req(self, shift=s + "_up")
            syst_map[s + "down"] = MergeHistograms.req(self, shift=s + "_down")
        return syst_map

    def create_branch_map(self):
        return {0: self.dataset}

    def store_parts(self):
        parts = super(MergeShiftograms, self).store_parts()
        systs = ""
        for s in self.systematics:
            systs += s + "_"
        systs = systs[:-1]
        parts.insert_after("dataset", "systematics", systs)
        return parts

    # naming scheme for output? how to include all systematics?
    def output(self):
        return self.local_target(f"shiftograms_{self.dataset}.pickle")

    def run(self):
        with self.publish_step("Hello from MergeShiftograms"):
            merged = {}
            print(self.input().keys())
            syst_keys = self.input().keys()
            print(syst_keys)
            inputs_list = [self.input()[i].load(formatter="pickle") for i in syst_keys]
            inputs_dict = {k: [el[k] for el in inputs_list] for k in inputs_list[0].keys()}

            for k in inputs_dict.keys():
                h_out = inputs_dict[k][0]
                for i, h_in in enumerate(inputs_dict[k]):
                    if(i == 0):
                        continue
                    h_out += h_in
                merged[k] = h_out

            self.output().dump(merged, formatter="pickle")
