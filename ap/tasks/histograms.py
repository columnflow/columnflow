# coding: utf-8

"""
Task to produce and merge histograms
"""

import law
import luigi

from ap.tasks.framework import ConfigTask, HTCondorWorkflow
from ap.util import ensure_proxy, dev_sandbox

from ap.tasks.selection import SelectedEventsConsumer
from ap.tasks.reduction import ReduceEvents


class CreateHistograms(SelectedEventsConsumer, law.LocalWorkflow, HTCondorWorkflow):

    sandbox = dev_sandbox("bash::$AP_BASE/sandboxes/venv_columnar.sh")

    shifts = ReduceEvents.shifts

    def workflow_requires(self):
        reqs = super(CreateHistograms, self).workflow_requires()
        reqs["events"] = ReduceEvents.req(self)
        return reqs

    def requires(self):
        return {
            "events": ReduceEvents.req(self),
        }

    def output(self):
        return self.local_target(f"histograms_{self.branch}.pickle")

    @law.decorator.safe_output
    @law.decorator.localize
    @ensure_proxy
    def run(self):
        import hist
        from ap.columnar_util import (
            ChunkedReader, mandatory_coffea_columns,
        )
        from ap.selection import Selector

        # prepare inputs and outputs
        inputs = self.input()

        # declare output: dict of histograms
        histograms = {}

        # create a temp dir for saving intermediate files
        tmp_dir = law.LocalDirectoryTarget(is_tmp=True)
        tmp_dir.touch()

        # get shift dependent aliases
        # aliases = self.shift_inst.x("column_aliases", {})

        # define nano columns that need to be loaded
        variables = Selector.get("variables")
        load_columns = set(mandatory_coffea_columns) | variables.used_columns

        # iterate over chunks of events and diffs
        with ChunkedReader(
            [inputs["events"].path],
            source_type=["awkward_parquet"],
            read_options=[{"iteritems_options": {"filter_name": load_columns}}],
        ) as reader:
            msg = f"iterate through {reader.n_entries} events ..."
            for events, pos in self.iter_progress(reader, reader.n_chunks, msg=msg):
                print(events)
                print(len(events))
                print(events[0].fields)

                events = events[0]

                # weights
                lumi = self.config_inst.x.luminosity.get("nominal")
                sampleweight = lumi / self.config_inst.get_dataset(self.dataset).n_events
                weight = sampleweight * events.LHEWeight.originalXWGTUP

                results = variables(events)

                # get all viable category ids (only leaf categories)
                # cat_ids = []
                # for cat in self.config_inst.get_leaf_categories():
                #    cat_ids.append(cat.id)

                # define & fill histograms
                var_names = self.config_inst.variables.names()
                with self.publish_step("looping over all variables in config ...."):
                    for var_name in var_names:
                        with self.publish_step("var: %s" % var_name):
                            var = self.config_inst.variables.get(var_name)
                            h_var = (
                                hist.Hist.new
                                # .IntCat(cat_ids, name="category")  # , growth=True)
                                # quick fix to access categories correct in plot task
                                .IntCat(range(0, 10), name="category")
                                .StrCategory([], name="shift", growth=True)
                                .Var(var.bin_edges, name=var_name, label=var.get_full_x_title())
                                .Weight()
                            )
                            fill_kwargs = {
                                "category": events.cat_array,
                                "shift": self.shift,
                                var_name: results.columns[var_name],
                                "weight": weight,
                            }
                            print(results.columns[var_name])
                            print(self.shift)
                            h_var.fill(**fill_kwargs)
                            if var_name in histograms:
                                histograms[var_name] += h_var
                            else:
                                histograms[var_name] = h_var

        # merge output files
        self.output().dump(histograms, formatter="pickle")


class MergeHistograms(SelectedEventsConsumer, law.tasks.ForestMerge, HTCondorWorkflow):

    # sandbox = "bash::$AP_BASE/sandboxes/cmssw_default.sh"
    sandbox = "bash::$AP_BASE/sandboxes/venv_columnar.sh"

    merge_factor = 10

    shifts = CreateHistograms.shifts

    @classmethod
    def modify_param_values(cls, params):
        params = cls._call_super_cls_method(SelectedEventsConsumer.modify_param_values, params)
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


# Should be ConfigTask and SelectedEventsConsumer
class MergeShiftHistograms(ConfigTask, law.LocalWorkflow, HTCondorWorkflow):

    # sandbox = "bash::$AP_BASE/sandboxes/cmssw_default.sh"
    sandbox = "bash::$AP_BASE/sandboxes/venv_columnar.sh"

    dataset = luigi.Parameter(
        default="st_tchannel_t",
        description="dataset",
    )
    shift_sources = law.CSVParameter(
        default=("jec",),
        description="List of uncertainties to consider",
    )

    def workflow_requires(self):
        syst_map = super(MergeShiftHistograms, self).workflow_requires()
        syst_map["nominal"] = MergeHistograms.req(self, shift="nominal")
        for s in self.shift_sources:
            print(s)
            syst_map[s + "_up"] = MergeHistograms.req(self, shift=s + "_up")
            syst_map[s + "_down"] = MergeHistograms.req(self, shift=s + "_down")
        print(syst_map)
        return syst_map

    def requires(self):
        syst_map = {}
        syst_map["nominal"] = MergeHistograms.req(self, shift="nominal")
        for s in self.shift_sources:
            syst_map[s + "up"] = MergeHistograms.req(self, shift=s + "_up")
            syst_map[s + "down"] = MergeHistograms.req(self, shift=s + "_down")
        return syst_map

    def create_branch_map(self):
        return {0: self.dataset}

    def store_parts(self):
        parts = super(MergeShiftHistograms, self).store_parts()
        systs = ""
        for s in self.shift_sources:
            systs += s + "_"
        systs = systs[:-1]
        parts.insert_after("dataset", "shift_sources", systs)
        return parts

    def output(self):
        return self.local_target(f"shiftograms_{self.dataset}.pickle")

    def run(self):
        with self.publish_step("Hello from MergeShiftHistograms"):
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
