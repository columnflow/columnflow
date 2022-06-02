# coding: utf-8

"""
Task to produce and merge histograms.
"""

import law

from ap.tasks.framework import DatasetTask, HTCondorWorkflow
from ap.tasks.selection import SelectorMixin
from ap.tasks.reduction import ReduceEvents
from ap.util import ensure_proxy, dev_sandbox


class CreateHistograms(DatasetTask, SelectorMixin, law.LocalWorkflow, HTCondorWorkflow):

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
        from ap.columnar_util import ChunkedReader, mandatory_coffea_columns
        from ap.selection import Selector

        # prepare inputs and outputs
        inputs = self.input()

        # declare output: dict of histograms
        histograms = {}

        # create a temp dir for saving intermediate files
        tmp_dir = law.LocalDirectoryTarget(is_tmp=True)
        tmp_dir.touch()

        # define nano columns that need to be loaded
        variables = Selector.get("variables")
        load_columns = set(mandatory_coffea_columns) | variables.used_columns

        # iterate over chunks of events and diffs
        with ChunkedReader(
            inputs["events"].path,
            source_type="awkward_parquet",
            read_options={"iteritems_options": {"filter_name": load_columns}},
        ) as reader:
            msg = f"iterate through {reader.n_entries} events ..."
            for events, pos in self.iter_progress(reader, reader.n_chunks, msg=msg):
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


class MergeHistograms(DatasetTask, SelectorMixin, law.tasks.ForestMerge, HTCondorWorkflow):

    sandbox = dev_sandbox("bash::$AP_BASE/sandboxes/venv_columnar.sh")

    shifts = CreateHistograms.shifts

    # in each step, merge 10 into 1
    merge_factor = 10

    @classmethod
    def modify_param_values(cls, params):
        params = cls._call_super_cls_method(DatasetTask.modify_param_values, params)
        params = cls._call_super_cls_method(law.tasks.ForestMerge.modify_param_values, params)
        return params

    def create_branch_map(self):
        # DatasetTask implements a custom branch map, but we want to use the one in ForestMerge
        return law.tasks.ForestMerge.create_branch_map(self)

    def merge_workflow_requires(self):
        # TODO(riga): hard-coded branches for the hackathon, to be removed afterwards
        return CreateHistograms.req(self, _exclude=["branches"], branches=[(0, 2)])

    def merge_requires(self, start_leaf, end_leaf):
        return [CreateHistograms.req(self, branch=i) for i in range(start_leaf, end_leaf)]

    def merge_output(self):
        return self.local_target("histograms.pickle")

    def merge(self, inputs, output):
        with self.publish_step("Hello from MergeHistograms"):
            inputs_list = [inp.load(formatter="pickle") for inp in inputs]
            inputs_dict = {
                var_name: [hists[var_name] for hists in inputs_list]
                for var_name in inputs_list[0]
            }

            # do the merging
            merged = {
                var_name: sum(inputs_dict[var_name][1:], inputs_dict[var_name][0])
                for var_name in inputs_dict
            }

            output.dump(merged, formatter="pickle")


class MergeShiftedHistograms(DatasetTask, SelectorMixin, law.LocalWorkflow, HTCondorWorkflow):

    sandbox = dev_sandbox("bash::$AP_BASE/sandboxes/venv_columnar.sh")

    shift_sources = law.CSVParameter(
        default=("jec",),
        description="comma-separated source of shifts without direction to consider; default: "
        "('jec',)",
    )

    # disable the shift parameter
    shift = None
    allow_empty_shift = True

    def workflow_requires(self):
        reqs = super(MergeShiftedHistograms, self).workflow_requires()

        # add nominal and both directions per shift source
        req = lambda shift: MergeHistograms.req(self, shift=shift, tree_index=0)
        reqs["nominal"] = req("nominal")
        for s in self.shift_sources:
            reqs[f"{s}_up"] = req(f"{s}_up")
            reqs[f"{s}_down"] = req(f"{s}_down")

        return reqs

    def requires(self):
        reqs = {}

        # add nominal and both directions per shift source
        req = lambda shift: MergeHistograms.req(self, shift=shift, tree_index=0, _exclude={"branch"})
        reqs["nominal"] = req("nominal")
        for s in self.shift_sources:
            reqs[f"{s}_up"] = req(f"{s}_up")
            reqs[f"{s}_down"] = req(f"{s}_down")

        return reqs

    def create_branch_map(self):
        # create a dummy branch map so that this task could as a job
        return {0: None}

    def store_parts(self):
        parts = super(MergeShiftedHistograms, self).store_parts()

        # add sorted shifts sources, add hash after the first five
        sources = sorted(self.shift_sources)
        sources_str = "_".join(sources[:5])
        if len(sources) > 5:
            sources_str += f"_{law.util.create_hash(sources[5:])}"
        parts.insert_after("dataset", "shift_sources", sources_str)

        return parts

    def output(self):
        return self.local_target("shifted_histograms.pickle")

    def run(self):
        with self.publish_step("Hello from MergeShiftedHistograms"):
            inputs_list = [
                inp["collection"][0].load(formatter="pickle")
                for inp in self.input().values()
            ]
            inputs_dict = {
                var_name: [hists[var_name] for hists in inputs_list]
                for var_name in inputs_list[0]
            }

            # do the merging
            merged = {
                var_name: sum(inputs_dict[var_name][1:], inputs_dict[var_name][0])
                for var_name in inputs_dict
            }

            self.output().dump(merged, formatter="pickle")
