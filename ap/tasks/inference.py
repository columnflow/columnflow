# coding: utf-8

"""
Tasks related to the creation of datacards for inference purposes.
"""

from collections import OrderedDict

import law

from ap.tasks.framework.base import AnalysisTask, wrapper_factory
from ap.tasks.framework.mixins import (
    CalibratorsMixin, SelectorStepsMixin, ProducersMixin, MLModelsMixin, InferenceModelMixin,
)
from ap.tasks.framework.remote import HTCondorWorkflow
from ap.tasks.histograms import MergeHistograms, MergeShiftedHistograms
from ap.util import dev_sandbox


class CreateDatacards(
    InferenceModelMixin,
    MLModelsMixin,
    ProducersMixin,
    SelectorStepsMixin,
    CalibratorsMixin,
    law.LocalWorkflow,
    HTCondorWorkflow,
):

    sandbox = dev_sandbox("bash::$AP_BASE/sandboxes/venv_columnar.sh")

    def create_branch_map(self):
        return list(self.inference_model_inst.categories)

    def workflow_requires(self):
        reqs = super().workflow_requires()

        # simply require the requirements of all branch tasks right now
        reqs["merged_hists"] = set(sum((
            law.util.flatten(t.requires())
            for t in self.get_branch_tasks().values()
        ), []))

        return reqs

    def requires(self):
        cat_obj = self.branch_data
        reqs = {
            proc_obj.name: {
                dataset: MergeShiftedHistograms.req(
                    self,
                    dataset=dataset,
                    shift_sources=tuple(
                        param_obj.source
                        for param_obj in proc_obj.parameters
                        if param_obj.type.is_shape and param_obj.source
                    ),
                    variables=(cat_obj.variable,),
                    branch=-1,
                    _exclude={"branches"},
                )
                for dataset in proc_obj.mc
            }
            for proc_obj in cat_obj.processes
        }
        if cat_obj.data:
            reqs["data"] = {
                dataset: MergeHistograms.req(
                    self,
                    dataset=dataset,
                    variables=(cat_obj.variable,),
                    branch=-1,
                    tree_index=0,
                    _exclude={"branches"},
                )
                for dataset in cat_obj.data
            }

        return reqs

    def output(self):
        cat_obj = self.branch_data
        basename = lambda name, ext: f"{name}__cat_{cat_obj.source}__var_{cat_obj.variable}.{ext}"

        return {
            "card": self.local_target(basename("datacard", "txt")),
            "shapes": self.local_target(basename("shapes", "root")),
        }

    @law.decorator.safe_output
    @law.decorator.localize(output=False)
    def run(self):
        import hist
        from ap.inference.datacard import DatacardWriter

        # prepare inputs
        inputs = self.input()

        # prepare config objects
        cat_obj = self.branch_data
        variable_inst = self.config_inst.get_variable(cat_obj.variable)
        category_inst = self.config_inst.get_category(cat_obj.source)
        leaf_category_insts = category_inst.get_leaf_categories() or [category_inst]

        # histogram data per process
        hists = OrderedDict()

        with self.publish_step(f"extracting {variable_inst.name} in {category_inst.name} ..."):
            for proc_obj_name, inp in inputs.items():
                if proc_obj_name == "data":
                    proc_obj = None
                    process_inst = self.config_inst.get_process("data")
                else:
                    proc_obj = self.inference_model_inst.get_process(proc_obj_name, category=cat_obj.name)
                    process_inst = self.config_inst.get_process(proc_obj.source)
                sub_process_insts = [sub for sub, _, _ in process_inst.walk_processes(include_self=True)]

                h_proc = None
                for dataset, _inp in inp.items():
                    dataset_inst = self.config_inst.get_dataset(dataset)

                    # skip when the dataset is already known to not contain any sub process
                    if not any(map(dataset_inst.has_process, sub_process_insts)):
                        self.logger.warning(
                            f"dataset '{dataset}' does not contain process '{process_inst.name}' "
                            "or any of its subprocesses which indicates a misconfiguration in the "
                            f"inference model '{self.inference_model}'",
                        )
                        continue

                    # open the histogram and work on a copy
                    h = _inp["collection"][0].load(formatter="pickle")[variable_inst.name].copy()

                    # axis selections
                    h = h[{
                        "process": [
                            hist.loc(p.id)
                            for p in sub_process_insts
                            if p.id in h.axes["process"]
                        ],
                        "category": [
                            hist.loc(c.id)
                            for c in leaf_category_insts
                            if c.id in h.axes["category"]
                        ],
                    }]

                    # axis reductions
                    h = h[{"process": sum, "category": sum}]

                    # add the histogram for this dataset
                    if h_proc is None:
                        h_proc = h
                    else:
                        h_proc += h

                # there must be a histogram
                if h_proc is None:
                    raise Exception(f"no histograms found for process '{process_inst.name}'")

                # create the nominal hist
                hists[proc_obj_name] = OrderedDict()
                nominal_shift_inst = self.config_inst.get_shift("nominal")
                hists[proc_obj_name]["nominal"] = h_proc[
                    {"shift": hist.loc(nominal_shift_inst.id)}
                ]

                # per shift
                if proc_obj:
                    for param_obj in proc_obj.parameters:
                        if param_obj.type.is_shape or not param_obj.source:
                            continue
                        hists[proc_obj_name][param_obj.name] = {}
                        for d in ["up", "down"]:
                            shift_inst = self.config_inst.get_shift(f"{param_obj.source}_{d}")
                            hists[proc_obj_name][param_obj.name][d] = h_proc[
                                {"shift": hist.loc(shift_inst.id)}
                            ]

            # forward objects to the datacard writer
            outputs = self.output()
            writer = DatacardWriter(self.inference_model_inst, {cat_obj.name: hists})
            writer.write(outputs["card"].path, outputs["shapes"].path)


CreateDatacardsWrapper = wrapper_factory(
    base_cls=AnalysisTask,
    require_cls=CreateDatacards,
    enable=["configs", "skip_configs"],
)
