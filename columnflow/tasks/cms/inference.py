# coding: utf-8

"""
Tasks related to the creation of datacards for inference purposes.
"""

from collections import OrderedDict

import law

from columnflow.tasks.framework.base import Requirements, AnalysisTask, wrapper_factory
from columnflow.tasks.framework.mixins import (
    CalibratorsMixin, SelectorStepsMixin, ProducersMixin, MLModelsMixin, InferenceModelMixin,
)
from columnflow.tasks.framework.remote import RemoteWorkflow
from columnflow.tasks.histograms import MergeHistograms, MergeShiftedHistograms
from columnflow.util import dev_sandbox
from columnflow.config_util import get_datasets_from_process


class CreateDatacards(
    InferenceModelMixin,
    MLModelsMixin,
    ProducersMixin,
    SelectorStepsMixin,
    CalibratorsMixin,
    law.LocalWorkflow,
    RemoteWorkflow,
):
    sandbox = dev_sandbox(law.config.get("analysis", "default_columnar_sandbox"))

    # upstream requirements
    reqs = Requirements(
        RemoteWorkflow.reqs,
        MergeHistograms=MergeHistograms,
        MergeShiftedHistograms=MergeShiftedHistograms,
    )

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
        # helper to find automatic datasets
        def get_mc_datasets(proc_obj: dict) -> list[str]:
            # when datasets are defined on the process object itself, return them
            if proc_obj.config_mc_datasets:
                return proc_obj.config_mc_datasets

            # if not, check the config
            return [
                dataset_inst.name
                for dataset_inst in get_datasets_from_process(self.config_inst, proc_obj.config_process)
            ]

        cat_obj = self.branch_data
        reqs = {
            proc_obj.name: {
                dataset: self.reqs.MergeShiftedHistograms.req(
                    self,
                    dataset=dataset,
                    shift_sources=tuple(
                        param_obj.config_shift_source
                        for param_obj in proc_obj.parameters
                        if self.inference_model_inst.require_shapes_for_parameter(param_obj)
                    ),
                    variables=(cat_obj.config_variable,),
                    branch=-1,
                    _exclude={"branches"},
                )
                for dataset in get_mc_datasets(proc_obj)
            }
            for proc_obj in cat_obj.processes
        }
        if cat_obj.config_data_datasets:
            reqs["data"] = {
                dataset: self.reqs.MergeHistograms.req(
                    self,
                    dataset=dataset,
                    variables=(cat_obj.config_variable,),
                    branch=-1,
                    _exclude={"branches"},
                )
                for dataset in cat_obj.config_data_datasets
            }

        return reqs

    def output(self):
        cat_obj = self.branch_data
        basename = lambda name, ext: f"{name}__cat_{cat_obj.config_category}__var_{cat_obj.config_variable}.{ext}"

        return {
            "card": self.target(basename("datacard", "txt")),
            "shapes": self.target(basename("shapes", "root")),
        }

    @law.decorator.log
    @law.decorator.safe_output
    def run(self):
        import hist
        from columnflow.inference.cms.datacard import DatacardWriter

        # prepare inputs
        inputs = self.input()

        # prepare config objects
        cat_obj = self.branch_data
        category_inst = self.config_inst.get_category(cat_obj.config_category)
        variable_inst = self.config_inst.get_variable(cat_obj.config_variable)
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
                    process_inst = self.config_inst.get_process(proc_obj.config_process)
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
                    h = _inp["collection"][0]["hists"][variable_inst.name].load(formatter="pickle").copy()

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
                        # skip the parameter when varied hists are not needed
                        if not self.inference_model_inst.require_shapes_for_parameter(param_obj):
                            continue
                        # store the varied hists
                        hists[proc_obj_name][param_obj.name] = {}
                        for d in ["up", "down"]:
                            shift_inst = self.config_inst.get_shift(f"{param_obj.config_shift_source}_{d}")
                            hists[proc_obj_name][param_obj.name][d] = h_proc[
                                {"shift": hist.loc(shift_inst.id)}
                            ]

            # forward objects to the datacard writer
            outputs = self.output()
            writer = DatacardWriter(self.inference_model_inst, {cat_obj.name: hists})
            with outputs["card"].localize("w") as tmp_card, outputs["shapes"].localize("w") as tmp_shapes:
                writer.write(tmp_card.path, tmp_shapes.path, shapes_path_ref=outputs["shapes"].basename)


CreateDatacardsWrapper = wrapper_factory(
    base_cls=AnalysisTask,
    require_cls=CreateDatacards,
    enable=["configs", "skip_configs"],
)
