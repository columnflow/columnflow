# coding: utf-8

"""
Base tasks for writing serialized statistical inference models.
"""

from __future__ import annotations

import law
import order as od

from columnflow.tasks.framework.base import Requirements
from columnflow.tasks.framework.mixins import (
    CalibratorClassesMixin, SelectorClassMixin, ReducerClassMixin, ProducerClassesMixin, HistProducerClassMixin,
    InferenceModelMixin, HistHookMixin, MLModelsMixin,
)
from columnflow.tasks.framework.remote import RemoteWorkflow
from columnflow.tasks.histograms import MergeHistograms, MergeShiftedHistograms
from columnflow.util import dev_sandbox, DotDict, maybe_import
from columnflow.config_util import get_datasets_from_process

hist = maybe_import("hist")


class SerializeInferenceModelBase(
    CalibratorClassesMixin,
    SelectorClassMixin,
    ReducerClassMixin,
    ProducerClassesMixin,
    MLModelsMixin,
    HistProducerClassMixin,
    InferenceModelMixin,
    HistHookMixin,
    law.LocalWorkflow,
    RemoteWorkflow,
):
    sandbox = dev_sandbox(law.config.get("analysis", "default_columnar_sandbox"))

    # support multiple configs
    single_config = False

    # upstream requirements
    reqs = Requirements(
        RemoteWorkflow.reqs,
        MergeHistograms=MergeHistograms,
        MergeShiftedHistograms=MergeShiftedHistograms,
    )

    @classmethod
    def get_mc_datasets(cls, config_inst: od.Config, proc_obj: DotDict) -> list[str]:
        """
        Helper to find mc datasets.

        :param config_inst: The config instance.
        :param proc_obj: process object from an InferenceModel
        :return: List of dataset names corresponding to the process *proc_obj*.
        """
        # the config instance should be specified in the config data of the proc_obj
        if not (config_data := proc_obj.config_data.get(config_inst.name)):
            return []

        # when datasets are defined on the process object itself, interpret them as patterns
        if config_data.mc_datasets:
            return [
                dataset.name
                for dataset in config_inst.datasets
                if (
                    dataset.is_mc and
                    law.util.multi_match(dataset.name, config_data.mc_datasets, mode=any)
                )
            ]

        # if the proc object is dynamic, it is calculated and the fly (e.g. via a hist hook)
        # and doesn't have any additional requirements
        if proc_obj.is_dynamic:
            return []

        # otherwise, check the config
        return [
            dataset_inst.name
            for dataset_inst in get_datasets_from_process(config_inst, config_data.process)
        ]

    @classmethod
    def get_data_datasets(cls, config_inst: od.Config, cat_obj: DotDict) -> list[str]:
        """
        Helper to find data datasets.

        :param config_inst: The config instance.
        :param cat_obj: category object from an InferenceModel
        :return: List of dataset names corresponding to the category *cat_obj*.
        """
        # the config instance should be specified in the config data of the proc_obj
        if not (config_data := cat_obj.config_data.get(config_inst.name)):
            return []

        if not config_data.data_datasets:
            return []

        return [
            dataset.name
            for dataset in config_inst.datasets
            if (
                dataset.is_data and
                law.util.multi_match(dataset.name, config_data.data_datasets, mode=any)
            )
        ]

    def create_branch_map(self):
        return list(self.inference_model_inst.categories)

    def _requires_cat_obj(self, cat_obj: DotDict, **req_kwargs):
        reqs = {}
        for config_inst in self.config_insts:
            if not (config_data := cat_obj.config_data.get(config_inst.name)):
                continue

            # add merged shifted histograms for mc
            reqs[config_inst.name] = {
                proc_obj.name: {
                    dataset: self.reqs.MergeShiftedHistograms.req_different_branching(
                        self,
                        config=config_inst.name,
                        dataset=dataset,
                        shift_sources=tuple(
                            param_obj.config_data[config_inst.name].shift_source
                            for param_obj in proc_obj.parameters
                            if (
                                config_inst.name in param_obj.config_data and
                                self.inference_model_inst.require_shapes_for_parameter(param_obj)
                            )
                        ),
                        variables=(config_data.variable,),
                        **req_kwargs,
                    )
                    for dataset in self.get_mc_datasets(config_inst, proc_obj)
                }
                for proc_obj in cat_obj.processes
                if config_inst.name in proc_obj.config_data and not proc_obj.is_dynamic
            }
            # add merged histograms for data, but only if
            # - data in that category is not faked from mc, or
            # - at least one process object is dynamic (that usually means data-driven)
            if (
                (not cat_obj.data_from_processes or any(proc_obj.is_dynamic for proc_obj in cat_obj.processes)) and
                (data_datasets := self.get_data_datasets(config_inst, cat_obj))
            ):
                reqs[config_inst.name]["data"] = {
                    dataset: self.reqs.MergeHistograms.req_different_branching(
                        self,
                        config=config_inst.name,
                        dataset=dataset,
                        variables=(config_data.variable,),
                        **req_kwargs,
                    )
                    for dataset in data_datasets
                }

        return reqs

    def workflow_requires(self):
        reqs = super().workflow_requires()

        reqs["merged_hists"] = hist_reqs = {}
        for cat_obj in self.branch_map.values():
            cat_reqs = self._requires_cat_obj(cat_obj)
            for config_name, proc_reqs in cat_reqs.items():
                hist_reqs.setdefault(config_name, {})
                for proc_name, dataset_reqs in proc_reqs.items():
                    hist_reqs[config_name].setdefault(proc_name, {})
                    for dataset_name, task in dataset_reqs.items():
                        hist_reqs[config_name][proc_name].setdefault(dataset_name, set()).add(task)

        return reqs

    def requires(self):
        cat_obj = self.branch_data
        return self._requires_cat_obj(cat_obj, branch=-1, workflow="local")

    def load_process_hists(
        self,
        inputs: dict,
        cat_obj: DotDict,
        config_inst: od.Config,
    ) -> dict[od.Process, hist.Hist]:
        # loop over all configs required by the datacard category and gather histograms
        config_data = cat_obj.config_data.get(config_inst.name)

        # collect histograms per config process
        hists: dict[od.Process, hist.Hist] = {}
        with self.publish_step(
            f"extracting {config_data.variable} in {config_data.category} for config {config_inst.name}...",
        ):
            for proc_obj_name, inp in inputs[config_inst.name].items():
                if proc_obj_name == "data":
                    process_inst = config_inst.get_process("data")
                else:
                    proc_obj = self.inference_model_inst.get_process(proc_obj_name, category=cat_obj.name)
                    process_inst = config_inst.get_process(proc_obj.config_data[config_inst.name].process)
                sub_process_insts = [sub for sub, _, _ in process_inst.walk_processes(include_self=True)]

                # loop over per-dataset inputs and extract histograms containing the process
                h_proc = None
                for dataset_name, _inp in inp.items():
                    dataset_inst = config_inst.get_dataset(dataset_name)

                    # skip when the dataset is already known to not contain any sub process
                    if not any(map(dataset_inst.has_process, sub_process_insts)):
                        self.logger.warning(
                            f"dataset '{dataset_name}' does not contain process '{process_inst.name}' or any of "
                            "its subprocesses which indicates a misconfiguration in the inference model "
                            f"'{self.inference_model}'",
                        )
                        continue

                    # open the histogram and work on a copy
                    h = _inp["collection"][0]["hists"][config_data.variable].load(formatter="pickle").copy()

                    # axis selections
                    h = h[{
                        "process": [
                            hist.loc(p.id)
                            for p in sub_process_insts
                            if p.id in h.axes["process"]
                        ],
                    }]

                    # axis reductions
                    h = h[{"process": sum}]

                    # add the histogram for this dataset
                    if h_proc is None:
                        h_proc = h
                    else:
                        h_proc += h

                # there must be a histogram
                if h_proc is None:
                    raise Exception(f"no histograms found for process '{process_inst.name}'")

                # save histograms mapped to processes
                hists[process_inst] = h_proc

        return hists
