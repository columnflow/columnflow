# coding: utf-8

"""
Base tasks for writing serialized statistical inference models.
"""

from __future__ import annotations

import pickle

import law
import order as od

from columnflow.tasks.framework.base import Requirements
from columnflow.tasks.framework.mixins import (
    CalibratorClassesMixin, SelectorClassMixin, ReducerClassMixin, ProducerClassesMixin, HistProducerClassMixin,
    InferenceModelMixin, HistHookMixin, MLModelsMixin,
)
from columnflow.tasks.framework.remote import RemoteWorkflow
from columnflow.tasks.histograms import MergeShiftedHistograms
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

    @law.workflow_property(cache=True)
    def combined_config_data(self) -> dict[od.ConfigInst, dict[str, dict | set]]:
        # prepare data extracted from the inference model
        config_data = {
            config_inst: {
                # all variables used in this config in any datacard category
                "variables": set(),
                # plain set of names of real data datasets
                "data_datasets": set(),
                # per name of mc dataset, the set of shift sources and the name of the datacard process object
                "mc_datasets": {},
            }
            for config_inst in self.config_insts
        }

        # iterate over all model categories
        for cat_obj in self.inference_model_inst.categories:
            # keep track of per-category information for consistency checks
            variables = set()
            categories = set()

            # iterate over configs relevant for this category
            config_insts = [config_inst for config_inst in self.config_insts if config_inst.name in cat_obj.config_data]
            for config_inst in config_insts:
                data = config_data[config_inst]

                # variables
                data["variables"].add(cat_obj.config_data[config_inst.name].variable)

                # data datasets, but only if
                #   - data in that category is not faked from mc processes, or
                #   - at least one process object is dynamic (that usually means data-driven)
                if not cat_obj.data_from_processes or any(proc_obj.is_dynamic for proc_obj in cat_obj.processes):
                    data["data_datasets"].update(self.get_data_datasets(config_inst, cat_obj))

                # mc datasets over all process objects
                #   - the process is not dynamic
                for proc_obj in cat_obj.processes:
                    mc_datasets = self.get_mc_datasets(config_inst, proc_obj)
                    for dataset_name in mc_datasets:
                        if dataset_name not in data["mc_datasets"]:
                            data["mc_datasets"][dataset_name] = {
                                "proc_name": proc_obj.name,
                                "shift_sources": set(),
                            }
                        elif data["mc_datasets"][dataset_name]["proc_name"] != proc_obj.name:
                            raise ValueError(
                                f"dataset '{dataset_name}' was already assigned to datacard process "
                                f"'{data['mc_datasets'][dataset_name]['proc_name']}' and cannot be re-assigned to "
                                f"'{proc_obj.name}' in config '{config_inst.name}'",
                            )

                    # shift sources
                    for param_obj in proc_obj.parameters:
                        if config_inst.name not in param_obj.config_data:
                            continue
                        # only add if a shift is required for this parameter
                        if (
                            (param_obj.type.is_shape and not param_obj.transformations.any_from_rate) or
                            (param_obj.type.is_rate and param_obj.transformations.any_from_shape)
                        ):
                            shift_source = param_obj.config_data[config_inst.name].shift_source
                            for mc_dataset in mc_datasets:
                                data["mc_datasets"][mc_dataset]["shift_sources"].add(shift_source)

                # for consistency checks later
                variables.add(cat_obj.config_data[config_inst.name].variable)
                categories.add(cat_obj.config_data[config_inst.name].category)

            # consistency checks: the config-based variable and category names must be identical
            if len(variables) != 1:
                raise ValueError(
                    f"found diverging variables to be used in datacard category '{cat_obj.name}' across configs "
                    f"{', '.join(c.name for c in config_insts)}: {variables}",
                )
            if len(categories) != 1:
                raise ValueError(
                    f"found diverging categories to be used in datacard category '{cat_obj.name}' across configs "
                    f"{', '.join(c.name for c in config_insts)}: {categories}",
                )

        return config_data

    def create_branch_map(self):
        # dummy branch map
        return {0: None}

    def _hist_requirements(self, **kwargs):
        # gather data from inference model to define requirements in the structure
        # config_name -> dataset_name -> MergeHistogramsTask
        reqs = {}
        for config_inst, data in self.combined_config_data.items():
            reqs[config_inst.name] = {}
            # mc datasets
            for dataset_name in sorted(data["mc_datasets"]):
                reqs[config_inst.name][dataset_name] = self.reqs.MergeShiftedHistograms.req_different_branching(
                    self,
                    config=config_inst.name,
                    dataset=dataset_name,
                    shift_sources=tuple(sorted(data["mc_datasets"][dataset_name]["shift_sources"])),
                    variables=tuple(sorted(data["variables"])),
                    **kwargs,
                )
            # data datasets
            for dataset_name in sorted(data["data_datasets"]):
                reqs[config_inst.name][dataset_name] = self.reqs.MergeShiftedHistograms.req_different_branching(
                    self,
                    config=config_inst.name,
                    dataset=dataset_name,
                    shift_sources=(),
                    variables=tuple(sorted(data["variables"])),
                    **kwargs,
                )

        return reqs

    def workflow_requires(self):
        reqs = super().workflow_requires()
        reqs["merged_hists"] = self._hist_requirements()
        return reqs

    def requires(self):
        return self._hist_requirements(branch=-1, workflow="local")

    def load_process_hists(
        self,
        config_inst: od.Config,
        dataset_names: list[str],
        variable: str,
        inputs: dict,
    ) -> dict[str, dict[od.Process, hist.Hist]]:
        # collect histograms per variable and process
        hists: dict[od.Process, hist.Hist] = {}

        with self.publish_step(f"extracting '{variable}' for config {config_inst.name} ..."):
            for dataset_name in dataset_names:
                dataset_inst = config_inst.get_dataset(dataset_name)
                process_inst = dataset_inst.processes.get_first()

                # for real data, fallback to the main data process
                if process_inst.is_data:
                    process_inst = config_inst.get_process("data")

                # gather all subprocesses for a full query later
                sub_process_insts = [sub for sub, _, _ in process_inst.walk_processes(include_self=True)]

                # open the histogram and work on a copy
                inp = inputs[dataset_name]["collection"][0]["hists"][variable]
                try:
                    h = inp.load(formatter="pickle").copy()
                except pickle.UnpicklingError as e:
                    raise Exception(
                        f"failed to load '{variable}' histogram for dataset '{dataset_name}' in config "
                        f"'{config_inst.name}' from {inp.abspath}",
                    ) from e

                # there must be at least one matching sub process
                if not any(p.name in h.axes["process"] for p in sub_process_insts):
                    raise Exception(f"no '{variable}' histograms found for process '{process_inst.name}'")

                # select and reduce over relevant processes
                h = h[{"process": [hist.loc(p.name) for p in sub_process_insts if p.name in h.axes["process"]]}]
                h = h[{"process": sum}]

                # additional custom reductions
                h = self.modify_process_hist(process_inst, h)

                # store it
                if process_inst in hists:
                    hists[process_inst] += h
                else:
                    hists[process_inst] = h

        return hists

    def modify_process_hist(self, process_inst: od.Process, h: hist.Hist) -> hist.Hist:
        """
        Hook to modify a process histogram after it has been loaded. This can be helpful to reduce memory early on.

        :param process_inst: The process instance the histogram belongs to.
        :param histo: The histogram to modify.
        :return: The modified histogram.
        """
        return h
