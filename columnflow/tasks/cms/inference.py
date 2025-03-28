# coding: utf-8

"""
Tasks related to the creation of datacards for inference purposes.
"""

from collections import OrderedDict, defaultdict

import law
import order as od

from columnflow.tasks.framework.base import Requirements, AnalysisTask, wrapper_factory
from columnflow.tasks.framework.mixins import (
    CalibratorsMixin, SelectorStepsMixin, ProducersMixin, MLModelsMixin, InferenceModelMixin,
    HistHookMixin, WeightProducerMixin,
)
from columnflow.tasks.framework.remote import RemoteWorkflow
from columnflow.tasks.histograms import MergeHistograms, MergeShiftedHistograms
from columnflow.util import dev_sandbox, DotDict
from columnflow.config_util import get_datasets_from_process


class CreateDatacards(
    HistHookMixin,
    InferenceModelMixin,
    WeightProducerMixin,
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

    def get_mc_datasets(self, proc_obj: dict) -> list[str]:
        """
        Helper to find mc datasets.

        :param proc_obj: process object from an InferenceModel
        :return: List of dataset names corresponding to the process *proc_obj*.
        """
        # when datasets are defined on the process object itself, interpret them as patterns
        if proc_obj.config_mc_datasets:
            return [
                dataset.name
                for dataset in self.config_inst.datasets
                if (
                    dataset.is_mc and
                    law.util.multi_match(dataset.name, proc_obj.config_mc_datasets, mode=any)
                )
            ]

        # if the proc object is dynamic, it is calculated and the fly (e.g. via a hist hook)
        # and doesn't have any additional requirements
        if proc_obj.is_dynamic:
            return []

        # otherwise, check the config
        return [
            dataset_inst.name
            for dataset_inst in get_datasets_from_process(self.config_inst, proc_obj.config_process)
        ]

    def get_data_datasets(self, cat_obj: dict) -> list[str]:
        """
        Helper to find data datasets.

        :param cat_obj: category object from an InferenceModel
        :return: List of dataset names corresponding to the category *cat_obj*.
        """
        if not cat_obj.config_data_datasets:
            return []

        return [
            dataset.name
            for dataset in self.config_inst.datasets
            if (
                dataset.is_data and
                law.util.multi_match(dataset.name, cat_obj.config_data_datasets, mode=any)
            )
        ]

    def workflow_requires(self):
        reqs = super().workflow_requires()

        # initialize defaultdict, mapping datasets to variables + shift_sources
        mc_dataset_params = defaultdict(lambda: {"variables": set(), "shift_sources": set()})
        data_dataset_params = defaultdict(lambda: {"variables": set()})

        for cat_obj in self.branch_map.values():
            for proc_obj in cat_obj.processes:
                for dataset in self.get_mc_datasets(proc_obj):
                    # add all required variables and shifts per dataset
                    mc_dataset_params[dataset]["variables"].add(cat_obj.config_variable)
                    mc_dataset_params[dataset]["shift_sources"].update(
                        param_obj.config_shift_source
                        for param_obj in proc_obj.parameters
                        if self.inference_model_inst.require_shapes_for_parameter(param_obj)
                    )

            for dataset in self.get_data_datasets(cat_obj):
                data_dataset_params[dataset]["variables"].add(cat_obj.config_variable)

        # set workflow requirements per mc dataset
        reqs["merged_hists"] = set(
            self.reqs.MergeShiftedHistograms.req_different_branching(
                self,
                dataset=dataset,
                shift_sources=tuple(params["shift_sources"]),
                variables=tuple(params["variables"]),
            )
            for dataset, params in mc_dataset_params.items()
        )

        # add workflow requirements per data dataset
        for dataset, params in data_dataset_params.items():
            reqs["merged_hists"].add(
                self.reqs.MergeHistograms.req_different_branching(
                    self,
                    dataset=dataset,
                    variables=tuple(params["variables"]),
                ),
            )

        return reqs

    def requires(self):
        cat_obj = self.branch_data
        reqs = {
            proc_obj.name: {
                dataset: self.reqs.MergeShiftedHistograms.req_different_branching(
                    self,
                    dataset=dataset,
                    shift_sources=tuple(
                        param_obj.config_shift_source
                        for param_obj in proc_obj.parameters
                        if self.inference_model_inst.require_shapes_for_parameter(param_obj)
                    ),
                    variables=(cat_obj.config_variable,),
                    branch=-1,
                    workflow="local",
                )
                for dataset in self.get_mc_datasets(proc_obj)
            }
            for proc_obj in cat_obj.processes
            if not proc_obj.is_dynamic
        }
        if cat_obj.config_data_datasets:
            reqs["data"] = {
                dataset: self.reqs.MergeHistograms.req_different_branching(
                    self,
                    dataset=dataset,
                    variables=(cat_obj.config_variable,),
                    branch=-1,
                    workflow="local",
                )
                for dataset in self.get_data_datasets(cat_obj)
            }

        return reqs

    def output(self):
        hooks_repr = self.hist_hooks_repr
        cat_obj = self.branch_data

        def basename(name: str, ext: str) -> str:
            parts = [name, cat_obj.name, cat_obj.config_variable]
            if hooks_repr:
                parts.append(f"hooks_{hooks_repr}")
            return f"{'__'.join(map(str, parts))}.{ext}"

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
        hists: dict[od.Process, hist.Hist] = dict()

        with self.publish_step(f"extracting {variable_inst.name} in {category_inst.name} ..."):
            # loop over processes and forward them to any possible hist hooks
            for proc_obj_name, inp in inputs.items():
                if proc_obj_name == "data":
                    # there is not process object for data
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

                # save histograms in hist_hook format
                hists[process_inst] = h_proc

            # apply hist hooks
            hists = self.invoke_hist_hooks(hists)

            # define datacard processes to loop over
            cat_processes = list(cat_obj.processes)
            if cat_obj.config_data_datasets and not cat_obj.data_from_processes:
                cat_processes.append(DotDict({"name": "data"}))

            # after application of hist hooks, we can proceed with the datacard creation
            datacard_hists: OrderedDict[str, OrderedDict[str, hist.Hist]] = OrderedDict()
            for proc_obj in cat_processes:
                # obtain process information from inference model and config again
                proc_name = "data" if proc_obj.name == "data" else proc_obj.config_process
                process_inst = self.config_inst.get_process(proc_name)

                h_proc = hists.get(process_inst, None)
                if h_proc is None:
                    self.logger.warning(
                        f"found no histogram for process '{proc_obj.name}', please check your "
                        f"inference model '{self.inference_model}'",
                    )
                    continue

                # select relevant category
                h_proc = h_proc[{
                    "category": [
                        hist.loc(c.id)
                        for c in leaf_category_insts
                        if c.id in h_proc.axes["category"]
                    ],
                }][{"category": sum}]

                # create the nominal hist
                datacard_hists[proc_obj.name] = OrderedDict()
                nominal_shift_inst = self.config_inst.get_shift("nominal")
                datacard_hists[proc_obj.name]["nominal"] = h_proc[
                    {"shift": hist.loc(nominal_shift_inst.id)}
                ]

                # stop here for data
                if proc_obj.name == "data":
                    continue

                # create histograms per shift
                for param_obj in proc_obj.parameters:
                    # skip the parameter when varied hists are not needed
                    if not self.inference_model_inst.require_shapes_for_parameter(param_obj):
                        continue
                    # store the varied hists
                    datacard_hists[proc_obj.name][param_obj.name] = {}
                    for d in ["up", "down"]:
                        shift_inst = self.config_inst.get_shift(f"{param_obj.config_shift_source}_{d}")
                        datacard_hists[proc_obj.name][param_obj.name][d] = h_proc[
                            {"shift": hist.loc(shift_inst.id)}
                        ]

            # forward objects to the datacard writer
            outputs = self.output()
            writer = DatacardWriter(self.inference_model_inst, {cat_obj.name: datacard_hists})
            with outputs["card"].localize("w") as tmp_card, outputs["shapes"].localize("w") as tmp_shapes:
                writer.write(tmp_card.abspath, tmp_shapes.abspath, shapes_path_ref=outputs["shapes"].basename)


CreateDatacardsWrapper = wrapper_factory(
    base_cls=AnalysisTask,
    require_cls=CreateDatacards,
    enable=["configs", "skip_configs"],
)
