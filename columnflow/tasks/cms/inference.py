# coding: utf-8

"""
Tasks related to the creation of datacards for inference purposes.
"""

from __future__ import annotations

import law
import order as od

from columnflow.tasks.framework.base import AnalysisTask, wrapper_factory
from columnflow.tasks.framework.inference import SerializeInferenceModelBase
from columnflow.tasks.histograms import MergeHistograms

from columnflow.inference.cms.datacard import DatacardHists, ShiftHists, DatacardWriter


class CreateDatacards(SerializeInferenceModelBase):

    resolution_task_cls = MergeHistograms
    datacard_writer_cls = DatacardWriter

    def output(self):
        def basename(cat_obj, name, ext):
            parts = [name, cat_obj.name]
            if (hooks_repr := self.hist_hooks_repr):
                parts.append(f"hooks_{hooks_repr}")
            if cat_obj.postfix is not None:
                parts.append(cat_obj.postfix)
            return f"{'__'.join(map(str, parts))}.{ext}"

        return law.SiblingFileCollection({
            cat_obj.name: {
                "card": self.target(basename(cat_obj, "datacard", "txt")),
                "shapes": self.target(basename(cat_obj, "shapes", "root")),
            }
            for cat_obj in self.inference_model_inst.categories
        })

    @law.decorator.log
    @law.decorator.safe_output
    def run(self):
        import hist

        # prepare inputs and outputs
        inputs = self.input()
        outputs = self.output()

        # step 1: gather histograms per variable and process for each config
        input_hists: dict[str, dict[od.Config, dict[od.Process, hist.Hist]]] = {}
        for config_inst, data in self.combined_config_data.items():
            for variable in data["variables"]:
                if variable not in input_hists:
                    input_hists[variable] = {}
                input_hists[variable][config_inst] = self.load_process_hists(
                    config_inst,
                    list(data["mc_datasets"]) + list(data["data_datasets"]),
                    variable,
                    inputs[config_inst.name],
                )

        # loop over all configs required by the datacard category and gather histograms
        for cat_obj in self.inference_model_inst.categories:
            # check which configs contribute to this category
            config_insts = [config_inst for config_inst in self.config_insts if config_inst.name in cat_obj.config_data]
            if not config_insts:
                continue
            self.publish_message(f"processing inputs for datacard category '{cat_obj.name}'")

            # get config-based variable and category name
            variable = cat_obj.config_data[config_insts[0].name].variable
            category = cat_obj.config_data[config_insts[0].name].category

            # step 2: apply hist hooks for the selected variable
            _input_hists = self.invoke_hist_hooks(
                {config_inst: input_hists[variable][config_inst].copy() for config_inst in config_insts},
                hook_kwargs={"variable_name": variable, "category_name": category},
            )

            # step 3: transform to nested histogram as expected by the datacard writer
            datacard_hists: DatacardHists = {cat_obj.name: {}}
            for config_inst in _input_hists.keys():
                config_data = cat_obj.config_data.get(config_inst.name)

                # determine leaf categories to gather
                category_inst = config_inst.get_category(category)
                leaf_category_insts = category_inst.get_leaf_categories() or [category_inst]

                # eagerly remove data histograms in case data is supposed to be faked from mc processes
                if cat_obj.data_from_processes:
                    for process_inst in list(_input_hists[config_inst]):
                        if process_inst.is_data:
                            del _input_hists[config_inst][process_inst]

                # start the transformation
                proc_objs = list(cat_obj.processes)
                if config_data.data_datasets and not cat_obj.data_from_processes:
                    proc_objs.append(self.inference_model_inst.process_spec(name="data"))
                for proc_obj in proc_objs:
                    # get all process instances (keys in _input_hists) to be combined
                    if proc_obj.is_dynamic:
                        if not (process_name := proc_obj.config_data[config_inst.name].get("process", None)):
                            raise ValueError(
                                f"dynamic datacard process object misses 'process' entry in config data for "
                                f"'{config_inst.name}': {proc_obj}",
                            )
                        process_insts = [config_inst.get_process(process_name)]
                    else:
                        process_insts = [
                            config_inst.get_dataset(dataset_name).processes.get_first()
                            for dataset_name in proc_obj.config_data[config_inst.name].mc_datasets
                        ]

                    # collect per-process histograms
                    h_procs = []
                    for process_inst in process_insts:
                        # extract the histogram for the process
                        # (removed from hists to eagerly cleanup memory)
                        h_proc = _input_hists[config_inst].pop(process_inst, None)
                        if h_proc is None:
                            self.logger.error(
                                f"found no histogram to model datacard process '{proc_obj.name}', please check your "
                                f"inference model '{self.inference_model}'",
                            )
                            continue

                        # select relevant categories
                        h_proc = h_proc[{
                            "category": [
                                hist.loc(c.name)
                                for c in leaf_category_insts
                                if c.name in h_proc.axes["category"]
                            ],
                        }]
                        h_proc = h_proc[{"category": sum}]

                        h_procs.append(h_proc)

                    if h_procs is None:
                        continue

                    # combine them
                    h_proc = sum(h_procs[1:], h_procs[0].copy())

                    # create the nominal hist
                    datacard_hists[cat_obj.name].setdefault(proc_obj.name, {}).setdefault(config_inst.name, {})
                    shift_hists: ShiftHists = datacard_hists[cat_obj.name][proc_obj.name][config_inst.name]
                    shift_hists["nominal"] = h_proc[{
                        "shift": hist.loc(config_inst.get_shift("nominal").name),
                    }]

                    # no additional shifts need to be created for data
                    if proc_obj.name == "data":
                        continue

                    # create histograms per shape shift
                    for param_obj in proc_obj.parameters:
                        # skip the parameter when varied hists are not needed
                        if (
                            not param_obj.type.is_shape and
                            not any(trafo.from_shape for trafo in param_obj.transformations)
                        ):
                            continue
                        # store the varied hists
                        shift_source = (
                            param_obj.config_data[config_inst.name].shift_source
                            if config_inst.name in param_obj.config_data
                            else None
                        )
                        for d in ["up", "down"]:
                            if shift_source and f"{shift_source}_{d}" not in h_proc.axes["shift"]:
                                raise ValueError(
                                    f"cannot find '{shift_source}_{d}' in shift axis of histogram for process "
                                    f"'{proc_obj.name}' in config '{config_inst.name}' while handling parameter "
                                    f"'{param_obj.name}' in datacard category '{cat_obj.name}', available shifts "
                                    f"are: {list(h_proc.axes['shift'])}",
                                )
                            shift_hists[(param_obj.name, d)] = h_proc[{
                                "shift": hist.loc(f"{shift_source}_{d}" if shift_source else "nominal"),
                            }]

            # forward objects to the datacard writer
            outp = outputs[cat_obj.name]
            writer = self.datacard_writer_cls(self.inference_model_inst, datacard_hists)
            with outp["card"].localize("w") as tmp_card, outp["shapes"].localize("w") as tmp_shapes:
                writer.write(tmp_card.abspath, tmp_shapes.abspath, shapes_path_ref=outp["shapes"].basename)
            self.publish_message(f"datacard written to {outp['card'].abspath}")


CreateDatacardsWrapper = wrapper_factory(
    base_cls=AnalysisTask,
    require_cls=CreateDatacards,
    enable=["configs", "skip_configs"],
)
