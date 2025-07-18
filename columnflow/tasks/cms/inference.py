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


class CreateDatacards(SerializeInferenceModelBase):

    resolution_task_cls = MergeHistograms

    def output(self):
        hooks_repr = self.hist_hooks_repr
        cat_obj = self.branch_data

        def basename(name: str, ext: str) -> str:
            parts = [name, cat_obj.name]
            if hooks_repr:
                parts.append(f"hooks_{hooks_repr}")
            if cat_obj.postfix is not None:
                parts.append(cat_obj.postfix)
            return f"{'__'.join(map(str, parts))}.{ext}"

        return {
            "card": self.target(basename("datacard", "txt")),
            "shapes": self.target(basename("shapes", "root")),
        }

    @law.decorator.log
    @law.decorator.safe_output
    def run(self):
        import hist
        from columnflow.inference.cms.datacard import DatacardHists, ShiftHists, DatacardWriter

        # prepare inputs
        inputs = self.input()

        # loop over all configs required by the datacard category and gather histograms
        cat_obj = self.branch_data
        datacard_hists: DatacardHists = {cat_obj.name: {}}

        # step 1: gather histograms per process for each config
        input_hists: dict[od.Config, dict[od.Process, hist.Hist]] = {}
        for config_inst in self.config_insts:
            # skip configs that are not required
            if not cat_obj.config_data.get(config_inst.name):
                continue
            # load them
            input_hists[config_inst] = self.load_process_hists(inputs, cat_obj, config_inst)

        # step 2: apply hist hooks
        input_hists = self.invoke_hist_hooks(input_hists)

        # step 3: transform to nested histogram as expected by the datacard writer
        for config_inst in input_hists.keys():
            config_data = cat_obj.config_data.get(config_inst.name)

            # determine leaf categories to gather
            category_inst = config_inst.get_category(config_data.category)
            leaf_category_insts = category_inst.get_leaf_categories() or [category_inst]

            # start the transformation
            proc_objs = list(cat_obj.processes)
            if config_data.data_datasets and not cat_obj.data_from_processes:
                proc_objs.append(self.inference_model_inst.process_spec(name="data"))
            for proc_obj in proc_objs:
                # get the corresponding process instance
                if proc_obj.name == "data":
                    process_inst = config_inst.get_process("data")
                elif config_inst.name in proc_obj.config_data:
                    process_inst = config_inst.get_process(proc_obj.config_data[config_inst.name].process)
                else:
                    # skip process objects that rely on data from a different config
                    continue

                # extract the histogram for the process
                if not (h_proc := input_hists[config_inst].get(process_inst, None)):
                    self.logger.warning(
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
                }][{"category": sum}]

                # create the nominal hist
                datacard_hists[cat_obj.name].setdefault(proc_obj.name, {}).setdefault(config_inst.name, {})
                shift_hists: ShiftHists = datacard_hists[cat_obj.name][proc_obj.name][config_inst.name]
                shift_hists["nominal"] = h_proc[{

                    "shift": hist.loc(config_inst.get_shift("nominal").name),
                }]

                # no additional shifts need to be created for data
                if proc_obj.name == "data":
                    continue

                # create histograms per shift
                for param_obj in proc_obj.parameters:
                    # skip the parameter when varied hists are not needed
                    if not self.inference_model_inst.require_shapes_for_parameter(param_obj):
                        continue
                    # store the varied hists
                    if config_inst.name in param_obj.config_data.keys():
                        shift_source = param_obj.config_data[config_inst.name].shift_source
                        for d in ["up", "down"]:
                            shift_hists[(param_obj.name, d)] = h_proc[{
                                "shift": hist.loc(config_inst.get_shift(f"{shift_source}_{d}").name),
                            }]
                    else:
                        # when the parameter does not exist for this config, fill histogram with nominal value
                        for d in ["up", "down"]:
                            shift_hists[(param_obj.name, d)] = h_proc[{
                                "shift": hist.loc("nominal"),
                            }]

        # forward objects to the datacard writer
        outputs = self.output()
        writer = DatacardWriter(self.inference_model_inst, datacard_hists)
        with outputs["card"].localize("w") as tmp_card, outputs["shapes"].localize("w") as tmp_shapes:
            writer.write(tmp_card.abspath, tmp_shapes.abspath, shapes_path_ref=outputs["shapes"].basename)


CreateDatacardsWrapper = wrapper_factory(
    base_cls=AnalysisTask,
    require_cls=CreateDatacards,
    enable=["configs", "skip_configs"],
)
