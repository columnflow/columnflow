# coding: utf-8

"""
Tasks to be implemented: MergeSelectionMasks, PlotCutflow
"""

from collections import OrderedDict

import law

from ap.tasks.framework.base import DatasetTask, ShiftTask
from ap.tasks.framework.mixins import CalibratorsMixin, SelectorMixin, SelectorStepsMixin, PlotMixin
from ap.tasks.framework.remote import HTCondorWorkflow
from ap.tasks.plotting import ProcessPlotBase
from ap.tasks.selection import SelectEvents
from ap.util import dev_sandbox, ensure_proxy
from ap.util import DotDict
from ap.production import Producer


class MergeSelectionMasks(
        DatasetTask, SelectorMixin, CalibratorsMixin, law.tasks.ForestMerge, HTCondorWorkflow,
):

    sandbox = dev_sandbox("bash::$AP_BASE/sandboxes/venv_columnar.sh")

    shifts = set(SelectEvents.shifts)

    # recursively merge 8 files into one
    merge_factor = 8

    def create_branch_map(self):
        # DatasetTask implements a custom branch map, but we want to use the one in ForestMerge
        return law.tasks.ForestMerge.create_branch_map(self)

    def merge_workflow_requires(self):
        return {
            "normalizor": Producer.get("normalization_weights").run_requires(self),
            "selection": SelectEvents.req(self, _exclude={"branches"}),
        }

    def merge_requires(self, start_branch, end_branch):
        return {
            "normalizor": Producer.get("normalization_weights").run_requires(self),
            "selection": [SelectEvents.req(self, branch=b) for b in range(start_branch, end_branch)],
        }

    def trace_merge_workflow_inputs(self, inputs):
        return super().trace_merge_workflow_inputs(inputs["selection"])

    def trace_merge_inputs(self, inputs):
        inp = inputs["selection"].copy()
        # add "normalizor" field into the inputs of the first branch
        inp[0].update({"normalizor": inputs["normalizor"]})
        return super().trace_merge_inputs(inp)

    def merge_output(self):
        return self.local_target("masks.parquet")

    def merge(self, inputs, output):
        import awkward as ak
        from ap.columnar_util import sorted_ak_to_parquet

        if isinstance(inputs, list):
            # first merging step: opening inputs required
            output_chunks = []

            # create a temp dir for saving intermediate files
            tmp_dir = law.LocalDirectoryTarget(is_tmp=True)
            tmp_dir.touch()

            # setup normalizor using inputs in first branch
            normalizor = Producer.get("normalization_weights")
            normalizor.run_setup(self, inputs[0]["normalizor"])
            for inp in inputs:
                columns = ak.from_parquet(inp["columns"].path)
                steps = ak.from_parquet(inp["results"].path).steps

                normalizor(columns, **self.get_producer_kwargs(self))

                out = ak.zip({"steps": steps, "columns": columns})

                chunk = tmp_dir.child(f"tmp_{inp['results'].basename}", type="f")
                output_chunks.append(chunk)
                sorted_ak_to_parquet(out, chunk.path)
            law.pyarrow.merge_parquet_task(self, output_chunks, output, local=True)
        else:
            # second and following merging steps: directly merge inputs
            law.pyarrow.merge_parquet_task(self, inputs, output, local=True)


class CreateCutflowHistograms(
        DatasetTask, SelectorStepsMixin, CalibratorsMixin,
        law.LocalWorkflow, HTCondorWorkflow,
):

    sandbox = dev_sandbox("bash::$AP_BASE/sandboxes/venv_columnar.sh")

    shifts = set(MergeSelectionMasks.shifts)

    selector_steps_order_sensitive = True

    def workflow_requires(self):
        reqs = super(CreateCutflowHistograms, self).workflow_requires()
        reqs["masks"] = MergeSelectionMasks.req(self)
        return reqs

    def requires(self):
        reqs = {
            "masks": MergeSelectionMasks.req(self),
        }
        return reqs

    def output(self):
        return self.local_target("cutflow_hist.pickle")

    @law.decorator.safe_output
    @law.decorator.localize
    @ensure_proxy
    def run(self):
        import hist
        import awkward as ak
        from ap.columnar_util import (
            ChunkedReader, add_ak_aliases,
        )

        # prepare inputs and outputs
        inputs = self.input()

        # declare output: dict of histograms
        histograms = {}

        # create a temp dir for saving intermediate files
        tmp_dir = law.LocalDirectoryTarget(is_tmp=True)
        tmp_dir.touch()

        # get shift dependent aliases
        aliases = self.shift_inst.x("column_aliases", {})

        infile = inputs["masks"].path
        with ChunkedReader(
            infile,
            source_type="awkward_parquet",
        ) as reader:
            msg = f"iterate through {reader.n_entries} events ..."
            for events, pos in self.iter_progress(reader, reader.n_chunks, msg=msg):

                columns = events.columns

                # add aliases
                add_ak_aliases(columns, aliases, remove_src=True)

                # define axes that stay equal for each variable
                hist_axes = [
                    # hist.storage.Weight(),
                    hist.axis.IntCategory([], name="process", growth=True),
                    hist.axis.IntCategory([], name="category", growth=True),
                    hist.axis.StrCategory(["noCuts"] + list(self.selector_steps), name="selection_steps"),
                    hist.axis.IntCategory([], name="shift", growth=True),
                ]

                # for now: take the Schnittmenge from variables in config and columns
                # this does not take into account variables such as Jet.pt.0
                variables = [v for v in columns.fields if v in self.config_inst.variables.names()]
                for var_name in variables:
                    with self.publish_step("var: %s" % var_name):
                        variable_inst = self.config_inst.variables.get(var_name)
                        if var_name not in histograms:
                            var_axis = hist.axis.Variable(
                                variable_inst.bin_edges,
                                name=var_name,
                                label=variable_inst.get_full_x_title(),
                            )
                            histograms[var_name] = hist.Hist(*hist_axes, var_axis, hist.storage.Weight())

                        category_ids = columns.category_ids
                        # pad the category_ids when the event is not categorized at all
                        category_ids = ak.fill_none(ak.pad_none(category_ids, 1, axis=-1), -1)

                        # fill hist before applying cuts
                        fill_kwargs = {
                            var_name: columns[variable_inst.expression],
                            "process": columns.process_id,
                            "category": category_ids,
                            "shift": self.shift_inst.id,
                            "weight": columns.normalization_weight,
                        }
                        arrays = (ak.flatten(a) for a in ak.broadcast_arrays(*fill_kwargs.values()))
                        histograms[var_name].fill(**dict(zip(fill_kwargs, arrays)), selection_steps="noCuts")

                        selector_steps = tuple(self.selector_steps)

                        # set the default selector_steps when no default is defined in config
                        if not selector_steps:
                            selector_steps = tuple(sorted(events.steps.fields))

                        # fill hist after each individual cut
                        mask = True
                        for step in selector_steps:
                            if step not in events.steps.fields:
                                raise ValueError(
                                    f"selection step {step} is not defined in the Selector {self.selector}",
                                )
                            mask = (mask) & (events.steps[step])
                            fill_kwargs = {
                                var_name: columns[variable_inst.expression][mask],
                                "process": columns.process_id[mask],
                                "category": category_ids[mask],
                                "shift": self.shift_inst.id,
                                "weight": columns.normalization_weight[mask],
                            }
                            arrays = (ak.flatten(a) for a in ak.broadcast_arrays(*fill_kwargs.values()))
                            histograms[var_name].fill(**dict(zip(fill_kwargs, arrays)), selection_steps=step)

        # merge output files
        self.output().dump(histograms, formatter="pickle")


class PlotCutflow(
        ShiftTask,
        SelectorStepsMixin,
        CalibratorsMixin,
        ProcessPlotBase,
        law.LocalWorkflow,
        HTCondorWorkflow,
):

    sandbox = "bash::$AP_BASE/sandboxes/cmssw_default.sh"

    shifts = set(CreateCutflowHistograms.shifts)

    selector_steps_order_sensitive = True

    def create_branch_map(self):
        return [DotDict({"category": cat_name}) for cat_name in sorted(self.categories)]

    def workflow_requires(self):
        reqs = super(PlotCutflow, self).workflow_requires()
        reqs["cutflow_hists"] = CreateCutflowHistograms.req(self)
        return reqs

    def requires(self):
        return {d: CreateCutflowHistograms.req(self, dataset=d, _exclude={"branches"}) for d in self.datasets}

    def output(self):
        b = self.branch_data
        return self.local_target(f"cutflow__cat_{b.category}.pdf")

    @PlotMixin.view_output_plots
    def run(self):
        # import numpy as np
        import hist
        import matplotlib.pyplot as plt
        import mplhep

        # prepare config objects
        category_inst = self.config_inst.get_category(self.branch_data.category)
        leaf_category_insts = category_inst.get_leaf_categories() or [category_inst]
        process_insts = list(map(self.config_inst.get_process, self.processes))
        sub_process_insts = {
            proc: [sub for sub, _, _ in proc.walk_processes(include_self=True)]
            for proc in process_insts
        }

        # histogram data per process
        hists = OrderedDict()

        with self.publish_step(f"plotting cutflow in {category_inst.name}"):
            for dataset, inp in self.input().items():
                dataset_inst = self.config_inst.get_dataset(dataset)
                print(inp)
                # take the first variable from input dict
                variable_inst = self.config_inst.get_variable(list(sorted(inp.load(formatter="pickle").keys()))[0])
                h_in = inp.load(formatter="pickle")[variable_inst.name]

                # sanity checks
                n_shifts = len(h_in.axes["shift"])
                if n_shifts != 1:
                    raise Exception(f"shift axis is supposed to only contain 1 bin, found {n_shifts}")

                # loop and extract one histogram per process
                for process_inst in process_insts:
                    # skip when the dataset is already known to not contain any sub process
                    if not any(map(dataset_inst.has_process, sub_process_insts[process_inst])):
                        continue

                    # work on a copy
                    h = h_in.copy()

                    # axis selections
                    h = h[{
                        "process": [
                            hist.loc(p.id)
                            for p in sub_process_insts[process_inst]
                            if p.id in h.axes["process"]
                        ],
                        "category": [
                            hist.loc(c.id)
                            for c in leaf_category_insts
                            if c.id in h.axes["category"]
                        ],
                    }]

                    # axis reductions
                    h = h[{"process": sum, "category": sum, "shift": sum, variable_inst.name: sum}]

                    # add the histogram
                    if process_inst in hists:
                        hists[process_inst] += h
                    else:
                        hists[process_inst] = h

            # there should be hists to plot
            if not hists:
                raise Exception("no histograms found to plot")

            mc_hists = [h for process_inst, h in hists.items() if process_inst.is_mc]
            mc_colors = [process_inst.color for process_inst in hists if process_inst.is_mc]
            mc_labels = [process_inst.label for process_inst in hists if process_inst.is_mc]

            # normalize histograms on number of events before applying cuts
            mc_hists = [h / h[{"selection_steps": "noCuts"}].value for h in mc_hists]

            # create the stack
            h_mc_stack = None
            if mc_hists:
                h_mc_stack = hist.Stack(*mc_hists)

            # start plotting
            plt.style.use(mplhep.style.CMS)
            fig, ax = plt.subplots()

            plot_mc_kwargs = {
                "ax": ax,
                "stack": False,
                "histtype": "step",
                "color": mc_colors,
                "label": mc_labels,
            }
            h_mc_stack.plot(**plot_mc_kwargs)

            ax.set_xlabel("Selection steps")
            ax.set_ylabel("Selection efficiency")

            # legend
            legend_kwargs = {
                "title": "Processes",
                "ncol": 1,
                "loc": "upper right",
            }
            ax.legend(**legend_kwargs)

            # CMS label
            CMS_label_kwargs = {
                "ax": ax,
                "label": "Work in Progress",
                "fontsize": 22,
                "lumi": self.config_inst.x.luminosity.get("nominal") / 1000,
            }
            mplhep.cms.label(**CMS_label_kwargs)

            # save the plot
            plt.tight_layout()
            self.output().dump(fig, formatter="mpl")
