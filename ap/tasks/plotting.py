# coding: utf-8

"""
Task to plot different types of histograms
"""

from collections import OrderedDict

import law

from ap.tasks.framework.base import ShiftTask
from ap.tasks.framework.mixins import (
    CalibratorsMixin, SelectorStepsMixin, ProducersMixin, MLModelsMixin, PlotMixin, CategoriesMixin,
    VariablesMixin, DatasetsProcessesMixin, ShiftSourcesMixin,
)
from ap.tasks.framework.remote import HTCondorWorkflow
from ap.tasks.histograms import MergeHistograms, MergeShiftedHistograms
from ap.util import DotDict
from ap.plotting.plotter import plot_all


class ProcessPlotBase(
    CategoriesMixin,
    DatasetsProcessesMixin,
    PlotMixin,
):
    """
    Base class for tasks creating plots where contributions of different processes are shown.
    """

    def store_parts(self):
        parts = super().store_parts()
        parts["plot"] = f"datasets_{self.datasets_repr}__processes_{self.processes_repr}"
        return parts


class PlotVariables(
    ShiftTask,
    MLModelsMixin,
    ProducersMixin,
    SelectorStepsMixin,
    CalibratorsMixin,
    VariablesMixin,
    ProcessPlotBase,
    law.LocalWorkflow,
    HTCondorWorkflow,
):

    sandbox = "bash::$AP_BASE/sandboxes/cmssw_default.sh"

    shifts = set(MergeHistograms.shifts)

    def create_branch_map(self):
        return [
            DotDict({"category": cat_name, "variable": var_name})
            for cat_name in sorted(self.categories)
            for var_name in sorted(self.variables)
        ]

    def workflow_requires(self):
        reqs = super().workflow_requires()
        reqs["merged_hists"] = self.requires_from_branch()
        return reqs

    def requires(self):
        return {
            d: MergeHistograms.req(
                self,
                dataset=d,
                branch=-1,
                tree_index=0,
                _exclude={"branches"},
                _prefer_cli={"variables"},
            )
            for d in self.datasets
        }

    def output(self):
        b = self.branch_data
        return self.local_target(f"plot__cat_{b.category}__var_{b.variable}.pdf")

    @PlotMixin.view_output_plots
    def run(self):
        import hist

        # prepare config objects
        variable_inst = self.config_inst.get_variable(self.branch_data.variable)
        category_inst = self.config_inst.get_category(self.branch_data.category)
        leaf_category_insts = category_inst.get_leaf_categories() or [category_inst]
        process_insts = list(map(self.config_inst.get_process, self.processes))
        sub_process_insts = {
            proc: [sub for sub, _, _ in proc.walk_processes(include_self=True)]
            for proc in process_insts
        }

        # histogram data per process
        hists = {}

        with self.publish_step(f"plotting {variable_inst.name} in {category_inst.name}"):
            for dataset, inp in self.input().items():
                dataset_inst = self.config_inst.get_dataset(dataset)
                h_in = inp["collection"][0].load(formatter="pickle")[variable_inst.name]

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
                    h = h[{"process": sum, "category": sum, "shift": sum}]

                    # add the histogram
                    if process_inst in hists:
                        hists[process_inst] += h
                    else:
                        hists[process_inst] = h

            # there should be hists to plot
            if not hists:
                raise Exception("no histograms found to plot")

            # sort hists by process order
            hists = OrderedDict(
                (process_inst, hists[process_inst])
                for process_inst in sorted(hists, key=process_insts.index)
            )

            # create the stack and a fake data hist using the smeared sum
            data_hists = [h for process_inst, h in hists.items() if process_inst.is_data]
            mc_hists = [h for process_inst, h in hists.items() if process_inst.is_mc]
            mc_colors = [process_inst.color for process_inst in hists if process_inst.is_mc]
            mc_labels = [process_inst.label for process_inst in hists if process_inst.is_mc]

            h_data, h_mc, h_mc_stack = None, None, None
            if data_hists:
                h_data = sum(data_hists[1:], data_hists[0].copy())
            if mc_hists:
                h_mc = sum(mc_hists[1:], mc_hists[0].copy())
                h_mc_stack = hist.Stack(*mc_hists)

            # setup plotting configs
            # loop over plot_cfgs fields, hard-coded names "method", "hist", "kwargs", "ratio_kwargs"
            plot_cfgs = {
                "MC_stack": {
                    "method": "draw_from_stack",
                    "hist": h_mc_stack,
                    "kwargs": {"norm": 1, "label": mc_labels, "color": mc_colors},
                },
                "MC_uncert": {
                    "method": "draw_error_stairs",
                    "hist": h_mc,
                    "kwargs": {"label": "MC stat. unc."},
                    "ratio_kwargs": {"norm": h_mc.values()},
                },
            }
            # dummy since not implemented yet
            MC_lines = False
            if MC_lines:
                plot_cfgs["MC_lines"] = {
                    "method": "draw_from_stack",
                    # "hist": h_lines_stack,
                    # "kwargs": {"label": lines_label, "color": lines_colors, "stack": False, "histtype": "step"},
                }

            if data_hists:
                plot_cfgs["data"] = {
                    "method": "draw_errorbars",
                    "hist": h_data,
                    "kwargs": {"label": "Data"},
                    "ratio_kwargs": {"norm": h_mc.values()},
                }
            # hard-coded key names from style_cfgs, "ax_cfg", "rax_cfg", "legend_cfg", "CMS_label_cfg"
            style_cfgs = {
                "ax_cfg": {
                    "xlim": (variable_inst.x_min, variable_inst.x_max),
                    "ylabel": variable_inst.get_full_y_title(),
                    # "xlabel": variable_inst.get_full_x_title(),
                },
                "rax_cfg": {
                    "ylabel": "Data / MC",
                    "xlabel": variable_inst.get_full_x_title(),
                },
                "legend_cfg": {},
                "CMS_label_cfg": {
                    "lumi": self.config_inst.x.luminosity.get("nominal") / 1000,  # pb -> fb
                },
            }

            # ToDo: allow updating plot_cfgs and style_cfgs from config

            with self.publish_step("Starting plotting routine ..."):
                fig = plot_all(plot_cfgs, style_cfgs, ratio=True)

            self.output().dump(fig, formatter="mpl")


class PlotShiftedVariables(
    MLModelsMixin,
    ProducersMixin,
    SelectorStepsMixin,
    CalibratorsMixin,
    VariablesMixin,
    ProcessPlotBase,
    ShiftSourcesMixin,
    law.LocalWorkflow,
    HTCondorWorkflow,
):

    sandbox = "bash::$AP_BASE/sandboxes/cmssw_default.sh"

    def store_parts(self):
        parts = super().store_parts()
        parts["plot"] += f"__shifts_{self.shift_sources_repr}"
        return parts

    def create_branch_map(self):
        return [
            DotDict({"category": cat_name, "variable": var_name, "shift_source": source})
            for cat_name in sorted(self.categories)
            for var_name in sorted(self.variables)
            for source in sorted(self.shift_sources)
        ]

    def workflow_requires(self):
        reqs = super().workflow_requires()
        reqs["merged_hists"] = self.requires_from_branch()
        return reqs

    def requires(self):
        return {
            d: MergeShiftedHistograms.req(
                self,
                dataset=d,
                branch=-1,
                _exclude={"branches"},
                _prefer_cli={"variables"},
            )
            for d in self.datasets
        }

    def output(self):
        b = self.branch_data
        return self.local_target(f"plot__cat_{b.category}__var_{b.variable}.pdf")

    @PlotMixin.view_output_plots
    def run(self):
        import hist
        import matplotlib.pyplot as plt
        import mplhep

        # prepare config objects
        variable_inst = self.config_inst.get_variable(self.branch_data.variable)
        category_inst = self.config_inst.get_category(self.branch_data.category)
        leaf_category_insts = category_inst.get_leaf_categories() or [category_inst]
        process_insts = list(map(self.config_inst.get_process, self.processes))
        sub_process_insts = {
            proc: [sub for sub, _, _ in proc.walk_processes(include_self=True)]
            for proc in process_insts
        }
        shift_inst_up = self.config_inst.get_shift(f"{self.branch_data.shift_source}_up")
        shift_inst_down = self.config_inst.get_shift(f"{self.branch_data.shift_source}_down")

        # histogram data per process
        hists = OrderedDict()

        with self.publish_step(f"plotting {variable_inst.name} in {category_inst.name}"):
            for dataset, inp in self.input().items():
                dataset_inst = self.config_inst.get_dataset(dataset)
                h_in = inp["collection"][0].load(formatter="pickle")[variable_inst.name]

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
                    h = h[{"process": sum, "category": sum}]

                    # add the histogram
                    if process_inst in hists:
                        hists[process_inst] += h
                    else:
                        hists[process_inst] = h

            # there should be hists to plot
            if not hists:
                raise Exception("no histograms found to plot")

            # create the stack and the sum
            h_sum = sum(list(hists.values())[1:], list(hists.values())[0].copy())
            h_stack = h_sum.stack("shift")
            label = [self.config_inst.get_shift(h_sum.axes["shift"][i]).label for i in range(3)]

            plt.style.use(mplhep.style.CMS)
            fig, (ax, rax) = plt.subplots(2, 1, gridspec_kw=dict(height_ratios=[3, 1], hspace=0), sharex=True)

            h_stack.plot(
                ax=ax,
                color=["black", "red", "blue"],  # hard-coded sequence of colors
                label=label,
            )
            ax.legend(title=process_inst.name)  # TODO
            ax.set_ylabel(variable_inst.get_full_y_title())
            ax.set_xlim(variable_inst.x_min, variable_inst.x_max)

            # nominal shift id is always 0
            norm = h_sum[{"shift": hist.loc(0)}].view().value
            rax.step(
                x=h_sum.axes[variable_inst.name].edges[1:],
                y=h_sum[{"shift": hist.loc(shift_inst_up.id)}].view().value / norm,
                color="red",
            )
            rax.step(
                x=h_sum.axes[variable_inst.name].edges[1:],
                y=h_sum[{"shift": hist.loc(shift_inst_down.id)}].view().value / norm,
                color="blue",
            )
            rax.axhline(y=1., color="black")
            rax.set_ylim(0.25, 1.75)
            rax.set_xlabel(variable_inst.get_full_x_title())
            rax.set_xlim(variable_inst.x_min, variable_inst.x_max)

            lumi = self.config_inst.x.luminosity.get("nominal") / 1000  # pb -> fb
            mplhep.cms.label(ax=ax, lumi=lumi, label="Work in Progress", fontsize=22)

            self.output().dump(plt, formatter="mpl")
