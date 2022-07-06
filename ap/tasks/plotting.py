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


class ProcessPlotBase(
    CategoriesMixin,
    VariablesMixin,
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
        import numpy as np
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

            # start plotting
            plt.style.use(mplhep.style.CMS)
            fig, (ax, rax) = plt.subplots(2, 1, gridspec_kw=dict(height_ratios=[3, 1], hspace=0), sharex=True)

            if h_mc:
                h_mc_stack.plot(
                    ax=ax,
                    stack=True,
                    histtype="fill",
                    label=mc_labels,
                    color=mc_colors,
                )
                ax.stairs(
                    edges=h_mc.axes[variable_inst.name].edges,
                    baseline=h_mc.view().value - np.sqrt(h_mc.view().variance),
                    values=h_mc.view().value + np.sqrt(h_mc.view().variance),
                    hatch="///",
                    label="MC Stat. unc.",
                    facecolor="none",
                    linewidth=0,
                    color="black",
                )

            if h_data:
                # compute asymmetric poisson errors for data
                # TODO: passing the output of poisson_interval to as yerr to mpl.plothist leads to
                #       buggy error bars and the documentation is clearly wrong (mplhep 0.3.12,
                #       hist 2.4.0), so adjust the output to make up for that, but maybe update or
                #       remove the next lines if this is fixed to not correct it "twice"
                from hist.intervals import poisson_interval
                yerr = poisson_interval(h_data.view().value, h_data.view().variance)
                yerr[np.isnan(yerr)] = 0
                yerr[0] = h_data.view().value - yerr[0]
                yerr[1] -= h_data.view().value

                h_data.plot1d(
                    ax=ax,
                    histtype="errorbar",
                    color="k",
                    label="Data",
                    yerr=yerr,
                )

            # styles
            legend_args = ()
            if h_data:
                handles, labels = ax.get_legend_handles_labels()
                handles.insert(0, handles.pop())
                labels.insert(0, labels.pop())
                legend_args = (handles, labels)
            ax.legend(*legend_args)
            ax.set_xlim(variable_inst.x_min, variable_inst.x_max)
            ax.set_ylabel(variable_inst.get_full_y_title())
            if variable_inst.log_y:
                ax.set_yscale("log")

            # mc stat errors in the ratio
            if h_mc:
                rax.stairs(
                    edges=h_mc.axes[variable_inst.name].edges,
                    baseline=1 - np.sqrt(h_mc.view().variance) / h_mc.view().value,
                    values=1 + np.sqrt(h_mc.view().variance) / h_mc.view().value,
                    facecolor="grey",
                    linewidth=0,
                    hatch="///",
                    color="grey",
                )

            # data points in ratio
            if h_data and h_mc:
                rax.errorbar(
                    x=h_data.axes[variable_inst.name].centers,
                    y=h_data.view().value / h_mc.view().value,
                    yerr=yerr / h_mc.view().value,
                    color="k",
                    linestyle="none",
                    marker="o",
                    elinewidth=1,
                )

            # ratio styles
            rax.axhline(y=1.0, linestyle="dashed", color="gray")
            rax.set_ylabel("Data / MC", loc="center")
            rax.set_ylim(0.85, 1.15)
            rax.set_xlabel(variable_inst.get_full_x_title())
            ax.set_xlim(variable_inst.x_min, variable_inst.x_max)

            # labels
            lumi = self.config_inst.x.luminosity.get("nominal") / 1000  # pb -> fb
            mplhep.cms.label(ax=ax, lumi=lumi, label="Work in Progress", fontsize=22)

            # save the plot
            plt.tight_layout()
            self.output().dump(plt, formatter="mpl")


class PlotShiftedVariables(
    MLModelsMixin,
    ProducersMixin,
    SelectorStepsMixin,
    CalibratorsMixin,
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
            # h_final = hist.Stack(*hists.values())
            h_sum = sum(list(hists.values())[1:], list(hists.values())[0].copy())

            plt.style.use(mplhep.style.CMS)
            fig, (ax, rax) = plt.subplots(2, 1, gridspec_kw=dict(height_ratios=[3, 1], hspace=0), sharex=True)

            h_sum.plot1d(
                ax=ax,
                overlay="shift",
                color=["black", "red", "blue"],
            )
            ax.legend(title=process_inst.name)  # TODO
            ax.set_ylabel(variable_inst.get_full_y_title())
            ax.set_xlim(variable_inst.x_min, variable_inst.x_max)

            # nominal shift id is always 0
            norm = h_sum[{"shift": 0}].view().value
            rax.step(
                x=h_sum.axes[variable_inst.name].edges[1:],
                y=h_sum[{"shift": shift_inst_up.id}].view().value / norm,
                color="red",
            )
            rax.step(
                x=h_sum.axes[variable_inst.name].edges[1:],
                y=h_sum[{"shift": shift_inst_down.id}].view().value / norm,
                color="blue",
            )
            rax.axhline(y=1., color="black")
            rax.set_ylim(0.25, 1.75)
            rax.set_xlabel(variable_inst.get_full_x_title())
            rax.set_xlim(variable_inst.x_min, variable_inst.x_max)

            lumi = self.config_inst.x.luminosity.get("nominal") / 1000  # pb -> fb
            mplhep.cms.label(ax=ax, lumi=lumi, label="Work in Progress", fontsize=22)

            self.output().dump(plt, formatter="mpl")
