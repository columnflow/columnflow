# coding: utf-8

"""
Task to plot different types of histograms
"""

from collections import OrderedDict

import law

from ap.tasks.framework.mixins import (
    CalibratorsSelectorMixin, ProducersMixin, PlotMixin, CategoriesMixin, VariablesMixin,
    DatasetsProcessesMixin, ShiftSourcesMixin,
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
    ProducersMixin,
    CalibratorsSelectorMixin,
    ProcessPlotBase,
    law.LocalWorkflow,
    HTCondorWorkflow,
):

    sandbox = "bash::$AP_BASE/sandboxes/cmssw_default.sh"

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
        hists = OrderedDict()

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

            # create the stack and a fake data hist using the smeared sum
            h_final = hist.Stack(*hists.values())
            h_sum = sum(list(hists.values())[1:], list(hists.values())[0].copy())
            h_data = h_sum.copy().reset()
            h_data.fill(np.repeat(h_sum.axes[0].centers, np.random.poisson(h_sum.view().value)))

            plt.style.use(mplhep.style.CMS)
            fig, (ax, rax) = plt.subplots(2, 1, gridspec_kw=dict(height_ratios=[3, 1], hspace=0), sharex=True)

            h_final.plot(
                ax=ax,
                stack=True,
                histtype="fill",
                label=[process_inst.label for process_inst in hists],
                color=[process_inst.color for process_inst in hists],
            )
            ax.stairs(
                edges=h_sum.axes[variable_inst.name].edges,
                baseline=h_sum.view().value - np.sqrt(h_sum.view().variance),
                values=h_sum.view().value + np.sqrt(h_sum.view().variance),
                hatch="///",
                label="MC Stat. unc.",
                facecolor="none",
                linewidth=0,
                color="black",
            )
            h_data.plot1d(
                ax=ax,
                histtype="errorbar",
                color="k",
                label="Pseudodata",
            )

            ax.set_ylabel(variable_inst.get_full_y_title())
            ax.legend(title="Processes")
            if variable_inst.log_y:
                ax.set_yscale("log")

            from hist.intervals import ratio_uncertainty
            rax.errorbar(
                x=h_data.axes[variable_inst.name].centers,
                y=h_data.view().value / h_sum.view().value,
                yerr=ratio_uncertainty(h_data.view().value, h_sum.view().value, "poisson"),
                color="k",
                linestyle="none",
                marker="o",
                elinewidth=1,
            )
            rax.stairs(
                edges=h_sum.axes[variable_inst.name].edges,
                baseline=1 - np.sqrt(h_sum.view().variance) / h_sum.view().value,
                values=1 + np.sqrt(h_sum.view().variance) / h_sum.view().value,
                facecolor="grey",
                linewidth=0,
                hatch="///",
                color="grey",
            )

            rax.axhline(y=1.0, linestyle="dashed", color="gray")
            rax.set_ylabel("Data / MC", loc="center")
            rax.set_ylim(0.9, 1.1)
            rax.set_xlabel(variable_inst.get_full_x_title())

            lumi = self.config_inst.x.luminosity.get("nominal") / 1000  # pb -> fb
            mplhep.cms.label(ax=ax, lumi=lumi, label="Work in Progress", fontsize=22)

            plt.tight_layout()

            # save the plot
            self.output().dump(plt, formatter="mpl")


class PlotShiftedVariables(
    ProducersMixin,
    CalibratorsSelectorMixin,
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
            ax.legend(title=process_inst.name)
            ax.set_ylabel(variable_inst.get_full_y_title())

            norm = h_sum[{"shift": "nominal"}].view().value
            rax.step(
                x=h_sum.axes[variable_inst.name].edges[1:],
                y=h_sum[{"shift": shift_inst_up.name}].view().value / norm,
                color="red",
            )
            rax.step(
                x=h_sum.axes[variable_inst.name].edges[1:],
                y=h_sum[{"shift": shift_inst_down.name}].view().value / norm,
                color="blue",
            )
            rax.axhline(y=1., color="black")
            rax.set_ylim(0.25, 1.75)
            rax.set_xlabel(variable_inst.get_full_x_title())

            lumi = self.config_inst.x.luminosity.get("nominal") / 1000  # pb -> fb
            mplhep.cms.label(ax=ax, lumi=lumi, label="Work in Progress", fontsize=22)

            self.output().dump(plt, formatter="mpl")
