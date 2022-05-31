# coding: utf-8

"""
Task to quickly plot data from branch 0 (skips MergeHistograms)
"""

import law

from ap.order_util import getDatasetNamesFromProcesses, getDatasetNamesFromProcess

from ap.tasks.framework import ConfigTask, HTCondorWorkflow
from ap.util import ensure_proxy

from ap.tasks.histograms import CreateHistograms


class TestPlotting(ConfigTask, law.LocalWorkflow, HTCondorWorkflow):

    sandbox = "bash::$AP_BASE/sandboxes/cmssw_default.sh"
    # sandbox = "bash::$AP_BASE/sandboxes/venv_columnar.sh"

    processes = law.CSVParameter(
        default=(),
        description="List of processes to plot"
    )
    variables = law.CSVParameter(
        default=(),
        description="List of variables to plot"
    )
    categories = law.CSVParameter(
        default=("incl"),
        description="List of categories to create plots for"
    )

    def store_parts(self):
        # print("Hello from store_parts")
        parts = super(TestPlotting, self).store_parts()
        # add process names after config name
        procs = ""
        if not self.processes:
            self.processes = self.config_inst.analysis.get_processes(self.config_inst).names()  # required here to check if output already exists. These two lines can possibly be removed from requires
        for p in self.processes:
            procs += p + "_"
        procs = procs[:-1]
        parts.insert_after("config", "processes", procs)
        return parts

    def create_branch_map(self):
        # print('Hello from create_branch_map')
        if not self.variables:
            self.variables = self.config_inst.variables.names()
        branch_map = {}
        for i, var in enumerate(self.variables):
            for j, cat in enumerate(self.categories):
                branch_map[i * len(self.categories) + j] = {"variable": var, "category": cat}
        return branch_map

    def workflow_requires(self):
        c = self.config_inst
        # determine which datasets to require
        if not self.processes:
            self.processes = c.analysis.get_processes(c).names()
        self.datasets = getDatasetNamesFromProcesses(c, self.processes)
        return {d: CreateHistograms.req(self, branch=0, dataset=d) for d in self.datasets}

    def requires(self):
        # print('Hello from requires')
        c = self.config_inst
        # determine which datasets to require
        if not self.processes:
            self.processes = c.analysis.get_processes(c).names()
        self.datasets = getDatasetNamesFromProcesses(c, self.processes)
        return {d: CreateHistograms.req(self, branch=0, dataset=d) for d in self.datasets}

    def output(self):
        # print('Hello from output')
        return self.local_target(f"plot_{self.branch_data['category']}_{self.branch_data['variable']}.pdf")

    @law.decorator.safe_output
    @ensure_proxy
    def run(self):
        with self.publish_step(f"Hello from TestPlotting for variable {self.branch_data['variable']}, category {self.branch_data['category']}"):
            import numpy as np
            import hist
            import matplotlib.pyplot as plt
            import mplhep
            plt.style.use(mplhep.style.CMS)

            c = self.config_inst

            histograms = []
            h_total = None
            colors = []
            label = []
            category = self.branch_data['category']

            with self.publish_step("Adding histograms together ..."):
                for p in self.processes:
                    # print("-------- process:", p)
                    h_proc = None
                    for d in getDatasetNamesFromProcess(c, p):
                        # print("----- dataset:", d)
                        h_in = self.input()[d].load(formatter="pickle")[self.branch_data['variable']]

                        if category == "incl":
                            leaf_cats = [cat.id for cat in c.get_leaf_categories()]
                        elif c.get_category(category).is_leaf_category:
                            leaf_cats = [c.get_category(category).id]
                        else:
                            leaf_cats = [cat.id for cat in c.get_category(category).get_leaf_categories()]

                        h_in = h_in[{"category": leaf_cats}]
                        h_in = h_in[{"category": sum}]
                        h_in = h_in[{"shift": "nominal"}]
                        print("dataset {}: {}".format(d, h_in[::sum]))

                        if h_proc is None:
                            h_proc = h_in.copy()
                        else:
                            h_proc += h_in

                    if h_total is None:
                        h_total = h_proc.copy()
                    else:
                        h_total += h_proc
                    histograms.append(h_proc)
                    colors.append(c.get_process(p).color)
                    label.append(c.get_process(p).label)

                h_final = hist.Stack(*histograms)
                h_data = h_total.copy()
                h_data.reset()
                h_data.fill(np.repeat(h_total.axes[0].centers, np.random.poisson(h_total.view().value)))

            with self.publish_step("Starting plotting routine ..."):
                fig, (ax, rax) = plt.subplots(2, 1, gridspec_kw=dict(height_ratios=[3, 1], hspace=0), sharex=True)

                h_final.plot(
                    ax=ax,
                    stack=True,
                    histtype="fill",
                    label=label,
                    color=colors,
                )
                ax.stairs(
                    edges=h_total.axes[self.branch_data['variable']].edges,
                    baseline=h_total.view().value - np.sqrt(h_total.view().variance),
                    values=h_total.view().value + np.sqrt(h_total.view().variance),
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

                ax.set_ylabel(c.get_variable(self.branch_data['variable']).get_full_y_title())
                ax.legend(title="Processes")
                if c.get_variable(self.branch_data['variable']).log_y:
                    ax.set_yscale('log')

                from hist.intervals import ratio_uncertainty
                rax.errorbar(
                    x=h_data.axes[self.branch_data['variable']].centers,
                    y=h_data.view().value / h_total.view().value,
                    yerr=ratio_uncertainty(h_data.view().value, h_total.view().value, "poisson"),
                    color="k",
                    linestyle="none",
                    marker="o",
                    elinewidth=1,
                )
                rax.stairs(
                    edges=h_total.axes[self.branch_data['variable']].edges,
                    baseline=1 - np.sqrt(h_total.view().variance) / h_total.view().value,
                    values=1 + np.sqrt(h_total.view().variance) / h_total.view().value,
                    facecolor="grey",
                    linewidth=0,
                    hatch="///",
                    color="grey",
                )

                rax.axhline(y=1.0, linestyle="dashed", color="gray")
                rax.set_ylabel("Data / MC", loc="center")
                rax.set_ylim(0.9, 1.1)
                rax.set_xlabel(c.variables.get(self.branch_data['variable']).get_full_x_title())

                mplhep.cms.label(ax=ax, lumi=c.x.luminosity / 1000, label="Work in Progress", fontsize=22)

                # mplhep.plot.yscale_legend(ax=ax) # legend optimizer (takes quite long and potentially produces too much whitespace)
                plt.tight_layout()

            self.output().dump(plt, formatter="mpl")
            self.publish_message(f"TestPlotting task done for variable {self.branch_data['variable']}, category {self.branch_data['category']}")
