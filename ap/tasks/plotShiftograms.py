# coding: utf-8

"""
Task to plot the first histograms
"""


import law

from ap.order_util import getDatasetNamesFromProcesses, getDatasetNamesFromProcess

from ap.tasks.framework import ConfigTask, HTCondorWorkflow

from ap.tasks.mergeHistograms import MergeShiftograms


class PlotShiftograms(ConfigTask, law.LocalWorkflow, HTCondorWorkflow):

    sandbox = "bash::$AP_BASE/sandboxes/cmssw_default.sh"
    # sandbox = "bash::$AP_BASE/sandboxes/venv_columnar.sh"

    processes = law.CSVParameter(
        default=("st_tchannel_t"),
        description="List of processes to create plots for"
    )
    variables = law.CSVParameter(
        default=("HT"),
        description="List of variables to plot"
    )
    categories = law.CSVParameter(
        default=("incl"),
        description="List of categories to create plots for"
    )
    systematics = law.CSVParameter(
        default=("jec"),
        description="List of systematic uncertainties to consider"
    )

    def create_branch_map(self):
        # print('Hello from create_branch_map')
        if not self.variables:
            self.variables = self.config_inst.variables.names()
        branch_map = {}
        for i, var in enumerate(self.variables):
            for j, cat in enumerate(self.categories):
                for k, proc in enumerate(self.processes):
                    for l, syst in enumerate(self.systematics):
                        branch_map[i + len(self.variables) * (j + len(self.categories) * (k + len(self.processes) * l))] = {"variable": var, "category": cat, "process": proc, "systematic": syst}
        return branch_map

    def workflow_requires(self):
        self.datasets = getDatasetNamesFromProcesses(self.config_inst, self.processes)
        req_map = {}
        for d in self.datasets:
            req_map[d] = MergeShiftograms.req(self)
        return req_map

    def requires(self):
        self.datasets = getDatasetNamesFromProcesses(self.config_inst, self.processes)
        req_map = {}
        for d in self.datasets:
            req_map[d] = MergeShiftograms.req(self, dataset=d)
        return req_map

    def output(self):
        return self.local_target(f"systplot_{self.branch_data['category']}_{self.branch_data['process']}_{self.branch_data['variable']}_{self.branch_data['systematic']}.pdf")

    def run(self):
        with self.publish_step(f"Hello from PlotShiftograms for process {self.branch_data['process']}, category {self.branch_data['category']}, variable {self.branch_data['variable']}, systematic {self.branch_data['systematic']}"):
            import matplotlib.pyplot as plt
            import mplhep
            plt.style.use(mplhep.style.CMS)

            c = self.config_inst
            category = self.branch_data['category']

            h_proc = None
            for d in getDatasetNamesFromProcess(c, self.branch_data['process']):
                h_in = self.input()[d].load(formatter="pickle")[self.branch_data['variable']]
                # categorization
                # for now, only leaf categories are considered
                if category == "incl":
                    leaf_cats = [cat.id for cat in c.get_leaf_categories()]
                elif c.get_category(category).is_leaf_category:
                    leaf_cats = [c.get_category(category).id]
                else:
                    leaf_cats = [cat.id for cat in c.get_category(category).get_leaf_categories()]

                # Note: this only works because the category axis is sorted and does not skip an integer
                # h_in[{"category": [0,5]}] gives the categories that are at the position 0 and 5
                h_in = h_in[{"category": leaf_cats}]
                h_in = h_in[{"category": sum}]

                if h_proc is None:
                    h_proc = h_in.copy()
                else:
                    h_proc += h_in

            with self.publish_step("Starting plotting routine ..."):
                fig, (ax, rax) = plt.subplots(2, 1, gridspec_kw=dict(height_ratios=[3, 1], hspace=0), sharex=True)

                print("------")
                print(h_proc.axes)
                print(h_proc.view())
                h_proc.plot1d(
                    ax=ax,
                    overlay="shift",
                    color=["black", "red", "blue"],
                )
                print("------")
                ax.legend(title=self.branch_data['process'])
                ax.set_ylabel(c.get_variable(self.branch_data['variable']).get_full_y_title())

                norm = h_proc[{"shift": "nominal"}].view().value
                rax.step(
                    x=h_proc.axes[self.branch_data['variable']].edges[+1:],
                    y=h_proc[{"shift": self.branch_data['systematic'] + "_up"}].view().value / norm,
                    color="red",
                )
                print("------")
                rax.step(
                    x=h_proc.axes[self.branch_data['variable']].edges[+1:],
                    y=h_proc[{"shift": self.branch_data['systematic'] + "_down"}].view().value / norm,
                    color="blue",
                )
                rax.axhline(y=1., color="black")
                rax.set_ylim(0.25, 1.75)
                rax.set_xlabel(c.variables.get(self.branch_data['variable']).get_full_x_title())
                print("------")

                # mplhep.cms.label(ax=ax, lumi=c.x.luminosity / 1000, label="Work in Progress", fontsize=22)

            self.output().dump(plt, formatter="mpl")
            self.publish_message(f"Plotting task done for process {self.branch_data['process']}, category {self.branch_data['category']}, variable {self.branch_data['variable']}, systematic {self.branch_data['systematic']}")
