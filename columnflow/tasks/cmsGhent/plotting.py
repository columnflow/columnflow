import law
import luigi
from collections import OrderedDict

from columnflow.tasks.plotting import PlotVariablesBaseSingleShift
from columnflow.tasks.framework.plotting import PlotBase1D, PlotBase
from columnflow.tasks.framework.decorators import view_output_plots
from columnflow.util import DotDict


class PlotVariablesCatsPerProcessBase(PlotVariablesBaseSingleShift):

    exclude_index = True

    initial = luigi.Parameter(
        default="incl",
        description="Name of category that is considered as initial reference.",
    )

    def create_branch_map(self):
        cats = self.categories
        if self.initial not in cats:
            cats = [self.initial, *cats]
        return [
            DotDict({
                "categories": cats,
                "process": proc_name,
                "variable": var_name,
            })
            for proc_name in sorted(self.processes)
            for var_name in sorted(self.variables)
        ]

    def output(self):
        b = self.branch_data
        cat_tag = "_".join(b.categories)
        return {"plots": [
            self.local_target(name)
            for name in self.get_plot_names(f"plot__proc_{b.process}__cat_{cat_tag}__var_{b.variable}")
        ]}

    @law.decorator.log
    @view_output_plots
    def run(self):
        import hist

        # get the shifts to extract and plot
        plot_shifts = law.util.make_list(self.get_plot_shifts())

        # prepare config objects
        variable_tuple = self.variable_tuples[self.branch_data.variable]
        variable_insts = [
            self.config_inst.get_variable(var_name)
            for var_name in variable_tuple
        ]
        category_insts = [self.config_inst.get_category(c) for c in self.branch_data.categories]
        process_inst = self.config_inst.get_process(self.branch_data.process)
        sub_process_insts = [sub for sub, _, _ in process_inst.walk_processes(include_self=True)]

        # histogram data for process
        process_hist = 0

        with self.publish_step(f"plotting {self.branch_data.variable} for {process_inst.name}"):
            for dataset, inp in self.input().items():
                dataset_inst = self.config_inst.get_dataset(dataset)
                h_in = inp["collection"][0]["hists"].targets[self.branch_data.variable].load(formatter="pickle")

                # extract one histogram for process
                # skip when the dataset is already known to not contain any sub process
                if not any(map(dataset_inst.has_process, sub_process_insts)):
                    continue

                # work on a copy
                h = h_in.copy()

                # axis selections
                h = h[{
                    "process": [
                        hist.loc(p.id)
                        for p in sub_process_insts
                        if p.id in h.axes["process"]
                    ],
                    "category": [
                        hist.loc(c.id)
                        for c in category_insts
                        if c.id in h.axes["category"]
                    ],
                    "shift": [
                        hist.loc(s.id)
                        for s in plot_shifts
                        if s.id in h.axes["shift"]
                    ],
                }]

                # axis reductions
                h = h[{"process": sum}]

                # add the histogram
                process_hist = h + process_hist

            # there should be hists to plot
            if not process_hist:
                raise Exception(
                    "no histograms found to plot; possible reasons:\n" +
                    "  - requested variable requires columns that were missing during histogramming\n" +
                    "  - selected --processes did not match any value on the process axis of the input histogram",
                )

            process_hists = OrderedDict(
                (cat.name, h[{"category": hist.loc(cat.id)}])
                for cat in category_insts
            )
            # call the plot function
            fig, _ = self.call_plot_func(
                self.plot_function,
                hists=process_hists,
                config_inst=self.config_inst,
                category_inst=process_inst.copy_shallow(),
                variable_insts=[var_inst.copy_shallow() for var_inst in variable_insts],
                style_config={
                    "legend_cfg": {"title": process_inst.label},
                    "rax_cfg": {"ylabel": "Category / " + self.initial},
                },
                initial=self.initial,
                **self.get_plot_parameters(),
            )

            # save the plot
            for outp in self.output()["plots"]:
                outp.dump(fig, formatter="mpl")


class PlotVariables1DCatsPerProcess(
    PlotVariablesCatsPerProcessBase,
    PlotBase1D,
):
    plot_function = PlotBase.plot_function.copy(
        default="columnflow.plotting.plot_functions_1d.plot_variable_variants",
    )
