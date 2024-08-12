import law
import order as od
import luigi
from collections import OrderedDict

from columnflow.tasks.plotting import PlotVariablesBaseSingleShift, PlotVariables1D
from columnflow.tasks.framework.plotting import PlotBase1D, PlotBase, PlotBase2D
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
                "category": law.util.create_hash(cats),
                "categories": cats,
                "process": proc_name,
                "variable": var_name,
            })
            for proc_name in sorted(self.processes)
            for var_name in sorted(self.variables)
        ]

    def output(self):
        return {"plots": [
            self.local_target(name)
            for name in self.get_plot_names("plot")
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


class PlotVariables2DMigration(
    PlotVariablesCatsPerProcessBase,
    PlotBase2D,
):
    plot_function = PlotBase.plot_function.copy(
        default="columnflow.plotting.cmsGhent.plot_functions_2d.plot_migration_matrices",
    )

    def create_branch_map(self):
        return [
            DotDict({
                "category": self.initial + "_" + cat_name,
                "categories": [self.initial, cat_name],
                "process": proc_name,
                "variable": var_name,
            })
            for cat_name in sorted(self.categories)
            for proc_name in sorted(self.processes)
            for var_name in sorted(self.variables)
        ]


class MultiVarMixin:

    def requires(self):
        variables = ",".join([",".join(self.variable_tuples[vr]) for vr in self.variables])
        reqs = {
            dataset: self.reqs.MergeHistograms.req(
                self,
                variables=variables,
                branch=-1,
                dataset=dataset,
                _exclude={"branches"},
                _prefer_cli={"variables"},
            )
            for dataset in self.datasets
        }
        return reqs

    def run_var(
            self,
            variable_inst: od.Variable,
            sub_process_insts: dict[od.Process, list[od.Process]],
            category_insts: list[od.Category],
            plot_shifts: list[od.Shift],
    ):
        import hist

        hists = {}
        process_insts = list(sub_process_insts)

        for dataset, inp in self.input().items():
            dataset_inst = self.config_inst.get_dataset(dataset)
            h_in = inp["collection"][0]["hists"].targets[variable_inst.name].load(formatter="pickle")

            # loop and extract one histogram per process
            for process_inst in process_insts:
                # skip when the dataset is already known to not contain any sub process
                if not any(
                        dataset_inst.has_process(sub_process_inst.name)
                        for sub_process_inst in sub_process_insts[process_inst]
                ):
                    continue

                # select processes and reduce axis
                h = h_in.copy()
                h = h[{
                    "process": [
                        hist.loc(p.id)
                        for p in sub_process_insts[process_inst]
                        if p.id in h.axes["process"]
                    ],
                }]
                h = h[{"process": sum}]

                # add the histogram
                if process_inst in hists:
                    hists[process_inst] += h
                else:
                    hists[process_inst] = h

        # there should be hists to plot
        if not hists:
            raise Exception(
                "no histograms found to plot; possible reasons:\n"
                "  - requested variable requires columns that were missing during histogramming\n"
                "  - selected --processes did not match any value on the process axis of the input histogram",
            )

        # update histograms using a custom hook
        hists = self.invoke_hist_hook(hists)

        # add new processes to the end of the list
        for process_inst in hists:
            if process_inst not in process_insts:
                process_insts.append(process_inst)

        # axis selections and reductions, including sorting by process order
        _hists = OrderedDict()
        for process_inst in sorted(hists, key=process_insts.index):
            h = hists[process_inst]
            # selections
            h = h[{
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
            # reductions
            h = h[{"category": sum}]
            # store
            _hists[process_inst] = h
        return _hists


class PlotVariables1DMultiVar(MultiVarMixin, PlotVariables1D):

    exclude_index = False

    plot_function = PlotVariables1D.plot_function.copy(
        default="columnflow.plotting.cmsGhent.plot_functions_1d.plot_multi_variables",
    )

    skip_ratio = PlotVariables1D.skip_ratio.copy(
        default=True,
    )

    @law.decorator.log
    @view_output_plots
    def run(self):
        # get the shifts to extract and plot
        plot_shifts = law.util.make_list(self.get_plot_shifts())

        # prepare config objects
        variable_tuple = self.variable_tuples[self.branch_data.variable]
        variable_insts = [
            self.config_inst.get_variable(var_name)
            for var_name in variable_tuple
        ]
        category_inst = self.config_inst.get_category(self.branch_data.category)
        leaf_category_insts = [category_inst]
        process_insts = list(map(self.config_inst.get_process, self.processes))
        sub_process_insts = {
            proc: [sub for sub, _, _ in proc.walk_processes(include_self=True)]
            for proc in process_insts
        }

        # histogram data per variable
        hists = {}

        with self.publish_step(f"plotting {self.branch_data.variable} in {category_inst.name}"):

            # histogram data per variable

            for variable_inst in variable_insts:
                var_hists = self.run_var(variable_inst, sub_process_insts, leaf_category_insts, plot_shifts)
                var_hists = list(var_hists.values())
                hists[variable_inst] = sum(var_hists[1:], var_hists[0])

            # sort hists by varianle order
            hists = OrderedDict(
                (variable_inst, hists[variable_inst])
                for variable_inst in sorted(hists, key=variable_insts.index)
            )
            # call the plot function
            fig, _ = self.call_plot_func(
                self.plot_function,
                hists=hists,
                config_inst=self.config_inst,
                category_inst=category_inst.copy_shallow(),
                **self.get_plot_parameters(),
            )

            # save the plot
            for outp in self.output()["plots"]:
                outp.dump(fig, formatter="mpl")


class PlotROC(MultiVarMixin, PlotVariablesBaseSingleShift):

    exclude_index = False

    plot_function = PlotBase.plot_function.copy(
        default="columnflow.plotting.cmsGhent.plot_functions_1d.plot_roc",
        add_default_to_description=True,
    )

    def create_branch_map(self):
        return [
            DotDict({"category": cat_name})
            for cat_name in sorted(self.categories)
        ]

    def plot_parts(self) -> law.util.InsertableDict:
        parts = law.util.InsertableDict()
        parts["processes"] = f"proc_{self.processes_repr}"
        parts["category"] = f"cat_{self.branch_data.category}"
        parts["variable"] = f"var_{self.variables_repr}"

        if self.hist_hook not in ("", law.NO_STR, None):
            parts["hook"] = f"hook_{self.hist_hook}"

        return parts

    @law.decorator.log
    @view_output_plots
    def run(self):
        # get the shifts to extract and plot
        plot_shifts = law.util.make_list(self.get_plot_shifts())

        # prepare config objects
        variable_tuples = [self.variable_tuples[vr] for vr in self.variables]
        variable_insts_groups = [
            [self.config_inst.get_variable(var_name) for var_name in variable_tuple]
            for variable_tuple in variable_tuples
        ]
        category_inst = self.config_inst.get_category(self.branch_data.category)
        leaf_category_insts = [category_inst]
        process_insts = list(map(self.config_inst.get_process, self.processes))
        sub_process_insts = {
            proc: [sub for sub, _, _ in proc.walk_processes(include_self=True)]
            for proc in process_insts
        }

        # histogram data per variable
        hists = {}

        with self.publish_step(f"plotting {self.variables} in {category_inst.name}"):

            # histogram data per variable

            for variable_insts in variable_insts_groups:
                for variable_inst in variable_insts:
                    var_hists = self.run_var(variable_inst, sub_process_insts, leaf_category_insts, plot_shifts)
                    var_hists = list(var_hists.values())
                    hists[variable_inst] = sum(var_hists[1:], var_hists[0])

            # call the plot function
            fig, _ = self.call_plot_func(
                self.plot_function,
                hists=hists,
                config_inst=self.config_inst,
                category_inst=category_inst.copy_shallow(),
                variable_insts_groups=variable_insts_groups,
                **self.get_plot_parameters(),
            )

            # save the plot
            for outp in self.output()["plots"]:
                outp.dump(fig, formatter="mpl")
