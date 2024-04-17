
from collections import OrderedDict

from ..cutflow import *
from columnflow.tasks.framework.decorators import view_output_plots

import luigi
import law
from columnflow.util import maybe_import

np = maybe_import("numpy")

PlotCutflow.relative = luigi.BoolParameter(
        default=False,
        significant=False,
        description="name of the variable to use for obtaining event counts; default: 'False'",
    )

@law.decorator.log
@view_output_plots
def PlotCutflow_run(self):
    import hist

    # prepare config objects
    category_inst = self.config_inst.get_category(self.branch_data)
    leaf_category_insts = category_inst.get_leaf_categories() or [category_inst]
    process_insts = list(map(self.config_inst.get_process, self.processes))
    sub_process_insts = {
        proc: [sub for sub, _, _ in proc.walk_processes(include_self=True)]
        for proc in process_insts
    }

    # histogram data per process
    hists = {}

    with self.publish_step(f"plotting cutflow in {category_inst.name}"):
        for dataset, inp in self.input().items():
            dataset_inst = self.config_inst.get_dataset(dataset)
            h_in = inp[self.variable].load(formatter="pickle")

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
                h = h[{"process": sum, "category": sum, self.variable: sum}]

                # add the histogram
                if process_inst in hists:
                    hists[process_inst] += h
                else:
                    hists[process_inst] = h

        # there should be hists to plot
        if not hists:
            raise Exception("no histograms found to plot")
        
        total = sum(hists.values()).values() if self.relative else np.ones((len(self.selector_steps) + 1, 1))

        # sort hists by process order
        hists = OrderedDict(
            (process_inst.copy_shallow(), hists[process_inst] / total)
            for process_inst in sorted(hists, key=process_insts.index)
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


PlotCutflow.run = PlotCutflow_run



