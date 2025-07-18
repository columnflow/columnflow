# coding: utf-8

"""
Custom tasks for inspecting the outputs of certain columnflow tasks.
"""

import luigi

from columnflow.tasks.framework.histograms import (
    HistogramsUserSingleShiftBase,
    HistogramsUserMultiShiftBase,
)


class InspectHistograms(HistogramsUserSingleShiftBase):
    """
    Task to inspect histograms of a single shift after Reduction, Production and MLEvaluation.
    """

    debugger = luigi.BoolParameter(
        default=True,
        description="Whether to start a ipython debugger session or not; default: True",
    )

    def output(self):
        return {"always_incomplete_dummy": self.target("dummy.txt")}

    def run(self):
        """
        Loads histograms for all configs, variables, and datasets,
        sums them up for each variable and
        slices them according to the processes, categories, and shift,
        The resulting histograms are stored in a dictionary with variable names as keys.
        If `debugger` is set to True, an IPython debugger session is started for
        interactive inspection of the histograms.
        """
        shifts = {self.shift, "nominal"}
        hists = {}

        for variable in self.variables:
            for i, config_inst in enumerate(self.config_insts):
                hist_per_config = None
                sub_processes = self.processes[i]
                for dataset in self.datasets[i]:
                    # sum over all histograms of the same variable and config
                    if hist_per_config is None:
                        hist_per_config = self.load_histogram(config_inst, dataset, variable)
                    else:
                        hist_per_config += self.load_histogram(config_inst, dataset, variable)

                # slice histogram per config according to the sub_processes and categories
                hist_per_config = self.slice_histogram(
                    histogram=hist_per_config,
                    config_inst=config_inst,
                    processes=sub_processes,
                    categories=self.categories,
                    shifts=shifts,
                )

                if variable in hists.keys():
                    hists[variable] += hist_per_config
                else:
                    hists[variable] = hist_per_config

        if self.debugger:
            from IPython import embed
            embed()


class InspectShiftedHistograms(HistogramsUserMultiShiftBase):
    """
    Task to inspect histograms of multiple shifts after Reduction, Production and MLEvaluation.
    """

    debugger = luigi.BoolParameter(
        default=True,
        description="Whether to start a ipython debugger session or not; default: True",
    )

    def output(self):
        return {"always_incomplete_dummy": self.target("dummy.txt")}

    def run(self):
        """
        Loads histograms for all configs, variables, and datasets,
        sums them up for each variable and
        slices them according to the processes, categories, and shift,
        The resulting histograms are stored in a dictionary with variable names as keys.
        If `debugger` is set to True, an IPython debugger session is started for
        interactive inspection of the histograms.
        """
        shifts = ["nominal"] + self.shifts
        hists = {}

        for variable in self.variables:
            for i, config_inst in enumerate(self.config_insts):
                hist_per_config = None
                sub_processes = self.processes[i]
                for dataset in self.datasets[i]:
                    # sum over all histograms of the same variable and config
                    if hist_per_config is None:
                        hist_per_config = self.load_histogram(config_inst, dataset, variable)
                    else:
                        hist_per_config += self.load_histogram(config_inst, dataset, variable)

                # slice histogram per config according to the sub_processes and categories
                hist_per_config = self.slice_histogram(
                    histogram=hist_per_config,
                    config_inst=config_inst,
                    processes=sub_processes,
                    categories=self.categories,
                    shifts=shifts,
                )

                if variable in hists.keys():
                    hists[variable] += hist_per_config
                else:
                    hists[variable] = hist_per_config

        if self.debugger:
            from IPython import embed
            embed()
