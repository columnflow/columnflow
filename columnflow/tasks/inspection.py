# coding: utf-8

"""
Custom tasks for inspecting the outputs of certain columnflow tasks.
"""

import luigi

from columnflow.tasks.framework.histograms import (
    HistogramsUserSingleShiftBase,
    HistogramsUserMultiShiftBase,
)


class InspectHistograms(
    HistogramsUserSingleShiftBase,
):
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
        hists = {}

        for dataset in self.datasets:
            for variable in self.variables:
                h_in = self.load_histogram(dataset, variable)
                h_in = self.slice_histogram(h_in, self.processes, self.categories, self.shift)

                if variable in hists.keys():
                    hists[variable] += h_in
                else:
                    hists[variable] = h_in

        if self.debugger:
            from IPython import embed
            embed()


class InspectShiftedHistograms(
    HistogramsUserMultiShiftBase,
):
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
        shifts = ["nominal"] + self.shifts
        hists = {}

        for dataset in self.datasets:
            for variable in self.variables:
                h_in = self.load_histogram(dataset, variable)
                h_in = self.slice_histogram(h_in, self.processes, self.categories, shifts)

                if variable in hists.keys():
                    hists[variable] += h_in
                else:
                    hists[variable] = h_in

        if self.debugger:
            from IPython import embed
            embed()
