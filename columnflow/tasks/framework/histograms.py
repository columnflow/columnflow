# coding: utf-8

"""
Base tasks for working with multiple merged histograms.
"""

from __future__ import annotations

import law
import order as od

from columnflow.tasks.framework.base import Requirements, ShiftTask
from columnflow.tasks.framework.mixins import (
    CalibratorsMixin, SelectorStepsMixin, ProducersMixin, MLModelsMixin, WeightProducerMixin,
    VariablesMixin, DatasetsProcessesMixin, CategoriesMixin,
    ShiftSourcesMixin,
)
from columnflow.tasks.histograms import MergeHistograms, MergeShiftedHistograms
from columnflow.util import dev_sandbox, maybe_import

ak = maybe_import("awkward")
hist = maybe_import("hist")


class HistogramsUserBase(
    DatasetsProcessesMixin,
    CategoriesMixin,
    VariablesMixin,
    MLModelsMixin,
    WeightProducerMixin,
    ProducersMixin,
    SelectorStepsMixin,
    CalibratorsMixin,
):
    sandbox = dev_sandbox(law.config.get("analysis", "default_columnar_sandbox"))

    def store_parts(self):
        parts = super().store_parts()
        parts.insert_before("version", "datasets", f"datasets_{self.datasets_repr}")
        return parts

    def load_histogram(
        self,
        dataset: str | od.Dataset,
        variable: str | od.Variable,
    ) -> hist.Hist:
        """
        Helper function to load the histogram from the input for a given dataset and variable.

        :param dataset: The dataset name or instance.
        :param variable: The variable name or instance.
        :return: The loaded histogram.
        """
        if isinstance(dataset, od.Dataset):
            dataset = dataset.name
        if isinstance(variable, od.Variable):
            variable = variable.name
        histogram = self.input()[dataset]["collection"][0]["hists"].targets[variable].load(formatter="pickle")
        return histogram

    def slice_histogram(
        self,
        histogram: hist.Hist,
        processes: str | list[str],
        categories: str | list[str],
        shifts: str | list[str],
        reduce_axes: bool = False,
    ) -> hist.Hist:
        """
        Slice a histogram by processes, categories, and shifts.

        This function takes a histogram and slices it based on the specified processes, categories, and shifts.
        It returns the sliced histogram.

        :param histogram: The histogram to slice. It must have the axes "process", "category", and "shift".
        :param processes: The process name(s) to slice by. Can be a single process name or a list of process names.
        :param categories: The category name(s) to slice by. Can be a single category name or a list of category names.
        :param shifts: The shift name(s) to slice by. Can be a single shift name or a list of shift names.
        :param reduce_axes: Whether to sum over the process, category, and shift axes after slicing.
        :return: The sliced histogram.
        """
        import hist

        def flatten_nested_list(nested_list):
            return [item for sublist in nested_list for item in sublist]

        # transform into lists if necessary
        processes = law.util.make_list(processes)
        categories = law.util.make_list(categories)
        shifts = law.util.make_list(shifts)

        # get all leaf categories
        category_insts = list(map(self.config_inst.get_category, categories))
        leaf_category_insts = set(flatten_nested_list([
            category_inst.get_leaf_categories() or [category_inst]
            for category_inst in category_insts
        ]))

        # get all sub processes
        process_insts = list(map(self.config_inst.get_process, processes))
        sub_process_insts = set(flatten_nested_list([
            [sub for sub, _, _ in proc.walk_processes(include_self=True)]
            for proc in process_insts
        ]))

        # get all shift instances
        shift_insts = [self.config_inst.get_shift(shift) for shift in shifts]

        # work on a copy
        h = histogram.copy()

        # axis selections
        h = h[{
            "process": [
                hist.loc(p.id)
                for p in sub_process_insts
                if p.id in h.axes["process"]
            ],
            "category": [
                hist.loc(c.id)
                for c in leaf_category_insts
                if c.id in h.axes["category"]
            ],
            "shift": [
                hist.loc(s.id)
                for s in shift_insts
                if s.id in h.axes["shift"]
            ],
        }]

        if reduce_axes:
            # axis reductions
            h = h[{"process": sum, "category": sum, "shift": sum}]

        return h


class HistogramsUserSingleShiftBase(
    HistogramsUserBase,
    ShiftTask,
):

    # upstream requirements
    reqs = Requirements(
        MergeHistograms=MergeHistograms,
    )

    def requires(self):
        return {
            d: self.reqs.MergeHistograms.req(
                self,
                dataset=d,
                branch=-1,
                _exclude={"branches"},
                _prefer_cli={"variables"},
            )
            for d in self.datasets
        }

    def workflow_requires(self):
        reqs = super().workflow_requires()
        reqs["merged_hists"] = self.requires_from_branch()

        return reqs


class HistogramsUserMultiShiftBase(
    HistogramsUserBase,
    ShiftSourcesMixin,
):
    # upstream requirements
    reqs = Requirements(
        MergeShiftedHistograms=MergeShiftedHistograms,
    )

    def requires(self):
        return {
            d: self.reqs.MergeShiftedHistograms.req(
                self,
                dataset=d,
                branch=-1,
                _exclude={"branches"},
                _prefer_cli={"variables"},
            )
            for d in self.datasets
        }

    def workflow_requires(self):
        reqs = super().workflow_requires()
        reqs["merged_hists"] = self.requires_from_branch()

        return reqs
