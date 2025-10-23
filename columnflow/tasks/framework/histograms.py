# coding: utf-8

"""
Base tasks for working with multiple merged histograms.
"""

from __future__ import annotations

import law
import order as od

from columnflow.tasks.framework.base import Requirements, ShiftTask
from columnflow.tasks.framework.mixins import (
    CalibratorClassesMixin, SelectorClassMixin, ReducerClassMixin, ProducerClassesMixin, MLModelsMixin,
    HistProducerClassMixin, VariablesMixin, DatasetsProcessesMixin, CategoriesMixin, ShiftSourcesMixin,
)
from columnflow.tasks.histograms import MergeHistograms, MergeShiftedHistograms
from columnflow.util import dev_sandbox, maybe_import
from columnflow.types import TYPE_CHECKING

if TYPE_CHECKING:
    hist = maybe_import("hist")


class HistogramsUserBase(
    CalibratorClassesMixin,
    SelectorClassMixin,
    ReducerClassMixin,
    ProducerClassesMixin,
    HistProducerClassMixin,
    MLModelsMixin,
    DatasetsProcessesMixin,
    CategoriesMixin,
    VariablesMixin,
):
    single_config = False

    sandbox = dev_sandbox(law.config.get("analysis", "default_columnar_sandbox"))

    def store_parts(self) -> law.util.InsertableDict:
        parts = super().store_parts()
        parts.insert_before("version", "datasets", f"datasets_{self.datasets_repr}")
        return parts

    def load_histogram(
        self,
        inputs: dict,
        config: str | od.Config,
        dataset: str | od.Dataset,
        variable: str | od.Variable,
        update_label: bool = True,
    ) -> hist.Hist:
        """
        Helper function to load the histogram from the input for a given dataset and variable.

        :param inputs: The inputs dictionary containing the histograms.
        :param config: The config name or instance.
        :param dataset: The dataset name or instance.
        :param variable: The variable name or instance.
        :param update_label: Whether to update the label of the variable axis in the histogram.
        If True, the label will be updated based on the first config instance's variable label.
        :return: The loaded histogram.
        """

        if isinstance(dataset, od.Dataset):
            dataset = dataset.name
        if isinstance(variable, od.Variable):
            variable = variable.name
        if isinstance(config, od.Config):
            config = config.name
        histogram = inputs[config][dataset]["collection"][0]["hists"].targets[variable].load(formatter="pickle")

        if update_label:
            # get variable label from first config instance
            for var_name in variable.split("-"):
                label = self.config_insts[0].get_variable(var_name).x_title
                ax_names = [ax.name for ax in histogram.axes]
                if var_name in ax_names:
                    # update the label of the variable axis
                    histogram.axes[var_name].label = label
        return histogram

    def slice_histogram(
        self,
        histogram: hist.Hist,
        config_inst: od.Config,
        processes: str | list[str] | None = None,
        categories: str | list[str] | None = None,
        shifts: str | list[str] | None = None,
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

        selection_dict = {}

        if processes:
            # transform into lists if necessary
            processes = law.util.make_list(processes)
            # get all sub processes

            process_insts = list(map(config_inst.get_process, processes))
            sub_process_insts = set(flatten_nested_list([
                [sub for sub, _, _ in proc.walk_processes(include_self=True)]
                for proc in process_insts
            ]))
            selection_dict["process"] = [
                hist.loc(p.name)
                for p in sub_process_insts
                if p.name in histogram.axes["process"]
            ]
        if categories:
            # transform into lists if necessary
            categories = law.util.make_list(categories)

            # get all leaf categories
            category_insts = list(map(config_inst.get_category, categories))
            leaf_category_insts = set(flatten_nested_list([
                category_inst.get_leaf_categories() or [category_inst]
                for category_inst in category_insts
            ]))
            selection_dict["category"] = [
                hist.loc(c.name)
                for c in leaf_category_insts
                if c.name in histogram.axes["category"]
            ]

        if shifts:
            # transform into lists if necessary
            shifts = law.util.make_list(shifts)

            # get all shift instances
            shift_insts = [config_inst.get_shift(shift) for shift in shifts]
            selection_dict["shift"] = [
                hist.loc(s.name)
                for s in shift_insts
                if s.name in histogram.axes["shift"]
            ]

        # work on a copy
        h = histogram.copy()

        # axis selections
        h = h[selection_dict]

        if reduce_axes:
            # axis reductions
            h = h[{"process": sum, "category": sum, "shift": sum}]

        return h


class HistogramsUserSingleShiftBase(
    ShiftTask,
    HistogramsUserBase,
):
    # use the MergeHistograms task to trigger upstream TaskArrayFunction initialization
    resolution_task_cls = MergeHistograms

    # upstream requirements
    reqs = Requirements(
        MergeHistograms=MergeHistograms,
    )

    def workflow_requires(self):
        reqs = super().workflow_requires()
        reqs["merged_hists"] = self.requires_from_branch()
        return reqs

    def requires(self):
        datasets = [self.datasets] if self.single_config else self.datasets
        return {
            config_inst.name: {
                d: self.reqs.MergeHistograms.req_different_branching(
                    self,
                    config=config_inst.name,
                    dataset=d,
                    branch=-1,
                    _prefer_cli={"variables"},
                )
                for d in datasets[i]
                if config_inst.has_dataset(d)
            }
            for i, config_inst in enumerate(self.config_insts)
        }


class HistogramsUserMultiShiftBase(
    ShiftSourcesMixin,
    HistogramsUserBase,
):
    # use the MergeHistograms task to trigger upstream TaskArrayFunction initialization
    resolution_task_cls = MergeHistograms

    # upstream requirements
    reqs = Requirements(
        MergeShiftedHistograms=MergeShiftedHistograms,
    )

    def workflow_requires(self):
        reqs = super().workflow_requires()
        reqs["merged_hists"] = self.requires_from_branch()
        return reqs

    def requires(self):
        datasets = [self.datasets] if self.single_config else self.datasets
        return {
            config_inst.name: {
                d: self.reqs.MergeShiftedHistograms.req_different_branching(
                    self,
                    config=config_inst.name,
                    dataset=d,
                    branch=-1,
                    _prefer_cli={"variables"},
                )
                for d in datasets[i]
            }
            for i, config_inst in enumerate(self.config_insts)
        }
