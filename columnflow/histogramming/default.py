# coding: utf-8

"""
Default histogram producers that define columnflow's default behavior.
"""

from __future__ import annotations

import law
import order as od

from columnflow.histogramming import HistProducer, hist_producer
from columnflow.columnar_util import has_ak_column, Route
from columnflow.hist_util import create_hist_from_variables, fill_hist, translate_hist_intcat_to_strcat
from columnflow.util import maybe_import
from columnflow.types import TYPE_CHECKING, Any

np = maybe_import("numpy")
ak = maybe_import("awkward")
if TYPE_CHECKING:
    hist = maybe_import("hist")


@hist_producer()
def cf_default(self: HistProducer, events: ak.Array, **kwargs) -> ak.Array:
    """
    Default histogram producer that implements all hooks necessary to ensure columnflow's default behavior:

        - create_hist: defines the histogram structure
        - __call__: receives an event chunk and updates it, and creates event weights (1's in this case)
        - fill: receives the data and fills the histogram
        - post_process_hist: post-processes the histogram before it is saved
    """
    return events, ak.Array(np.ones(len(events), dtype=np.float32))


@cf_default.create_hist
def cf_default_create_hist(
    self: HistProducer,
    variables: list[od.Variable],
    task: law.Task,
    **kwargs,
) -> hist.Hist:
    """
    Define the histogram structure for the default histogram producer.
    """
    return create_hist_from_variables(
        *variables,
        categorical_axes=(
            ("category", "intcat"),
            ("process", "intcat"),
            ("shift", "intcat"),
        ),
        weight=True,
    )


@cf_default.fill_hist
def cf_default_fill_hist(self: HistProducer, h: hist.Hist, data: dict[str, Any], task: law.Task) -> None:
    """
    Fill the histogram with the data.
    """
    fill_hist(h, data, last_edge_inclusive=task.last_edge_inclusive)


@cf_default.post_process_hist
def cf_default_post_process_hist(self: HistProducer, h: hist.Hist, task: law.Task) -> hist.Hist:
    """
    Post-process the histogram, converting integer to string axis for consistent lookup across configs where ids might
    be different.
    """
    axis_names = {ax.name for ax in h.axes}

    # translate axes
    if "category" in axis_names:
        category_map = {cat.id: cat.name for cat in self.config_inst.get_leaf_categories()}
        h = translate_hist_intcat_to_strcat(h, "category", category_map)
    if "process" in axis_names:
        process_map = {proc_id: self.config_inst.get_process(proc_id).name for proc_id in h.axes["process"]}
        h = translate_hist_intcat_to_strcat(h, "process", process_map)
    if "shift" in axis_names:
        shift_map = {task.global_shift_inst.id: task.global_shift_inst.name}
        h = translate_hist_intcat_to_strcat(h, "shift", shift_map)

    return h


@cf_default.hist_producer()
def all_weights(self: HistProducer, events: ak.Array, **kwargs) -> ak.Array:
    """
    HistProducer that combines all event weights from the *event_weights* aux entry from either the config or the
    dataset. The weights are multiplied together to form the full event weight.

    The expected structure of the *event_weights* aux entry is a dictionary with the weight column name as key and a
    list of shift sources as values. The shift sources are used to declare the shifts that the produced event weight
    depends on. Example:

    .. code-block:: python

        from columnflow.config_util import get_shifts_from_sources
        # add weights and their corresponding shifts for all datasets
        cfg.x.event_weights = {
            "normalization_weight": [],
            "muon_weight": get_shifts_from_sources(config, "mu_sf"),
            "btag_weight": get_shifts_from_sources(config, "btag_hf", "btag_lf"),
        }
        for dataset_inst in cfg.datasets:
            # add dataset-specific weights and their corresponding shifts
            dataset.x.event_weights = {}
            if not dataset_inst.has_tag("skip_pdf"):
                dataset_inst.x.event_weights["pdf_weight"] = get_shifts_from_sources(config, "pdf")
    """
    weight = ak.Array(np.ones(len(events)))

    # build the full event weight
    if self.dataset_inst.is_mc and len(events):
        # multiply weights from global config `event_weights` aux entry
        for column in self.config_inst.x.event_weights:
            weight = weight * Route(column).apply(events)

        # multiply weights from dataset-specific `event_weights` aux entry
        for column in self.dataset_inst.x("event_weights", []):
            if has_ak_column(events, column):
                weight = weight * Route(column).apply(events)
            else:
                self.logger.warning_once(
                    f"missing_dataset_weight_{column}",
                    f"weight '{column}' for dataset {self.dataset_inst.name} not found",
                )

    return events, weight


@all_weights.init
def all_weights_init(self: HistProducer) -> None:
    weight_columns = set()

    if self.dataset_inst.is_data:
        return

    # add used weight columns and declare shifts that the produced event weight depends on
    if self.config_inst.has_aux("event_weights"):
        weight_columns |= {Route(column) for column in self.config_inst.x.event_weights}
        for shift_insts in self.config_inst.x.event_weights.values():
            self.shifts |= {shift_inst.name for shift_inst in shift_insts}

    # optionally also for weights defined by a dataset
    if self.dataset_inst.has_aux("event_weights"):
        weight_columns |= {Route(column) for column in self.dataset_inst.x("event_weights", [])}
        for shift_insts in self.dataset_inst.x.event_weights.values():
            self.shifts |= {shift_inst.name for shift_inst in shift_insts}

    # add weight columns to uses
    self.uses |= weight_columns
