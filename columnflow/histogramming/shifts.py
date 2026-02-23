# coding: utf-8

"""
Default histogram producers that define columnflow's default behavior.
"""

from __future__ import annotations

import law
import order as od

from columnflow.histogramming import HistProducer, hist_producer
from columnflow.util import maybe_import
from columnflow.hist_util import create_hist_from_variables, fill_hist, translate_hist_intcat_to_strcat
from columnflow.columnar_util import has_ak_column, Route, optional_column as optional
from columnflow.types import Any

np = maybe_import("numpy")
ak = maybe_import("awkward")
hist = maybe_import("hist")


@hist_producer()
def shifts_hist(self: HistProducer, events: ak.Array, **kwargs) -> ak.Array:
    """
    WeightProducer that combines all event weights from the *event_weights* aux entry from either
    the config or the dataset. The weights are multiplied together to form the full event weight.

    The expected structure of the *event_weights* aux entry is a dictionary with the weight column
    name as key and a list of shift sources as values. The shift sources are used to declare the
    shifts that the produced event weight depends on. Example:

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
    # build the full event weight
    weight = ak.Array(np.ones(len(events)))
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

        if self.subshift_insts:
            all_weights = []
            for subshift_inst in self.subshift_insts:
                subshift_weight = weight
                for nominal, variation in subshift_inst.x.column_aliases.items():
                    # only apply aliases when relevant to event weights
                    if (
                        (nominal in self.dataset_inst.x("event_weights", [])) |
                        (nominal in self.config_inst.x("event_weights", []))
                    ):
                        nominal_arr = Route(nominal).apply(events).to_numpy()
                        modifier = np.ones_like(nominal_arr)
                        nonzero = np.nonzero(nominal_arr)
                        modifier[nonzero] = Route(variation).apply(events).to_numpy()[nonzero] / nominal_arr[nonzero]
                        subshift_weight = subshift_weight * modifier
                all_weights.append(subshift_weight)
            return events, np.transpose(all_weights)

    if self.dataset_inst.is_data and len(events) and self.config_inst.x.data_event_weights:
        # for data we apply a seperate set of weights if applicable
        for column in self.config_inst.x.data_event_weights:
            weight = weight * Route(column).apply(events)

    return events, weight


@shifts_hist.init
def shifts_hist_init(self: HistProducer) -> None:
    if not getattr(self, "dataset_inst", None):
        return

    weight_columns = set()

    # add used weight columns and declare shifts that the produced event weight depends on
    if self.dataset_inst.is_mc:
        if self.config_inst.has_aux("event_weights"):
            weight_columns |= {Route(column) for column in self.config_inst.x.event_weights}
            for shift_insts in self.config_inst.x.event_weights.values():
                self.shifts |= {shift_inst.name for shift_inst in shift_insts}

        # optionally also for weights defined by a dataset
        if self.dataset_inst.has_aux("event_weights"):
            weight_columns |= {Route(column) for column in self.dataset_inst.x("event_weights", [])}
            for shift_insts in self.dataset_inst.x.event_weights.values():
                self.shifts |= {shift_inst.name for shift_inst in shift_insts}
    else:
        if self.config_inst.has_aux("data_event_weights"):
            weight_columns |= {Route(column) for column in self.config_inst.x.data_event_weights}
            for shift_insts in self.config_inst.x.data_event_weights.values():
                self.shifts |= {shift_inst.name for shift_inst in shift_insts}

    # add weight columns to uses
    self.uses |= weight_columns


@shifts_hist.post_init
def shifts_hist_post_init(self: HistProducer, task: law.Task, **kwargs) -> None:

    config_inst = self.config_inst
    subshifts = task.local_shift_inst.x("subshifts", [])
    self.subshift_insts = set([config_inst.get_shift(s) for s in subshifts if config_inst.has_shift(s)])

    self.uses |= set(map(optional, {c for s in self.subshift_insts for c in s.x.column_aliases.values()}))


@shifts_hist.create_hist
def shifts_hist_create_hist(
    self: HistProducer,
    variables: list[od.Variable],
    task: law.Task,
    **kwargs,
) -> hist.Histogram:
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


@shifts_hist.fill_hist
def shifts_hist_fill_hist(self: HistProducer, h: hist.Hist, data: dict[str, Any], task: law.Task, **kwargs) -> None:

    # make sure that nan values in data are not propagated to the histogram (removed from data instead)
    variable_insts = [self.config_inst.get_variable(key) for key in list(
        data.keys()) if key not in ("category", "process", "shift", "weight")]
    nan_masks = ak.all([~(ak.is_none(ak.nan_to_none(data[variable_inst.name])))
                       for variable_inst in variable_insts], axis=0)

    new_data = {}
    for key, value in data.items():
        if (type(value) is ak.Array) | (type(value) is np.ndarray):
            new_data[key] = value[nan_masks]
        else:
            new_data[key] = value

    # make sure that the histogram is integer if discrete_x
    for variable_inst in variable_insts:
        if variable_inst.discrete_x:
            new_data[variable_inst.name] = ak.to_numpy(new_data[variable_inst.name]).astype(int)

    _fill = lambda d: fill_hist(h, d, last_edge_inclusive=task.last_edge_inclusive)
    if self.subshift_insts:
        if (new_data["shift"] == task.global_shift_inst.id):
            for i, subshift_inst in enumerate(self.subshift_insts):
                if subshift_inst not in self.sel_subshift_insts:
                    _fill(new_data | {"shift": subshift_inst.id, "weight": new_data["weight"][:, i]})
        else:
            subshift_idx = [s.id for s in self.subshift_insts].index(new_data["shift"])
            _fill(new_data | {"weight": new_data["weight"][:, subshift_idx]})

    else:
        _fill(new_data)


@shifts_hist.post_process_hist
def shifts_hist_post_process_hist(self: HistProducer, h: hist.Histogram, task: law.Task) -> hist.Histogram:
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
        shift_map = {s.id: s.name for s in self.subshift_insts or [task.global_shift_inst]}
        h = translate_hist_intcat_to_strcat(h, "shift", shift_map)

    return h


@shifts_hist.requires
def shifts_hist_requires(self: HistProducer, task: law.Task, reqs: dict) -> None:
    """
    In the case of subshift add the required producers  for the selection dependent shifts
    """

    config_inst = self.config_inst
    subshifts = task.local_shift_inst.x("subshifts", [])
    self.subshift_insts = set([config_inst.get_shift(s) for s in subshifts if config_inst.has_shift(s)])
    self.sel_subshift_insts = {s for s in self.subshift_insts if "selection_dependent" in s.tags}

    if not self.sel_subshift_insts:
        return

    reqs["producers"] = {
        subshift_inst.name: [
            task.reqs.ProduceColumns.req(
                task,
                shift=subshift_inst.name,
                producer=producer_inst.cls_name,
                producer_inst=producer_inst,
            )
            for producer_inst in task.producer_insts
            if producer_inst.produced_columns
        ]
        for subshift_inst in self.sel_subshift_insts
    }

    if task.ml_model_insts:
        reqs["ml"] = {
            subshift_inst.name: [
                task.reqs.MLEvaluation.req(
                    task,
                    shift=subshift_inst.name,
                    ml_model=ml_model_inst.cls_name,
                )
                for ml_model_inst in task.ml_model_insts
            ]
            for subshift_inst in self.sel_subshift_insts
        }


@shifts_hist.setup
def shifts_hist_setup(
    self: HistProducer,
    reqs: dict,
    inputs: dict,
    task: law.Task,
    reader_targets: law.util.InsertableDict,
) -> None:

    if not self.sel_subshift_insts:
        return

    for req, input in inputs.items():
        for shift, shift_input in input.items():
            for i, inp in enumerate(shift_input):
                if req == "producers":
                    reader_targets[f"{shift}__{req}_{i}"] = inp["columns"]
                else:
                    reader_targets[f"{shift}__{req}_{i}"] = inp["mlcolumns"]

    return
