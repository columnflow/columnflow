# coding: utf-8

"""
Stat-related methods.
"""
from __future__ import annotations

import functools

from columnflow.selection import Selector, SelectionResult, selector
from columnflow.selection.stats import increment_stats
from columnflow.production import Producer, producer
from columnflow.production.cms.btag import btag_weights
from __cf_short_name_lc__.production.weights import event_weights_to_normalize

from columnflow.util import maybe_import
from columnflow.ml import MLModel

np = maybe_import("numpy")
ak = maybe_import("awkward")


@selector(
    uses={increment_stats, btag_weights, event_weights_to_normalize},
)
def __cf_short_name_lc___increment_stats(
    self: Selector,
    events: ak.Array,
    results: SelectionResult,
    stats: dict,
    **kwargs,
) -> ak.Array:
    # collect important information from the results
    event_mask = results.event
    event_mask_no_bjet = results.event  # currently no b-jet selection, will change to a result without b jet selection
    n_jets = results.x.n_jets

    # weight map definition
    weight_map = {
        # "num" operations
        "num_events": Ellipsis,  # all events
        "num_events_selected": event_mask,  # selected events only
        "num_events_selected_no_bjet": event_mask_no_bjet,
    }

    if self.dataset_inst.is_mc:
        weight_map["num_negative_weights"] = (events.mc_weight < 0)
        # "sum" operations
        weight_map["sum_mc_weight"] = events.mc_weight  # weights of all events
        weight_map["sum_mc_weight_selected"] = (events.mc_weight, event_mask)  # weights of selected events
        weight_map["sum_mc_weight_no_bjet"] = (events.mc_weight, event_mask_no_bjet)
        weight_map["sum_mc_weight_selected_no_bjet"] = (events.mc_weight, event_mask_no_bjet)

        weight_columns = list(
            set(self[event_weights_to_normalize].produced_columns) |
            set(self[btag_weights].produced_columns),
        )
        weight_columns = sorted([col.string_nano_column for col in weight_columns])

        # mc weight times correction weight (with variations) without any selection
        for name in weight_columns:
            if "weight" not in name:
                # skip non-weight columns here
                continue

            weight_map[f"sum_mc_weight_{name}"] = (events.mc_weight * events[name], Ellipsis)

            # weights for selected events
            weight_map[f"sum_mc_weight_{name}_selected"] = (events.mc_weight * events[name], event_mask)

            if name.startswith("btag_weight"):
                # weights for selected events, excluding the bjet selection
                weight_map[f"sum_mc_weight_{name}_selected_no_bjet"] = (
                    (events.mc_weight * events[name], event_mask_no_bjet)
                )

    group_map = {
        "process": {
            "values": events.process_id,
            "mask_fn": (lambda v: events.process_id == v),
        },
        "njet": {
            "values": results.x.n_jets,
            "mask_fn": (lambda v: n_jets == v),
        },
    }

    group_combinations = [("process", "njet")]

    self[increment_stats](
        events,
        results,
        stats,
        weight_map=weight_map,
        group_map=group_map,
        group_combinations=group_combinations,
        **kwargs,
    )

    return events


@__cf_short_name_lc__increment_stats.init
def __cf_short_name_lc___increment_stats_init(self: Selector) -> None:
    if not getattr(self, "dataset_inst", None):
        return

    if self.dataset_inst.is_mc:
        self.uses |= {"mc_weight"}
