# coding: utf-8

"""
Reducer definition for achieving columnsflow's default reduction behavior in three steps:
  - remove unwanted events (using "event" mask of selection results)
  - create new collections (using "objects" mapping of selection results)
  - only keep certain columns after the reduction
"""

from __future__ import annotations

from collections import defaultdict

import law

from columnflow.reduction import Reducer, reducer
from columnflow.reduction.util import create_event_mask, create_collections_from_masks
from columnflow.util import maybe_import

ak = maybe_import("awkward")


@reducer()
def cf_default_keep_columns(self: Reducer, events: ak.Array, selection: ak.Array, **kwargs) -> ak.Array:
    """
    Reducer that does nothing but to define the columns to keep after the reduction in a backwards-compatible way using
    the "keep_columns" auxiliary config field as was the default in previous columnflow versions.
    """
    return events


@cf_default_keep_columns.post_init
def cf_default_keep_columns_post_init(self: Reducer, task: law.Task, **kwargs) -> None:
    for c in self.config_inst.x.keep_columns.get(task.task_family, ["*"]):
        self.produces.update(task._expand_keep_column(c))


@reducer(
    # disable the check for used columns
    check_used_columns=False,
    # whether to add cf_default_keep_columns as a dependency to achieve backwards compatibility
    add_keep_columns=True,
    # whether to register the shifts of the upstream selector as shifts of this reducer
    mirror_selector_shifts=True,
)
def cf_default(self: Reducer, events: ak.Array, selection: ak.Array, task: law.Task, **kwargs) -> ak.Array:
    # build the event mask
    event_mask = create_event_mask(selection, task.selector_steps)

    # apply it
    events = events[event_mask]

    # add collections
    if "objects" in selection.fields:
        events = create_collections_from_masks(events, selection.objects[event_mask])

    return events


@cf_default.init
def cf_default_init(self: Reducer, **kwargs) -> None:
    if self.add_keep_columns:
        self.uses.add(cf_default_keep_columns.PRODUCES)
        self.produces.add(cf_default_keep_columns.PRODUCES)

    # mirror selector shifts
    if self.mirror_selector_shifts and "selector_shifts" in self.inst_dict:
        self.shifts |= self.selector_shifts


@cf_default.post_init
def cf_default_post_init(self: Reducer, task: law.Task, **kwargs) -> None:
    # the updates to used columns are only necessary if the task invokes the reducer
    if not task.invokes_reducer:
        return

    # add used columns pointing to the selection steps
    # (all starting with "steps." for ReduceEvents to decide to load them from selection result data)
    for step in task.selector_steps:
        self.uses.add(f"steps.{step}")

    # based on the columns to write, determine which collections need to be read to produce new collections
    # (masks must start with "objects." for ReduceEvents to decide to load them from selection result data)
    output_collection_fields = defaultdict(set)
    for route in self.produced_columns:
        if len(route) > 1:
            output_collection_fields[route[0]].add(route)

    # iterate through collections and update used colums
    for src_col, dst_cols in task.collection_map.items():
        for dst_col in dst_cols:
            # skip if the collection does not need to be loaded at all
            if not law.util.multi_match(dst_col, output_collection_fields.keys()):
                continue
            # read the object mask
            self.uses.add(f"objects.{src_col}.{dst_col}")
            # make sure that the corresponding columns of the source collection are loaded
            self.uses.update(src_col + route[1:] for route in output_collection_fields[dst_col])
