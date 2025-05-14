# coding: utf-8

"""
Helpful reduction utilities.
"""

from __future__ import annotations

__all__ = []

import functools

import law

from columnflow.util import maybe_import
from columnflow.columnar_util import set_ak_column

ak = maybe_import("awkward")


logger = law.logger.get_logger(__name__)

full_slice = slice(None, None)


def create_event_mask(selection: ak.Array, requested_steps: tuple[str]) -> ak.Array | slice:
    """
    Creates and returns an event mask based on a *selection* results array and a tuple of *requested_steps* according to
    the following checks (in that order):

        - When not empty, *requested_steps* are considered fields of the *selection.steps* array and subsequently
          concatenated with a logical AND operation.
        - Otherwise, if the *event* field is present in the *selection* array, it is used instead.
        - Otherwise, a empty slice object is returned.
    """
    # build the event mask from requested steps
    if requested_steps:
        # check if all steps are present
        missing_steps = set(requested_steps) - set(selection.steps.fields)
        if missing_steps:
            raise Exception(f"selector steps {','.join(missing_steps)} requested but missing in {selection.steps}")
        return functools.reduce(
            (lambda a, b: a & b),
            (selection["steps", step] for step in requested_steps),
        )

    # use the event field if present
    if "event" in selection.fields:
        return selection.event

    # fallback to an empty slice
    return full_slice


def create_collections_from_masks(
    events: ak.Array,
    object_masks: dict[str, dict[str, ak.Array]] | ak.Array,
) -> ak.Array:
    """
    Adds new collections to an *ak_array* based on *object_masks* and returns a new view.
    *object_masks* should be a nested dictionary such as, for instance,

    .. code-block:: python

        {
            "Jet": {
                "BJet": ak.Array([[1, 0, 3], ...]),
                "LJet": ak.Array([2], ...),
            },
            ...
        }

    where outer keys refer to names of source collections and inner keys to names of collections to
    create by applying the corresponding mask or indices to the source collection. The example above
    would create two collections "BJet" and "LJet" based on the source collection "Jet".
    """
    if isinstance(object_masks, dict):
        object_masks = ak.Array(object_masks)

    for src_name in object_masks.fields:
        # get all destination collections
        dst_names = list(object_masks[src_name].fields)

        # when a source is named identically, handle it last
        if src_name in dst_names:
            # move to the end
            dst_names.remove(src_name)
            dst_names.append(src_name)

        # add collections
        for dst_name in dst_names:
            object_mask = object_masks[src_name, dst_name]
            dst_collection = events[src_name][object_mask]
            events = set_ak_column(events, dst_name, dst_collection)

    return events
