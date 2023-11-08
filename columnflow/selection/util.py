# coding: utf-8

"""
Helpful utilities often used in selections.
"""

from __future__ import annotations

__all__ = [
    "sorted_indices_from_mask", "create_collections_from_masks",
]

from columnflow.util import maybe_import
from columnflow.columnar_util import set_ak_column

ak = maybe_import("awkward")


def sorted_indices_from_mask(
    mask: ak.Array,
    metric: ak.Array,
    sort_axis: int = -1,
    ascending: bool = True,
) -> ak.Array:
    """
    Takes a boolean *mask* and converts it to an array of indices, sorted using a *metric* of equal
    size along a *sort_axis* and, by default, in *ascending* order. Example:

    .. code-block:: python

        mask = [[True, False, False, True], ...]
        metric = [[5.0, 1.0, 0.9, 4.1], ...]

        sorted_indices_from_mask(mask, metric)
        # -> [[3, 0], ...]
    """
    indices = ak.argsort(metric, axis=sort_axis, ascending=ascending)
    return indices[mask[indices]]


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
