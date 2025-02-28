# coding: utf-8

"""
General producers that might be utilized in various places.
"""
from __future__ import annotations

from functools import partial

from columnflow.types import Iterable, Sequence, Union
from columnflow.production import Producer, producer
from columnflow.util import maybe_import
from columnflow.columnar_util import attach_coffea_behavior as attach_coffea_behavior_fn

ak = maybe_import("awkward")
coffea = maybe_import("coffea")


@producer(call_force=True)
def attach_coffea_behavior(
    self: Producer,
    events: ak.Array,
    collections: Union[dict, Sequence, None] = None,
    **kwargs,
) -> ak.Array:
    """
    Add coffea's NanoEvents behavior to collections.

    This might become relevant in case some of the collections have been invalidated in a potential
    previous step. All information on source collection names, :external+coffea:doc:`index` type
    names, attributes to check whether the correct behavior is already attached, and fields to
    potentially skip is taken from :py:obj:`columnar_util.default_coffea_collections`.

    However, this information is updated by *collections* when it is a dict. In case it is a list,
    its items are interpreted as names of collections defined as keys in the default collections for
    which the behavior should be attached.

    :param events: Array containing the events
    :param collections: Attach behavior for these collections. Defaults to
        :py:obj:`columnar_util.default_coffea_collections`.
    :return: Array with correct behavior attached for collections
    """
    return attach_coffea_behavior_fn(events, collections=collections)


#
# general awkward array functions
#

def ak_extract_fields(arr: ak.Array, fields: list[str], **kwargs):
    """
    Build an array containing only certain `fields` of an input array `arr`,
    preserving behaviors.
    """
    # reattach behavior
    if "behavior" not in kwargs:
        kwargs["behavior"] = arr.behavior

    return ak.zip(
        {
            field: getattr(arr, field)
            for field in fields
        },
        **kwargs,
    )


#
# functions for operating on lorentz vectors
#

_lv_base = partial(ak_extract_fields, behavior=coffea.nanoevents.methods.nanoaod.behavior)

lv_xyzt = partial(_lv_base, fields=["x", "y", "z", "t"], with_name="LorentzVector")
lv_xyzt.__doc__ = """Construct a `LorentzVectorArray` from an input array."""

lv_mass = partial(_lv_base, fields=["pt", "eta", "phi", "mass"], with_name="PtEtaPhiMLorentzVector")
lv_mass.__doc__ = """Construct a `PtEtaPhiMLorentzVectorArray` from an input array."""

lv_energy = partial(_lv_base, fields=["pt", "eta", "phi", "energy"], with_name="PtEtaPhiELorentzVector")
lv_energy.__doc__ = """Construct a `PtEtaPhiELorentzVectorArray` from an input array."""


def lv_sum(lv_arrays: Iterable[ak.Array]):
    """
    Return the sum of identically-structured arrays containing Lorentz vectors.
    """
    # don't use `reduce` or list comprehensions
    # to keep memory use as low as possible
    tmp_lv_sum = None
    for lv in lv_arrays:
        if tmp_lv_sum is None:
            tmp_lv_sum = lv
        else:
            tmp_lv_sum = tmp_lv_sum + lv

    return tmp_lv_sum


#
# functions for matching between collections of Lorentz vectors
#

def delta_r_match(
    src_lv: ak.Array,
    dst_lvs: ak.Array,
    max_dr: float | None = None,
    as_index: bool = False,
):
    """
    Match entries in the source array *src_lv* to the closest entry
    in the destination array *dst_lvs* using delta-R as a metric.

    The array *src_lv* should contain a single entry per event and
    *dst_lvs* should be a list of possible matches.

    The parameter *max_dr* optionally indicates the maximum possible
    delta-R value for a match (if the best possible match has a higher
    value, it is not considered a valid match).

    Returns a tuple (*best_match*, *dst_lvs_filtered*), where *best_match*
    is an array containing either the best match in *dst_lvs* per event
    (if *as_index* is false), or the index to be applied to *dst_lvs* in order
    to obtain the best match (if *as_index* is true). The second tuple
    entry, *dst_lvs_filtered*, is a view of *dst_lvs* with the best matches
    removed, and can be used for subsequent matching.
    """
    # calculate delta_r for all possible src-dst pairs
    delta_r = ak.singletons(src_lv).metric_table(dst_lvs)

    # invalidate entries above delta_r threshold
    if max_dr is not None:
        delta_r = ak.mask(delta_r, delta_r < max_dr)

    # get index and value of best match
    best_match_dst_idx = ak.argmin(delta_r, axis=2)

    # filter dst_lvs to remove the best matches (if any)
    keep = (ak.local_index(dst_lvs, axis=1) != ak.firsts(best_match_dst_idx))
    keep = ak.fill_none(keep, True)
    dst_lvs_filtered = ak.mask(dst_lvs, keep)
    dst_lvs_filtered = ak.where(ak.is_none(dst_lvs_filtered, axis=0), [[]], dst_lvs_filtered)

    # return either index or four-vector of best match
    best_match = best_match_dst_idx if as_index else ak.firsts(dst_lvs[best_match_dst_idx])
    return best_match, dst_lvs_filtered


def delta_r_match_multiple(
    src_lvs: ak.Array,
    dst_lvs: ak.Array,
    max_dr: float | None = None,
    as_index: bool = False,
):
    """
    Like *delta_r_match*, except source array *src_lvs* can contain more than
    one entry per event. The matching is done sequentially for each entry in
    *src_lvs*, with previous matches being filtered from the destination array
    each time to prevent double counting.
    """

    # save the index structure of the supplied source array
    src_lvs_idx = ak.local_index(src_lvs, axis=1)

    # pad sub-lists to the same length (but at least 1)
    max_num = max(1, ak.max(ak.num(src_lvs)))
    src_lvs = ak.pad_none(src_lvs, max_num)

    # run matching for each position,
    # filtering the destination array each time
    # and collecting the match indices
    best_match_dst_idxs = []
    dst_lvs_filtered = dst_lvs
    for i in range(max_num):
        best_match_dst_idx, dst_lvs_filtered = delta_r_match(
            src_lvs[:, i],
            dst_lvs_filtered,
            max_dr=max_dr,
            as_index=True,
        )
        best_match_dst_idxs.append(best_match_dst_idx)

    # concatenate matching results
    best_match_idxs = ak.concatenate(best_match_dst_idxs, axis=-1)

    # remove padding to make result index-compatible with input
    best_match_idxs = best_match_idxs[src_lvs_idx]

    # return either index or four-vector of best match
    best_match = best_match_idxs if as_index else dst_lvs[best_match_idxs]
    return best_match, dst_lvs_filtered
