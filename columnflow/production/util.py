# coding: utf-8

"""
General producers that might be utilized in various places.
"""

from __future__ import annotations

from columnflow.types import Sequence, Union
from columnflow.production import Producer, producer
from columnflow.util import maybe_import
from columnflow.columnar_util import attach_coffea_behavior as attach_coffea_behavior_fn

ak = maybe_import("awkward")


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
