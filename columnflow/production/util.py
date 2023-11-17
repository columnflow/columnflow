# coding: utf-8

"""
General producers that might be utilized in various places.
"""

from __future__ import annotations

from columnflow.types import Sequence, Union
from columnflow.production import Producer, producer
from columnflow.util import maybe_import
from columnflow.columnar_util import set_ak_column, attach_behavior

ak = maybe_import("awkward")


#: Information on behavior of certain collections to (re-)attach it via attach_coffea_behavior.
default_collections = {
    "Jet": {
        "type_name": "Jet",
        "check_attr": "metric_table",
        "skip_fields": "*Idx*G",
    },
    "FatJet": {
        "type_name": "FatJet",
        "check_attr": "metric_table",
        "skip_fields": "*Idx*G",
    },
    "SubJet": {
        "type_name": "Jet",
        "check_attr": "metric_table",
        "skip_fields": "*Idx*G",
    },
    "Muon": {
        "type_name": "Muon",
        "check_attr": "metric_table",
        "skip_fields": "*Idx*G",
    },
    "Electron": {
        "type_name": "Electron",
        "check_attr": "metric_table",
        "skip_fields": "*Idx*G",
    },
    "Tau": {
        "type_name": "Tau",
        "check_attr": "metric_table",
        "skip_fields": "*Idx*G",
    },
    # NOTE: disabled at the moment since the MissingET in coffea handles columns such as
    #       "MET.pt_jer/jec_up/down" as systematics itself and attaching this behavior creates
    #       multiple MET objects per event (that still, for some reason, are identical); it would
    #       be nice if one could entirely disable this coffea feature as we are treating systematics
    #       on our own
    # "MET": {
    #     "type_name": "MissingET",
    #     "check_attr": "metric_table",
    #     "skip_fields": "*Idx*G",
    # },
}


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
    potentially skip is taken from :py:obj:`default_collections`.

    However, this information is updated by *collections* when it is a dict. In case it is a list,
    its items are interpreted as names of collections defined as keys in
    :py:obj:`default_collections` for which the behavior should be attached.

    :param events: Array containing the events
    :param collections: Attach behavior for these collections. If :py:class:`dict`, the
        :py:obj:`default_collections` are updated with the information in *collections*. If
        :py:class:`list`, only update this set of *collections* as specified in the
        :py:obj:`default_collections`.
    :return: Array with correct behavior attached for collections
    """
    # update or reduce collection info
    _collections = default_collections
    if isinstance(collections, dict):
        _collections = _collections.copy()
        _collections.update(collections)
    elif isinstance(collections, (list, tuple)):
        _collections = {
            name: info
            for name, info in _collections.items()
            if name in collections
        }

    for name, info in _collections.items():
        if not info:
            continue

        # get the collection to update
        if name not in events.fields:
            continue
        coll = events[name]

        # when a check_attr is defined, do nothing in case it already exists
        if info.get("check_attr") and getattr(coll, info["check_attr"], None) is not None:
            continue

        # default infos
        type_name = info.get("type_name") or name
        skip_fields = info.get("skip_fields")

        # apply the behavior
        events = set_ak_column(events, name, attach_behavior(coll, type_name, skip_fields=skip_fields))

    return events
