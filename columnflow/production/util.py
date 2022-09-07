# coding: utf-8

"""
General producers that might be utilized in various places.
"""

from typing import Optional, Union, Sequence

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
    "MET": {
        "type_name": "MissingET",
        "check_attr": "metric_table",
        "skip_fields": "*Idx*G",
    },
}


@producer
def attach_coffea_behavior(
    self: Producer,
    events: ak.Array,
    collections: Optional[Union[dict, Sequence]] = None,
    **kwargs,
) -> ak.Array:
    """
    Rebuild certain collections with original coffea behavior in case some of them might have been
    invalidated in a potential previous step. All information on source collection names, type
    names, attributes to check whether the correct behavior is already attached, and fields to
    potentially skip is taken from :py:obj:`default_collections`.

    However, this information is updated by *collections* when it is a dict. In case it is a list,
    its items are interpreted as names of collections defined as keys in
    :py:obj:`default_collections` for which the behavior should be attached.
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
