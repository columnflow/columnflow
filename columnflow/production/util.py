# coding: utf-8

"""
General producers that might be utilized in various places
"""

from columnflow.production import Producer, producer
from columnflow.util import maybe_import
from columnflow.columnar_util import set_ak_column, attach_behavior

ak = maybe_import("awkward")


@producer
def attach_coffea_behavior(self: Producer, events: ak.Array, **kwargs) -> ak.Array:
    """
    Rebuild certain collections with original coffea behavior in case some of them might have been
    invalidated in a potential previous step.
    """
    if "Jet" in events.fields and getattr(events.Jet, "metric_table", None) is None:
        events = set_ak_column(events, "Jet", attach_behavior(events.Jet, "Jet", skip_fields="*Idx*G"))

    if "FatJet" in events.fields and getattr(events.FatJet, "metric_table", None) is None:
        events = set_ak_column(events, "FatJet", attach_behavior(events.FatJet, "FatJet", skip_fields="*Idx*G"))

    if "SubJet" in events.fields and getattr(events.SubJet, "metric_table", None) is None:
        events = set_ak_column(events, "SubJet", attach_behavior(events.SubJet, "Jet", skip_fields="*Idx*G"))

    if "Muon" in events.fields and getattr(events.Muon, "metric_table", None) is None:
        events = set_ak_column(events, "Muon", attach_behavior(events.Muon, "Muon", skip_fields="*Idx*G"))

    if "Electron" in events.fields and getattr(events.Electron, "metric_table", None) is None:
        events = set_ak_column(events, "Electron", attach_behavior(events.Electron, "Electron", skip_fields="*Idx*G"))

    if "Tau" in events.fields and getattr(events.Tau, "metric_table", None) is None:
        events = set_ak_column(events, "Tau", attach_behavior(events.Tau, "Tau", skip_fields="*Idx*G"))

    if "MET" in events.fields and getattr(events.MET, "metric_table", None) is None:
        events = set_ak_column(events, "MET", attach_behavior(events.MET, "MissingET", skip_fields="*Idx*G"))

    return events
