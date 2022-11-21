# coding: utf-8

"""
Selection methods defining categories based on selection step results.
"""

from columnflow.util import maybe_import
from columnflow.selection import Selector, SelectionResult, selector

np = maybe_import("numpy")
ak = maybe_import("awkward")


@selector(uses={"event"})
def sel_incl(self: Selector, events: ak.Array, results: SelectionResult, **kwargs) -> ak.Array:
    return ak.ones_like(events.event)


@selector(uses={"event"})
def sel_1e(self: Selector, events: ak.Array, results: SelectionResult, **kwargs) -> ak.Array:
    return (ak.num(results.objects.Electron.Electron, axis=-1) == 1) & (ak.num(results.objects.Muon.Muon, axis=-1) == 0)


@selector(uses={"event"})
def sel_1mu(self: Selector, events: ak.Array, results: SelectionResult, **kwargs) -> ak.Array:
    return (ak.num(results.objects.Electron.Electron, axis=-1) == 0) & (ak.num(results.objects.Muon.Muon, axis=-1) == 1)
