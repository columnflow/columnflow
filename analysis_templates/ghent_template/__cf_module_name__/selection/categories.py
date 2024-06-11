"""
Selection methods defining categories based on selection step results.
"""

from columnflow.util import maybe_import
from columnflow.categorization import Categorizer, categorizer
from columnflow.selection import SelectionResult

np = maybe_import("numpy")
ak = maybe_import("awkward")


@categorizer(uses={"event"}, call_force=True)
def catid_selection_incl(self: Categorizer, events: ak.Array, **kwargs) -> tuple[ak.Array, ak.Array]:
    mask = ak.ones_like(events.event) > 0
    return events, mask

#
# Categorizer called as part of cf.SelectEvents
#


@categorizer(uses={"event"}, call_force=True)
def catid_selection_2e(
    self: Categorizer, events: ak.Array, results: SelectionResult, **kwargs,
) -> tuple[ak.Array, ak.Array]:
    mask = ((ak.num(results.objects.Electron.Electron, axis=-1) == 2) &
            (ak.num(results.objects.Muon.Muon, axis=-1) == 0))
    return events, mask


@categorizer(uses={"event"}, call_force=True)
def catid_selection_1e1mu(
    self: Categorizer, events: ak.Array, results: SelectionResult, **kwargs,
) -> tuple[ak.Array, ak.Array]:
    mask = ((ak.num(results.objects.Electron.Electron, axis=-1) == 1) &
            (ak.num(results.objects.Muon.Muon, axis=-1) == 1))
    return events, mask


@categorizer(uses={"event"}, call_force=True)
def catid_selection_2mu(
    self: Categorizer, events: ak.Array, results: SelectionResult, **kwargs,
) -> tuple[ak.Array, ak.Array]:
    mask = ((ak.num(results.objects.Electron.Electron, axis=-1) == 0) &
            (ak.num(results.objects.Muon.Muon, axis=-1) == 2))
    return events, mask

#
# Categorizer called as part of cf.ProduceColumns
#


@categorizer(uses={"Electron.pt", "Muon.pt"}, call_force=True)
def catid_2e(self: Categorizer, events: ak.Array, **kwargs) -> tuple[ak.Array, ak.Array]:
    mask = ((ak.sum(events.Electron.pt > 0, axis=-1) == 2) & (ak.sum(events.Muon.pt > 0, axis=-1) == 0))
    return events, mask


@categorizer(uses={"Electron.pt", "Muon.pt"}, call_force=True)
def catid_1e1mu(self: Categorizer, events: ak.Array, **kwargs) -> tuple[ak.Array, ak.Array]:
    mask = ((ak.sum(events.Electron.pt > 0, axis=-1) == 1) & (ak.sum(events.Muon.pt > 0, axis=-1) == 1))
    return events, mask


@categorizer(uses={"Electron.pt", "Muon.pt"}, call_force=True)
def catid_2mu(self: Categorizer, events: ak.Array, **kwargs) -> tuple[ak.Array, ak.Array]:
    mask = ((ak.sum(events.Electron.pt > 0, axis=-1) == 0) & (ak.sum(events.Muon.pt > 0, axis=-1) == 2))
    return events, mask
