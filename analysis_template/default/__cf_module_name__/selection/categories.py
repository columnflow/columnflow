# coding: utf-8

"""
Selection methods defining masks for categories.
"""

from columnflow.util import maybe_import
from columnflow.selection import Selector, selector


np = maybe_import("numpy")
ak = maybe_import("awkward")


@selector(uses={"event"})
def sel_incl(self: Selector, events: ak.Array, **kwargs) -> ak.Array:
    return ak.ones_like(events.event)


@selector(uses={"event"})
def sel_2j(self: Selector, events: ak.Array, **kwargs) -> ak.Array:
    return ak.num(events.Jet, axis=-1) == 2
