# coding: utf-8

"""
Column production methods for testing purposes.
"""

from ap.util import maybe_import
from ap.production import producer

ak = maybe_import("awkward")


@producer(uses={"nJet", "Jet.pt"}, produces={"Jet.pt2"})
def test(events, **kwargs):
    events["Jet", "pt2"] = events.Jet.pt ** 2.0
    return events
