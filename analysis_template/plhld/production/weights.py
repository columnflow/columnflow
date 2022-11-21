# coding: utf-8

"""
Column production methods related to generic event weights.
"""

from columnflow.production import Producer, producer
from columnflow.production.pileup import pu_weight
from columnflow.production.normalization import normalization_weights
from columnflow.util import maybe_import

ak = maybe_import("awkward")


@producer(
    uses={normalization_weights, pu_weight},
    produces={normalization_weights, pu_weight},
    shifts={"minbias_xs_up", "minbias_xs_down", "scale_up", "scale_down", "pdf_up", "pdf_down"},
)
def event_weights(self: Producer, events: ak.Array, **kwargs) -> ak.Array:
    """
    Opinionated wrapper of several event weight producers. It declares dependence all shifts that
    might possibly change any of the weights.
    """
    # compute normalization weights
    events = self[normalization_weights](events, **kwargs)

    # compute pu weights
    events = self[pu_weight](events, **kwargs)

    return events
