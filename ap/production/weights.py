# coding: utf-8

"""
Column production methods related to generic event weights.
"""

from ap.production import producer
from ap.production.pileup import pu_weights
from ap.production.normalization import normalization_weights
from ap.util import maybe_import

ak = maybe_import("awkward")


@producer(
    uses={normalization_weights, pu_weights},
    produces={normalization_weights, pu_weights},
    shifts={"minbias_xs_up", "minbias_xs_down"},
)
def event_weights(events: ak.Array, **kwargs) -> ak.Array:
    """
    Opinionated wrapper of several event weight producers. It declares dependence all shifts that
    might possibly change any of the weights.
    """
    # compute normalization weights
    events = normalization_weights(events, **kwargs)

    # compute pu weights
    events = pu_weights(events, **kwargs)

    return events
