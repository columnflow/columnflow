# coding: utf-8

"""
Column production methods related to generic event weights.
"""

import order as od

from ap.production import Producer, producer
from ap.production.pileup import pu_weights
from ap.production.normalization import normalization_weights
from ap.util import maybe_import
from ap.columnar_util import set_ak_column, has_ak_column

ak = maybe_import("awkward")


@producer(
    uses={"Jet.pt"},
    produces={"top_pt_weight", "top_pt_weight_up", "top_pt_weight_down"},
)
def top_pt_weights(events: ak.Array, dataset_inst: od.Dataset, **kwargs) -> ak.Array:
    """
    Adds a dummy top pt weight and its variations to *events* in case *dataset_inst* represents a
    ttbar dataset.
    """
    if has_ak_column(events, "top_pt_weight"):
        return events

    # skip when not a ttbar dataset
    if not dataset_inst.x("is_ttbar", False):
        return events

    # save dummy, asymmetric top pt weights
    set_ak_column(events, "top_pt_weight", ak.ones_like(events.Jet.pt) * 1.0)
    set_ak_column(events, "top_pt_weight_up", ak.ones_like(events.Jet.pt) * 1.1)
    set_ak_column(events, "top_pt_weight_down", ak.ones_like(events.Jet.pt) * 1.0)

    return events


@producer(
    uses={normalization_weights, pu_weights, top_pt_weights},
    produces={normalization_weights, pu_weights, top_pt_weights},
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

    # compute top pt weights
    events = top_pt_weights(events, **kwargs)

    return events


@event_weights.update
def event_weights_update(self: Producer, dataset_inst: od.Dataset, **kwargs) -> None:
    """
    Performs an update of the :py:obj:`event_weights` producer based on the *dataset_inst*.
    """
    # add top pt weight shifts for ttbar
    if dataset_inst.x("is_ttbar", False):
        self.shifts |= {"top_pt_up", "top_pt_down"}
