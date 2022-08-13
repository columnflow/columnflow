# coding: utf-8

"""
Column production methods related to generic event weights.
"""

from columnflow.production import Producer, producer
from columnflow.production.pileup import pu_weights
from columnflow.production.normalization import normalization_weights
from columnflow.util import maybe_import
from columnflow.columnar_util import set_ak_column

ak = maybe_import("awkward")


@producer(
    uses={"Jet.pt"},
    produces={"top_pt_weight", "top_pt_weight_up", "top_pt_weight_down"},
)
def top_pt_weights(self: Producer, events: ak.Array, **kwargs) -> ak.Array:
    """
    Adds a dummy top pt weight and its variations to *events* in case the internal
    py:attr:`dataset_inst` represents a ttbar dataset.
    """
    # skip when not a ttbar dataset
    if not self.dataset_inst.x("is_ttbar", False):
        return events

    # save dummy, asymmetric top pt weights
    set_ak_column(events, "top_pt_weight", ak.ones_like(events.event) * 1.0)
    set_ak_column(events, "top_pt_weight_up", ak.ones_like(events.event) * 1.1)
    set_ak_column(events, "top_pt_weight_down", ak.ones_like(events.event) * 1.0)

    return events


@producer(
    uses={normalization_weights, pu_weights, top_pt_weights},
    produces={normalization_weights, pu_weights, top_pt_weights},
    shifts={"minbias_xs_up", "minbias_xs_down"},
)
def event_weights(self: Producer, events: ak.Array, **kwargs) -> ak.Array:
    """
    Opinionated wrapper of several event weight producers. It declares dependence all shifts that
    might possibly change any of the weights.
    """
    # compute normalization weights
    self[normalization_weights](events, **kwargs)

    # compute pu weights
    self[pu_weights](events, **kwargs)

    # compute top pt weights
    self[top_pt_weights](events, **kwargs)

    return events


@event_weights.init
def event_weights_init(self: Producer) -> None:
    """
    Performs an update of the :py:obj:`event_weights` producer based on, when existing, the internal
    py:attr:`dataset_inst` attribute.
    """
    # add top pt weight shifts for ttbar, or when the dataset_inst is not even set, meaning that
    # the owning task is a ConfigTask or higher
    if not getattr(self, "dataset_inst", None) or self.dataset_inst.x("is_ttbar", False):
        self.shifts |= {"top_pt_up", "top_pt_down"}
