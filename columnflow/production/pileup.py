# coding: utf-8

"""
Column production methods related to pileup weights.
"""

from columnflow.production import Producer, producer
from columnflow.util import maybe_import
from columnflow.columnar_util import set_ak_column

ak = maybe_import("awkward")


@producer(
    uses={"PV.npvs"},
    produces={"pu_weight", "pu_weight_minbias_xs_up", "pu_weight_minbias_xs_down"},
)
def pu_weights(self: Producer, events: ak.Array, **kwargs) -> ak.Array:
    """
    Based on the number of primary vertices, assigns each event pileup weights using the profile
    of pileup ratios at the py:attr:`pu_weights` attribute provided by the requires and setup
    functions below.
    """
    # stop here for data
    if self.dataset_inst.is_data:
        return events

    # compute the indices for looking up weights
    indices = events.PV.npvs.to_numpy() - 1
    max_bin = len(self.pu_weights) - 1
    indices[indices > max_bin] = max_bin

    # save the weights
    set_ak_column(events, "pu_weight", self.pu_weights.nominal[indices])
    set_ak_column(events, "pu_weight_minbias_xs_up", self.pu_weights.minbias_xs_up[indices])
    set_ak_column(events, "pu_weight_minbias_xs_down", self.pu_weights.minbias_xs_down[indices])

    return events


@pu_weights.requires
def pu_weights_requires(self: Producer, reqs: dict) -> None:
    """
    Adds the requirements needed the underlying task to derive the pileup weights into *reqs*.
    """
    # do nothing for data
    if self.dataset_inst.is_data:
        return reqs

    from columnflow.tasks.external import CreatePileupWeights
    reqs["pu_weights"] = CreatePileupWeights.req(self.task)


@pu_weights.setup
def pu_weights_setup(self: Producer, inputs: dict) -> None:
    """
    Loads the pileup weights added through the requirements and saves them in the
    py:attr:`pu_weights` attribute for simpler access in the actual callable.
    """
    # set to None for data
    if self.dataset_inst.is_data:
        self.pu_weights = None
        return

    self.pu_weights = ak.zip(inputs["pu_weights"].load(formatter="json"))
