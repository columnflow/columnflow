# coding: utf-8

"""
Column production methods related to pileup weights.
"""

import functools

from columnflow.production import Producer, producer
from columnflow.util import maybe_import
from columnflow.columnar_util import set_ak_column


np = maybe_import("numpy")
ak = maybe_import("awkward")

# helper
set_ak_column_f32 = functools.partial(set_ak_column, value_type=np.float32)


@producer(
    uses={"PV.npvs"},
    produces={"pu_weight", "pu_weight_minbias_xs_up", "pu_weight_minbias_xs_down"},
)
def pu_weight(self: Producer, events: ak.Array, **kwargs) -> ak.Array:
    """
    Based on the number of primary vertices, assigns each event pileup weights using the profile
    of pileup ratios at the py:attr:`pu_weights` attribute provided by the requires and setup
    functions below.
    """
    # fail when running on data
    if self.dataset_inst.is_data:
        raise ValueError("attempt to compute pileup weights in data")

    # compute the indices for looking up weights
    indices = events.PV.npvs.to_numpy() - 1
    max_bin = len(self.pu_weights) - 1
    indices[indices > max_bin] = max_bin

    # save the weights
    events = set_ak_column_f32(events, "pu_weight", self.pu_weights.nominal[indices])
    events = set_ak_column_f32(events, "pu_weight_minbias_xs_up", self.pu_weights.minbias_xs_up[indices])
    events = set_ak_column_f32(events, "pu_weight_minbias_xs_down", self.pu_weights.minbias_xs_down[indices])

    return events


@pu_weight.requires
def pu_weight_requires(self: Producer, reqs: dict) -> None:
    """
    Adds the requirements needed the underlying task to derive the pileup weights into *reqs*.
    """
    if "pu_weights" in reqs:
        return

    from columnflow.tasks.external import CreatePileupWeights
    reqs["pu_weights"] = CreatePileupWeights.req(self.task)


@pu_weight.setup
def pu_weight_setup(self: Producer, reqs: dict, inputs: dict) -> None:
    """
    Loads the pileup weights added through the requirements and saves them in the
    py:attr:`pu_weights` attribute for simpler access in the actual callable.
    """
    self.pu_weights = ak.zip(inputs["pu_weights"].load(formatter="json"))
