# coding: utf-8

"""
Column production methods related to pileup weights.
"""

import law
import order as od

from ap.production import Producer, producer
from ap.util import maybe_import
from ap.columnar_util import set_ak_column, has_ak_column

ak = maybe_import("awkward")


@producer(
    uses={"PV.npvs"},
    produces={"pu_weight", "pu_weight_minbias_xs_up", "pu_weight_minbias_xs_down"},
)
def pu_weights(
    events: ak.Array,
    pu_weights: ak.Array,
    dataset_inst: od.Dataset,
    **kwargs,
) -> ak.Array:
    """
    Based on the number of primary vertices, assigns each event pileup weights using the profile
    of pileup ratios *pu_weights* that is provided by the requires and setup functions below.
    """
    if has_ak_column(events, "pu_weight"):
        return events

    # stop here for data
    if dataset_inst.is_data:
        return events

    # compute the indices for looking up weights
    indices = events.PV.npvs.to_numpy() - 1
    max_bin = len(pu_weights) - 1
    indices[indices > max_bin] = max_bin

    # save the weights
    set_ak_column(events, "pu_weight", pu_weights.nominal[indices])
    set_ak_column(events, "pu_weight_minbias_xs_up", pu_weights.minbias_xs_up[indices])
    set_ak_column(events, "pu_weight_minbias_xs_down", pu_weights.minbias_xs_down[indices])

    return events


@pu_weights.requires
def pu_weights_requires(self: Producer, task: law.Task, reqs: dict) -> dict:
    """
    Adds the requirements needed by *task* to derive the pileup weights into *reqs*.
    """
    if reqs.get("pu_weights") is not None or task.dataset_inst.is_data:
        return reqs

    from ap.tasks.external import CreatePileupWeights
    reqs["pu_weights"] = CreatePileupWeights.req(task)

    return reqs


@pu_weights.setup
def pu_weights_setup(
    self: Producer,
    task: law.Task,
    inputs: dict,
    call_kwargs: dict,
    **kwargs,
) -> None:
    """
    Loads the pileup weights added through the requirements and saves them in the *call_kwargs* for
    simpler access in the actual callable.
    """
    if call_kwargs.get("pu_weights") is not None or task.dataset_inst.is_data:
        call_kwargs.setdefault("pu_weights", None)
        return

    call_kwargs["pu_weights"] = ak.zip(inputs["pu_weights"].load(formatter="json"))
