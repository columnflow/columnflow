# coding: utf-8

"""
Column production methods related to pileup weights.
"""

from ap.util import maybe_import
from ap.production import producer

ak = maybe_import("awkward")


@producer(
    uses={"PV_npvs"},
    produces={"pu_weight_nominal", "pu_weight_minbias_xs_up", "pu_weight_minbias_xs_down"},
)
def pu_weights(events, *, pu_weights=None, **kwargs):
    # compute the indices for looking up weights
    indices = events.PV.npvs.to_numpy() - 1
    max_bin = len(pu_weights) - 1
    indices[indices > max_bin] = max_bin

    # save the weights
    events["pu_weight"] = ak.zip({
        "nominal": pu_weights.nominal[indices],
        "minbias_xs_up": pu_weights.minbias_xs_up[indices],
        "minbias_xs_down": pu_weights.minbias_xs_down[indices],
    })

    return events


@pu_weights.requires
def requires(task, reqs):
    if "pu_weights" not in reqs:
        from ap.tasks.external import CreatePileupWeights
        reqs["pu_weights"] = CreatePileupWeights.req(task)
    return reqs


@pu_weights.setup
def setup(task, inputs, producer_kwargs, **kwargs):
    producer_kwargs["pu_weights"] = ak.zip(inputs["pu_weights"].load(formatter="json"))
