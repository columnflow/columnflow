# coding: utf-8

"""
Column production methods related to sample normalization event weights.
"""

from typing import Dict, Union

import law
import order as od

from ap.production import Producer, producer
from ap.util import maybe_import
from ap.columnar_util import set_ak_column

ak = maybe_import("awkward")


@producer(
    uses={"LHEWeight.originalXWGTUP"},
    produces={"normalization_weight"},
)
def normalization_weights(
    events: ak.Array,
    selection_stats: Dict[str, Union[int, float]],
    config_inst: od.Config,
    dataset_inst: od.Dataset,
    **kwargs,
) -> ak.Array:
    """
    Uses luminosity information of *config_inst*, the cross section of a process obtained through
    *dataset_inst* and the sum of event weights from *selection_stats* to assign each event a
    normalization weight.
    """
    # get lumi and cross section of the first process contained in the dataset
    # TODO: the process should depend on the process id per event for which we need a LUT
    #       that is cached through a simple mutable argument
    lumi = config_inst.x.luminosity.nominal
    xs = dataset_inst.processes.get_first().get_xsec(config_inst.campaign.ecm).nominal

    # compute the weight and store it
    norm_weight = events.LHEWeight.originalXWGTUP * lumi * xs / selection_stats["sum_mc_weight"]
    set_ak_column(events, "normalization_weight", norm_weight)

    return events


@normalization_weights.requires
def normalization_weights_requires(self: Producer, task: law.Task, reqs: dict) -> dict:
    """
    Adds the requirements needed by *task* to access selection stats into *reqs*.
    """
    # TODO: for actual sample stitching, we don't need the selection stats for that dataset, but
    #       rather the one merged for either all datasets, or the "stitching group"
    #       (i.e. all datasets that might contain any of the sub processes found in a dataset)
    if "selection_stats" not in reqs:
        from ap.tasks.selection import MergeSelectionStats
        reqs["selection_stats"] = MergeSelectionStats.req(task, tree_index=0)
    return reqs


@normalization_weights.setup
def normalization_weights_setup(
    self: Producer,
    task: law.Task,
    inputs: dict,
    call_kwargs: dict,
    **kwargs,
):
    """
    Loads the selection stats added through the requirements and saves them in the *call_kwargs* for
    simpler access in the actual callable.
    """
    call_kwargs["selection_stats"] = inputs["selection_stats"].load(formatter="json")
