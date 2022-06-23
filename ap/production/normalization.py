# coding: utf-8

"""
Column production methods related to sample normalization event weights.
"""

from typing import Dict, Union

import law
import order as od

from ap.production import Producer, producer
from ap.production.processes import process_ids
from ap.util import maybe_import
from ap.columnar_util import set_ak_column, has_ak_column

np = maybe_import("numpy")
sp = maybe_import("scipy")
maybe_import("scipy.sparse")
ak = maybe_import("awkward")


@producer(
    uses={process_ids, "LHEWeight.originalXWGTUP"},
    produces={process_ids, "normalization_weight"},
)
def normalization_weights(
    events: ak.Array,
    selection_stats: Dict[str, Union[int, float]],
    xs_table: sp.sparse.lil.lil_matrix,
    config_inst: od.Config,
    dataset_inst: od.Dataset,
    **kwargs,
) -> ak.Array:
    """
    Uses luminosity information of *config_inst*, the cross section of a process obtained through
    *dataset_inst* and the sum of event weights from *selection_stats* to assign each event a
    normalization weight.
    """
    if has_ak_column(events, "normalization_weight"):
        return events

    # add process ids
    events = process_ids(events, config_inst=config_inst, dataset_inst=dataset_inst, **kwargs)

    # stop here for data
    if dataset_inst.is_data:
        return events

    # get the lumi
    lumi = config_inst.x.luminosity.nominal

    # read the cross section per process from the lookup table
    xs = np.array(xs_table[0, np.asarray(events.process_id)].todense())[0]

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
    # do nothing when requirements are already present or for data
    if reqs.get("selection_stats") is not None or task.dataset_inst.is_data:
        return reqs

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
    Sets up objects required by the computation of normalization weights and stores them in
    *call_kwargs*:

        - "selection_stats": The stats dict loaded from the output of MergeSelectionsStats.
        - "xs_table": A sparse array serving as a lookup table for all processes known to the
                      config of the *task*, with keys being process ids.
    """
    if call_kwargs.get("selection_stats") is not None:
        # do nothing when already present
        return
    if task.dataset_inst.is_data:
        # set to None for data
        call_kwargs.setdefault("selection_stats", None)
        call_kwargs.setdefault("xs_table", None)
        return

    # load the selection stats
    call_kwargs["selection_stats"] = inputs["selection_stats"].load(formatter="json")

    # create a lookup table as a sparse array with all known processes
    process_insts = [
        process_inst
        for process_inst, _, _ in task.config_inst.walk_processes()
        if process_inst.is_mc
    ]
    max_id = max(process_inst.id for process_inst in process_insts)
    xs_table = sp.sparse.lil_matrix((1, max_id + 1), dtype=np.float32)
    for process_inst in process_insts:
        xs_table[0, process_inst.id] = process_inst.get_xsec(task.config_inst.campaign.ecm).nominal
    call_kwargs["xs_table"] = xs_table
