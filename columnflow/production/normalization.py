# coding: utf-8

"""
Column production methods related to sample normalization event weights.
"""

from columnflow.production import Producer, producer
from columnflow.production.processes import process_ids
from columnflow.util import maybe_import
from columnflow.columnar_util import set_ak_column


np = maybe_import("numpy")
sp = maybe_import("scipy")
maybe_import("scipy.sparse")
ak = maybe_import("awkward")


@producer(
    uses={process_ids, "mc_weight"},
    produces={process_ids, "normalization_weight"},
)
def normalization_weights(self: Producer, events: ak.Array, **kwargs) -> ak.Array:
    """
    Uses luminosity information of internal py:attr:`config_inst`, the cross section of a process
    obtained through :py:class:`category_ids` and the sum of event weights from the
    py:attr:`selection_stats` attribute to assign each event a normalization weight.
    """
    # fail when running on data
    if self.dataset_inst.is_data:
        raise ValueError("attempt to compute normalization weights in data")

    # add process ids
    events = self[process_ids](events, **kwargs)

    # get the lumi
    lumi = self.config_inst.x.luminosity.nominal

    # read the cross section per process from the lookup table
    process_id = np.asarray(events.process_id)
    xs = np.array(self.xs_table[0, process_id].todense())[0]

    # read the sum of event weights per process from the lookup table
    sum_weights = np.array(self.sum_weights_table[0, process_id].todense())[0]

    # compute the weight and store it
    norm_weight = events.mc_weight * lumi * xs / sum_weights
    events = set_ak_column(events, "normalization_weight", norm_weight, value_type=np.float32)

    return events


@normalization_weights.requires
def normalization_weights_requires(self: Producer, reqs: dict) -> None:
    """
    Adds the requirements needed by the underlying py:attr:`task` to access selection stats into
    *reqs*.
    """
    # TODO: for actual sample stitching, we don't need the selection stats for that dataset, but
    #       rather the one merged for either all datasets, or the "stitching group"
    #       (i.e. all datasets that might contain any of the sub processes found in a dataset)
    from columnflow.tasks.selection import MergeSelectionStats
    reqs["selection_stats"] = MergeSelectionStats.req(
        self.task,
        tree_index=0,
        branch=-1,
        _exclude=MergeSelectionStats.exclude_params_forest_merge,
    )


@normalization_weights.setup
def normalization_weights_setup(self: Producer, reqs: dict, inputs: dict) -> None:
    """
    Sets up objects required by the computation of normalization weights and stores them as instance
    attributes:

        - py:attr:`selection_stats`: The stats dict loaded from the output of MergeSelectionsStats.
        - py:attr:`sum_weights_table`: A sparse array serving as a lookup table for the sum of event
          weights per process id.
        - py:attr:`xs_table`: A sparse array serving as a lookup table for cross sections of all
          processes known to the config of the task, with keys being process ids.
    """
    # load the selection stats
    selection_stats = inputs["selection_stats"]["collection"][0].load(formatter="json")

    # for the lookup tables below, determine the maximum process id
    process_insts = [
        process_inst
        for process_inst, _, _ in self.config_inst.walk_processes()
        if process_inst.is_mc
    ]
    max_id = max(process_inst.id for process_inst in process_insts)

    # create a event weight sum lookup table with all known processes
    sum_weights_table = sp.sparse.lil_matrix((1, max_id + 1), dtype=np.float32)
    for process_id, sum_weights in selection_stats["sum_mc_weight_per_process"].items():
        sum_weights_table[0, int(process_id)] = sum_weights
    self.sum_weights_table = sum_weights_table

    # create a cross section lookup table with all known processes
    xs_table = sp.sparse.lil_matrix((1, max_id + 1), dtype=np.float32)
    for process_inst in process_insts:
        xs_table[0, process_inst.id] = process_inst.get_xsec(self.config_inst.campaign.ecm).nominal
    self.xs_table = xs_table
