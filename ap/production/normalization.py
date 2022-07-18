# coding: utf-8

"""
Column production methods related to sample normalization event weights.
"""

from ap.production import Producer, producer
from ap.production.processes import process_ids
from ap.util import maybe_import
from ap.columnar_util import set_ak_column

np = maybe_import("numpy")
sp = maybe_import("scipy")
maybe_import("scipy.sparse")
ak = maybe_import("awkward")


@producer(
    uses={process_ids, "LHEWeight.originalXWGTUP"},
    produces={process_ids, "normalization_weight"},
)
def normalization_weights(self: Producer, events: ak.Array, **kwargs) -> ak.Array:
    """
    Uses luminosity information of internal py:attr:`config_inst`, the cross section of a process
    obtained through :py:class:`category_ids` and the sum of event weights from the
    py:attr:`selection_stats` attribute to assign each event a normalization weight.
    """
    # add process ids
    self[process_ids](events, **kwargs)

    # stop here for data
    if self.dataset_inst.is_data:
        return events

    # get the lumi
    lumi = self.config_inst.x.luminosity.nominal

    # read the cross section per process from the lookup table
    xs = np.array(self.xs_table[0, np.asarray(events.process_id)].todense())[0]

    # compute the weight and store it
    norm_weight = events.LHEWeight.originalXWGTUP * lumi * xs / self.selection_stats["sum_mc_weight"]
    set_ak_column(events, "normalization_weight", norm_weight)

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
    # do nothing for data
    if self.dataset_inst.is_data:
        return reqs

    from ap.tasks.selection import MergeSelectionStats
    reqs["selection_stats"] = MergeSelectionStats.req(self.task, tree_index=0)


@normalization_weights.setup
def normalization_weights_setup(self: Producer, inputs: dict) -> None:
    """
    Sets up objects required by the computation of normalization weights and stores them as instance
    attributes:

        - py:attr:`selection_stats`: The stats dict loaded from the output of MergeSelectionsStats.
        - py:attr:`xs_table`: A sparse array serving as a lookup table for all processes known to
          the config of the task, with keys being process ids.
    """
    # set to None for data
    if self.dataset_inst.is_data:
        self.selection_stats = None
        self.xs_table = None
        return

    # load the selection stats
    self.selection_stats = inputs["selection_stats"].load(formatter="json")

    # create a lookup table as a sparse array with all known processes
    process_insts = [
        process_inst
        for process_inst, _, _ in self.config_inst.walk_processes()
        if process_inst.is_mc
    ]
    max_id = max(process_inst.id for process_inst in process_insts)
    xs_table = sp.sparse.lil_matrix((1, max_id + 1), dtype=np.float32)
    for process_inst in process_insts:
        xs_table[0, process_inst.id] = process_inst.get_xsec(self.config_inst.campaign.ecm).nominal
    self.xs_table = xs_table
