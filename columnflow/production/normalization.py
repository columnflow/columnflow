# coding: utf-8

"""
Column production methods related to sample normalization event weights.
"""

from collections import defaultdict

import law

from columnflow.production import Producer, producer
from columnflow.util import maybe_import, InsertableDict
from columnflow.columnar_util import set_ak_column

np = maybe_import("numpy")
sp = maybe_import("scipy")
maybe_import("scipy.sparse")
ak = maybe_import("awkward")


@producer(
    uses={"process_id", "mc_weight"},
    # name of the output column
    weight_name="normalization_weight",
    # whether to allow stitching datasets
    allow_stitching=True,
    # only run on mc
    mc_only=True,
)
def normalization_weights(self: Producer, events: ak.Array, **kwargs) -> ak.Array:
    """
    Uses luminosity information of internal py:attr:`config_inst`, the cross section of a process
    obtained through :py:class:`category_ids` and the sum of event weights from the
    py:attr:`normalization_selection_stats` attribute to assign each event a normalization weight.
    The normalization weight is stored in a new column named after the py:attr:`weight_name` attribute.
    When py:attr`allow_stitching` is set to True, the sum of event weights is computed for all
    datasets in the py:attr:`stitching_datasets` attribute of the internal py:attr:`dataset_inst`.
    If datasets contain multiple processes considered in the stitching procedure, the py:attr:`dataset_inst`
    must contain all considered processes and the py:attr:`process_id` column must be reconstructed
    on a per-event basis. Example:

    .. code-block:: python

        procs = get_root_processes_from_campaign(campaign)
        stitching_groups = {
            "dy_lep_m50": [
                "dy_lep_m50_amcatnlo",
                "dy_lep_m50_0j_amcatnlo",
                "dy_lep_m50_1j_amcatnlo",
                "dy_lep_m50_2j_amcatnlo",
            ],
        }
        replace_processes_map = {
            "dy_lep_m50": ["dy_lep_m50_0j", "dy_lep_m50_1j", "dy_lep_m50_2j"],
        }
        for dataset in stitching_groups["dy_lep_m50"]:
            dataset_inst = config.get_dataset(dataset)
            # assign the stitching datasets to the datasets
            dataset_inst.x.stitching_datasets = stitching_groups["dy_lep_m50"]

            for proc in dataset.processes:
                # replace the processes with the binned ones
                if proc.name in replace_processes_map:
                    dataset.remove_process(proc)
                    for binned_proc in replace_processes_map[proc.name]:
                        dataset.add_process(procs.n(binned_proc))
    """
    # get the lumi
    lumi = self.config_inst.x.luminosity.nominal

    # read the cross section per process from the lookup table
    process_id = np.asarray(events.process_id)
    xs = np.array(self.xs_table[0, process_id].todense())[0]

    # read the sum of event weights per process from the lookup table
    sum_weights = np.array(self.sum_weights_table[0, process_id].todense())[0]

    # compute the weight and store it
    norm_weight = events.mc_weight * lumi * xs / sum_weights
    events = set_ak_column(events, self.weight_name, norm_weight, value_type=np.float32)

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

    self.selection_stats_key = f"{'stitched_' if self.allow_stitching else 'norm_'}selection_stats"

    if self.allow_stitching and self.dataset_inst.x("stitching_datasets", None):
        stats_datasets = law.util.make_list(self.dataset_inst.x.stitching_datasets)
    else:
        stats_datasets = [self.dataset_inst.name]

    # check that all datasets are known
    for dataset in stats_datasets:
        if not self.config_inst.has_dataset(dataset):
            raise Exception(
                f"unknown dataset '{dataset}' required for normalization weights computation",
            )

    from columnflow.tasks.selection import MergeSelectionStats
    reqs[self.selection_stats_key] = {
        dataset: MergeSelectionStats.req(
            self.task,
            dataset=dataset,
            tree_index=0,
            branch=-1,
            _exclude=MergeSelectionStats.exclude_params_forest_merge,
        )
        for dataset in stats_datasets
    }
    return reqs


@normalization_weights.setup
def normalization_weights_setup(
    self: Producer,
    reqs: dict,
    inputs: dict,
    reader_targets: InsertableDict,
) -> None:
    """
    Sets up objects required by the computation of normalization weights and stores them as instance
    attributes:

        - py:attr:`sum_weights_table`: A sparse array serving as a lookup table for the sum of event
          weights per process id.
        - py:attr:`xs_table`: A sparse array serving as a lookup table for cross sections of all
          processes known to the config of the task, with keys being process ids.
    """
    # load the selection stats
    print(self.cls_name, self.selection_stats_key, inputs[self.selection_stats_key])
    normalization_selection_stats = [
        inp["collection"][0]["stats"].load(formatter="json")
        for inp in inputs[self.selection_stats_key].values()
    ]

    if len(normalization_selection_stats) > 1:
        # if necessary, merge the selection stats
        from columnflow.tasks.selection import MergeSelectionStats
        merged_selection_stats = defaultdict(float)
        for stats in normalization_selection_stats:
            MergeSelectionStats.merge_counts(merged_selection_stats, stats)
    else:
        merged_selection_stats = normalization_selection_stats[0]

    # for the lookup tables below, determine the maximum process id
    process_insts = [
        process_inst
        for process_inst, _, _ in self.config_inst.walk_processes()
        if process_inst.is_mc
    ]
    max_id = max(process_inst.id for process_inst in process_insts)

    # ensure that the selection stats do not contain any process that was not previously registered
    unregistered_process_ids = [
        int(process_id) for process_id in merged_selection_stats["sum_mc_weight_per_process"]
        if int(process_id) > max_id
    ]
    if unregistered_process_ids:
        id_str = ",".join(map(str, unregistered_process_ids))
        raise Exception(
            f"selection stats contain ids ({id_str}) of processes that were not previously " +
            f"registered to the config '{self.config_inst.name}'",
        )

    # create a event weight sum lookup table with all known processes
    sum_weights_table = sp.sparse.lil_matrix((1, max_id + 1), dtype=np.float32)
    for process_id, sum_weights in merged_selection_stats["sum_mc_weight_per_process"].items():
        sum_weights_table[0, int(process_id)] = sum_weights
    self.sum_weights_table = sum_weights_table

    # create a cross section lookup table with all known processes with a cross section
    xs_table = sp.sparse.lil_matrix((1, max_id + 1), dtype=np.float32)
    for process_inst in process_insts:
        if self.config_inst.campaign.ecm not in process_inst.xsecs.keys():
            continue
        xs_table[0, process_inst.id] = process_inst.get_xsec(self.config_inst.campaign.ecm).nominal
    self.xs_table = xs_table


@normalization_weights.init
def normalization_weights_init(self: Producer) -> None:
    """
    Initializes the normalization weights producer by setting up the normalization weight column.
    """
    self.produces.add(self.weight_name)


unstitched_normalization_weights = normalization_weights.derive(
    "unstitched_normalization_weights", cls_dict={
        "weight_name": "unstitched_normalization_weight",
        "allow_stitching": False,
    },
)
