# coding: utf-8

"""
Column production methods related to sample normalization event weights.
"""

from collections import defaultdict

import law
import order as od

from columnflow.production import Producer, producer
from columnflow.util import maybe_import, InsertableDict
from columnflow.columnar_util import set_ak_column

np = maybe_import("numpy")
sp = maybe_import("scipy")
maybe_import("scipy.sparse")
ak = maybe_import("awkward")

logger = law.logger.get_logger(__name__)


def get_inclusive_dataset(self: Producer) -> od.Dataset:
    """
    Helper function to obtain the inclusive dataset from a list of datasets that are required to
    stitch this *dataset_inst*.
    """
    process_map = {d.processes.get_first(): d for d in self.stitching_datasets}

    process_inst = self.dataset_inst.processes.get_first()
    incl_dataset = None
    while process_inst:
        if process_inst in process_map:
            incl_dataset = process_map[process_inst]
        process_inst = process_inst.parent_processes.get_first(default=None)

    if not incl_dataset:
        raise Exception("inclusive dataset not found")

    unmatched_processes = {p for p in process_map if not incl_dataset.has_process(p, deep=True)}
    if unmatched_processes:
        raise Exception(f"processes {unmatched_processes} not found in inclusive dataset")

    return incl_dataset


def get_stitching_datasets(self: Producer) -> list[od.Dataset]:
    """
    Helper function to obtain all datasets that are required to stitch this *dataset_inst*.
    """
    stitching_datasets = {
        d for d in self.config_inst.datasets
        if (
            d.has_process(self.dataset_inst.processes.get_first(), deep=True) or
            self.dataset_inst.has_process(d.processes.get_first(), deep=True)
        )
    }
    return list(stitching_datasets)


def get_br_from_inclusive_dataset(stats: dict) -> dict:
    """
    Helper function to compute the branching ratios from the inclusive sample.
    """
    # get the sum of weights inclusive and per process
    sum_mc_weight = stats["sum_mc_weight"]
    sum_mc_weight_per_process = stats["sum_mc_weight_per_process"]

    if not sum_mc_weight == sum(sum_mc_weight_per_process.values()):
        raise Exception(
            "sum of event weights does not match the sum of event weights per process",
        )

    # compute the branching ratios
    branching_ratios = {
        int(process_id): sum_weights / sum_mc_weight
        for process_id, sum_weights in sum_mc_weight_per_process.items()
    }
    return branching_ratios


@producer(
    uses={"process_id", "mc_weight"},
    # name of the output column
    weight_name="normalization_weight",
    # whether to allow stitching datasets
    allow_stitching=False,
    get_xsecs_from_inclusive_dataset=False,
    get_stitching_datasets=get_stitching_datasets,
    get_inclusive_dataset=get_inclusive_dataset,
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
    datasets with a leaf process contained in the leaf processes of the py:attr:`dataset_inst`.
    For stitching, the process_id needs to be reconstructed for each leaf process on a per event basis.
    """
    # get the lumi
    lumi = self.config_inst.x.luminosity.nominal

    # read the cross section per process from the lookup table
    process_id = np.asarray(events.process_id)

    unique_process_ids = set(process_id)
    invalid_ids = unique_process_ids - set(self.xs_table.rows[0])
    if invalid_ids:
        logger.warning(
            f"process_id field contains process ids {invalid_ids} that were not assigned a cross section",
        )

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
    self.selection_stats_key = f"{'stitched_' if self.allow_stitching else 'norm_'}selection_stats"

    if self.allow_stitching:
        self.stitching_datasets = self.get_stitching_datasets()
    else:
        self.stitching_datasets = [self.dataset_inst]

    # check that all datasets are known
    for dataset in self.stitching_datasets:
        if not self.config_inst.has_dataset(dataset):
            raise Exception(
                f"unknown dataset '{dataset}' required for normalization weights computation",
            )

    from columnflow.tasks.selection import MergeSelectionStats
    reqs[self.selection_stats_key] = {
        dataset.name: MergeSelectionStats.req(
            self.task,
            dataset=dataset.name,
            tree_index=0,
            branch=-1,
            _exclude=MergeSelectionStats.exclude_params_forest_merge,
        )
        for dataset in self.stitching_datasets
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
    normalization_selection_stats = {
        dataset: self.task.cached_value(
            key=f"selection_stats_{dataset}",
            func=lambda: inp["collection"][0]["stats"].load(formatter="json"),
        )
        for dataset, inp in inputs[self.selection_stats_key].items()
    }
    # if necessary, merge the selection stats across datasets
    if len(normalization_selection_stats) > 1:
        from columnflow.tasks.selection import MergeSelectionStats
        merged_selection_stats = defaultdict(float)
        for stats in normalization_selection_stats.values():
            MergeSelectionStats.merge_counts(merged_selection_stats, stats)
    else:
        merged_selection_stats = normalization_selection_stats[self.dataset_inst.name]

    # for the lookup tables below, determine the maximum process id
    process_insts = {
        process_inst
        for dataset_inst in self.stitching_datasets
        for process_inst, _, _ in dataset_inst.walk_processes(include_self=False)
        if process_inst.is_mc
    }
    max_id = max(process_inst.id for process_inst in process_insts)

    # ensure that the selection stats do not contain any process that was not previously registered
    allowed_ids = set(int(process_id) for process_id in merged_selection_stats["sum_mc_weight_per_process"])

    unregistered_process_ids = allowed_ids - {p.id for p in process_insts}
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
    if self.allow_stitching and self.get_xsecs_from_inclusive_dataset:
        inclusive_dataset = self.get_inclusive_dataset()
        logger.info(f"using inclusive dataset {inclusive_dataset.name} for cross section lookup")

        # get the branching ratios from the inclusive sample
        branching_ratios = get_br_from_inclusive_dataset(normalization_selection_stats[inclusive_dataset.name])
        inclusive_xsec = inclusive_dataset.processes.get_first().get_xsec(self.config_inst.campaign.ecm).nominal
        for process_id, br in branching_ratios.items():
            xs_table[0, process_id] = inclusive_xsec * br
    else:
        for process_inst in process_insts:
            if self.config_inst.campaign.ecm not in process_inst.xsecs.keys():
                logger.warning(f"cross section for {process_inst.name} at {self.config_inst.campaign.ecm} not found")
                continue
            xs_table[0, process_inst.id] = process_inst.get_xsec(self.config_inst.campaign.ecm).nominal

    self.xs_table = xs_table


@normalization_weights.init
def normalization_weights_init(self: Producer) -> None:
    """
    Initializes the normalization weights producer by setting up the normalization weight column.
    """
    self.produces.add(self.weight_name)


stitched_normalization_weights_brs_from_processes = normalization_weights.derive(
    "stitched_normalization_weights_brs_from_processes", cls_dict={
        "weight_name": "stitched_normalization_weight_brs_from_processes",
        "get_xsecs_from_inclusive_dataset": False,
        "allow_stitching": True,
    },
)


stitched_normalization_weights = normalization_weights.derive(
    "stitched_normalization_weights", cls_dict={
        "weight_name": "stitched_normalization_weight",
        "get_xsecs_from_inclusive_dataset": True,
        "allow_stitching": True,
    },
)
