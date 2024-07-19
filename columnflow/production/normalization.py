# coding: utf-8

"""
Column production methods related to sample normalization event weights.
"""

from __future__ import annotations

from collections import defaultdict

import law
import order as od
import scinum as sn

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


def get_br_from_inclusive_dataset(self: Producer, inclusive_dataset: od.Dataset, stats: dict) -> dict[int, float]:
    """
    Helper function to compute the branching ratios from the inclusive sample.
    """
    # define a helper variables and mapping between process ids and dataset names
    proc_ds_map = {
        d.processes.get_first().id: d
        for d in self.config_inst.datasets
        if d.name in stats.keys()
    }
    inclusive_proc = inclusive_dataset.processes.get_first()
    N = lambda x: sn.Number(x, np.sqrt(x))  # alias for Number with counting error

    # create a dictionary of branching ratios based on all gen weight sums
    br: dict[int, dict[int, sn.Number]] = defaultdict(dict)
    for proc, _, child_procs in inclusive_dataset.walk_processes():
        if proc.id not in proc_ds_map or proc.is_leaf_process:
            continue

        # get the mc weights for the "mother" dataset and add an entry for the process
        sum_mc_weight = stats[proc_ds_map[proc.id].name]["sum_mc_weight"]
        sum_mc_weight_per_process = stats[proc_ds_map[proc.id].name]["sum_mc_weight_per_process"]
        # use the number of events to compute the error on the branching ratio
        num_events = stats[proc_ds_map[proc.id].name]["num_events"]
        num_events_per_process = stats[proc_ds_map[proc.id].name]["num_events_per_process"]

        # compute the branching ratios for the children wrt the mother process
        for child_proc in child_procs:
            # skip processes that are not covered by any dataset or irrelevant for the used dataset
            # (identified as leaf processes that have no occurrences in the stats)
            # (or as non-leaf processes that are not in the stitching datasets)
            is_leaf = child_proc.is_leaf_process
            if (
                (is_leaf and str(child_proc.id) not in sum_mc_weight_per_process) or
                (not is_leaf and child_proc.id not in proc_ds_map)
            ):
                continue

            proc_ids = [child_proc.id] if is_leaf else [p.id for p in child_proc.get_leaf_processes()]
            # compute the uncertainty on the branching ratio using number of events
            _br = N(sum(num_events_per_process.get(str(proc_id), 0) for proc_id in proc_ids)) / N(num_events)
            # uncertainty is independent of the mc weights, so we can use the same relative uncertainty
            br[proc.id][child_proc.id] = sn.Number(
                sum(sum_mc_weight_per_process.get(str(proc_id), 0) for proc_id in proc_ids) / sum_mc_weight,
                _br(sn.UP, unc=True, factor=True) * 1j,  # same relative uncertainty
            )

    branching_ratios: dict[int, float] = {}

    def multiply_branching_ratios(proc_id: int, proc_br: sn.Number) -> tuple[int, float] | None:
        """
        Recursively multiply the branching ratios from the nested dictionary.
        """
        # when the br for proc_id can be created from sub processes, calculate it via product
        if proc_id in br:
            for child_id, child_br in br[proc_id].items():
                # multiply the branching ratios assuming no correlation
                prod_br = child_br.mul(proc_br, rho=0, inplace=False)
                multiply_branching_ratios(child_id, prod_br)
            return

        # warn the user if the relative (statistical) error is large
        rel_unc = proc_br(sn.UP, unc=True, factor=True)
        if rel_unc > 0.01:
            logger.warning(
                "large error on the branching ratio for process "
                f"{inclusive_proc.get_process(proc_id).name} with process id {proc_id} "
                f"({rel_unc * 100:.2f}%)",
            )

        # just store the nominal value
        branching_ratios[proc_id] = proc_br.nominal

        return proc_id, proc_br

    # fill all branching ratios
    for proc_id, brs in br[inclusive_proc.id].items():
        multiply_branching_ratios(proc_id, brs)

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
    get_br_from_inclusive_dataset=get_br_from_inclusive_dataset,
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
    # read the process id column
    process_id = np.asarray(events.process_id)

    # ensure all ids were assigned a cross section
    unique_process_ids = set(process_id)
    invalid_ids = unique_process_ids - self.xs_process_ids
    if invalid_ids:
        raise Exception(
            f"process_id field contains id(s) {invalid_ids} for which no cross sections were "
            f"found; process ids with cross sections: {self.xs_process_ids}",
        )

    # read the weight per process (defined as lumi * xsec / sum_weights) from the lookup table
    process_weight = np.squeeze(np.asarray(self.process_weight_table[0, process_id].todense()))

    # compute the weight and store it
    norm_weight = events.mc_weight * process_weight
    events = set_ak_column(events, self.weight_name, norm_weight, value_type=np.float32)

    return events


@normalization_weights.requires
def normalization_weights_requires(self: Producer, reqs: dict) -> None:
    """
    Adds the requirements needed by the underlying py:attr:`task` to access selection stats into
    *reqs*.
    """
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
    reqs["selection_stats"] = {
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

        - py: attr: `process_weight_table`: A sparse array serving as a lookup table for the
        calculated process weights. This weight is defined as the product of the luminosity, the
        cross section, divided by the sum of event weights per process.
    """
    # load the selection stats
    normalization_selection_stats = {
        dataset: self.task.cached_value(
            key=f"selection_stats_{dataset}",
            func=lambda: inp["collection"][0]["stats"].load(formatter="json"),
        )
        for dataset, inp in inputs["selection_stats"].items()
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

    # get the luminosity from the config
    lumi = self.config_inst.x.luminosity.nominal

    # create a event weight lookup table
    weight_table = sp.sparse.lil_matrix((1, max_id + 1), dtype=np.float32)
    if self.allow_stitching and self.get_xsecs_from_inclusive_dataset:
        inclusive_dataset = self.get_inclusive_dataset()
        logger.info(f"using inclusive dataset {inclusive_dataset.name} for cross section lookup")

        # get the branching ratios from the inclusive sample
        inclusive_proc = inclusive_dataset.processes.get_first()
        if self.dataset_inst == inclusive_dataset and process_insts == {inclusive_proc}:
            branching_ratios = {inclusive_proc.id: 1.0}
        else:
            branching_ratios = self.get_br_from_inclusive_dataset(inclusive_dataset, normalization_selection_stats)
        inclusive_xsec = inclusive_proc.get_xsec(self.config_inst.campaign.ecm).nominal
        for process_id, br in branching_ratios.items():
            sum_weights = merged_selection_stats["sum_mc_weight_per_process"][str(process_id)]
            weight_table[0, process_id] = lumi * inclusive_xsec * br / sum_weights
    else:
        for process_inst in process_insts:
            if self.config_inst.campaign.ecm not in process_inst.xsecs.keys():
                continue
            sum_weights = merged_selection_stats["sum_mc_weight_per_process"][str(process_inst.id)]
            xsec = process_inst.get_xsec(self.config_inst.campaign.ecm).nominal
            weight_table[0, process_inst.id] = lumi * xsec / sum_weights

    self.process_weight_table = weight_table
    self.xs_process_ids = set(self.process_weight_table.rows[0])


@normalization_weights.init
def normalization_weights_init(self: Producer) -> None:
    """
    Initializes the normalization weights producer by setting up the normalization weight column.
    """
    self.produces.add(self.weight_name)


stitched_normalization_weights = normalization_weights.derive(
    "stitched_normalization_weights", cls_dict={
        "weight_name": "normalization_weight",
        "get_xsecs_from_inclusive_dataset": True,
        "allow_stitching": True,
    },
)

stitched_normalization_weights_brs_from_processes = stitched_normalization_weights.derive(
    f"{stitched_normalization_weights.cls_name}_brs_from_processes", cls_dict={
        "get_xsecs_from_inclusive_dataset": False,
    },
)
