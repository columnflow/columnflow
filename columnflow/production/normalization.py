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
from columnflow.util import maybe_import, DotDict
from columnflow.columnar_util import set_ak_column
from columnflow.types import Any

np = maybe_import("numpy")
sp = maybe_import("scipy")
maybe_import("scipy.sparse")
ak = maybe_import("awkward")


logger = law.logger.get_logger(__name__)


def get_inclusive_dataset(self: Producer) -> od.Dataset:
    """
    Helper function to obtain the inclusive dataset from a list of datasets that are required to stitch this
    *dataset_inst*.
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


def get_br_from_inclusive_dataset(
    self: Producer,
    inclusive_dataset: od.Dataset,
    stats: dict,
) -> dict[int, float]:
    """
    Helper function to compute the branching ratios from the inclusive sample. This is done with ratios of event weights
    isolated per dataset and thus independent of the overall mc weight normalization.
    """
    # define helper variables and mapping between process ids and dataset names
    proc_ds_map = {
        d.processes.get_first().id: d
        for d in self.config_inst.datasets
        if d.name in stats.keys()
    }
    inclusive_proc = inclusive_dataset.processes.get_first()
    N = lambda x: sn.Number(x, np.sqrt(x))  # alias for Number with counting error

    # create a dictionary "parent process id" -> {"child process id" -> "branching ratio", ...}
    # each ratio is based on gen weight sums
    child_brs: dict[int, dict[int, sn.Number]] = defaultdict(dict)
    for proc, _, child_procs in inclusive_dataset.walk_processes():
        # the process must be covered by a dataset and should not be a leaf process
        if proc.id not in proc_ds_map or proc.is_leaf_process:
            continue
        dataset_name = proc_ds_map[proc.id].name

        # get the mc weights for the "mother" dataset and add an entry for the process
        sum_mc_weight: float = stats[dataset_name]["sum_mc_weight"]
        sum_mc_weight_per_process: dict[str, float] = stats[dataset_name]["sum_mc_weight_per_process"]
        # use the number of events to compute the error on the branching ratio
        num_events: int = stats[dataset_name]["num_events"]
        num_events_per_process: dict[str, int] = stats[dataset_name]["num_events_per_process"]

        # loop over all child processes
        for child_proc in child_procs:
            # skip processes that are not covered by any dataset or irrelevant for the used dataset
            # (identified as leaf processes that have no occurrences in the stats
            # or as non-leaf processes that are not in the stitching datasets)
            is_leaf = child_proc.is_leaf_process
            if (
                (is_leaf and str(child_proc.id) not in sum_mc_weight_per_process) or
                (not is_leaf and child_proc.id not in proc_ds_map)
            ):
                continue

            # determine relevant leaf processes that will be summed over
            # (since the all stats are only derived for those)
            leaf_proc_ids = (
                [child_proc.id]
                if is_leaf or str(child_proc.id) in sum_mc_weight_per_process
                else [
                    p.id for p, _, _ in child_proc.walk_processes()
                    if str(p.id) in sum_mc_weight_per_process
                ]
            )

            # compute the br and its uncertainty using the bare number of events
            # NOTE: we assume that the uncertainty is independent of the mc weights, so we can use
            # the same relative uncertainty; this is a simplification, but should be fine for most
            # cases; we can improve this by switching from jsons to hists when storing sum of weights
            leaf_sum = lambda d: sum(d.get(str(proc_id), 0) for proc_id in leaf_proc_ids)
            br_nom = leaf_sum(sum_mc_weight_per_process) / sum_mc_weight
            br_unc = N(leaf_sum(num_events_per_process)) / N(num_events)
            child_brs[proc.id][child_proc.id] = sn.Number(
                br_nom,
                br_unc(sn.UP, unc=True, factor=True) * 1j,  # same relative uncertainty
            )

    # define actual per-process branching ratios
    branching_ratios: dict[int, float] = {}

    def multiply_branching_ratios(proc_id: int, proc_br: sn.Number) -> None:
        """
        Recursively multiply the branching ratios from the nested dictionary.
        """
        # when the br for proc_id can be created from sub processes, calculate it via product
        if proc_id in child_brs:
            for child_id, child_br in child_brs[proc_id].items():
                # multiply the branching ratios assuming no correlation
                prod_br = child_br.mul(proc_br, rho=0, inplace=False)
                multiply_branching_ratios(child_id, prod_br)
            return

        # warn the user if the relative (statistical) error is large
        rel_unc = proc_br(sn.UP, unc=True, factor=True)
        if rel_unc > 0.05:
            logger.warning(
                f"large error on the branching ratio for process {inclusive_proc.get_process(proc_id).name} with "
                f"process id {proc_id} ({rel_unc * 100:.2f}%)",
            )

        # just store the nominal value
        branching_ratios[proc_id] = proc_br.nominal

    # fill all branching ratios
    for proc_id, br in child_brs[inclusive_proc.id].items():
        multiply_branching_ratios(proc_id, br)

    return branching_ratios


@producer(
    uses={"process_id", "mc_weight"},
    # name of the output column
    weight_name="normalization_weight",
    # which luminosity to apply, uses the value stored in the config when None
    luminosity=None,
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
    Uses luminosity information of internal py:attr:`config_inst`, the cross section of a process obtained through
    :py:class:`category_ids` and the sum of event weights from the py:attr:`selection_stats` attribute to assign each
    event a normalization weight. The normalization weight is stored in a new column named after the
    py:attr:`weight_name` attribute.

    The computation of all weights requires that the selection statistics ("stats" output of :py:class:`SelectEvents`)
    contains a field ``"sum_mc_weight_per_process"`` which itself is a dictionary mapping process ids to the sum of
    event weights for that process.

    *luminosity* is used to scale the yield of the simulation. When *None*, the ``luminosity`` auxiliary field of the
    config is used.

    When py:attr`allow_stitching` is set to True, the sum of event weights is computed for all datasets with a leaf
    process contained in the leaf processes of the py:attr:`dataset_inst`. For stitching, the process_id needs to be
    reconstructed for each leaf process on a per event basis. Moreover, when stitching is enabled, an additional
    normalization weight is computed for the inclusive dataset only and stored in a column named
    `<weight_name>_inclusive_only`. This weight resembles the normalization weight for the inclusive dataset, as if it
    were unstitched and should therefore only be applied, when using the inclusive dataset as a standalone dataset.
    """
    # read the process id column
    process_id = np.asarray(events.process_id)

    # ensure all ids were assigned a cross section
    unique_process_ids = set(process_id)
    invalid_ids = unique_process_ids - self.xs_process_ids
    if invalid_ids:
        raise Exception(
            f"process_id field contains id(s) {invalid_ids} for which no cross sections were found; process ids with "
            f"cross sections: {self.xs_process_ids}",
        )

    # read the weight per process (defined as lumi * xsec / sum_weights) from the lookup table
    process_weight = np.squeeze(np.asarray(self.process_weight_table[0, process_id].todense()))

    # compute the weight and store it
    norm_weight = events.mc_weight * process_weight
    events = set_ak_column(events, self.weight_name, norm_weight, value_type=np.float32)

    # if we are stitching, we also compute the inclusive weight for debugging purposes
    if (
        self.allow_stitching and
        self.get_xsecs_from_inclusive_dataset and
        self.dataset_inst == self.inclusive_dataset
    ):
        incl_norm_weight = events.mc_weight * self.inclusive_weight
        events = set_ak_column(events, self.weight_name_incl, incl_norm_weight, value_type=np.float32)

    return events


@normalization_weights.requires
def normalization_weights_requires(
    self: Producer,
    task: law.Task,
    reqs: dict[str, DotDict[str, Any]],
    **kwargs,
) -> None:
    """
    Adds the requirements needed by the underlying py:attr:`task` to access selection stats into *reqs*.
    """
    # check that all datasets are known
    for dataset in self.stitching_datasets:
        if not self.config_inst.has_dataset(dataset):
            raise Exception(f"unknown dataset '{dataset}' required for normalization weights computation")

    from columnflow.tasks.selection import MergeSelectionStats
    reqs["selection_stats"] = {
        dataset.name: MergeSelectionStats.req_different_branching(
            task,
            dataset=dataset.name,
            branch=-1 if task.is_workflow() else 0,
        )
        for dataset in self.stitching_datasets
    }

    return reqs


@normalization_weights.setup
def normalization_weights_setup(
    self: Producer,
    task: law.Task,
    reqs: dict[str, DotDict[str, Any]],
    inputs: dict[str, Any],
    reader_targets: law.util.InsertableDict,
    **kwargs,
) -> None:
    """
    Sets up objects required by the computation of normalization weights and stores them as instance attributes:

        - py: attr: `process_weight_table`: A sparse array serving as a lookup table for the calculated process weights.
            This weight is defined as the product of the luminosity, the cross section, divided by the sum of event
            weights per process.
    """
    # load the selection stats
    selection_stats = {
        dataset: task.cached_value(
            key=f"selection_stats_{dataset}",
            func=lambda: inp["stats"].load(formatter="json"),
        )
        for dataset, inp in inputs["selection_stats"].items()
    }
    # if necessary, merge the selection stats across datasets
    if len(selection_stats) > 1:
        from columnflow.tasks.selection import MergeSelectionStats
        merged_selection_stats = defaultdict(float)
        for stats in selection_stats.values():
            MergeSelectionStats.merge_counts(merged_selection_stats, stats)
    else:
        merged_selection_stats = selection_stats[self.dataset_inst.name]

    # determine all proceses at any depth in the stitching datasets
    process_insts = {
        process_inst
        for dataset_inst in self.stitching_datasets
        for process_inst, _, _ in dataset_inst.walk_processes()
    }

    # determine ids of processes that were identified in the selection stats
    allowed_ids = set(map(int, merged_selection_stats["sum_mc_weight_per_process"]))

    # complain if there are processes seen/id'ed during selection that are not part of the datasets
    unknown_process_ids = allowed_ids - {p.id for p in process_insts}
    if unknown_process_ids:
        raise Exception(
            f"selection stats contain ids of processes that were not previously registered to the config "
            f"'{self.config_inst.name}': {', '.join(map(str, unknown_process_ids))}",
        )

    # likewise, drop processes that were not seen during selection
    process_insts = {p for p in process_insts if p.id in allowed_ids}
    max_id = max(process_inst.id for process_inst in process_insts)

    # get the luminosity
    lumi = self.config_inst.x.luminosity if self.luminosity is None else self.luminosity
    lumi = lumi.nominal if isinstance(lumi, sn.Number) else float(lumi)

    # create a event weight lookup table
    process_weight_table = sp.sparse.lil_matrix((1, max_id + 1), dtype=np.float32)
    if self.allow_stitching and self.get_xsecs_from_inclusive_dataset:
        inclusive_dataset = self.inclusive_dataset
        logger.debug(f"using inclusive dataset {inclusive_dataset.name} for cross section lookup")

        # extract branching ratios from inclusive dataset(s)
        inclusive_proc = inclusive_dataset.processes.get_first()
        if self.dataset_inst == inclusive_dataset and process_insts == {inclusive_proc}:
            branching_ratios = {inclusive_proc.id: 1.0}
        else:
            branching_ratios = self.get_br_from_inclusive_dataset(
                inclusive_dataset=inclusive_dataset,
                stats=selection_stats,
            )
            if not branching_ratios:
                raise Exception(
                    f"no branching ratios could be computed based on the inclusive dataset {inclusive_dataset}",
                )

        # compute the weight the inclusive dataset would have on its own without stitching
        inclusive_xsec = inclusive_proc.get_xsec(self.config_inst.campaign.ecm).nominal
        self.inclusive_weight = (
            lumi * inclusive_xsec / selection_stats[inclusive_dataset.name]["sum_mc_weight"]
            if self.dataset_inst == inclusive_dataset
            else 0
        )

        # fill the process weight table
        for proc_id, br in branching_ratios.items():
            sum_weights = merged_selection_stats["sum_mc_weight_per_process"][str(proc_id)]
            process_weight_table[0, proc_id] = lumi * inclusive_xsec * br / sum_weights

        # fill in cross sections of missing leaf processes
        missing_proc_ids = set(proc.id for proc in inclusive_proc.get_leaf_processes()) - set(branching_ratios.keys())
        for proc_id in missing_proc_ids:
            process_inst = inclusive_proc.get_process(proc_id)
            if (
                self.config_inst.campaign.ecm in process_inst.xsecs and
                str(proc_id) in merged_selection_stats["sum_mc_weight_per_process"]
            ):
                xsec = process_inst.get_xsec(self.config_inst.campaign.ecm).nominal
                sum_weights = merged_selection_stats["sum_mc_weight_per_process"][str(proc_id)]
                process_weight_table[0, process_inst.id] = lumi * xsec / sum_weights
                logger.warning(
                    f"added cross section for missing leaf process {process_inst.name} ({proc_id}) from xsec entry",
                )
            else:
                logger.warning(f"no cross section found for missing leaf process {process_inst.name} ({proc_id})")
    else:
        # fill the process weight table with per-process cross sections
        for process_inst in process_insts:
            if self.config_inst.campaign.ecm not in process_inst.xsecs.keys():
                raise KeyError(
                    f"no cross section registered for process {process_inst} for center-of-mass energy of "
                    f"{self.config_inst.campaign.ecm}",
                )
            sum_weights = merged_selection_stats["sum_mc_weight_per_process"][str(process_inst.id)]
            xsec = process_inst.get_xsec(self.config_inst.campaign.ecm).nominal
            process_weight_table[0, process_inst.id] = lumi * xsec / sum_weights

    self.process_weight_table = process_weight_table
    self.xs_process_ids = set(self.process_weight_table.rows[0])


@normalization_weights.init
def normalization_weights_init(self: Producer, **kwargs) -> None:
    """
    Initializes the normalization weights producer by setting up the normalization weight column.
    """
    self.produces.add(self.weight_name)
    if self.allow_stitching:
        self.stitching_datasets = self.get_stitching_datasets()
        self.inclusive_dataset = self.get_inclusive_dataset()
    else:
        self.stitching_datasets = [self.dataset_inst]

    if (
        self.allow_stitching and
        self.get_xsecs_from_inclusive_dataset and
        self.dataset_inst == self.inclusive_dataset
    ):
        self.weight_name_incl = f"{self.weight_name}_inclusive"
        self.produces.add(self.weight_name_incl)


stitched_normalization_weights = normalization_weights.derive(
    "stitched_normalization_weights",
    cls_dict={
        "weight_name": "normalization_weight",
        "get_xsecs_from_inclusive_dataset": True,
        "allow_stitching": True,
    },
)

stitched_normalization_weights_brs_from_processes = stitched_normalization_weights.derive(
    "stitched_normalization_weights_brs_from_processes",
    cls_dict={
        "get_xsecs_from_inclusive_dataset": False,
    },
)
