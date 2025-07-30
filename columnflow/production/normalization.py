# coding: utf-8

"""
Column production methods related to sample normalization event weights.
"""

from __future__ import annotations

import copy
import itertools
import dataclasses
import collections

import law
import order as od
import scinum as sn

from columnflow.production import Producer, producer
from columnflow.util import maybe_import, DotDict
from columnflow.columnar_util import set_ak_column
from columnflow.types import Any, Sequence

np = maybe_import("numpy")
sp = maybe_import("scipy")
maybe_import("scipy.sparse")
ak = maybe_import("awkward")


logger = law.logger.get_logger(__name__)


def get_stitching_datasets(self: Producer) -> tuple[od.Dataset, list[od.Dataset]]:
    """
    Helper function to obtain information about stitching datasets:

        - the inclusive dataset, which is the dataset that contains all processes
        - all datasets that are required to stitch this *dataset_inst*
    """
    # first collect all datasets that are needed to stitch the current dataset
    required_datasets = {
        d
        for d in self.config_inst.datasets
        if (
            d.has_process(self.dataset_inst.processes.get_first(), deep=True) or
            self.dataset_inst.has_process(d.processes.get_first(), deep=True)
        )
    }

    # determine the inclusive dataset
    process_map = {d.processes.get_first(): d for d in required_datasets}
    process_inst = self.dataset_inst.processes.get_first()
    inclusive_dataset = None
    while process_inst:
        if process_inst in process_map:
            inclusive_dataset = process_map[process_inst]
        process_inst = process_inst.parent_processes.get_first(default=None)
    if not inclusive_dataset:
        raise Exception("inclusive dataset not found")
    # cross-check if there are processes in the required datasets that are not covered by the inclusive dataset
    unmatched_processes = {p for p in process_map if not inclusive_dataset.has_process(p, deep=True)}
    if unmatched_processes:
        raise Exception(f"processes {unmatched_processes} not found in inclusive dataset")

    return inclusive_dataset, list(required_datasets)


def get_br_from_inclusive_datasets(
    self: Producer,
    process_insts: Sequence[od.Process] | set[od.Process],
    dataset_selection_stats: dict[str, dict[str, float | dict[str, float]]],
    merged_selection_stats: dict[str, float | dict[str, float]],
) -> dict[od.Process, float]:
    """
    Helper function to compute the branching ratios from sum of weights of inclusive samples.
    """
    # step 1: per desired process, collect datasets that contain them
    process_datasets = collections.defaultdict(set)
    for process_inst in process_insts:
        for dataset_name, dstats in dataset_selection_stats.items():
            if str(process_inst.id) in dstats["sum_mc_weight_per_process"]:
                process_datasets[process_inst].add(self.config_inst.get_dataset(dataset_name))

    # step 2: per dataset, collect all "lowest level" processes that are contained in them
    dataset_processes = collections.defaultdict(set)
    for dataset_name in dataset_selection_stats:
        dataset_inst = self.config_inst.get_dataset(dataset_name)
        dataset_process_inst = dataset_inst.processes.get_first()
        for process_inst in process_insts:
            if process_inst == dataset_process_inst or dataset_process_inst.has_process(process_inst, deep=True):
                dataset_processes[dataset_inst].add(process_inst)

    # step 3: per process, structure the assigned datasets and corresponding processes in DAGs, from more inclusive down
    #         to more exclusive phase spaces; there should usually just be a single DAG per process, but in case of
    #         complex overlap between datasets, there might be multiple options across which BRs can be computed; this
    #         is resolved in step 4
    @dataclasses.dataclass
    class Node:
        process_inst: od.Process
        dataset_inst: od.Dataset | None = None
        next: set[Node] = dataclasses.field(default_factory=set)

        def __hash__(self):
            return hash((self.process_inst, self.dataset_inst))

    process_dags = {}
    for process_inst, dataset_insts in process_datasets.items():
        # first, per dataset, remember all sub (more exclusive) datasets
        # (the O(n^2) is not necessarily optimal, but we are dealing with very small numbers here, thus acceptable)
        sub_datasets = {}
        for d_incl, d_excl in itertools.permutations(dataset_insts, 2):
            if d_incl.processes.get_first().has_process(d_excl.processes.get_first(), deep=True):
                sub_datasets.setdefault(d_incl, set()).add(d_excl)
        # then, reduce to pairs describing a "maximum spanning tree" (via transitive edge pruning)
        pruned_relations = []
        for d_incl, d_excls in sub_datasets.items():
            for d_excl in d_excls:
                queue = collections.deque([d_incl])
                visited = set([d_incl])
                found = False
                while queue and not found:
                    _d_incl = queue.popleft()
                    for _d_excl in sub_datasets.get(_d_incl, []):
                        if _d_excl == d_excl:
                            # skip the direct edge
                            if _d_incl == d_incl:
                                continue
                            # otherwise declare found
                            found = True
                            break
                        if _d_excl not in visited:
                            visited.add(_d_excl)
                            queue.append(_d_excl)
                if not found:
                    pruned_relations.append((d_incl, d_excl))
        # finally, expand to a DAG structure
        nodes = {}
        excl_nodes = set()
        for d_incl, d_excl in pruned_relations:
            if d_incl not in nodes:
                nodes[d_incl] = Node(d_incl.processes.get_first(), d_incl)
            if d_excl not in nodes:
                nodes[d_excl] = Node(d_excl.processes.get_first(), d_excl)
            nodes[d_incl].next.add(nodes[d_excl])
            excl_nodes.add(nodes[d_excl])
        # mark the root node as the head of the DAG
        dag = (set(nodes.values()) - excl_nodes).pop()
        # add another node to leaves that only contains the process instance
        for node in excl_nodes:
            if node.next or node.process_inst == process_inst:
                continue
            if process_inst not in nodes:
                nodes[process_inst] = Node(process_inst)
            node.next.add(nodes[process_inst])
        process_dags[process_inst] = dag

    # step 4: per process, compute the branching ratio for each possible path in the DAG, while keeping track of the
    #         statistical precision of each combination, evaluated based on the raw number of events; then pick the
    #         most precise path; again, there should usually be just a single path, but multiple ones are possible when
    #         datasets have complex overlap
    def get_single_br(dataset_inst: od.Dataset, process_inst: od.Process) -> sn.Number | None:
        # process_inst might refer to a mid-layer process, so check which low-layer processes it is made of
        lowest_process_ids = (
            [process_inst.id]
            if process_inst in process_insts
            else [
                low_process_inst.id
                for low_process_inst in process_insts
                if process_inst.has_process(low_process_inst, deep=True)
            ]
        )
        # extract stats
        process_sum_weights = sum(
            dataset_selection_stats[dataset_inst.name]["sum_mc_weight_per_process"].get(str(process_id), 0.0)
            for process_id in lowest_process_ids
        )
        dataset_sum_weights = sum(dataset_selection_stats[dataset_inst.name]["sum_mc_weight_per_process"].values())
        process_num_events = sum(
            dataset_selection_stats[dataset_inst.name]["num_events_per_process"].get(str(process_id), 0.0)
            for process_id in lowest_process_ids
        )
        dataset_num_events = sum(dataset_selection_stats[dataset_inst.name]["num_events_per_process"].values())
        # when there are no events, return None
        if process_num_events == 0:
            logger.warning(
                f"found no events for process '{process_inst.name}' ({process_inst.id}) with subprocess ids "
                f"'{','.join(map(str, lowest_process_ids))}' in selection stats of dataset {dataset_inst.name}",
            )
            return None
        # compute the ratio of events, assuming uncorrelated poisson counting errors, then get its relative error
        num_ratio = (
            sn.Number(process_num_events, {"nom": process_num_events**0.5}) /
            sn.Number(dataset_num_events, {"denom": dataset_num_events**0.5})
        ).combine_uncertainties()
        rel_unc = num_ratio(sn.UP, unc=True, factor=True)
        # compute the branching ratio, using the same relative uncertainty
        br = sn.Number(process_sum_weights / dataset_sum_weights, rel_unc * 1j)
        return br

    process_brs = {}
    for process_inst, dag in process_dags.items():
        brs = []
        queue = collections.deque([(dag, (br := sn.Number(1.0, 0.0)), (br,), (dag,))])
        while queue:
            node, br, br_path, dag_path = queue.popleft()
            if not node.next:
                brs.append((br, br_path, dag_path))
                continue
            for sub_node in node.next:
                sub_br = get_single_br(node.dataset_inst, sub_node.process_inst)
                if sub_br is not None:
                    queue.append((sub_node, br * sub_br, br_path + (sub_br,), dag_path + (sub_node,)))
        # select the most certain one
        best_br, best_br_path, best_dag_path = min(brs, key=lambda tpl: tpl[0](sn.UP, unc=True, factor=True))
        process_brs[process_inst] = best_br.nominal
        # show a warning in case the relative uncertainty is large
        if (rel_unc := best_br(sn.UP, unc=True, factor=True)) > 0.1:
            path_str = "\n-> ".join(
                f"{(node.dataset_inst or node.process_inst).name} (br = {br.str(format=3)})"
                for br, node in zip(best_br_path, best_dag_path)
            )
            logger.warning(
                f"large error on the branching ratio of {rel_unc * 100:.2f}% for process '{process_inst.name}' "
                f"({process_inst.id}), calculated along\n   {path_str}",
            )

    return process_brs


def update_dataset_selection_stats(
    self: Producer,
    dataset_selection_stats: dict[str, dict[str, float | dict[str, float]]],
) -> dict[str, dict[str, float | dict[str, float]]]:
    """
    Hook to optionally update the per-dataset selection stats.
    """
    return dataset_selection_stats


@producer(
    uses={"process_id", "mc_weight"},
    # name of the output column
    weight_name="normalization_weight",
    # which luminosity to apply, uses the value stored in the config when None
    luminosity=None,
    # whether to allow stitching datasets
    allow_stitching=False,
    get_xsecs_from_inclusive_datasets=False,
    get_stitching_datasets=get_stitching_datasets,
    get_br_from_inclusive_datasets=get_br_from_inclusive_datasets,
    update_dataset_selection_stats=update_dataset_selection_stats,
    update_dataset_selection_stats_br=None,
    update_dataset_selection_stats_sum_weights=None,
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
    unique_process_ids = set(np.unique(process_id))
    invalid_ids = unique_process_ids - self.known_process_ids
    if invalid_ids:
        invalid_names = [
            f"{self.config_inst.get_process(proc_id).name} ({proc_id})"
            for proc_id in invalid_ids
        ]
        raise Exception(
            f"process_id field contains entries {', '.join(invalid_names)} for which no cross sections were found; "
            f"process ids with cross sections: {self.known_process_ids}",
        )

    # read the weight per process (defined as lumi * xsec / sum_weights) from the lookup table
    process_weight = np.squeeze(np.asarray(self.process_weight_table[process_id, 0].todense()))

    # compute the weight and store it
    norm_weight = events.mc_weight * process_weight
    events = set_ak_column(events, self.weight_name, norm_weight, value_type=np.float32)

    # when stitching, also compute the inclusive-only weight
    if self.allow_stitching and self.dataset_inst == self.inclusive_dataset:
        incl_norm_weight = events.mc_weight * self.inclusive_weight
        events = set_ak_column(events, self.weight_name_incl, incl_norm_weight, value_type=np.float32)

    return events


@normalization_weights.init
def normalization_weights_init(self: Producer, **kwargs) -> None:
    """
    Initializes the normalization weights producer by setting up the normalization weight column.
    """
    # declare the weight name to be a produced column
    self.produces.add(self.weight_name)

    # when stitching is enabled, store specific information
    if self.allow_stitching:
        # remember the inclusive dataset and all datasets needed to determine the weights of processes in _this_ dataset
        self.inclusive_dataset, self.required_datasets = self.get_stitching_datasets()

        # potentially also store the weight needed for only using the inclusive dataset
        if self.dataset_inst == self.inclusive_dataset:
            self.weight_name_incl = f"{self.weight_name}_inclusive"
            self.produces.add(self.weight_name_incl)
    else:
        self.required_datasets = [self.dataset_inst]


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
    for dataset in self.required_datasets:
        if not self.config_inst.has_dataset(dataset):
            raise Exception(f"unknown dataset '{dataset}' required for normalization weights computation")

    from columnflow.tasks.selection import MergeSelectionStats
    reqs["selection_stats"] = {
        dataset.name: MergeSelectionStats.req_different_branching(
            task,
            dataset=dataset.name,
            branch=-1 if task.is_workflow() else 0,
        )
        for dataset in self.required_datasets
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
        - py: attr: `known_process_ids`: A set of all process ids that are known by the lookup table.
    """
    # load the selection stats
    dataset_selection_stats = {
        dataset: task.cached_value(
            key=f"selection_stats_{dataset}",
            func=lambda: inp["stats"].load(formatter="json"),
        )
        for dataset, inp in inputs["selection_stats"].items()
    }

    # separately treat stats for extracting BRs and sum of mc weights
    def extract_stats(*update_funcs):
        # create copy
        stats = copy.deepcopy(dataset_selection_stats)
        # update through one of the functions
        for update_func in update_funcs:
            if callable(update_func):
                stats = update_func(stats)
            break
        # merge
        if len(stats) > 1:
            from columnflow.tasks.selection import MergeSelectionStats
            merged_stats = collections.defaultdict(float)
            for _stats in stats.values():
                MergeSelectionStats.merge_counts(merged_stats, _stats)
        else:
            merged_stats = stats[self.dataset_inst.name]
        return stats, merged_stats

    dataset_selection_stats_br, merged_selection_stats_br = extract_stats(
        self.update_dataset_selection_stats_br,
        self.update_dataset_selection_stats,
    )
    _, merged_selection_stats_sum_weights = extract_stats(
        self.update_dataset_selection_stats_sum_weights,
        self.update_dataset_selection_stats,
    )

    # get all process ids and instances seen and assigned during selection of this dataset
    # (i.e., all possible processes that might be encountered during event processing)
    process_ids = set(map(int, dataset_selection_stats_br[self.dataset_inst.name]["sum_mc_weight_per_process"]))
    process_insts = set(map(self.config_inst.get_process, process_ids))

    # consistency check: when the main process of the current dataset is part of these "lowest level" processes,
    # there should only be this single process, otherwise the manual (sub) process assignment does not match the
    # general dataset -> main process info
    if self.dataset_inst.processes.get_first() in process_insts and len(process_insts) > 1:
        raise Exception(
            f"dataset '{self.dataset_inst.name}' has main process '{self.dataset_inst.processes.get_first().name}' "
            "assigned to it (likely as per cmsdb), but the dataset selection stats for this dataset contain multiple "
            "sub processes, which is likely a misconfiguration of the manual sub process assignment upstream; found "
            f"sub processes: {', '.join(f'{process_inst.name} ({process_inst.id})' for process_inst in process_insts)}",
        )

    # setup the event weight lookup table
    process_weight_table = sp.sparse.lil_matrix((max(process_ids) + 1, 1), dtype=np.float32)

    # get the luminosity
    lumi = float(self.config_inst.x.luminosity if self.luminosity is None else self.luminosity)

    # prepare info for the inclusive dataset
    inclusive_proc = self.inclusive_dataset.processes.get_first()
    inclusive_xsec = inclusive_proc.get_xsec(self.config_inst.campaign.ecm).nominal

    # compute the weight the inclusive dataset would have on its own without stitching
    if self.allow_stitching and self.dataset_inst == self.inclusive_dataset:
        inclusive_sum_weights = sum(
            dataset_selection_stats[self.inclusive_dataset.name]["sum_mc_weight_per_process"].values(),
        )
        self.inclusive_weight = inclusive_xsec * lumi / inclusive_sum_weights

    # fill weights into the lut, depending on whether stitching is allowed / needed or not
    do_stitch = (
        self.allow_stitching and
        self.get_xsecs_from_inclusive_datasets and
        len(self.required_datasets) > 1
    )
    if do_stitch:
        logger.debug(
            f"using inclusive dataset '{self.inclusive_dataset.name}' and process '{inclusive_proc.name}' for cross "
            "section lookup",
        )

        # extract branching ratios
        branching_ratios = self.get_br_from_inclusive_datasets(
            process_insts,
            dataset_selection_stats_br,
            merged_selection_stats_br,
        )

        # fill the process weight table
        for process_inst, br in branching_ratios.items():
            sum_weights = merged_selection_stats_sum_weights["sum_mc_weight_per_process"][str(process_inst.id)]
            process_weight_table[process_inst.id, 0] = br * inclusive_xsec * lumi / sum_weights
    else:
        # fill the process weight table with per-process cross sections
        for process_inst in process_insts:
            if self.config_inst.campaign.ecm not in process_inst.xsecs:
                raise KeyError(
                    f"no cross section registered for process {process_inst} for center-of-mass energy of "
                    f"{self.config_inst.campaign.ecm}",
                )
            xsec = process_inst.get_xsec(self.config_inst.campaign.ecm).nominal
            sum_weights = merged_selection_stats_sum_weights["sum_mc_weight_per_process"][str(process_inst.id)]
            process_weight_table[process_inst.id, 0] = xsec * lumi / sum_weights

    # store lookup table and known process ids
    self.process_weight_table = process_weight_table
    self.known_process_ids = process_ids


stitched_normalization_weights = normalization_weights.derive(
    "stitched_normalization_weights",
    cls_dict={
        "weight_name": "normalization_weight",
        "get_xsecs_from_inclusive_datasets": True,
        "allow_stitching": True,
    },
)

stitched_normalization_weights_brs_from_processes = stitched_normalization_weights.derive(
    "stitched_normalization_weights_brs_from_processes",
    cls_dict={
        "get_xsecs_from_inclusive_datasets": False,
    },
)
