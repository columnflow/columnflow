# coding: utf-8

"""
Column production methods related to generic event weights.
"""

from columnflow.production import Producer, producer
from columnflow.production.pileup import pu_weight
from columnflow.util import maybe_import, safe_div
from columnflow.columnar_util import set_ak_column


ak = maybe_import("awkward")
np = maybe_import("numpy")


@producer(
    uses={
        pu_weight.PRODUCES,
        # custom columns created upstream, probably by a producer
        "process_id",
    },
    # produced columns are defined in the init function below
)
def normalized_pu_weight(self: Producer, events: ak.Array, **kwargs) -> ak.Array:
    # fail when running on data
    if self.dataset_inst.is_data:
        raise ValueError("attempt to compute normalized pileup weights in data")

    for weight_name in self[pu_weight].produces:
        if not weight_name.startswith("pu_weight"):
            continue

        # create a weight vector starting with ones
        norm_weight_per_pid = np.ones(len(events), dtype=np.float32)

        # fill weights with a new mask per unique process id (mostly just one)
        for pid in self.unique_process_ids:
            pid_mask = events.process_id == pid
            norm_weight_per_pid[pid_mask] = self.ratio_per_pid[weight_name][pid]

        # multiply with actual weight
        norm_weight_per_pid = norm_weight_per_pid * events[weight_name]

        # store it
        norm_weight_per_pid = ak.values_astype(norm_weight_per_pid, np.float32)
        events = set_ak_column(events, f"normalized_{weight_name}", norm_weight_per_pid, value_type=np.float32)

    return events


@normalized_pu_weight.init
def normalized_pu_weight_init(self: Producer) -> None:
    self.produces |= {
        f"normalized_{weight_name}"
        for weight_name in self[pu_weight].produces
        if weight_name.startswith("pu_weight")
    }


@normalized_pu_weight.requires
def normalized_pu_weight_requires(self: Producer, reqs: dict) -> None:
    from columnflow.tasks.selection import MergeSelectionStats
    reqs["selection_stats"] = MergeSelectionStats.req(
        self.task,
        tree_index=0,
        branch=-1,
        _exclude=MergeSelectionStats.exclude_params_forest_merge,
    )


@normalized_pu_weight.setup
def normalized_pu_weight_setup(self: Producer, reqs: dict, inputs: dict) -> None:
    # load the selection stats
    stats = inputs["selection_stats"]["collection"][0].load(formatter="json")

    # get the unique process ids in that dataset
    key = "sum_mc_weight_pu_weight_per_process"
    self.unique_process_ids = list(map(int, stats[key].keys()))

    # helper to get numerators and denominators
    def numerator_per_pid(pid):
        key = "sum_mc_weight_per_process"
        return stats[key].get(str(pid), 0.0)

    def denominator_per_pid(weight_name, pid):
        key = f"sum_mc_weight_{weight_name}_per_process"
        return stats[key].get(str(pid), 0.0)

    # extract the ratio per weight and pid
    self.ratio_per_pid = {
        weight_name: {
            pid: safe_div(numerator_per_pid(pid), denominator_per_pid(weight_name, pid))
            for pid in self.unique_process_ids
        }
        for weight_name in self[pu_weight].produces
        if weight_name.startswith("pu_weight")
    }


@producer(
    uses={
        "pdf_weight", "pdf_weight_up", "pdf_weight_down",
    },
    produces={
        "normalized_pdf_weight", "normalized_pdf_weight_up", "normalized_pdf_weight_down",
    },
)
def normalized_pdf_weight(self: Producer, events: ak.Array, **kwargs) -> ak.Array:
    # fail when running on data
    if self.dataset_inst.is_data:
        raise ValueError("attempt to compute normalized pdf weights in data")

    for postfix in ["", "_up", "_down"]:
        # create the normalized weight
        avg = self.average_pdf_weights[postfix]
        normalized_weight = events[f"pdf_weight{postfix}"] / avg

        # store it
        events = set_ak_column(events, f"normalized_pdf_weight{postfix}", normalized_weight, value_type=np.float32)

    return events


@normalized_pdf_weight.requires
def normalized_pdf_weight_requires(self: Producer, reqs: dict) -> None:
    from columnflow.tasks.selection import MergeSelectionStats
    reqs["selection_stats"] = MergeSelectionStats.req(
        self.task,
        tree_index=0,
        branch=-1,
        _exclude=MergeSelectionStats.exclude_params_forest_merge,
    )


@normalized_pdf_weight.setup
def normalized_pdf_weight_setup(self: Producer, reqs: dict, inputs: dict) -> None:
    # load the selection stats
    stats = inputs["selection_stats"]["collection"][0].load(formatter="json")

    # save average weights
    self.average_pdf_weights = {
        postfix: safe_div(stats[f"sum_pdf_weight{postfix}"], stats["n_events"])
        for postfix in ["", "_up", "_down"]
    }


@producer(
    uses={
        "murmuf_weight", "murmuf_weight_up", "murmuf_weight_down",
    },
    produces={
        "normalized_murmuf_weight", "normalized_murmuf_weight_up", "normalized_murmuf_weight_down",
    },
)
def normalized_murmuf_weight(self: Producer, events: ak.Array, **kwargs) -> ak.Array:
    # fail when running on data
    if self.dataset_inst.is_data:
        raise ValueError("attempt to compute normalized mur/muf weights in data")

    for postfix in ["", "_up", "_down"]:
        # create the normalized weight
        avg = self.average_murmuf_weights[postfix]
        normalized_weight = events[f"murmuf_weight{postfix}"] / avg

        # store it
        events = set_ak_column(events, f"normalized_murmuf_weight{postfix}", normalized_weight, value_type=np.float32)

    return events


@normalized_murmuf_weight.requires
def normalized_murmuf_weight_requires(self: Producer, reqs: dict) -> None:
    from columnflow.tasks.selection import MergeSelectionStats
    reqs["selection_stats"] = MergeSelectionStats.req(
        self.task,
        tree_index=0,
        branch=-1,
        _exclude=MergeSelectionStats.exclude_params_forest_merge,
    )


@normalized_murmuf_weight.setup
def normalized_murmuf_weight_setup(self: Producer, reqs: dict, inputs: dict) -> None:
    # load the selection stats
    stats = inputs["selection_stats"]["collection"][0].load(formatter="json")

    # save average weights
    self.average_murmuf_weights = {
        postfix: safe_div(stats[f"sum_murmuf_weight{postfix}"], stats["n_events"])
        for postfix in ["", "_up", "_down"]
    }
