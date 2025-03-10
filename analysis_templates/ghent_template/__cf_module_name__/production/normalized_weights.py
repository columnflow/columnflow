# coding: utf-8

"""
Column production methods related to generic event weights.
"""

from typing import Iterable, Callable, Collection

import law

from columnflow.production import Producer, producer
from columnflow.util import maybe_import, safe_div, InsertableDict
from columnflow.columnar_util import set_ak_column

ak = maybe_import("awkward")
np = maybe_import("numpy")


logger = law.logger.get_logger(__name__)


def normalized_weight_factory(
    producer_name: str,
    weight_producers: Iterable[Producer],
    add_uses: Collection = tuple(),
    **kwargs,
) -> Callable:

    @producer(
        uses={"process_id"}.union(weight_producers, *[w.produces for w in weight_producers], add_uses),
        cls_name=producer_name,
        mc_only=True,
        # skip the checking existence of used/produced columns because not all columns are there
        check_used_columns=False,
        check_produced_columns=False,
        # remaining produced columns are defined in the init function below
    )
    def normalized_weight(self: Producer, events: ak.Array, **kwargs) -> ak.Array:

        # check existence of requested weights to normalize and run producer if missing
        missing_weights = self.weight_names.difference(events.fields)

        if missing_weights:
            # try to produce missing weights
            for prod in self.weight_producers:
                if (
                        self[prod].produced_columns.difference(events.fields) and
                        self[prod].used_columns.intersection(events.fields)
                ):
                    logger.info(f"Rerun producer {self[prod].cls_name}")
                    events = self[prod](events, **kwargs)

        # Create normalized weight columns if possible
        if not_reproduced := missing_weights.difference(events.fields):
            logger.info(f"Weight columns {not_reproduced} could not be reproduced")

        for weight_name in self.weight_names.intersection(events.fields):
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
            events = set_ak_column(events, f"normalized_{weight_name}", norm_weight_per_pid)

        return events

    @normalized_weight.init
    def normalized_weight_init(self: Producer) -> None:
        self.weight_producers = weight_producers

        # resolve weight names
        self.weight_names = set()
        for col in self.used_columns:
            col = col.string_nano_column
            if "weight" in col and "normalized" not in col:
                self.weight_names.add(col)

        self.produces |= set(f"normalized_{weight_name}" for weight_name in self.weight_names)

    @normalized_weight.requires
    def normalized_weight_requires(self: Producer, reqs: dict) -> None:
        from columnflow.tasks.selection import MergeSelectionStats
        reqs["selection_stats"] = MergeSelectionStats.req(
            self.task,
            branch=-1,
        )

    @normalized_weight.setup
    def normalized_weight_setup(self: Producer, reqs: dict, inputs: dict, reader_targets: InsertableDict) -> None:
        # load the selection stats
        stats = inputs["selection_stats"]["collection"][0]["stats"].load(formatter="json")

        # get the unique process ids in that dataset
        key = "sum_mc_weight_per_process"
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
            for weight_name in self.weight_names
        }

    return normalized_weight
