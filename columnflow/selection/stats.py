# coding: utf-8

"""
Selector helpers for book keeping of selection and event weight statistics for
aggregation over datasets.
"""

from __future__ import annotations

from collections import defaultdict

from columnflow.selection import Selector, SelectionResult, selector
from columnflow.util import maybe_import


np = maybe_import("numpy")
ak = maybe_import("awkward")


@selector(
    # configuration flags, the set of "used" columns is defined in the init based on these flags
    per_process=False,
    per_njet=False,
)
def increment_stats(
    self: Selector,
    events: ak.Array,
    results: SelectionResult,
    stats: dict,
    weight_map: dict[str, ak.Array | tuple[ak.Array, ak.Array]] | None = None,
    **kwargs,
) -> ak.Array:
    """
    Unexposed selector that does not actually select objects but that instead increments selection
    metrics in a given dictionary *stats* given a chunk of *events* and the corresponding selection
    *results*.

    By default, only two fields are incremented in *stats*: ``"n_events"`` and
    ``"n_events_selected"``. The latter is based on the full selection mask taken from
    ``results.main.event``. However, a *weight_map* can be defined to configure additional fields to
    be added. The key of each entry will result in a new field ``sum_<weight_key>``. Values should
    consist of 2-tuples with the actual weights to be considered and a mask to be applied to it. If
    all events should be considered, the mask can refer to an ``Ellipsis``.

    Example:

    .. code-block:: python

        # weight map definition
        weight_map = {
            "mc_weight": (events.mc_weight, Ellipsis),
            "mc_weight_selected": (events.mc_weight, results.main.event),
        }

        # usage within an exposed selector
        # (where results are generated, and events and stats were passed by SelectEvents)
        self[increment_stats_per_process](events, results, stats, weight_map)

    When *per_process* is *True*, all entries in the weight map are also evaluated per process id.
    In this case, a column ``"process_id"`` must exist in the *events* chunk.

    When *per_njet* is *True*, all entries in the weight map are also evaluated per jet
    multiplicity. In this case, the auxiliary data of the *results* object must contain a field
    *n_jets* from which the unique number of jets in the *events* chunk is extracted.

    When both *per_process* and *per_njet* are *True*, the evaluation is also done for each
    process - jet multiplicity combination.
    """
    # get the plain event mask
    event_mask = results.main.event

    # increment plain counts
    stats["n_events"] += len(events)
    stats["n_events_selected"] += ak.sum(event_mask, axis=0)

    # if required, get a list of unique jet multiplicities present in the chunk
    unique_process_ids = np.unique(events.process_id) if self.per_process else None

    # if required, get a list of unique jet multiplicity values present in the chunk
    # (this is required to be stored in the selection result as auxiliary data)
    unique_n_jets = np.unique(results.x.n_jets) if self.per_njet else None

    # get and store the weights per entry in the map
    for name, weights in weight_map.items():
        mask = Ellipsis
        if isinstance(weights, (tuple, list)) and len(weights) == 2:
            weights, mask = weights

        # when mask is an Ellipsis, it cannot be & joined to other masks, so use True instead
        joinable_mask = True if mask is Ellipsis else mask

        # sum for all processes
        stats[f"sum_{name}"] += ak.sum(weights[mask])

        # per process
        if self.per_process:
            stats.setdefault(f"sum_{name}_per_process", defaultdict(float))
            for p in unique_process_ids:
                stats[f"sum_{name}_per_process"][int(p)] += ak.sum(
                    weights[(events.process_id == p) & joinable_mask],
                )

        # per jet multiplicity
        if self.per_njet:
            stats.setdefault(f"sum_{name}_per_njet", defaultdict(float))
            for n in unique_n_jets:
                stats[f"sum_{name}_per_njet"][int(n)] += ak.sum(
                    weights[(results.x.n_jets == n) & joinable_mask],
                )

        # per process and jet multuplicity
        if self.per_process and self.per_njet:
            stats.setdefault(f"sum_{name}_per_process_and_njet", defaultdict(lambda: defaultdict(float)))
            for p in unique_process_ids:
                for n in unique_n_jets:
                    stats[f"sum_{name}_per_process_and_njet"][int(p)][int(n)] += ak.sum(
                        weights[
                            (events.process_id == p) &
                            (results.x.n_jets == n) &
                            joinable_mask
                        ],
                    )

    return events


@increment_stats.init
def increment_stats_init(self: Selector) -> None:
    """
    Custom initialization function.
    """
    if self.per_process:
        self.uses |= {"process_id"}
