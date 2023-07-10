# coding: utf-8

"""
Selector helpers for book keeping of selection and event weight statistics for
aggregation over datasets.
"""

from __future__ import annotations

import itertools
from functools import reduce
from collections import defaultdict
from operator import and_, getitem as getitem_
from typing import Sequence, Callable

from columnflow.selection import Selector, SelectionResult, selector
from columnflow.util import maybe_import


np = maybe_import("numpy")
ak = maybe_import("awkward")


@selector()
def increment_stats(
    self: Selector,
    events: ak.Array,
    results: SelectionResult,
    stats: dict,
    event_mask: ak.Array | None = None,
    weight_map: dict[str, ak.Array | tuple[ak.Array, ak.Array]] | None = None,
    group_map: dict[str, dict[str, ak.Array | Callable]] | None = None,
    group_combinations: Sequence[tuple[str]] | None = None,
    **kwargs,
) -> ak.Array:
    """
    Unexposed selector that does not actually select objects but that instead increments selection
    metrics in a given dictionary *stats* given a chunk of *events* and the corresponding selection
    *results*.

    By default, only two fields are incremented in *stats*: ``"n_events"`` and
    ``"n_events_selected"``. The latter is based on the full selection mask taken from *event_mask*
    that defaults to ``results.main.event``. However, a *weight_map* can be defined to configure
    additional fields to be added. The key of each entry will result in a new field
    ``sum_<weight_key>``. Values should consist of 2-tuples with the actual weights to be considered
    and a mask to be applied to it. If all events should be considered, the mask can refer to an
    ``Ellipsis``.

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

    Each sum of weights can also be extracted for each unique element in a so-called group, such as
    per process id, or per jet multiplicity bin. For this purpose, a *group_map* can be defined,
    mapping the name of a group (e.g. ``"process"`` or ``"njet"``) to a dictionary with the fields

        - ``"values"``, unique values to loop over,
        - ``"mask_fn"``, a function that is supposed to return a mask given a single value, and
        - ``"combinations_only"`` (optional), a boolean flag (*False* by default) that decides
            whether this group is not to be evaluated on its own, but only as part of a combination
            with other groups (see below).

    Example:

    .. code-block:: python

        group_map = {
            "process": {
                "values": events.process_id,
                "mask_fn": (lambda v: events.process_id == v),
            },
            "njet": {
                "values": results.x.n_jets,
                "mask_fn": (lambda v: results.x.n_jets == v),
            },
        }

    Based on the *weight_map* in the example above, this will result in four additional fields in
    *stats*, i.e., ``"sum_mc_weight_per_process"``, ``"sum_mc_weight_selected_per_process"``,
    ``"sum_mc_weight_per_njet"``, ``"sum_mc_weight_selected_per_njet"``. Each of these new fields
    will refer to a dictionary with keys corresponding to the unique values defined in the
    *group_map* above.

    In addition, combinations of groups can be configured using *group_combinations*. It accepts a
    sequence of tuples whose elements should be names of groups in *group_names*. As the name
    suggests, combinations of all possible values between groups are evaluated and stored in a
    nested dictionary.

    Example:

    .. code-block:: python

        group_combinations = [("process", "njet")]

    In this case, *stats* will obtain additional fields ``"sum_mc_weight_per_process_and_njet"``
    and ``"sum_mc_weight_selected_per_process_and_njet"``, referring to nested dictionaries whose
    structure depends on the exact order of group names per tuple.
    """
    # when no event mask is given, fallback to the main results object
    if event_mask is None:
        event_mask = results.main.event

    # increment plain counts
    stats.setdefault("n_events", 0)
    stats.setdefault("n_events_selected", 0)
    stats["n_events"] += len(events)
    stats["n_events_selected"] += int(ak.sum(event_mask, axis=0))

    # make values in group map unique
    unique_group_values = {
        group_name: np.unique(ak.flatten(group_data["values"], axis=None))
        for group_name, group_data in group_map.items()
    }

    # treat groups as combinations of a single group
    group_combinations = list(group_combinations or [])
    for group_name, group_data in list(group_map.items())[::-1]:
        if group_data.get("combinations_only", False) or (group_name,) in group_combinations:
            continue
        group_combinations.insert(0, (group_name,))

    # prepare default types (nested defaultdict's) down to a certain depth
    group_defaults = {
        1: (lambda: defaultdict(float)),
    }
    for i in range(2, max([len(group_names) for group_names in group_combinations] + [0]) + 1):
        # use a self-executing closure to avoid reliance inside the lambda on i in the loop body
        group_defaults[i] = (lambda i: (lambda: defaultdict(group_defaults[i - 1])))(i)

    # get and store the weights per entry in the map
    for name, weights in weight_map.items():
        mask = Ellipsis
        if isinstance(weights, (tuple, list)):
            if len(weights) == 1:
                weights = weights[0]
            elif len(weights) == 2:
                weights, mask = weights
            else:
                raise Exception(f"cannot interpret as weights and optional mask: '{weights}'")

        # when mask is an Ellipsis, it cannot be & joined to other masks, so use True instead
        joinable_mask = True if mask is Ellipsis else mask

        # sum for all groups, with and without the seleciton
        stats[f"sum_{name}"] += float(ak.sum(weights[mask]))
        stats[f"sum_{name}_selected"] += float(ak.sum(weights[event_mask & joinable_mask]))

        # per group combination
        for group_names in group_combinations:
            group_key = f"sum_{name}_per_" + "_and_".join(group_names)
            group_key_sel = f"sum_{name}_selected_per_" + "_and_".join(group_names)

            # set the default structures
            if group_key not in stats:
                stats[group_key] = group_defaults[len(group_names)]()
            if group_key_sel not in stats:
                stats[group_key_sel] = group_defaults[len(group_names)]()

            # set values
            for values in itertools.product(*(unique_group_values[g] for g in group_names)):
                # evaluate and join the masks
                group_mask = reduce(
                    and_,
                    (group_map[g]["mask_fn"](v) for g, v in zip(group_names, values)),
                )
                # find the innermost dict to perform the in-place item assignment, then increment
                str_values = list(map(str, values))
                innermost_dict = reduce(getitem_, [stats[group_key]] + str_values[:-1])
                innermost_dict[str_values[-1]] += float(ak.sum(
                    weights[group_mask & joinable_mask],
                ))
                innermost_dict_sel = reduce(getitem_, [stats[group_key_sel]] + str_values[:-1])
                innermost_dict_sel[str_values[-1]] += float(ak.sum(
                    weights[event_mask & group_mask & joinable_mask],
                ))

    return events
