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

from columnflow.types import Sequence, Callable
from columnflow.selection import Selector, SelectionResult, selector
from columnflow.util import maybe_import, InsertableDict

np = maybe_import("numpy")
ak = maybe_import("awkward")


@selector(
    call_force=True,
)
def increment_stats(
    self: Selector,
    events: ak.Array,
    results: SelectionResult,
    stats: dict,
    weight_map: dict[str, ak.Array | tuple[ak.Array, ak.Array]] | None = None,
    group_map: dict[str, dict[str, ak.Array | Callable]] | None = None,
    group_combinations: Sequence[tuple[str]] | None = None,
    **kwargs,
) -> tuple[ak.Array, SelectionResult]:
    """
    Unexposed selector that does not actually select objects but that instead increments selection
    metrics in a given dictionary *stats* given a chunk of *events* and the corresponding selection
    *results*.

    A weight_map* can be defined to configure the actual fields to be added. The key of each entry
    should either start with ``"num``, to state that it will refer to a plain number of events, or
    ``"sum"``, to state that the field describes the sum of a specific column (usualky weights).
    Different types of values are accepted, depending on the type of "operation":

        - ``"num"``: An event mask, or an *Ellipsis* to select all events.
        - ``"sum"``: Either a column to sum over, or a 2-tuple containing the column to sum, and
                     an event mask to only sum over certain events.

    Example:

    .. code-block:: python

        # weight map definition
        weight_map = {
            # "num" operations
            "num_events": Ellipsis,  # all events
            "num_events_selected": results.event,  # selected events only
            # "sum" operations
            "sum_mc_weight": events.mc_weight,  # weights of all events
            "sum_mc_weight_selected": (events.mc_weight, results.event),  # weights of selected events
        }

        # usage within an exposed selector
        # (where results are generated, and events and stats were passed by SelectEvents)
        self[increment_stats_per_process](events, results, stats, weight_map=weight_map, **kwargs)

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

    Based on the *weight_map* in the example above, this will result in eight additional fields in
    *stats*, e.g, ``"sum_mc_weight_per_process"``, ``"sum_mc_weight_selected_per_process"``,
    ``"sum_mc_weight_per_njet"``, ``"sum_mc_weight_selected_per_njet"``, etc. (same of "num"). Each
    of these new fields will refer to a dictionary with keys corresponding to the unique values
    defined in the *group_map* above.

    In addition, combinations of groups can be configured using *group_combinations*. It accepts a
    sequence of tuples whose elements should be names of groups in *group_names*. As the name
    suggests, combinations of all possible values between groups are evaluated and stored in a
    nested dictionary.

    Example:

    .. code-block:: python

        group_combinations = [("process", "njet")]

    In this case, *stats* will obtain additional fields, such as
    ``"sum_mc_weight_per_process_and_njet"`` and ``"sum_mc_weight_selected_per_process_and_njet"``,
    referring to nested dictionaries whose structure depends on the exact order of group names per
    tuple.
    """
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

    # get and store the weights per entry in the map
    for weight_name, obj in weight_map.items():
        # check whether the weight is either a "num" or "sum" field
        if weight_name.startswith("num"):
            op = self.NUM
        elif weight_name.startswith("sum"):
            op = self.SUM
        else:
            raise Exception(
                f"weight '{weight_name}' starting with unknown operation; should either start with "
                "'num' or 'sum'",
            )

        # interpret obj based on the aoperation to be applied
        weights = None
        weight_mask = Ellipsis
        if isinstance(obj, (tuple, list)):
            if op == self.NUM:
                raise Exception(
                    f"weight map entry '{weight_name}' should refer to a mask, "
                    f"but found a sequence: {obj}",
                )
            if len(obj) == 1:
                weights = obj[0]
            elif len(obj) == 2:
                weights, weight_mask = obj
            else:
                raise Exception(f"cannot interpret as weights and optional mask: '{obj}'")
        elif op == self.NUM:
            weight_mask = obj
        else:  # SUM
            weights = obj

        # when mask is an Ellipsis, it cannot be AND joined to other masks, so convert to true mask
        if weight_mask is Ellipsis:
            weight_mask = np.ones(len(events), dtype=bool)

        # apply the operation
        if op == self.NUM:
            stats[f"{weight_name}"] += int(ak.sum(weight_mask))
        else:  # SUM
            stats[f"{weight_name}"] += float(ak.sum(weights[weight_mask]))

        # per group combination
        for group_names in group_combinations:
            group_key = f"{weight_name}_per_" + "_and_".join(group_names)

            # set the default structures
            if group_key not in stats:
                dtype = int if op == self.NUM else float
                stats[group_key] = self.defaultdicts[dtype][len(group_names)]()

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
                if op == self.NUM:
                    innermost_dict[str_values[-1]] += int(ak.sum(weight_mask & group_mask))
                else:  # SUM
                    innermost_dict[str_values[-1]] += float(ak.sum(weights[weight_mask & group_mask]))

    return events, results


@increment_stats.setup
def increment_stats_setup(
    self: Selector,
    reqs: dict,
    inputs: dict,
    reader_targets: InsertableDict,
) -> None:
    # flags to descibe "number" and "sum" fields
    self.NUM, self.SUM = range(2)

    # store nested defaultdict's with a certain maximum nesting depth
    self.defaultdicts = {
        float: {1: (lambda: defaultdict(float))},
        int: {1: (lambda: defaultdict(int))},
    }
    for i in range(2, 10 + 1):
        # use a self-executing closure to avoid reliance inside the lambda on i in the loop body
        self.defaultdicts[float][i] = (lambda i: (lambda: defaultdict(self.defaultdicts[float][i - 1])))(i)
        self.defaultdicts[int][i] = (lambda i: (lambda: defaultdict(self.defaultdicts[int][i - 1])))(i)


@selector(
    uses={increment_stats},
    produces={increment_stats},
    call_force=True,
)
def increment_event_stats(
    self: Selector,
    events: ak.Array,
    results: SelectionResult,
    stats: dict,
    **kwargs,
) -> tuple[ak.Array, SelectionResult]:
    """
    Simplified version of :py:class:`increment_stats` that only increments the number of events and
    the number of selected events.
    """
    weight_map = {
        "num_events": Ellipsis,
        "num_events_selected": results.event,
    }
    return self[increment_stats](events, results, stats, weight_map=weight_map, **kwargs)
