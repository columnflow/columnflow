# coding: utf-8

"""
Empty selectors that still produce the minimal set of columns potentially required in downstream
tasks.
"""

from collections import defaultdict

from columnflow.selection import Selector, SelectionResult, selector
from columnflow.selection.stats import increment_stats
from columnflow.production.processes import process_ids
from columnflow.production.cms.mc_weight import mc_weight
from columnflow.columnar_util import set_ak_column
from columnflow.util import maybe_import

np = maybe_import("numpy")
ak = maybe_import("awkward")


@selector(
    uses={
        process_ids, mc_weight, increment_stats,
    },
    produces={
        process_ids, mc_weight, "category_ids",
    },
    exposed=True,
)
def empty(
    self: Selector,
    events: ak.Array,
    stats: defaultdict,
    **kwargs,
) -> tuple[ak.Array, SelectionResult]:
    """
    Empty selector that only writes a minimal set of columns that are potentially required in
    downstream tasks, such as cutflow and plotting related tasks.

    :param events: The input events.
    :param stats: The statistics dictionary.
    :param **kwargs: Additional keyword arguments that are passed to all other
        :py:class:`TaskArrayFunction`'s.

    :returns: A tuple containing the original events and a SelectionResult object with a trivial
        event mask.
    """
    # create process ids
    events = self[process_ids](events, **kwargs)

    # add corrected mc weights
    if self.dataset_inst.is_mc:
        events = self[mc_weight](events, **kwargs)

    # inclusive category id
    category_ids = (np.ones(len(events), dtype=np.int64) * self.inclusive_category_id)[..., None]
    events = set_ak_column(events, "category_ids", category_ids)

    # empty selection result with a trivial event mask
    results = SelectionResult(event=ak.Array(np.ones(len(events), dtype=np.bool_)))

    # increment stats
    weight_map = {
        "num_events": Ellipsis,
        "num_events_selected": Ellipsis,
    }
    if self.dataset_inst.is_mc:
        weight_map["sum_mc_weight"] = events.mc_weight
        weight_map["sum_mc_weight_selected"] = (events.mc_weight, Ellipsis)
    group_map = {
        # per process
        "process": {
            "values": events.process_id,
            "mask_fn": (lambda v: events.process_id == v),
        },
    }
    events, _ = self[increment_stats](
        events,
        results,
        stats,
        weight_map=weight_map,
        group_map=group_map,
        **kwargs,
    )

    return events, results


@empty.init
def empty_init(self: Selector) -> None:
    """
    Initializes the selector by finding the id of the inclusive category.

    :raises ValueError: If the inclusive category cannot be found.
    """
    # find the id of the inclusive category
    if "incl" in self.config_inst.categories:
        self.inclusive_category_id = self.config_inst.categories.n.incl.id
    elif 1 in self.config_inst.categories:
        self.inclusive_category_id = 1
    else:
        raise ValueError(f"could not find inclusive category for {self.cls_name} selector")
