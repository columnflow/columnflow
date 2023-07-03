# coding: utf-8

"""
Column production methods related defining categories.
"""

from __future__ import annotations

from collections import defaultdict

import law

from columnflow.selection import Selector
from columnflow.production import Producer, producer
from columnflow.util import maybe_import
from columnflow.columnar_util import set_ak_column


np = maybe_import("numpy")
ak = maybe_import("awkward")

logger = law.logger.get_logger(__name__)


@producer(produces={"category_ids"})
def category_ids(
    self: Producer,
    events: ak.Array,
    target_events: ak.Array | None = None,
    **kwargs,
) -> ak.Array:
    """
    Assigns each event an array of category ids.
    """
    category_ids = []

    for cat_inst in self.config_inst.get_leaf_categories():
        # start with a true mask
        cat_mask = np.ones(len(events)) > 0

        # loop through selectors
        for selector in self.category_to_selectors[cat_inst]:
            # run the selector for events that still match the mask, then AND concat
            _cat_mask = self[selector](events[cat_mask], **kwargs)
            cat_mask[cat_mask] &= np.asarray(_cat_mask == 1)

            # stop if no events are left
            if not ak.any(cat_mask):
                break

        # covert to nullable array with the category ids or none, then apply ak.singletons
        ids = ak.where(cat_mask, np.float64(cat_inst.id), np.float64(np.nan))
        category_ids.append(ak.singletons(ak.nan_to_none(ids)))

    # combine
    category_ids = ak.concatenate(category_ids, axis=1)

    # save, optionally on a target events array
    if target_events is None:
        target_events = events
    target_events = set_ak_column(target_events, "category_ids", category_ids, value_type=np.int64)

    return target_events


@category_ids.init
def category_ids_init(self: Producer) -> None:
    # store a mapping from leaf category to selector classes for faster lookup
    self.category_to_selectors = defaultdict(list)

    # add all selectors obtained from leaf category selection expressions to the used columns
    for cat_inst in self.config_inst.get_leaf_categories():
        # treat all selections as lists
        for sel in law.util.make_list(cat_inst.selection):
            if Selector.derived_by(sel):
                selector = sel
            elif Selector.has_cls(sel):
                selector = Selector.get_cls(sel)
            else:
                raise Exception(
                    f"selection '{sel}' of category '{cat_inst.name}' cannot be resolved to an "
                    "existing Selector object",
                )

            # variables should refer to unexposed selectors as they should usually not
            # return SelectionResult's but a flat per-event mask
            if selector.exposed:
                logger.warning(
                    f"selection of category {cat_inst.name} seems to refer to an exposed selector "
                    "whose return value is most likely incompatible with category masks",
                )

            # update dependency sets
            self.uses.add(selector)
            self.produces.add(selector)

            self.category_to_selectors[cat_inst].append(selector)
