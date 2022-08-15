# coding: utf-8

"""
Column production methods related defining categories.
"""

import law

from columnflow.selection import Selector
from columnflow.production import Producer, producer
from columnflow.util import maybe_import
from columnflow.columnar_util import set_ak_column

np = maybe_import("numpy")
ak = maybe_import("awkward")

logger = law.logger.get_logger(__name__)


@producer(produces={"category_ids"})
def category_ids(self: Producer, events: ak.Array, **kwargs) -> ak.Array:
    """
    Assigns each event an array of category ids.
    """
    category_ids = []

    # TODO: we maybe don't want / need to loop through all leaf categories
    for cat_inst in self.config_inst.get_leaf_categories():
        # get the selector class
        selector = self.category_to_selector[cat_inst]

        # get the category mask
        cat_mask = self[selector](events, **kwargs)

        # covert to nullable array with the category ids or none, then apply ak.singletons
        cat_ids = ak.singletons(ak.Array(np.where(np.asarray(cat_mask), cat_inst.id, None)))
        category_ids.append(cat_ids)

    category_ids = ak.concatenate(category_ids, axis=1)
    set_ak_column(events, "category_ids", category_ids)

    return events


@category_ids.init
def category_ids_init(self: Producer) -> None:
    # store a mapping from leaf category to selector class for faster lookup
    self.category_to_selector = {}

    # add all selectors obtained from leaf category selection expressions to the used columns
    for cat_inst in self.config_inst.get_leaf_categories():
        sel = cat_inst.selection
        if Selector.derived_by(sel):
            selector = sel
        elif Selector.has_cls(sel):
            selector = Selector.get_cls(sel)
        else:
            raise Exception(
                f"selection '{sel}' of category '{cat_inst.name}' cannot be resolved to a existing "
                "Selector object",
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

        self.category_to_selector[cat_inst] = selector
