# coding: utf-8

"""
Column production methods related defining categories.
"""

import law
import order as od

from ap.selection import Selector
from ap.production import Producer, producer
from ap.util import maybe_import
from ap.columnar_util import set_ak_column

np = maybe_import("numpy")
ak = maybe_import("awkward")


logger = law.logger.get_logger(__name__)


@producer(produces={"category_ids"})
def category_ids(
    self: Producer,
    events: ak.Array,
    config_inst: od.Config,
    dataset_inst: od.Dataset,
    **kwargs,
) -> ak.Array:
    """
    Assigns each event an array of category ids.
    """
    category_ids = []

    # TODO: we maybe don't want / need to loop through all leaf categories
    for cat_inst in config_inst.get_leaf_categories():
        # get the selector
        selector = self.category_to_selector[cat_inst]

        # get the category mask
        cat_mask = selector(events, config_inst=config_inst, dataset_inst=dataset_inst, **kwargs)

        # covert to nullable array with the category ids or none, then apply ak.singletons
        cat_ids = ak.singletons(ak.Array(np.where(np.asarray(cat_mask), cat_inst.id, None)))
        category_ids.append(cat_ids)

    category_ids = ak.concatenate(category_ids, axis=1)
    set_ak_column(events, "category_ids", category_ids)

    return events


@category_ids.update
def category_ids_update(self: Producer, config_inst: od.Config, **kwargs) -> None:
    # store a mapping from leaf category to selector instance for faster lookup
    self.category_to_selector = {}

    # add all selectors obtained from leaf category selection expressions to the used columns
    for cat_inst in config_inst.get_leaf_categories():
        sel = cat_inst.selection
        if isinstance(sel, Selector):
            selector = sel
        elif isinstance(sel, str) and Selector.has(sel):
            selector = Selector.get(sel)
        else:
            raise Exception(
                f"selection '{sel}' of category {cat_inst.name} does not refer to an existing "
                "selector",
            )

        # variables should refer to unexposed selectors as they should usually not
        # return SelectionResult's but a flat per-event mask
        if selector.exposed:
            logger.warning(
                f"selection of category {cat_inst.name} seems to refer to an exposed selector "
                "whose return value is most likely incompatible with category masks",
            )

        # create a copy
        selector = selector.updated_copy(config_inst=config_inst, **kwargs)

        # update sets
        self.uses.add(selector)
        self.produces.add(selector)
        self.shifts.add(selector)

        self.category_to_selector[cat_inst] = selector
