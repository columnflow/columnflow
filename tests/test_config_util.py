# coding: utf-8


__all__ = ["ConfigUtilTests"]

import unittest

from columnflow.util import maybe_import
from columnflow.config_util import get_events_from_categories

# from columnflow.analysis_templates.cms_minimal.__cf_module_name__.config.analysis__cf_short_name_lc__.py import cfg

import order as od

np = maybe_import("numpy")
ak = maybe_import("awkward")
dak = maybe_import("dask_awkward")
coffea = maybe_import("coffea")


class ConfigUtilTests(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.config_inst = cfg = od.Category("config", 1)

        cfg.add_category("main_1", id=1)
        main_2 = cfg.add_category("main_2", id=2)
        main_2.add_category("leaf_21", id=21)
        leaf_22 = main_2.add_category("leaf_22", id=22)
        leaf_22.add_category("leaf_221", id=221)

        # awkward array with category_ids (only leaf ids)
        self.events = ak.Array({
            "category_ids": [[1], [21, 221], [21], [221]],
            "dummy_field": [1, 2, 3, 4],
        })

    def test_get_events_from_categories(self):
        # check that category without leafs is working
        events = get_events_from_categories(self.events, ["main_1"], self.config_inst)
        self.assertTrue(ak.all(ak.any(events.category_ids == 1, axis=1)))
        self.assertTrue(ak.all(events.dummy_field == ak.Array([1])))

        # check that categories with leafs are working
        events = get_events_from_categories(self.events, ["main_2"], self.config_inst)
        self.assertTrue(ak.all(events.dummy_field == ak.Array([2, 3, 4])))

        # check that leaf category is working
        events = get_events_from_categories(self.events, ["leaf_221"], self.config_inst)
        self.assertTrue(ak.all(ak.any(events.category_ids == 221, axis=1)))
        self.assertTrue(ak.all(events.dummy_field == ak.Array([2, 4])))

        # check that leaf category is working
        events = get_events_from_categories(self.events, ["main_1", "main_2"], self.config_inst)
        self.assertTrue(ak.all(events.dummy_field == ak.Array([1, 2, 3, 4])))

        # check that directly passing category inst is working
        events = get_events_from_categories(self.events, self.config_inst.get_category("main_1"))
        self.assertTrue(ak.all(ak.any(events.category_ids == 1, axis=1)))
        self.assertTrue(ak.all(events.dummy_field == ak.Array([1])))

        # never select events from non-leaf categories or not existing categories
        events = get_events_from_categories(ak.Array({"category_ids": [[2], [-1], [99]]}), ["main_2"], self.config_inst)
        self.assertEqual(len(events), 0)
