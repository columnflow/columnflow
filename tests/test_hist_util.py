# coding: utf-8


__all__ = ["HistUtilTests"]

import unittest

from columnflow.util import maybe_import
from columnflow.hist_util import (
    add_hist_axis, create_hist_from_variables, create_columnflow_hist,
    translate_hist_intcat_to_strcat,
)
import order as od

np = maybe_import("numpy")
ak = maybe_import("awkward")
hist = maybe_import("hist")


class HistUtilTests(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.config_inst = cfg = od.Config(name="config", id=1)

        self.variable_example = cfg.add_variable(
            name="variable_example",
            binning=[0, 100, 200, 400, 800],
            x_title="Example variable variable",
            aux={"axis_type": "variable"},
        )
        self.regular_example = cfg.add_variable(
            name="regular_example",
            binning=(40, 0, 400),
            x_title="Example regular variable",
            aux={"axis_type": "regular"},
        )
        self.integer_example = cfg.add_variable(
            name="integer_example",
            binning=[0, 1, 2, 3, 4],  # we currently only take first and last bin edge
            x_title="Example integer variable",
            aux={"axis_type": "integer"},
        )
        self.intcat_example = cfg.add_variable(
            name="intcat_example",
            binning=[-10, 0, 1, 8, 2, 5, 999],
            x_title="Example intcat variable",
            aux={"axis_type": "intcat"},
        )
        self.strcat_example = cfg.add_variable(
            name="strcat_example",
            x_title="Example strcat variable",
            aux={"axis_type": "strcat"},
        )
        self.boolean_example = cfg.add_variable(
            name="boolean_example",
            x_title="Example boolean variable",
            aux={"axis_type": "boolean"},
        )

        self.axis_type_map = {
            "variable": hist.axis.Variable,
            "regular": hist.axis.Regular,
            "integer": hist.axis.Integer,
            "intcat": hist.axis.IntCategory,
            "strcat": hist.axis.StrCategory,
            "boolean": hist.axis.Boolean,
        }

        self.variable_examples = (
            self.variable_example,
            self.regular_example,
            self.integer_example,
            self.intcat_example,
            self.strcat_example,
            self.boolean_example,
        )

    def test_create_hist_from_variables(self):
        histogram = create_hist_from_variables(*self.variable_examples)

        for variable in self.variable_examples:
            # check default attributes
            self.assertIn(variable.name, histogram.axes.name)
            self.assertEqual(histogram.axes[variable.name].name, variable.name)
            self.assertEqual(histogram.axes[variable.name].label, variable.get_full_x_title())
            self.assertEqual(type(histogram.axes[variable.name]), self.axis_type_map[variable.x.axis_type])

        # check consistency with adding the axes one by one
        histogram_manually = hist.Hist.new
        for variable in self.variable_examples:
            histogram_manually = add_hist_axis(histogram_manually, variable)
        histogram_manually = histogram_manually.Weight()

        self.assertEqual(histogram, histogram_manually)

        # test with default categorical axes
        histogram = create_columnflow_hist(*self.variable_examples)

        expected_default_axes = {
            "category": hist.axis.IntCategory,
            "process": hist.axis.IntCategory,
            "shift": hist.axis.StrCategory,
        }
        for axis, axis_type in expected_default_axes.items():
            self.assertIn(axis, histogram.axes.name)
            self.assertEqual(histogram.axes[axis].name, axis)
            self.assertEqual(type(histogram.axes[axis]), axis_type)

    def test_translate_hist_intcat_to_strcat(self):
        # Create a histogram with an integer category axis
        h = hist.Hist(
            hist.axis.IntCategory([1, 2, 3], name="category", label="Category Axis"),
            storage=hist.storage.Double(),
        )

        # Fill the histogram with some data
        h.fill(category=[1, 2, 2, 3, 3, 3])

        # Define the mapping from integer to string categories
        id_map = {1: "one", 2: "two", 3: "three"}

        # Call the function to translate the axis
        translated_h = translate_hist_intcat_to_strcat(h, "category", id_map)

        # Validate the new histogram
        # Check that the axis has been correctly translated
        self.assertEqual(len(translated_h.axes), len(h.axes), "The number of axes should remain the same.")
        str_axis = translated_h.axes[0]
        self.assertIsInstance(str_axis, hist.axis.StrCategory, "The axis should be of type StrCategory.")
        self.assertEqual(str_axis.name, "category", "The axis name should remain unchanged.")
        self.assertEqual(str_axis.label, "Category Axis", "The axis label should remain unchanged.")

        # Check the string categories
        expected_categories = ["one", "two", "three"]
        self.assertEqual(list(str_axis), expected_categories, "The categories should match the expected string values.")

        # Check the data
        self.assertEqual(translated_h.sum(), h.sum(), "The total sum of data should remain unchanged.")
        for original, translated in zip(h.values(flow=True), translated_h.values(flow=True)):
            self.assertEqual(original, translated, "The data values should remain unchanged.")
