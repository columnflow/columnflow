from __future__ import annotations

__all__ = ["SelectionResultTests"]

import unittest
from copy import deepcopy

from columnflow.selection import SelectionResult
from columnflow.columnar_util import maybe_import, DotDict

np = maybe_import("numpy")
ak = maybe_import("awkward")
dak = maybe_import("dask_awkward")
coffea = maybe_import("coffea")


class SelectionResultTests(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super(SelectionResultTests, self).__init__(*args, **kwargs)

        # inputs for various tests
        self.working_init = {
            "event": ak.Array([True, False, True, True]),
            "steps": {
                "all": ak.Array([True, False, True, True]),
                "step1": ak.Array([True, False, True, False]),
                "step2": ak.Array([True, False, False, False]),
            },
            "objects": {
                "Jet": {
                    "Jet": ak.Array([[0, 1], [2, 3], [0], [0]]),
                },
            },
            "aux": {
                "some_mask": ak.Array([True, False, True, True]),
            },
            "some_top_level_field": {
                "foo": ak.Array([f"bar{i}" for i in range(4)]),
            },
        }

        self.add_test = {
            "event": ak.Array([True, True, False, True]),
            "steps": {
                "step3": ak.Array([True, False, True, False]),
            },
            "objects": {
                "Muon": {
                    "Muon": ak.Array([[0, 1], [2, 3], [0], [0]]),
                },
            },
        }

        sub_dict = deepcopy(self.working_init)
        self.test_configurations = {"full": deepcopy(self.working_init)}
        keys = list(sub_dict.keys())
        for k in keys:
            sub_dict.pop(k, None)
            self.test_configurations[f"no_{k}"] = deepcopy(sub_dict)

        # dictionary to test convertability to awkward arrays.
        # More explicitely, everything that cannot/should not be converted
        # is listed here with the structure
        # {
        #   ExceptionType: [list, of, configuration, dictionaries]
        # }
        self.not_convertable = {
            ValueError: [
                # Using non-boolean values as event masks must raise a ValueError
                {
                    "event": ak.Array([0, 1, 0, 0]),
                    "steps": {
                        "all": ak.Array([True, False, True, True]),
                        "step1": ak.Array([True, False, True, False]),
                        "step2": ak.Array([True, False, False, False]),
                    },
                    "objects": {
                        "Jet": {
                            "Jet": ak.Array([[0, 1], [2, 3], [0], [0]]),
                        },
                    },
                    "aux": {
                        "some_mask": ak.Array([True, False, True, True]),
                    },
                },
                # SelectionResults with optional additional top-level fields
                # with nested structures > 1 are not supported at the moment
                {
                    "some_top_level_field": {
                        "foo": {
                            "bar": {
                                "this": ak.Array(["is", "not", "allowed", "atm"]),
                            },
                        },
                    },
                },
            ],
            # handling anything but an ak.Array in the additional fields is
            # not implemented right now
            NotImplementedError: [
                {
                    "some_top_level_field": {
                        "foo": list(("this", "will", "not", "work")),
                    },
                },
                {
                    "some_top_level_field": {
                        "foo": set(("this", "will", "not", "work")),
                    },
                },
                {
                    "some_top_level_field": {
                        "foo": DotDict({"this": "will", "not": "work"}),
                    },
                },
            ],
        }
        self.not_addable = {
            KeyError: [
                # same object field must raise error
                {
                    "event": ak.Array([True, True, False, True]),
                    "steps": {
                        "step3": ak.Array([True, False, True, False]),
                    },
                    "objects": {
                        "Jet": {
                            "Jet": ak.Array([[0, 3], [2, 6], [0], [1]]),
                        },
                        "Muon": {
                            "Muon": ak.Array([[0, 1], [2, 3], [0], [0]]),
                        },
                    },
                },
                # same step field must raise error
                {
                    "event": ak.Array([True, True, False, True]),
                    "steps": {
                        "step2": ak.Array([True, False, True, False]),
                    },
                    "objects": {
                        "Muon": {
                            "Muon": ak.Array([[0, 1], [2, 3], [0], [0]]),
                        },
                    },
                },
            ],
        }

        # define invalid types to check for in test suite for add operation
        self.invalid_types_to_add = [
            int(1), float(3), bool(True), "foo", DotDict(),
        ]

    def setUp(self):
        self.selection_results = dict()
        for config_name, configuration in self.test_configurations.items():
            self.selection_results[config_name] = SelectionResult(**configuration)
        # if this was all successful, save the full version of a SelectionResult
        # for further testing

        self.full_result = self.selection_results["full"]
        self.result_to_add = SelectionResult(**self.add_test)
        self.not_addable_results = {
            exc: [SelectionResult(**config) for config in configurations]
            for exc, configurations in self.not_addable.items()
        }
        self.not_convertable_results = {
            exc: [SelectionResult(**config) for config in configurations]
            for exc, configurations in self.not_convertable.items()
        }

    def test_add(self):

        def convert_to_plain_objects(input):
            if isinstance(input, dict):
                keys = list(input.keys())

                # perform type check
                # if the object is of type dict[dict[Any]], loop through substructure
                if isinstance(input[keys[0]], dict):
                    return {
                        up_key: {
                            key: val.to_list() for key, val in up_val.items()
                        } for up_key, up_val in input.items()
                    }
                # otherwise, convert substructure directly
                else:
                    return {
                        key: val.to_list() for key, val in input.items()
                    }
            else:
                raise NotImplementedError(f"Cannot convert input of type {type(input)}")

        added = self.full_result + self.result_to_add
        added_event_mask = self.full_result.event & self.result_to_add.event
        self.assertListEqual(added.event.to_list(), added_event_mask.to_list())

        # need to convert ak arrays to lists in dictionaries for 'assert' functions

        # test adding steps
        added_steps = deepcopy(self.full_result.steps)
        added_steps.update(self.result_to_add.steps)
        self.assertDictEqual(
            convert_to_plain_objects(added.steps), convert_to_plain_objects(added_steps),
        )

        # test adding objects
        added_objects = deepcopy(self.full_result.objects)
        added_objects.update(self.result_to_add.objects)
        self.assertDictEqual(
            convert_to_plain_objects(added.objects), convert_to_plain_objects(added_objects),
        )

        # test auxiliary information section
        added_aux = deepcopy(self.full_result.aux)
        added_aux.update(self.result_to_add.aux)
        self.assertDictEqual(
            convert_to_plain_objects(added.aux), convert_to_plain_objects(added_aux),
        )

        # test arbitrary other top-level fields
        added_other = deepcopy(self.full_result.other)
        added_other.update(self.result_to_add.other)
        self.assertDictEqual(
            convert_to_plain_objects(added.other), convert_to_plain_objects(added_other),
        )

    def test_not_addable(self):
        # Ensure exception if same object field is defined
        # in two different selection results (add_test_same_objects)
        for exception_type, results in self.not_addable_results.items():
            for result in results:
                self.assertRaises(exception_type, self.full_result.__add__, result)

        # Ensure that add only works with Selection results
        for other in self.invalid_types_to_add:
            self.assertRaises(TypeError, self.full_result.__add__, other)

    def test_not_convertable_to_ak(self):
        """Test conversion to ak.Array
        """
        # first, test that invalid configurations throw the right error
        for exception_type, results in self.not_convertable_results.items():
            for result in results:
                self.assertRaises(exception_type, result.to_ak)

        # also account for the case if someone manually adds something to the
        # 'other' field that would overwrite something, e.g. objects

        result_copy = deepcopy(self.full_result)

        # manually add something to the 'other' field
        result_copy.other["objects"] = ak.Array(["this", "cannot", "be", "happening"])

        self.assertRaises(KeyError, result_copy.to_ak)

    def test_partial_conversion_to_ak(self):
        # test conversion also with partial SelectionResults
        for result in self.selection_results.values():
            self.assertIsInstance(result.to_ak(), ak.Array)

    def test_full_conversion_to_ak(self):
        converted_result = self.full_result.to_ak()

        # check output format
        self.assertIsInstance(converted_result, ak.Array)

        def recursive_assert(this, other):
            if not isinstance(this, DotDict):
                raise TypeError(f"The first instance in this function must be of"
                                f"type DotDict, received {type(this)}")

            keys = list(this.keys())
            for k in keys:
                sub_this = this[k]
                if isinstance(sub_this, dict):
                    # if this is a dictionary, we have a nested structure,
                    # so iterate through it accordingly
                    recursive_assert(sub_this, other[k])
                else:
                    # otherwise, we are at the lowest level. There should be
                    # an awkward array here with the `to_list` function
                    self.assertListEqual(sub_this.to_list(), other[k].to_list())

        # test conversion of full SelectionResult in more detail
        # first, test event mask
        self.assertListEqual(converted_result.event.to_list(), self.full_result.event.to_list())

        # test steps
        # test if all keys are present
        converted_steps = converted_result.steps.fields
        original_steps = list(self.full_result.steps.keys())
        self.assertListEqual(converted_steps, original_steps)

        # test if entries are really the same
        recursive_assert(self.full_result.steps, converted_result.steps)

        # test object arrays
        converted_objects = converted_result.objects.fields
        original_objects = list(self.full_result.objects.keys())
        self.assertListEqual(converted_objects, original_objects)

        recursive_assert(self.full_result.objects, converted_result.objects)

        # assert that aux is not part of the awkward array
        self.assertNotIn("aux", converted_result.fields)

        # check parsing of other objects
        recursive_assert(self.full_result.other, converted_result)

        # make sure there's nothing else in the ak.Array
        full_list = ["event", "steps", "objects"] + list(self.full_result.other.keys())

        self.assertListEqual(full_list, converted_result.fields)


if __name__ == "__main__":
    unittest.main()
