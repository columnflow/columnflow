# coding: utf-8


__all__ = ["TestRoute", "TestArrayFunction"]


import unittest
from collections import OrderedDict
from typing import List

from ap.util import maybe_import
from ap.columnar_util import (
    Route, ArrayFunction, get_ak_routes, has_ak_column, set_ak_column, remove_ak_column,
    add_ak_alias, add_ak_aliases, update_ak_array, flatten_ak_array, sort_ak_fields,
    sorted_ak_to_parquet,
)

ak = maybe_import("awkward")


class TestRoute(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # setting standardcases
        self.route = Route(["i", "like", "trains"])
        self.empty_route = Route()

    def test_join(self):
        # SHOULD:   join a sequence of strings with DOT notation
        result = "i.like.trains"

        # tuple list
        self.assertEqual(Route.join(("i", "like", "trains")), result)
        self.assertEqual(Route.join(["i", "like", "trains"]), result)

        # mixed string notation
        self.assertEqual(Route.join(["i.like", "trains"]), result)
        self.assertNotEqual(Route.join(["i_like", "trains"]), result)

    def test_join_nano(self):
        # SHOULD: join a sequence of strings with NANO notation
        join_nano = Route.join_nano
        result = "i_like_trains"

        # tuple list
        self.assertEqual(join_nano(("i", "like", "trains")), result)
        self.assertEqual(join_nano(["i", "like", "trains"]), result)

        # mixed string notation
        self.assertEqual(join_nano(["i_like", "trains"]), result)
        self.assertNotEqual(join_nano(["i.like", "trains"]), result)

    def test_split(self):
        # SHOULD: string in DOT format into List[str]
        split_result = Route.split("i.like.trains")
        result = ("i", "like", "trains")

        # equality with tuples checks if order is the same
        self.assertEqual(split_result, result)

        # all values in the split are instance of str
        self.assertTrue(all(isinstance(value, str) for value in split_result))

    def test_split_nano(self):
        # SHOULD: string in NANO format into List[str]
        split_result = Route.split_nano("i_like_trains")
        result = ("i", "like", "trains")

        # equality with tuples checks if order is the same
        self.assertEqual(split_result, result)

        # all values in the split are instance of str
        self.assertTrue(all(isinstance(value, str) for value in split_result))

    def test_cached_init(self):
        # SHOULD: Returns input, if it is a Route instance,
        # otherwise uses Route constructor with input

        # RouteInstance --> returns same object
        self.assertIs(Route(self.route), self.route)

        # if result of *check* with same fields as self.route, is equal to self.route
        # Sequence[str] --> RouteInstance
        route_from_sequence = Route(("i", "like", "trains"))
        self.assertEqual(route_from_sequence, self.route)
        # str --> RouteInstance
        route_from_str = Route(("i", "like", "trains"))
        self.assertEqual(route_from_str, self.route)

    def test_apply(self):
        # SHOULD: Select value from awkward array using it slice mechanic
        # slice_name is nested, each element of a tuple is a nested level
        # aw.Array["1","2"] = aw.Array["1"]["2"]

        arr = ak.Array({"i": {"like": {"trains": [0, 1, 2, 3]}}})

        aw_selection = self.route.apply(arr)
        aw_slice_direct = arr[tuple(self.route._fields)]
        aw_slice_standard = arr["i", "like", "trains"]

        # slice and select are the same in ALL entries
        # awkward.Array has no equal_all operation
        same_array = ak.all(
            [(aw_selection == aw_slice_direct) == (aw_selection == aw_slice_standard)])
        self.assertTrue(same_array)

    def test__init__(self):
        # SHOULD: create an EMPTY LIST if route=None
        # raise error if *route* is not None and not compatible

        # route = None --> _fields should be empty and list
        self.assertFalse(self.empty_route._fields)
        self.assertIsInstance(self.empty_route._fields, list)

        # route != None __> fields be filled and list
        self.assertTrue(self.route._fields)
        self.assertIsInstance(self.route._fields, list)

        # fields should never be shared among Route instances
        # this is tested because of the tricky behavior of
        # mutuable objects in init
        self.assertFalse(self.route._fields is self.empty_route._fields)
        self.assertFalse(Route() is self.empty_route._fields)

        # self._fields can not have a DOT in substring
        self.assertFalse(any("." in s for s in Route("i.like.trains").fields))

    def test_fields(self):
        # SHOULD: return tuple of strings, but no reference.

        # _fields same as fields
        self.assertEqual(self.route.fields, tuple(self.route._fields))
        self.assertFalse(self.route.fields is tuple(self.route._fields))

    def test_column(self):
        # SHOULD: return fields in DOT
        self.assertEqual(self.route.column, "i.like.trains")

    def test_nano_column(self):
        # SHOULD: return fields in NANO
        self.assertEqual(self.route.nano_column, "i_like_trains")

    def test__str__(self):
        self.assertEqual(str(self.route), "i.like.trains")

    def test__repr__(self):
        # TODO: No idea how to test
        pass

    def test__hash__(self):
        # SHOULD: Return the same hash if two Routes
        # stores the same fields in the same order.
        same_route = Route(("i", "like", "trains"))
        self.assertTrue(hash(same_route) == hash(self.route) ==
                        hash(("i", "like", "trains")))

        # checks if not true when order is different
        reverse_route = Route(("trains", "like", "i"))
        self.assertNotEqual(hash(reverse_route), hash(self.route))

    def test__len__(self):
        self.assertEqual(len(self.route), 3)
        self.assertEqual(len(self.empty_route), 0)

    def test__eq__(self):
        # SHOULD: return True if fields of one Route are the same as
        # fields from other Route
        # value of lists and tuples
        # the DOT Format of the str are equal
        # ELSE False
        self.assertFalse(self.route == self.empty_route)
        self.assertTrue(self.route == ("i", "like", "trains"))
        self.assertTrue(self.route.column == "i.like.trains")
        self.assertFalse(self.route == 0)

    def test__bool__(self):
        # SHOULD: Return if fields are empty or not
        # Since fields cannnot be nested, bool check of _fields is enough
        self.assertFalse(bool(self.empty_route._fields))
        self.assertTrue(bool(self.route._fields))

    def test__nonzero__(self):
        # same as test_bool__
        pass

    def test__add__(self):
        # SHOULD return same as *add*
        self.assertEqual(self.empty_route + self.route, ("i", "like", "trains"))
        self.assertEqual(self.empty_route + "i.like.trains", ("i", "like", "trains"))
        self.assertEqual(self.empty_route + ["i", "like", "trains"], ("i", "like", "trains"))

        # this __add__ should be not inplace
        self.assertFalse(self.empty_route)

    def test__radd__(self):
        # SHOULD: Add is same for left and right, if left argument add's fails
        self.assertEqual("test_string" + self.route, ("i", "like", "trains", "test_string"))

    def test__iadd__(self):
        # +=
        route = Route(self.route.fields)
        route += self.route
        self.assertEqual(route, ("i", "like", "trains") * 2)

    def test__getitem__(self):
        # Should return value of self._fields at index
        # if number is int, otherwise create new route instance
        # indexing field[int]
        self.assertEqual(self.route[2], "trains")
        # indexing field[slice]

        self.assertEqual(self.route[1:-1], "like")
        self.assertEqual(self.route[0:2], ("i", "like"))

        # slice -> new instance of Route
        copy_slice = self.route[:]
        self.assertIsInstance(copy_slice, Route)
        self.assertIsNot(copy_slice, self.route)

    def test__setitem__(self):
        # SHOULD: replace values in self._fields
        self.route[0] = "replaced"
        self.assertEqual(self.route, ("replaced", "like", "trains"))

    def test_add(self):
        # extend self._fields (a list)
        # if input is a...
        # - Sequence[str] -> extend str to _fields
        # - Route() instance -> extend Route._field
        # - str in DOT format

        input_as_sequence = ["i", "like", "trains"]
        input_in_dot = "i.like.trains"

        # case: str sequence
        sequence_route = Route()
        sequence_route.add(input_as_sequence)
        # case: str in dot
        str_route = Route()
        str_route.add(input_in_dot)
        # combine Routes instances

        self.empty_route.add(str_route)
        # comparison of fields are the same
        self.assertTrue(tuple(sequence_route._fields) == tuple(
            str_route._fields) == tuple(self.empty_route._fields))

        # raise error if something else is added
        with self.assertRaises(ValueError):
            self.empty_route.add(0)
        with self.assertRaises(ValueError):
            self.empty_route.add({})
        with self.assertRaises(ValueError):
            self.empty_route.add(None)

    def test_pop(self):
        # SHOULD: Remove entry at index from self._field AND
        # return the remove entry

        # pop element at index, default last element
        # default behavior: remove last element
        self.assertEqual(self.route.pop(), "trains")
        self.assertEqual(self.route.pop(0), "i")

        # after 2 pops only 1 element left
        self.assertTrue(len(self.route._fields), 1)

        # error when doing pop too often
        self.assertRaises(Exception, self.empty_route.pop, ())

    def test_reverse(self):
        # SHOULD: reverse fields INPLACE
        original = ("i", "like", "trains")
        reverse = ("trains", "like", "i")
        self.empty_route.add(original)

        # double reverse should restore the content
        self.route.reverse()
        self.assertEqual(tuple(self.route._fields), reverse)
        self.route.reverse()
        self.assertEqual(tuple(self.route._fields), original)

        # inplace operations should return nothing
        self.assertIsNone(self.route.reverse())

    def test_copy(self):
        # SHOULD: Copy instance.
        # route is not empty
        route_copy = self.route.copy()

        # same cls object
        self.assertIsInstance(route_copy, Route)
        # not a reference
        self.assertIsNot(self.route, route_copy)
        # same fields
        self.assertEqual(tuple(self.route._fields), tuple(route_copy._fields))


class test_add(ArrayFunction):

    uses = {"any_input_B"}
    produces = {"plus_100"}

    def call_func(self, arr):
        return arr + 100


class test_empty(ArrayFunction):

    uses = {"any_input_A"}
    produces = {"all_empty"}

    def call_func(self, arr):
        return ak.zeros_like(arr)


class test_combined(ArrayFunction):

    uses = {"met", "pT.all", test_empty, test_add}
    produces = {"pT.e", test_empty}

    def call_func(self, arr):
        return ak.zeros_like(arr)


class TestArrayFunction(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        test_array_dict = {"a_1": [0], "b_1": {"bb_1": [1], "bb_2": {"bbb_1": [2], "bbb_2": {
            "bbbb_1": {"bbbbb_1": [3]}, "bbbb_2": [4]}}}, "c_1": {"cc_1": [5]}}
        self.t_arr_A = ak.Array(test_array_dict)

        self.empty_arr_func = ArrayFunction.get_cls("test_empty")
        self.add_arr_func = ArrayFunction.get_cls("test_add")
        self.combined_arr_func = ArrayFunction.get_cls("test_combined")

    def test_IOFlag(self):
        # SHOULD:   Create unique id for the class
        #           Value of the id is not important
        flag = ArrayFunction.IOFlag
        self.assertIsNot(flag.USES, flag.PRODUCES)
        self.assertIsNot(flag.USES, flag.AUTO)

    def test_has_cls(self):
        # SHOULD:   True if name is already in cache
        self.assertTrue(ArrayFunction.has_cls("test_empty"))
        self.assertTrue(ArrayFunction.has_cls("test_add"))
        self.assertTrue(ArrayFunction.has_cls("test_combined"))

    def test_derive(self):
        # SHOULD:   Create new instance of the class and adds its instance to the class cache.

        new_cls = ArrayFunction.derive("new_cls")
        self.assertTrue(ArrayFunction.has_cls("new_cls"))
        self.assertTrue(ArrayFunction.derived_by(new_cls))

        # new ArrayFunction should be in cache
        self.assertIn("new_cls", ArrayFunction._subclasses)
        self.assertIn(new_cls, ArrayFunction._subclasses.values())

    def test_get_cls(self):
        # SHOULD:   Returns a cached instance, if <copy> is True, another instance is returned

        self.assertFalse(ArrayFunction.has_cls("foo"))
        self.assertIsNone(ArrayFunction.get_cls("foo", silent=True))

        self.assertIs(ArrayFunction.get_cls("test_empty"), test_empty)
        self.assertIs(ArrayFunction.get_cls("test_add"), test_add)
        self.assertIs(ArrayFunction.get_cls("test_combined"), test_combined)

    def test_AUTO(self):
        # SHOULD:   Named tuple of instance and unique class id

        #           check uniquennes between instances
        self.assertIsNot(self.empty_arr_func.AUTO, self.add_arr_func.AUTO)

    def test_USES(self):
        # SHOULD:   see test_AUTO
        self.assertIsNot(self.empty_arr_func.USES, self.add_arr_func.USES)

    def test_PRODUCES(self):
        # SHOULD:   see test_AUTO
        self.assertIsNot(self.empty_arr_func.PRODUCES, self.add_arr_func.PRODUCES)

    def test__get_columns(self):
        # SHOULD:   returns ALL *USES* or *PRODUCED* flagged columns depending on the used IOFlag
        #           if ArrayFunction is used in *uses* or *produces* their respective input should
        #           be found instead

        # create arr func with uses: "arr1, arr2, empty_arr_func.USES and add.arr_func.USES"
        flag = ArrayFunction.IOFlag

        inst = self.combined_arr_func()
        used_columns = inst._get_columns(io_flag=flag.USES)
        produced_columns = inst._get_columns(io_flag=flag.PRODUCES)

        # raise error if flag is AUTO
        inst = self.empty_arr_func()
        with self.assertRaises(ValueError):
            inst._get_columns(io_flag=flag.AUTO)

        # return a Set
        self.assertIsInstance(used_columns, set)
        self.assertIsInstance(produced_columns, set)

        # everythin within Set is a string
        self.assertTrue(all(isinstance(column, str) for column in used_columns.union(produced_columns)))

        # result should have USES: arr1, arr2, any_input_A, any_input_B
        # PRODUCES : "empty_arr", "pT.e"
        self.assertEqual(used_columns, set(["met", "pT.all", "any_input_A", "any_input_B"]))
        self.assertEqual(produced_columns, set(["pT.e", "all_empty"]))


class ColumnarUtilFunctionsTest(unittest.TestCase):

    def setUp(self):
        array_content = {"a": 0, "c_1": 1,
                         "b": {"bb1": 1, "bb2": {"bbb1": 2, "bbb2": {"b_bbb1": {"b_bbbb1": 4, "bbbbb2": 5},
                         "bbbb2": 3}}},
                         "d": {"d_1": 1}}
        self.ak_array = ak.Array([array_content])
        self.empty_ak_array = ak.Array([])

    def test_get_ak_routes(self):
        self.assertIsInstance(get_ak_routes(self.empty_ak_array), List)
        self.assertEqual(get_ak_routes(self.empty_ak_array), [])
        self.assertEqual(get_ak_routes(self.empty_ak_array, 1), [])
        self.assertEqual(get_ak_routes(self.empty_ak_array, -1), [])
        self.assertIsInstance(get_ak_routes(self.ak_array)[0], Route)

        # check all routes are in the list with the correct ordering
        self.assertEqual(get_ak_routes(self.ak_array),
                         ["a", "c_1", "b.bb1", "d.d_1", "b.bb2.bbb1", "b.bb2.bbb2.bbbb2",
                          "b.bb2.bbb2.b_bbb1.b_bbbb1", "b.bb2.bbb2.b_bbb1.bbbbb2"])
        # check positive max depth works correctly, even for routes with smaller depth and for routes merging together
        self.assertEqual(get_ak_routes(self.ak_array, 2), ["a", "c_1", "b.bb1", "b.bb2", "d.d_1"])
        # check negative max depth argument, also with some routes getting merged together
        self.assertEqual(get_ak_routes(self.ak_array, -1), ["b", "d", "b.bb2", "b.bb2.bbb2", "b.bb2.bbb2.b_bbb1"])
        # check negative max depth such that some route might have "length of -1"
        self.assertEqual(get_ak_routes(self.ak_array, -2), ["b", "b.bb2", "b.bb2.bbb2"])

    def test_has_ak_column(self):
        self.assertIsInstance(has_ak_column(self.ak_array, "a"), bool)
        self.assertTrue(has_ak_column(self.ak_array, "a"))
        self.assertTrue(has_ak_column(self.ak_array, "b.bb1"))

        # test input types
        self.assertTrue(has_ak_column(self.ak_array, Route("b.bb2.bbb2.b_bbb1.b_bbbb1")))
        self.assertTrue(has_ak_column(self.ak_array, ("b", "bb2", "bbb2", "b_bbb1", "b_bbbb1")))
        self.assertTrue(has_ak_column(self.ak_array, ["b", "bb2", "bbb2", "b_bbb1", "b_bbbb1"]))
        self.assertFalse(has_ak_column(self.ak_array, ("b", "bb2", "bbb2", "b_bbb1", "bbbbb1")))

        # test with empty Routes:
        self.assertTrue(has_ak_column(self.ak_array, ""))
        self.assertTrue(has_ak_column(self.ak_array, []))
        self.assertTrue(has_ak_column(self.ak_array, ()))
        self.assertTrue(has_ak_column(self.empty_ak_array, ""))

    def test_set_ak_column(self):
        array2_content = {"a": [0, 1]}
        ak_array2 = ak.Array(array2_content)

        # test adding a top level route
        value = [2, 3]
        ak_array3 = set_ak_column(ak_array2, Route("b"), value)
        self.assertEqual(ak_array3.fields, ["a", "b"])
        self.assertEqual(ak_array3["b"][0], 2)
        self.assertEqual(ak_array3["b"][1], 3)
        self.assertEqual(ak_array3["a"][0], 0)
        self.assertEqual(ak_array3["a"][1], 1)
        self.assertEqual(ak_array2.fields, ["a", "b"])
        self.assertEqual(ak_array2["b"][0], 2)
        self.assertEqual(ak_array2["b"][1], 3)

        # test adding a nested column
        value = [4, 5]
        ak_array4 = set_ak_column(ak_array3, Route("c.d"), value)
        self.assertEqual(ak_array4.fields, ["a", "b", "c"])
        self.assertEqual(ak_array4["c"].fields, ["d"])
        self.assertEqual(ak_array4[("c", "d")][0], 4)
        self.assertEqual(ak_array4[("c", "d")][1], 5)
        self.assertEqual(ak_array4["a"][0], 0)
        self.assertEqual(ak_array4["a"][1], 1)
        self.assertEqual(ak_array2.fields, ["a", "b", "c"])
        self.assertEqual(ak_array2[("c", "d")][0], 4)
        self.assertEqual(ak_array2[("c", "d")][1], 5)

        # test adding an embranchment to an existing nested column
        value = [6, 7]
        ak_array5 = set_ak_column(ak_array4, Route("c.e"), value)
        self.assertEqual(ak_array5["c"].fields, ["d", "e"])
        self.assertEqual(ak_array5[("c", "e")][0], 6)
        self.assertEqual(ak_array5[("c", "e")][1], 7)
        self.assertEqual(ak_array5["c", "d"][0], 4)
        self.assertEqual(ak_array5["c", "d"][1], 5)
        self.assertEqual(ak_array2["c"].fields, ["d", "e"])
        self.assertEqual(ak_array2[("c", "e")][0], 6)
        self.assertEqual(ak_array2[("c", "e")][1], 7)

        # test overwriting an existing column
        value = [8, 9]
        ak_array6 = set_ak_column(ak_array5, Route("c.e"), value)
        self.assertEqual(ak_array6[("c", "e")][0], 8)
        self.assertEqual(ak_array6[("c", "e")][1], 9)
        self.assertEqual(ak_array2[("c", "e")][0], 8)
        self.assertEqual(ak_array2[("c", "e")][1], 9)

    def test_remove_ak_column(self):

        # test if removal works for different input types of routes
        ak_array2_str = remove_ak_column(self.ak_array, "d.d_1")
        self.assertEqual(ak_array2_str.fields, ["a", "c_1", "b"])
        ak_array2_tuple = remove_ak_column(self.ak_array, ("d", "d_1"))
        self.assertEqual(ak_array2_tuple.fields, ["a", "c_1", "b"])
        ak_array2_list = remove_ak_column(self.ak_array, ["d", "d_1"])
        self.assertEqual(ak_array2_list.fields, ["a", "c_1", "b"])
        ak_array2_route = remove_ak_column(self.ak_array, Route("d.d_1"))
        self.assertEqual(ak_array2_route.fields, ["a", "c_1", "b"])

        # test if removal works for subroutes, top level or nested
        ak_array3 = remove_ak_column(self.ak_array, "b")
        self.assertEqual(ak_array3.fields, ["a", "c_1", "d"])
        ak_array4 = remove_ak_column(self.ak_array, "b.bb2.bbb2")
        self.assertEqual(ak_array4[("b", "bb2")].fields, ["bbb1"])

        # reset the array
        array_content = {"a": 0, "c_1": 1,
                         "b": {"bb1": 1, "bb2": {"bbb1": 2, "bbb2": {"b_bbb1": {"b_bbbb1": 4, "bbbbb2": 5},
                         "bbbb2": 3}}},
                         "d": {"d_1": 1}}
        self.ak_array = ak.Array([array_content])

        # removal of a single nested route
        ak_array5 = remove_ak_column(self.ak_array, "b.bb2.bbb2.b_bbb1.bbbbb2")
        self.assertEqual(ak_array5[("b", "bb2", "bbb2", "b_bbb1")].fields, ["b_bbbb1"])
        self.ak_array = ak.Array([array_content])

        # test error and silent
        self.assertRaises(ValueError, remove_ak_column, self.ak_array, "e")
        self.ak_array = ak.Array([array_content])
        ak_array6 = remove_ak_column(self.ak_array, "e", silent=True)
        self.assertEqual(ak_array6.fields, ["a", "c_1", "b", "d"])

        # test empty route
        self.assertRaises(ValueError, remove_ak_column, self.ak_array, Route())
        self.ak_array = ak.Array([array_content])
        ak_array7 = remove_ak_column(self.ak_array, Route(), silent=True)
        self.assertEqual(ak_array7.fields, ["a", "c_1", "b", "d"])

    def test_add_ak_alias(self):
        ak_array_aliasdd1 = add_ak_alias(self.ak_array, "d.d_1", "e")
        self.assertEqual(ak_array_aliasdd1["e"][0], 1)
        # test that it is an in place operation
        self.assertEqual(self.ak_array["e"][0], 1)

        # test adding an other alias for the same route
        ak_array_aliasdd1 = add_ak_alias(self.ak_array, "d.d_1", "f")
        self.assertEqual(ak_array_aliasdd1["f"][0], 1)

        # test overwrite an alias with the value of another route
        ak_array_aliasbbb2bbb1 = add_ak_alias(self.ak_array, "b.bb2.bbb1", "f")
        self.assertEqual(ak_array_aliasbbb2bbb1["f"][0], 2)

        # test removal of an source route
        ak_array_aliasremovedf = add_ak_alias(ak_array_aliasbbb2bbb1, "f", "e", remove_src=True)
        self.assertEqual(ak_array_aliasremovedf["e"][0], 2)
        self.assertEqual(ak_array_aliasremovedf.fields, ["a", "c_1", "b", "d", "e"])

        ak_array_alias_e_to_subcolumn = add_ak_alias(ak_array_aliasremovedf, "b.bb2.bbb2.b_bbb1", "e")
        self.assertEqual(ak_array_alias_e_to_subcolumn.fields, ["a", "c_1", "b", "d", "e"])
        self.assertEqual(ak_array_alias_e_to_subcolumn["e"].fields,
                         ak_array_alias_e_to_subcolumn[("b", "bb2", "bbb2", "b_bbb1")].fields)
        self.assertEqual(ak_array_alias_e_to_subcolumn[("e", "bbbbb2")],
                         ak_array_alias_e_to_subcolumn[("b", "bb2", "bbb2", "b_bbb1", "bbbbb2")])

        # test non existing src route
        self.assertRaises(ValueError, add_ak_alias, self.ak_array, "this_column_does_not_exist", "f")

    def test_add_ak_aliases(self):
        dictionary = {"f": "b.bb2.bbb2.b_bbb1"}
        dictionary2 = {"e": "d.d_1", "f": "b.bb2.bbb2.b_bbb1"}
        dictionary3 = {"e": "d.d_1", "f": "b.bb2.bbb2.b_bbb1.bbbbb2"}

        # test adding a single alias
        ak_array2 = add_ak_aliases(self.ak_array, dictionary)
        self.assertEqual(ak_array2.fields, ["a", "c_1", "b", "d", "f"])
        self.assertEqual(ak_array2["f"].fields, ak_array2[("b", "bb2", "bbb2", "b_bbb1")].fields)
        self.assertEqual(ak_array2[("f", "bbbbb2")], ak_array2[("b", "bb2", "bbb2", "b_bbb1", "bbbbb2")])
        self.assertEqual(self.ak_array.fields, ["a", "c_1", "b", "d", "f"])
        self.assertEqual(self.ak_array["f"].fields, self.ak_array[("b", "bb2", "bbb2", "b_bbb1")].fields)
        self.assertEqual(self.ak_array[("f", "bbbbb2")], self.ak_array[("b", "bb2", "bbb2", "b_bbb1", "bbbbb2")])

        # reset the test array
        array_content = {"a": 0, "c_1": 1,
                         "b": {"bb1": 1, "bb2": {"bbb1": 2, "bbb2": {"b_bbb1": {"b_bbbb1": 4, "bbbbb2": 5},
                         "bbbb2": 3}}},
                         "d": {"d_1": 1}}
        self.ak_array = ak.Array([array_content])
        # test with removal of the source column
        ak_array3 = add_ak_aliases(self.ak_array, dictionary, remove_src=True)
        self.assertEqual(ak_array3.fields, ["a", "c_1", "d", "f", "b"])
        self.assertEqual(ak_array3[("b", "bb2", "bbb2")].fields, ["bbbb2"])

        # test adding aliases for several columns
        self.ak_array = ak.Array([array_content])
        ak_array4 = add_ak_aliases(self.ak_array, dictionary2)
        self.assertEqual(ak_array4.fields, ["a", "c_1", "b", "d", "e", "f"])
        self.assertEqual(ak_array4["e"], ak_array4[("d", "d_1")])
        self.assertEqual(self.ak_array.fields, ["a", "c_1", "b", "d", "e", "f"])

        # test removing several source columns
        self.ak_array = ak.Array([array_content])
        ak_array5 = add_ak_aliases(self.ak_array, dictionary2, remove_src=True)
        self.assertEqual(ak_array5.fields, ["a", "c_1", "e", "f", "b"])
        self.assertEqual(ak_array5[("b", "bb2", "bbb2")].fields, ["bbbb2"])

        # test overwriting several aliases
        self.ak_array = ak.Array([array_content])
        ak_array6 = add_ak_aliases(self.ak_array, dictionary2)
        ak_array7 = add_ak_aliases(ak_array6, dictionary3)
        self.assertEqual(ak_array7["e"], ak_array7[Route("d.d_1").fields])
        self.assertEqual(ak_array7["f"], ak_array7[Route("b.bb2.bbb2.b_bbb1.bbbbb2").fields])
        self.assertEqual(self.ak_array["f"], ak_array7[Route("b.bb2.bbb2.b_bbb1.bbbbb2").fields])

    def test_update_ak_array(self):
        array1_content = {"a": [0, 1], "c_1": [1, 2]}
        array2_content = {"d": {"d_1": [2, 3]}, "b": {"bb1": [3, 4]}}
        array3_content = {"b": {"bb2": {"bbb1": [4, 5]}}}
        array4_content = {"b": {"bb2": {"bbb1": [5, 6]}}}
        ak_array1 = ak.Array(array1_content)
        ak_array2 = ak.Array(array2_content)
        ak_array3 = ak.Array(array3_content)
        ak_array4 = ak.Array(array4_content)

        # test an update without any updating array
        not_updated_array = update_ak_array(ak_array1)
        self.assertEqual(not_updated_array.fields, ["a", "c_1"])

        # test an update with only purely new columns
        updated_array1 = update_ak_array(ak_array1, ak_array2)
        self.assertEqual(updated_array1.fields, ["a", "c_1", "d", "b"])
        self.assertEqual(updated_array1["a"][0], 0)
        self.assertEqual(updated_array1[("d", "d_1")][0], 2)
        self.assertEqual(updated_array1[("b", "bb1")][0], 3)
        # test if in place
        self.assertEqual(ak_array1.fields, ["a", "c_1", "d", "b"])
        self.assertEqual(ak_array2.fields, ["d", "b"])
        self.assertEqual(ak_array1[("b", "bb1")][0], 3)

        # test an update with some columns with fields in common
        updated_array2 = update_ak_array(ak_array1, ak_array2, ak_array3)
        self.assertEqual(updated_array2[("b", "bb2", "bbb1")][0], 4)
        self.assertEqual(updated_array2[("b", "bb1")][0], 3)
        # test if in place
        self.assertEqual(ak_array1[("b", "bb1")][0], 3)

        # test an update with same columns, should overwrite value per default
        updated_array3 = update_ak_array(updated_array2, ak_array4)
        self.assertEqual(updated_array2[("b", "bb2", "bbb1")][0], 5)

        # test updates with concatenation
        updated_array4 = update_ak_array(updated_array3, ak_array4, concat_routes=True)
        self.assertEqual(updated_array4[("b", "bb2", "bbb1")][0, 0], 5)
        self.assertEqual(updated_array4[("b", "bb2", "bbb1")][0, 1], 5)
        self.assertEqual(updated_array4[("b", "bb2", "bbb1")][1, 0], 6)
        self.assertEqual(updated_array4[("b", "bb2", "bbb1")][1, 1], 6)

        updated_array4_2 = update_ak_array(updated_array4, ak_array4[..., None], concat_routes=True)
        self.assertEqual(updated_array4_2[("b", "bb2", "bbb1")][0, 0, 0], 5)
        self.assertEqual(updated_array4_2[("b", "bb2", "bbb1")][0, 1, 0], 5)
        self.assertEqual(updated_array4_2[("b", "bb2", "bbb1")][1, 0, 0], 6)
        self.assertEqual(updated_array4_2[("b", "bb2", "bbb1")][1, 1, 0], 6)

        # reset array to state before overwriting and concatenation
        ak_array1 = ak.Array([array1_content])
        updated_array2 = update_ak_array(ak_array1, ak_array2, ak_array3)

        # test update with empty list for concatenation, should overwrite value
        updated_array5 = update_ak_array(updated_array2, ak_array4, concat_routes=[])
        self.assertEqual(updated_array5[("b", "bb2", "bbb1")][0], 5)

        # test update with true route for concatenation
        ak_array1 = ak.Array([array1_content])
        updated_array2 = update_ak_array(ak_array1, ak_array2, ak_array3)
        updated_array6 = update_ak_array(updated_array2, ak_array4, concat_routes=["b.bb2.bbb1"])
        self.assertEqual(updated_array6[("b", "bb2", "bbb1")][0, 0], 4)
        self.assertEqual(updated_array6[("b", "bb2", "bbb1")][0, 1], 5)
        self.assertEqual(updated_array6[("b", "bb2", "bbb1")][1, 0], 5)
        self.assertEqual(updated_array6[("b", "bb2", "bbb1")][1, 1], 6)

        # test update with only partial route for concatenation = wrong route, value should be overwritten
        ak_array1 = ak.Array([array1_content])
        updated_array2 = update_ak_array(ak_array1, ak_array2, ak_array3)
        updated_array7 = update_ak_array(updated_array2, ak_array4, concat_routes=["b.bb2"])
        self.assertEqual(updated_array7[("b", "bb2", "bbb1")][0], 5)

        # same tests for add_routes
        ak_array1 = ak.Array([array1_content])
        updated_array2 = update_ak_array(ak_array1, ak_array2, ak_array3)
        updated_array8 = update_ak_array(updated_array2, ak_array4, add_routes=True)
        self.assertEqual(updated_array8[("b", "bb2", "bbb1")][0], 9)
        self.assertEqual(updated_array8[("b", "bb2", "bbb1")][1], 11)

        ak_array1 = ak.Array([array1_content])
        updated_array2 = update_ak_array(ak_array1, ak_array2, ak_array3)
        updated_array9 = update_ak_array(updated_array2, ak_array4, add_routes=["b.bb2.bbb1"])
        self.assertEqual(updated_array9[("b", "bb2", "bbb1")][0], 9)

        ak_array1 = ak.Array([array1_content])
        updated_array2 = update_ak_array(ak_array1, ak_array2, ak_array3)
        updated_array10 = update_ak_array(updated_array2, ak_array4, add_routes=["b.bb2"])
        self.assertEqual(updated_array10[("b", "bb2", "bbb1")][0], 5)

        # same tests for overwrite_routes
        ak_array1 = ak.Array([array1_content])
        updated_array2 = update_ak_array(ak_array1, ak_array2, ak_array3)
        updated_array11 = update_ak_array(updated_array2, ak_array4, overwrite_routes=["b.bb2.bbb1"])
        self.assertEqual(updated_array11[("b", "bb2", "bbb1")][0], 5)

        # try overwriting with a subroute: no update should be done
        ak_array1 = ak.Array([array1_content])
        updated_array2 = update_ak_array(ak_array1, ak_array2, ak_array3)
        self.assertRaises(Exception, update_ak_array, updated_array2, ak_array4, overwrite_routes=["b.bb2"])

        # As indicated in docstring: when no option is given to resolve conflict of two same column,
        # should raise exception
        ak_array1 = ak.Array([array1_content])
        updated_array2 = update_ak_array(ak_array1, ak_array2, ak_array3)
        self.assertRaises(Exception, update_ak_array, updated_array2, ak_array4, overwrite_routes=False)

        # test empty array as updating array or as array to be updated
        ak_array1 = ak.Array([array1_content])
        updated_array12 = update_ak_array(ak_array1, self.empty_ak_array)
        self.assertEqual(updated_array12.fields, ["a", "c_1"])

        # ValueError because of impossibility to add a new field in an empty akward array
        ak_array1 = ak.Array([array1_content])
        self.assertRaises(ValueError, update_ak_array, self.empty_ak_array, ak_array1)

        # add tests for different input types for route argument (route, str, tuple, list, other sequences)?

    def test_flatten_ak_array(self):
        # WARNING: for the tests of this function, it is assumed that the flattened columns
        # in the output OrderedDict is given by increasing order of nesting, as outputted by get_ak_routes
        array2_content = {"a": 0, "c_1": 1}
        array3_content = {"d": {"d_1": 1}, "b": {"bb1": 1}}
        ak_array2 = ak.Array([array2_content])
        ak_array3 = ak.Array([array3_content])

        flattened_array = flatten_ak_array(ak_array2)
        self.assertEqual(list(flattened_array.keys()), ak_array2.fields)
        flattened_array2 = flatten_ak_array(ak_array3)
        self.assertEqual(list(flattened_array2.keys()), ["d.d_1", "b.bb1"])

        # with routes argument to choose which routes should be saved
        flattened_array_withroute = flatten_ak_array(self.ak_array, routes=["d.d_1", "a"])
        self.assertEqual(list(flattened_array_withroute.keys()), ["a", "d.d_1"])
        self.assertEqual(flattened_array_withroute["a"], self.ak_array["a"])
        flattened_array_withroute2 = flatten_ak_array(self.ak_array, routes=["b.bb1", "b.bb2"])
        self.assertEqual(list(flattened_array_withroute2.keys()), ["b.bb1"])
        flattened_array_withroute3 = flatten_ak_array(self.ak_array, routes=[Route("d.d_1"), Route("a")])
        self.assertEqual(list(flattened_array_withroute3.keys()), ["a", "d.d_1"])
        self.assertEqual(flattened_array_withroute3["a"], self.ak_array["a"])
        flattened_array_withroute4 = flatten_ak_array(self.ak_array, routes=(Route("d.d_1"), Route("a")))
        self.assertEqual(list(flattened_array_withroute4.keys()), ["a", "d.d_1"])
        self.assertEqual(flattened_array_withroute4["a"], self.ak_array["a"])
        flattened_array_withroute5 = flatten_ak_array(self.ak_array, routes=("d.d_1", "a"))
        self.assertEqual(list(flattened_array_withroute5.keys()), ["a", "d.d_1"])
        self.assertEqual(flattened_array_withroute5["a"], self.ak_array["a"])
        flattened_array_withroute6 = flatten_ak_array(self.ak_array, routes={"d.d_1", "a"})
        self.assertEqual(list(flattened_array_withroute6.keys()), ["a", "d.d_1"])
        self.assertEqual(flattened_array_withroute6["a"], self.ak_array["a"])

        # test for Callables
        def having_a_fun_callable_is_the_joy_of_every_programmer(route):
            if route == "d.d_1" or route == "a" or route == "b.bb2.bbb1" or route == "b.bb2.bbb2":
                return True
            else:
                return False

        flattened_array_withroute7 = flatten_ak_array(self.ak_array,
                                                      having_a_fun_callable_is_the_joy_of_every_programmer)
        self.assertEqual(list(flattened_array_withroute7.keys()), ["a", "d.d_1", "b.bb2.bbb1"])
        self.assertEqual(flattened_array_withroute7["a"], self.ak_array["a"])

    def test_sort_ak_fields(self):
        array2_content = {
            "I": {
                "dontlike": 1,
                "like": {
                    "trains": 2,
                    "the": 3,
                },
                "zorro": 5,
                "asthma": 6,
            },
            "42": {
                "24": 1,
            },
        }
        ak_array2 = ak.Array([array2_content])

        ordered_ak_array2_content = OrderedDict([
            ("42", OrderedDict([("24", 1)])),
            ("I", OrderedDict([
                ("asthma", 6),
                ("dontlike", 1),
                ("like", OrderedDict([
                    ("the", 3),
                    ("trains", 2),
                ])),
                ("zorro", 5),
            ])),
        ])
        ordered_ak_array2 = ak.Array([ordered_ak_array2_content])

        ak_array3 = sort_ak_fields(ak_array2)
        # check if numbers are sorted before letters
        self.assertEqual(ak_array3.fields, ordered_ak_array2.fields)
        # check if nested structure gets ordered
        self.assertEqual(ak_array3["I"].fields, ordered_ak_array2["I"].fields)
        # check if deeper nested structure with same first letter gets ordered
        self.assertEqual(ak_array3[("I", "like")].fields, ordered_ak_array2[("I", "like")].fields)

        # add sort_fn to invert the name of the fields before ordering them (this sort_fn outputs a string!)
        def sorting_function(some_string):
            return some_string[::-1]

        ak_array4 = sort_ak_fields(ak_array2, sort_fn=sorting_function)
        self.assertEqual(ak_array4["I"].fields, ["asthma", "like", "dontlike", "zorro"])

        # add sort_fn with an int as output: this function outputs the length of the field names for the ordering
        def sorting_function_to_int(some_string):
            position = len(some_string)
            return position

        ak_array5 = sort_ak_fields(ak_array2, sort_fn=sorting_function_to_int)
        self.assertEqual(ak_array5.fields, ["I", "42"])
        self.assertEqual(ak_array5["I"].fields, ["like", "zorro", "asthma", "dontlike"])

        # check that the sorting algorithm is stable
        array_content_with_names_of_same_length = {"ccccc": 1, "aaaaa": 3, "bbbbb": 2}
        ak_array_same_length = ak.Array([array_content_with_names_of_same_length])
        ak_array_same_length_sorted = sort_ak_fields(ak_array_same_length)
        self.assertEqual(ak_array_same_length_sorted.fields, ["aaaaa", "bbbbb", "ccccc"])
        ak_array_same_length_intsorted = sort_ak_fields(ak_array_same_length_sorted, sort_fn=sorting_function_to_int)
        self.assertEqual(ak_array_same_length_intsorted.fields, ["aaaaa", "bbbbb", "ccccc"])

        # check that there is no problem with the empty array
        empty_ak_array_sorted = sort_ak_fields(self.empty_ak_array)
        self.assertEqual(empty_ak_array_sorted.fields, [])
        empty_ak_array_intsorted = sort_ak_fields(self.empty_ak_array, sort_fn=sorting_function)
        self.assertEqual(empty_ak_array_intsorted.fields, [])

        # add type check on field names for the function?

    def test_sorted_ak_to_parquet(self):
        array_content = {"I": {"dontlike": 1, "like": {"trains": 2, "the": 3}, "zorro": 5, "asthma": 6},
                         "42": {"24": 1}}
        ordered_ak_array_content = {"42": {"24": 1},
                                    "I": {"asthma": 6, "dontlike": 1, "like": {"the": 3, "trains": 2}, "zorro": 5}}
        ordered_ak_array = ak.Array([ordered_ak_array_content])
        ak_array_to_save = ak.Array([array_content])
        sorted_ak_to_parquet(ak_array_to_save, "array_test.parquet")

        ak_array = ak.from_parquet("array_test.parquet")
        self.assertEqual(ak_array[("42", "24")][0], 1)
        self.assertEqual(ak_array.fields, ordered_ak_array.fields)
        self.assertEqual(ak_array["I"].fields, ordered_ak_array["I"].fields)
        self.assertEqual(ak_array[("I", "like")].fields, ordered_ak_array[("I", "like")].fields)
