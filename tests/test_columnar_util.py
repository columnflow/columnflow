# coding: utf-8


__all__ = ["TestRoute", "TestArrayFunction"]


import unittest

from ap.util import maybe_import
from ap.columnar_util import Route, ArrayFunction

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

    def test_check(self):
        # SHOULD: Returns input, if it is a Route instance,
        # otherwise uses Route constructor with input

        # RouteInstance --> returns same object
        self.assertIs(Route.check(self.route), self.route)

        # if result of *check* with same fields as self.route, is equal to self.route
        # Sequence[str] --> RouteInstance
        route_from_sequence = Route.check(("i", "like", "trains"))
        self.assertEqual(route_from_sequence, self.route)
        # str --> RouteInstance
        route_from_str = Route.check(("i", "like", "trains"))
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
        self.assertEqual(self.empty_route + self.route,
                         ("i", "like", "trains"))
        self.assertEqual(self.empty_route + "i.like.trains",
                         ("i", "like", "trains"))
        self.assertEqual(self.empty_route +
                         ["i", "like", "trains"], ("i", "like", "trains"))

        # this __add__ should be not inplace
        self.assertFalse(self.empty_route)

    def test__radd__(self):
        # SHOULD: Add is same for left and right, if left argument add's fails
        self.assertEqual("test_string" + self.route,
                         ("i", "like", "trains", "test_string"))

    def test__iadd__(self):
        # +=
        self.route += self.route
        self.assertEqual(self.route, ("i", "like", "trains") * 2)

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
