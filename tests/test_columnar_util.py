# coding: utf-8


__all__ = ["ColumnarUtilTest"]


import unittest
from ap.columnar_util import add_ak_alias
import ap.columnar_util as c_util
from typing import Optional, Union, Sequence, Set, Tuple, List, Dict, Callable, Any
import awkward as ak


class test_Route(unittest.TestCase):

    def setUp(self):
        # setting standardcases
        self.route = c_util.Route(["i", "like", "trains"])
        self.empty_route = c_util.Route()

    def tearDown(self):
        del self.route
        del self.empty_route

    def test_join(self):
        # SHOULD:   join a sequence of strings with DOT notation
        join = c_util.Route.join
        result = "i.like.trains"

        # tuple list
        self.assertEqual(join(("i", "like", "trains")), result)
        self.assertEqual(join(["i", "like", "trains"]), result)

        # mixed string notation
        self.assertEqual(join(["i.like", "trains"]), result)
        # mixed with both notations?
        self.assertEqual(join(["i_like", "trains"]), result)

    def test_join_nano(self):
        # SHOULD: join a sequence of strings with NANO notation
        join_nano = c_util.Route.join_nano
        result = "i_like_trains"
        # tuple list
        self.assertEqual(join_nano(("i", "like", "trains")), result)
        self.assertEqual(join_nano(["i", "like", "trains"]), result)

        # mixed string notation
        self.assertEqual(join_nano(["i.like", "trains"]), result)
        # mixed with both notations?
        self.assertEqual(join_nano(["i_like", "trains"]), result)

    def test_split(self):
        # SHOULD: string in DOT format into List[str]
        split_result = self.empty_route.split("i.like.trains")
        result = ["i", "like", "trains"]

        # equality with tuples checks if order is the same
        self.assertEqual(tuple(split_result), tuple(result))
        # is list after operation
        self.assertIsInstance(split_result, list)

        # all values in the split are instance of str
        self.assertTrue(all(isinstance(value, str) for value in split_result))

    def test_split_nano(self):
        # SHOULD: string in NANO format into List[str]
        split_result = self.empty_route.split_nano("i_like_trains")
        result = ["i", "like", "trains"]

        # equality with tuples checks if order is the same
        self.assertEqual(tuple(split_result), tuple(result))
        # is list after operation
        self.assertIsInstance(split_result, list)

        # all values in the split are instance of str
        self.assertTrue(all(isinstance(value, str) for value in split_result))

    def test_check(self):
        # SHOULD: Returns input, if it is a Route instance,
        # otherwise uses Route constructor with input

        # RouteInstance --> returns same object
        self.assertIs(c_util.Route.check(self.route), self.route)

        # if result of *check* with same fields as self.route, is equal to self.route
        # Sequence[str] --> RouteInstance
        route_from_sequence = c_util.Route.check(("i", "like", "trains"))
        self.assertEqual(route_from_sequence, self.route)
        # str --> RouteInstance
        route_from_str = c_util.Route.check(("i", "like", "trains"))
        self.assertEqual(route_from_str, self.route)

    def test_select(self):
        # SHOULD: Select value from awkward array using it slice mechanic
        # slice_name is nested, each element of a tuple is a nested level
        # aw.Array["1","2"] = aw.Array["1"]["2"]

        select = c_util.Route.select
        # self.Route.fields = ["i","like","trains"]
        aw_dict = {"i": {"like": {"trains": [0, 1, 2, 3]}}}
        aw_arr = ak.Array(aw_dict)

        aw_selection = select(aw_arr, self.route)
        aw_slice_direct = aw_arr[tuple(self.route._fields)]
        aw_slice_standard = aw_arr["i", "like", "trains"]

        # slice and select are the same in ALL entries
        # awkward.Array has no equal_all operation
        same_array = ak.all(
            [(aw_selection == aw_slice_direct) == (aw_selection == aw_slice_standard)])
        self.assertTrue(same_array)

        # should raise error if route does not exist
        self.assertRaises(ValueError, select, aw_arr,
                          ("does", "not", "exists"))

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
        self.assertFalse(c_util.Route() is self.empty_route._fields)

        # raise if input is not Sequence[str], str or Route
        self.assertRaises(Exception, c_util.Route, [0, 1., "2", None])
        self.assertRaises(Exception, c_util.Route, (0, 1, "2", None))
        self.assertRaises(Exception, c_util.Route, 0)

        # self._fields can not have a DOT in substring
        self.assertFalse(
            any("." in s for s in c_util.Route("i.like", "trains")))
        # same but with strings in Sequence
        self.assertFalse(
            any("." in s for s in c_util.Route(["i.like", "trains"])))

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
        same_route = c_util.Route(("i", "like", "trains"))
        self.assertTrue(hash(same_route) == hash(self.route) ==
                        hash(("i", "like", "trains")))

        # checks if not true when order is different
        reverse_route = c_util.Route(("trains", "like", "i"))
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
        self.assertEqual('test_string' + self.route,
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
        self.assertIsInstance(copy_slice, c_util.Route)
        self.assertIsNot(copy_slice, self.route)

    def test__setitem__(self):
        # SHOULD: replace values in self._fields
        self.route[0] = "replaced"
        self.assertEqual(self.route, ("replaced", "like", "trains"))

        # _fields should only hold "str" to be meaningful
        self.route[0] = None
        self.route[1] = 1
        self.route[2] = self.empty_route

        is_str_type = [isinstance(self.route[index], str)
                       for index in range(0, 3)]
        self.assertTrue(all(is_str_type), msg="Function enables to place elements that are not of type str")

    def test_add(self):
        # extend self._fields (a list)
        # if input is a...
        # - Sequence[str] -> extend str to _fields
        # - Route() instance -> extend Route._field
        # - str in DOT format

        input_as_sequence = ["i", "like", "trains"]
        input_in_dot = "i.like.trains"

        result = ("i", "like", "trains")

        # case: str sequence
        sequence_route = c_util.Route()
        sequence_route.add(input_as_sequence)
        # case: str in dot
        str_route = c_util.Route()
        str_route.add(input_in_dot)
        # combine Routes instances

        self.empty_route.add(str_route)
        # comparison of fields are the same
        self.assertTrue(tuple(sequence_route._fields) == tuple(
            str_route._fields) == tuple(self.empty_route._fields))

        # raise error if something else is added
        self.assertRaises(ValueError, self.empty_route.add, 0)
        self.assertRaises(ValueError, self.empty_route.add, [0, 1.])
        self.assertRaises(ValueError, self.empty_route.add, None)

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
        self.assertIsInstance(route_copy, c_util.Route)
        # not a reference
        self.assertIsNot(self.route, route_copy)
        # same fields
        self.assertEqual(tuple(self.route._fields), tuple(route_copy._fields))


class test_ArrayFunction(unittest.TestCase):
    # 2 standard dummy functions
    def add_function(self, arr):
        return arr + 100

    def empty_function(self, arr):
        return ak.zeros_like(arr)

    def setUp(self):
        test_array_dict = {"a_1": [0], "b_1": {"bb_1": [1], "bb_2": {"bbb_1": [2], "bbb_2": {
            "bbbb_1": {"bbbbb_1": [3]}, "bbbb_2": [4]}}}, "c_1": {"cc_1": [5]}}

        self.t_arr_A = ak.Array(test_array_dict)

        self.empty_arr_func = c_util.ArrayFunction.new(
            self.empty_function, "test_empty", "any_input_A", "all_empty")

        self.add_arr_func = c_util.ArrayFunction.new(
            self.add_function, "test_add", "any_input_B", "plus_100")

        self.combined_arr_func = c_util.ArrayFunction.new(self.empty_function, name="combined",
        uses=("met", "pT.all", self.empty_arr_func, self.add_arr_func),
            produces=(self.empty_arr_func, "pT.e"))

    def test_IOFlag(self):
        # SHOULD:   Create unique id for the class
        #           Value of the id is not important
        flag = c_util.ArrayFunction.IOFlag
        self.assertIsNot(flag.USES, flag.PRODUCES,
                         msg="USES and PRODUCES flag be different")
        self.assertIsNot(flag.USES, flag.AUTO,
                         msg="USES and AUTO flag be different")

    def test_has(self):
        # SHOULD:   True if name is already in cache

        c_util.ArrayFunction.new(self.empty_function, "cached")
        self.assertIn("cached", c_util.ArrayFunction._instances,
                      msg="Cache does not contain \"cached\"")

    def test_new(self):
        # SHOULD:   Create new instance of the class and adds its instance to the class cache.

        # Raise Error if multiple instances has same name
        with self.assertRaises(ValueError) as error:
            name = "same_name"
            c_util.ArrayFunction.new(
                self.empty_function, name)
            c_util.ArrayFunction.new(self.add_function, name)

        # new ArrayFunction should be in cache
        newly_af = c_util.ArrayFunction.new(
            self.empty_function, "new_af")
        self.assertIn("new_af", c_util.ArrayFunction._instances,
                      msg="ArrayFunction is missing in cache")

        # normal instance should not be in cache
        wrong_af = c_util.ArrayFunction(self.empty_function, "wrong_af")
        self.assertNotIn(
            "wrong_af", c_util.ArrayFunction._instances, msg="Array")

        # new ArrayFunction should be instance of ArrayFunction
        self.assertIsInstance(newly_af, c_util.ArrayFunction)

    def test_get(self):
        # SHOULD:   Returns a cached instance, if <copy> is True, another instance is returned

        # raise error if name is not in cache:
        with self.assertRaises(ValueError) as error:
            c_util.ArrayFunction.get("not_registered", copy=False)

        # get instance if copy is False, else create new copy
        instanced_empty = c_util.ArrayFunction.get("test_empty", copy=False)
        copy_empty = c_util.ArrayFunction.get("test_empty", copy=True)

        self.assertIs(instanced_empty, self.empty_arr_func,
                      msg="Is a copy, but shouldn\'t be a copy ")
        self.assertIsNot(copy_empty, self.empty_arr_func,
                         msg="Isn\' a copy, but should be a copy ")

        # return ArrayFunction
        self.assertIsInstance(instanced_empty, c_util.ArrayFunction,
                              msg="Is not instance of ArrayFunction, but should be")
        self.assertIsInstance(copy_empty, c_util.ArrayFunction,
                              msg="Is not instance of ArrayFunction, but should be")

    def test__init__(self):
        # SHOULD: init witout populate the cachhe

        # cache should not contain a key with name "not_used_new" of the instance
        arrayfunction = c_util.ArrayFunction(func=self.empty_function,
                                    name="not_used_new",
                                    uses="nothing",
                                    produces="nothing_also")
        self.assertNotIn("not_used_new", c_util.ArrayFunction._instances, msg="Cache is used, but this should not be the case")

        # self.name set correctly if not None
        self.assertEqual(arrayfunction.name, "not_used_new")

        # self.name is really set to self.func.__name__, which is the name of the function
        array_function_without_name = c_util.ArrayFunction(func=self.empty_function)
        self.assertEqual(array_function_without_name.name,
                         self.empty_function.__name__, msg="Name is not set to name of the function")


        # # TODO: NOT SURE ABOUT THIS, ASK MARCEL
        # # <func> can be any Callable, but ArrayFunction instance is also a callable
        # # Should ArrayFunction Instances be able to used as Argument?
        # # Solution? use name of class/function instead?
        # # instance x --> type(x).__name__ OR x x.__class__.__name__
        # with self.assertRaises(Exception) as error:
        #     array_function_with_array_function = c_util.ArrayFunction(
        #         func=array_function_without_name)

        # self.uses and self.produces should have empty sets if None is provided
        self.assertTrue(array_function_without_name.uses == set())

        # self.uses and self.producedes are the same function
        # check input: str, ArrayFunction, Sequence[str, ArrayFunction], Set[str, ArrayFunction]
        # law.util.make_list returns for all cases a list with the same content
        use_str = "nothing"
        uses_sequence_str_arrayfunction = [use_str, arrayfunction]

        arrayfunction_input = c_util.ArrayFunction(
            func=self.empty_function, uses=arrayfunction)
        sequence_input = c_util.ArrayFunction(
            func=self.empty_function, uses=uses_sequence_str_arrayfunction)
        set_input = c_util.ArrayFunction(
            func=self.empty_function, uses=set(uses_sequence_str_arrayfunction))

        # all cases return the correct result
        self.assertEqual(arrayfunction.uses, set([use_str]))
        self.assertEqual(arrayfunction_input.uses, set([arrayfunction]))
        self.assertEqual(sequence_input.uses, set(uses_sequence_str_arrayfunction))
        self.assertEqual(set_input.uses, set(uses_sequence_str_arrayfunction))

    def test_AUTO(self):
        # SHOULD:   Named tuple of instance and unique class id

        #           check uniquennes between instances
        self.assertIsNot(self.empty_arr_func.AUTO, self.add_arr_func.AUTO)

    def test_USES(self):
        # SHOULD:   see test_AUTO
        self.assertIsNot(self.empty_arr_func.USES, self.add_arr_func.USES)

    def test_PRODUCES(self):
        # SHOULD:   see test_AUTO
        self.assertIsNot(self.empty_arr_func.PRODUCES,
                         self.add_arr_func.PRODUCES)

    def test__get_columns(self):
        # SHOULD:   returns ALL *USES* or *PRODUCED* flagged columns depending on the used IOFlag
        #           if ArrayFunction is used in *uses* or *produces* their respective input should
        #           be found instead

        # create arr func with uses: "arr1, arr2, empty_arr_func.USES and add.arr_func.USES"
        flag = c_util.ArrayFunction.IOFlag

        used_columns = self.combined_arr_func._get_columns(
            io_flag=flag.USES)
        produced_columns = self.combined_arr_func._get_columns(
            io_flag=flag.PRODUCES)

        # raise error if flag is AUTO
        with self.assertRaises(ValueError) as error:
            auto = self.empty_arr_func._get_columns(io_flag=flag.AUTO)

        # return a Set
        self.assertIsInstance(used_columns, set, msg="Returned Object is not a set")
        self.assertIsInstance(produced_columns, set, msg="Returned Object is not a set")

        # everythin within Set is a string
        self.assertTrue(all(isinstance(column, str)
                        for column in used_columns.union(produced_columns)), msg="Not all columns are strings")

        # result should have USES: arr1, arr2, any_input_A, any_input_B
        # PRODUCES : "empty_arr", "pT.e"
        self.assertEqual(used_columns, set(["met", "pT.all", "any_input_A", "any_input_B"]))
        self.assertEqual(produced_columns, set(["pT.e", "all_empty"]))

    def test__get_used_columns(self):
        # if get_columns passes this will pass too
        pass

    def test_used_columns(self):
        # if get_columns passes this will pass too
        pass

    def test__get_produced_columns(self):
        # if get_columns passes this will pass too
        pass

    def test_produced_columns(self):
        # if get_columns passes this will pass too
        pass

    def test__repr__(self):
        # SHOULD:   create unique string representation

        # contains class name and name of the array function, as well as the hex-id of the instance
        string_representation = repr(self.empty_arr_func)
        hex_id = hex(id(self.empty_arr_func))
        class_name = self.empty_arr_func.__class__.__name__
        name = self.empty_arr_func.name
        all_items = (hex_id, class_name, name)

        # check if all information are withtin __repr__
        self.assertTrue(
            all([item in string_representation for item in all_items]))

    def test__call__(self):
        # SHOULD:   should pass arguments to the function in ArrayFunction.func

        # should have the same result
        # coudnt find another way to check equality in an awkward array.
        arr_func_result = self.empty_arr_func(self.t_arr).to_numpy()
        func_result = self.empty_function(self.t_arr).to_numpy()
        self.assertTrue(all(func_result == arr_func_result))
