# coding: utf-8


__all__ = ["RouteTest", "ArrayFunctionTest", "ColumnarUtilFunctionsTest"]


import unittest

from columnflow.util import maybe_import
from columnflow.columnar_util import (
    Route, ArrayFunction, get_ak_routes, has_ak_column, set_ak_column, remove_ak_column,
    add_ak_alias, add_ak_aliases, update_ak_array, flatten_ak_array, sort_ak_fields,
    attach_behavior, layout_ak_array, flat_np_view,
)

np = maybe_import("numpy")
ak = maybe_import("awkward")
dak = maybe_import("dask_awkward")
coffea = maybe_import("coffea")


class RouteTest(unittest.TestCase):

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
        # right-add
        self.assertEqual("hi" + self.route, ("hi", "i", "like", "trains"))
        self.assertEqual(("hi", "there") + self.route, ("hi", "there", "i", "like", "trains"))

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


class ArrayFunctionTest(unittest.TestCase):

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
        self.data = {
            "event": [123123],
            "Jet": {
                "pt": [[45.0]],
                "eta": [[2.5]],
            },
            "Muon": {
                "pt": [[45.0, 55.0]],
                "eta": [[1.9, -0.5]],
            },
            "Jet_pt": [0.0],
        }
        self.array = ak.Array(self.data)
        self.empty_array = ak.Array([])

    def test_get_ak_routes(self):
        # check return types
        self.assertIsInstance(get_ak_routes(self.array), list)
        self.assertIsInstance(get_ak_routes(self.array)[0], Route)

        # check empty case, including max depth
        self.assertEqual(tuple(get_ak_routes(self.empty_array)), ())
        self.assertEqual(tuple(get_ak_routes(self.empty_array, 1)), ())
        self.assertEqual(tuple(get_ak_routes(self.empty_array, -1)), ())

        # check return values
        self.assertEqual(
            set(r.column for r in get_ak_routes(self.array)),
            {"event", "Jet.pt", "Jet.eta", "Muon.pt", "Muon.eta", "Jet_pt"},
        )
        self.assertEqual(
            set(r.column for r in get_ak_routes(self.array, 0)),
            {"event", "Jet.pt", "Jet.eta", "Muon.pt", "Muon.eta", "Jet_pt"},
        )
        self.assertEqual(
            set(r.column for r in get_ak_routes(self.array, 1)),
            {"event", "Jet", "Muon", "Jet_pt"},
        )
        self.assertEqual(
            set(r.column for r in get_ak_routes(self.array, -1)),
            {"Jet", "Muon"},
        )
        self.assertEqual(
            set(r.column for r in get_ak_routes(self.array, -2)),
            set(),
        )

    def test_has_ak_column(self):
        # check return types
        self.assertIsInstance(has_ak_column(self.array, "event"), bool)
        self.assertIsInstance(has_ak_column(self.array, "not_existing"), bool)

        # check return values
        self.assertTrue(has_ak_column(self.array, "event"))
        self.assertTrue(has_ak_column(self.array, "Jet"))
        self.assertTrue(has_ak_column(self.array, "Jet.pt"))
        self.assertTrue(has_ak_column(self.array, ("Jet", "pt")))
        self.assertTrue(has_ak_column(self.array, Route("Jet.pt")))
        self.assertTrue(has_ak_column(self.array, "Jet_pt"))
        self.assertFalse(has_ak_column(self.array, "not_existing"))
        self.assertFalse(has_ak_column(self.array, ""))
        self.assertFalse(has_ak_column(self.array, ()))

    def test_set_ak_column(self):
        # trivial case
        arr = set_ak_column(self.array, "lumi", [3456])
        self.assertTrue(has_ak_column(arr, "lumi"))
        self.test_get_ak_routes()

        # empty cases
        with self.assertRaises(ValueError):
            set_ak_column(self.array, "", [3456])
        self.test_get_ak_routes()

        # overwriting cases
        arr = set_ak_column(self.array, "event", [3456])
        self.assertEqual(arr.event[0], 3456)
        arr = set_ak_column(self.array, "Jet.pt", [[20.0, 30.0]])
        self.assertEqual(tuple(arr.Jet.pt[0]), (20.0, 30.0))
        arr = set_ak_column(self.array, "Jet.phi", [[0.1, 3.1]])
        self.assertEqual(tuple(arr.Jet.phi[0]), (0.1, 3.1))
        self.test_get_ak_routes()

        # deep addition
        arr = set_ak_column(self.array, "Electron.selected.pt", [[20.0, 30.0]])
        self.assertEqual(tuple(arr.Electron.selected.pt[0]), (20.0, 30.0))
        arr = set_ak_column(self.array, "Electron.selected.phi", [[0.1, 3.1]])
        self.assertEqual(tuple(arr.Electron.selected.phi[0]), (0.1, 3.1))

        # overwrite nested columns with single flat column
        arr = set_ak_column(self.array, "Jet", [111])
        self.assertEqual(
            set(r.column for r in get_ak_routes(arr)),
            {"event", "Jet", "Muon.pt", "Muon.eta", "Jet_pt"},
        )
        self.assertEqual(arr.Jet[0], 111)

        # value_type
        arr = set_ak_column(self.array, "Electron.pt", [[20.0, 30.0]])
        self.assertEqual(str(ak.type(arr.Electron.pt).content), "var * float64")
        arr = set_ak_column(self.array, "Electron.pt", [[20.0, 30.0]], value_type=np.float32)
        self.assertEqual(str(ak.type(arr.Electron.pt).content), "var * float32")

    def test_remove_ak_column(self):
        # removal at top level
        arr = remove_ak_column(self.array, "Jet_pt")
        self.assertEqual(
            set(r.column for r in get_ak_routes(arr)),
            {"event", "Jet.pt", "Jet.eta", "Muon.pt", "Muon.eta"},
        )

        # original array is unchangned
        self.test_get_ak_routes()

        # removal at level 1
        arr = remove_ak_column(self.array, "Jet")
        self.assertEqual(
            set(r.column for r in get_ak_routes(arr)),
            {"event", "Muon.pt", "Muon.eta", "Jet_pt"},
        )
        arr = remove_ak_column(arr, "Muon")
        self.assertEqual(
            set(r.column for r in get_ak_routes(arr)),
            {"event", "Jet_pt"},
        )
        self.test_get_ak_routes()

        # removal at level 2
        arr = remove_ak_column(self.array, "Jet.pt")
        self.assertEqual(
            set(r.column for r in get_ak_routes(arr)),
            {"event", "Jet.eta", "Muon.pt", "Muon.eta", "Jet_pt"},
        )
        arr = remove_ak_column(arr, "Jet.eta")
        self.assertEqual(
            set(r.column for r in get_ak_routes(arr)),
            {"event", "Muon.pt", "Muon.eta", "Jet_pt"},
        )
        self.test_get_ak_routes()

        # removal at level 2, not removing empty columns
        arr = remove_ak_column(self.array, "Jet.pt")
        arr = remove_ak_column(arr, "Jet.eta", remove_empty=False)
        self.assertEqual(
            set(r.column for r in get_ak_routes(arr)),
            {"event", "Jet", "Muon.pt", "Muon.eta", "Jet_pt"},
        )
        arr = remove_ak_column(arr, "Jet")
        self.assertEqual(
            set(r.column for r in get_ak_routes(arr)),
            {"event", "Muon.pt", "Muon.eta", "Jet_pt"},
        )
        self.test_get_ak_routes()

        # error handling
        with self.assertRaises(ValueError):
            remove_ak_column(self.array, "not_existing")
        remove_ak_column(self.array, "not_existing", silent=True)

        with self.assertRaises(ValueError):
            remove_ak_column(self.array, "")
        remove_ak_column(self.array, "", silent=True)

    def test_add_ak_alias(self):
        # simple case
        arr = add_ak_alias(self.array, "Muon.pt", "Muon_pt")
        self.assertEqual(
            set(r.column for r in get_ak_routes(arr)),
            {"event", "Jet.pt", "Jet.eta", "Muon.pt", "Muon.eta", "Jet_pt", "Muon_pt"},
        )
        self.assertEqual(tuple(arr.Muon.pt.to_list()[0]), tuple(arr.Muon_pt.to_list()[0]))
        self.test_get_ak_routes()

        # remove old columns
        arr = add_ak_alias(self.array, "Muon.pt", "Muon_pt", remove_src=True)
        self.assertEqual(
            set(r.column for r in get_ak_routes(arr)),
            {"event", "Jet.pt", "Jet.eta", "Muon.eta", "Jet_pt", "Muon_pt"},
        )
        arr = add_ak_alias(arr, "Muon.eta", "Muon_eta", remove_src=True)
        self.assertEqual(
            set(r.column for r in get_ak_routes(arr)),
            {"event", "Jet.pt", "Jet.eta", "Jet_pt", "Muon_pt", "Muon_eta"},
        )
        self.test_get_ak_routes()

        # add multiple aliases
        arr = add_ak_aliases(self.array, {"Muon_pt": "Muon.pt", "Muon_eta": "Muon.eta"})
        self.assertEqual(
            set(r.column for r in get_ak_routes(arr)),
            {"event", "Jet.pt", "Jet.eta", "Muon.pt", "Muon.eta", "Jet_pt", "Muon_pt", "Muon_eta"},
        )
        self.assertEqual(tuple(arr.Muon.pt.to_list()[0]), tuple(arr.Muon_pt.to_list()[0]))
        self.assertEqual(tuple(arr.Muon.eta.to_list()[0]), tuple(arr.Muon_eta.to_list()[0]))
        self.test_get_ak_routes()

        # remove multiple
        arr = add_ak_aliases(self.array, {"Muon_pt": "Muon.pt", "Muon_eta": "Muon.eta"}, remove_src=True)
        self.assertEqual(
            set(r.column for r in get_ak_routes(arr)),
            {"event", "Jet.pt", "Jet.eta", "Jet_pt", "Muon_pt", "Muon_eta"},
        )
        self.test_get_ak_routes()

        # error handling
        with self.assertRaises(ValueError):
            add_ak_alias(self.array, "not_existing", "")
        with self.assertRaises(ValueError):
            add_ak_alias(self.array, "not_existing", "Muon_pt")
        add_ak_alias(self.array, "Jet.pt", "Jet_pt")

    def test_update_ak_array(self):
        # define additional content
        arr_disjoint = ak.Array({
            "run": [123],
            "Electron": {
                "pt": [[12.0, 27.0]],
                "eta": [[0.1, -0.7]],
            },
        })
        arr_overlap = ak.Array({
            "run": [123],
            "event": [999],
            "Muon": {
                "pt": [[99.0, 42.0]],
            },
        })
        arr_concat = ak.Array({
            "event": [999],
            "Muon": {
                "pt": [[99.0, 42.0]],
                "eta": [[0.6, -1.9]],
            },
        })

        # trivial case
        arr = update_ak_array(self.array)
        self.assertEqual(
            set(r.column for r in get_ak_routes(arr)),
            {"event", "Jet.pt", "Jet.eta", "Muon.pt", "Muon.eta", "Jet_pt"},
        )
        self.test_get_ak_routes()

        # disjoint update
        arr = update_ak_array(self.array, arr_disjoint)
        self.assertEqual(
            set(r.column for r in get_ak_routes(arr)),
            {"event", "run", "Jet.pt", "Jet.eta", "Muon.pt", "Muon.eta", "Electron.pt", "Electron.eta", "Jet_pt"},
        )
        self.test_get_ak_routes()

        # overlap update
        arr = update_ak_array(self.array, arr_overlap)
        self.assertEqual(
            set(r.column for r in get_ak_routes(arr)),
            {"event", "run", "Jet.pt", "Jet.eta", "Muon.pt", "Muon.eta", "Jet_pt"},
        )
        self.assertEqual(arr.event[0], arr_overlap.event[0])
        self.test_get_ak_routes()

        # overlap update with overwrite flags
        arr = update_ak_array(self.array, arr_overlap, overwrite_routes=["Muon.*"])
        self.assertEqual(
            set(r.column for r in get_ak_routes(arr)),
            {"event", "run", "Jet.pt", "Jet.eta", "Muon.pt", "Muon.eta", "Jet_pt"},
        )
        self.assertEqual(arr.event[0], self.data["event"][0])
        self.test_get_ak_routes()

        # overlap update with add flags
        arr = update_ak_array(self.array, arr_overlap, overwrite_routes=["event"], add_routes=["Muon.*"])
        self.assertEqual(
            set(r.column for r in get_ak_routes(arr)),
            {"event", "run", "Jet.pt", "Jet.eta", "Muon.pt", "Muon.eta", "Jet_pt"},
        )
        self.assertEqual(arr.event[0], arr_overlap.event[0])
        self.assertEqual(
            tuple(arr.Muon.pt[0]),
            (
                self.array.Muon.pt[0, 0] + arr_overlap.Muon.pt[0, 0],
                self.array.Muon.pt[0, 1] + arr_overlap.Muon.pt[0, 1],
            ),
        )
        self.test_get_ak_routes()

        # concatenation update with flags
        arr = update_ak_array(self.array, arr_concat, concat_routes=["Muon.*"])
        self.assertEqual(len(arr.Muon.pt[0]), 4)
        self.assertEqual(
            set(r.column for r in get_ak_routes(arr)),
            {"event", "Jet.pt", "Jet.eta", "Muon.pt", "Muon.eta", "Jet_pt"},
        )
        self.test_get_ak_routes()

    def test_flatten_ak_array(self):
        # default case
        arr = flatten_ak_array(self.array)
        self.assertEqual(
            set(arr.keys()),
            {"event", "Jet.pt", "Jet.eta", "Muon.pt", "Muon.eta", "Jet_pt"},
        )

        # nano format
        arr = flatten_ak_array(self.array, nano_format=True)
        self.assertEqual(
            set(arr.keys()),
            {"event", "Jet_pt", "Jet_eta", "Muon_pt", "Muon_eta"},
        )

        # empty case
        arr = flatten_ak_array(self.empty_array)
        self.assertEqual(set(arr.keys()), set())

        # filter case
        arr = flatten_ak_array(self.array, routes=["event", "Muon.*"])
        self.assertEqual(
            set(arr.keys()),
            {"event", "Muon.pt", "Muon.eta"},
        )
        arr = flatten_ak_array(self.array, routes=lambda column: column.startswith("Jet"))
        self.assertEqual(
            set(arr.keys()),
            {"Jet.pt", "Jet.eta", "Jet_pt"},
        )

    def test_sort_ak_fields(self):
        # unsorted
        self.assertEqual(
            tuple(route.column for route in get_ak_routes(self.array)),
            ("event", "Jet_pt", "Jet.pt", "Jet.eta", "Muon.pt", "Muon.eta"),
        )

        # sorted
        self.assertEqual(
            tuple(route.column for route in get_ak_routes(sort_ak_fields(self.array))),
            ("Jet_pt", "event", "Jet.eta", "Jet.pt", "Muon.eta", "Muon.pt"),
        )

        # sorted with default sorting function
        sort_fn = lambda s: s[::-1]
        self.assertEqual(
            tuple(route.column for route in get_ak_routes(sort_ak_fields(self.array, sort_fn=sort_fn))),
            ("event", "Jet_pt", "Muon.eta", "Muon.pt", "Jet.eta", "Jet.pt"),
        )

    def test_attach_behavior(self):
        arr = update_ak_array(self.array, ak.Array({
            "run": [123],
            "luminosityBlock": [123],
            "Jet": {"phi": [[0.3]], "mass": [[4.5]]},
        }))

        # fake a valid array source and add the coffea nano schema
        src = flatten_ak_array(arr, nano_format=True)
        metadata = {"uuid": "some_uuid_123123", "num_rows": len(arr), "object_path": "not_existing"}
        setattr(src, "metadata", metadata)
        events = coffea.nanoevents.NanoEventsFactory.from_preloaded(src).events()
        self.assertEqual(len(events.Jet.p), 1)

        # transfer the behavior over to arr
        jet = arr.Jet
        with self.assertRaises(AttributeError):
            jet.p

        jet = attach_behavior(jet, "Jet", events.behavior)
        self.assertEqual(len(jet.p), 1)

    def test_layout_ak_array(self):
        arr = ak.Array({"Muon": {"pt": [[45.0, 55.0], [10.0, 20.0, 30.0]]}})

        # apply the same layout to a flat array
        other_pt = layout_ak_array(np.array([1, 2, 3, 4, 5]), arr.Muon.pt)
        self.assertEqual(len(other_pt), 2)
        self.assertEqual(len(other_pt[0]), 2)
        self.assertEqual(len(other_pt[1]), 3)

    def test_flat_np_view(self):
        arr = update_ak_array(self.array, ak.Array({"run": [123]}))
        muon_pt = flat_np_view(arr.Muon.pt, axis=1)

        self.assertEqual(tuple(arr.Muon.pt[0]), (45.0, 55.0))
        self.assertEqual(tuple(muon_pt), (45.0, 55.0))

        # test outer axis
        self.assertEqual(tuple(flat_np_view(arr.Muon.pt, axis=0)[0]), (45.0, 55.0))

        # scale values in the view and check again
        muon_pt *= 2.0
        self.assertEqual(tuple(arr.Muon.pt[0]), (90.0, 110.0))
        self.assertEqual(tuple(muon_pt), (90.0, 110.0))

        # error handling
        with self.assertRaises(ValueError):
            flat_np_view(arr.Muon.pt, axis=None)
