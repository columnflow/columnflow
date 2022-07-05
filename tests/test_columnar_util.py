# coding: utf-8


__all__ = ["ColumnarUtilTest"]


import unittest
from ap.columnar_util import add_ak_alias
import ap.columnar_util as c_util
from typing import Optional, Union, Sequence, Set, Tuple, List, Dict, Callable, Any


class test_Route(unittest.TestCase):



    def setUp(self):

        self.Route = c_util.Route()

  #     ak_array1 = ak.Array([array1_content])

    #     assert len(Route("b_bb1").fields) == 1
    #     assert len(Route("b.bb1").fields) == 2

        ak_array1_route1_str = "a"
        ak_array1_route2_str = "b.bb1"
        ak_array1_wrongroute1_str = "b_bb1"
        ak_array1_route3_str = "b.bb2.bbb2.bbbb1.b_bbbb1"
        route1_str=c_util.Route(ak_array1_route1_str)
        print("route1 str", route1_str, route1_str._fields, route1_str.fields)
        print("route1_str column and nano column", route1_str.column, route1_str.nano_column)

        route2_str=c_util.Route(ak_array1_route2_str)
        print("route2 str", route2_str, route2_str._fields, route2_str.fields)
        print("route2_str column and nano column", route2_str.column, route2_str.nano_column)

        wrongroute1_str=c_util.Route(ak_array1_wrongroute1_str)
        # print("wrongroute1 str", wrongroute1_str, wrongroute1_str._fields, wrongroute1_str.fields)
        # print("wrongroute1 str results", ak_array1[wrongroute1_str.fields])

        route3_str=c_util.Route(ak_array1_route3_str)
        print("route3_str column and nano column", route3_str.column, route3_str.nano_column)




    def tearDown(self):
        pass

    def test_join(self):
        test_subject = tuple("i", "like", "trains")
        self.assertEqual(self.Route.join(test_subject, "i.like.trains"))
        self.assertEqual(self.Route.join(list(test_subject), "i.like.trains"))
        self.assertEqual(self.Route.join("i, like, trains", "i.like.trains"))

    def test_join_nano(self):
        self.assertEqual(self.Route.join_nano(tuple("i", "like", "trains"), "i_like_trains"))
        self.assertEqual(self.Route.join_nano(list("i", "like", "trains"), "i_like_trains"))
        self.assertEqual(self.Route.join_nano("i, like, trains", "i_like_trains"))


    def test_split(self):
        split = self.Route.split
        self.assertEqual(tuple(split("i.like.trains")), tuple("i", "like", "trains") )
        self.assertEqual(split("i.like.trains"), list("i", "like", "trains") )


    def test_split_nano(self):
        self.assertEqual(tuple(self.Route.split("i_like_trains")), tuple("i", "like", "trains") )
        self.assertEqual(self.Route.split("i_like_trains"), list("i", "like", "trains") )

    def test_check(self):

        # RouteInstance --> RouteInstance
        self.assertIsInstance(self.Route.check(self.Route), c_util.Route)
        # Sequence[str] --> RouteInstance
        self.assertIsinstance(self.Route.check(tuple("i","like","trains")), c_util.Route)
        # str --> RouteInstance
        self.assertIsinstance(self.Route.check("i","like","trains"), c_util.Route)

        # check if fields are the same
        # print(c_util.Route.check(ak_array1_route2_list), c_util.Route.check(
        #     ak_array1_route2_list) == ak_array1_route2_list)
        # print(c_util.Route.check(ak_array1_route2_list) == route2_list)
        # print(c_util.Route.check(route2_list),
        #       c_util.Route.check(route2_list) == route2_list)
        # print(c_util.Route.check(ak_array1_route2_list).fields, c_util.Route.check(
        #     ak_array1_route2_list).fields == ak_array1_route2_list)
        # print(c_util.Route.check(ak_array1_route2_list).fields == route2_list.fields)
        # print(c_util.Route.check(route2_list).fields, c_util.Route.check(
        #     route2_list).fields == route2_list.fields)
        # print(hash(c_util.Route.check(route2_list)), hash(route2_list))
        # print(hash(c_util.Route.check(ak_array1_route2_list)), hash(route2_list))
        # # print(hash(c_util.Route.check(ak_array1_route2_list)),hash(ak_array1_route2_list))

    def test_select(self):
        pass

    def test_pop(self):
        self.Route.add("a1.b1",)
        # remove entry at index from self.field list


        # print(route3_str, route3_str.pop(0))
        # print(route3_str, route3_str.pop(1))
        # #default is -1
        # print(route3_str, route3_str.pop())
        # # useless, as pop occurs before print in the print statement, thought as a check of the removal of fields elements through pop
        # print(route3_str)

    def test_reverse(self):
        # test reverse
        print("\n test reverse \n")
        # route3_str = Route(ak_array1_route3_str)
        # print(route3_str)
        # route3_str.reverse()
        # print(route3_str)



    def test_copy(self):
        # test copy
        print("\n test copy \n")
        # first print: the copy object is different from the original object
        # second print: using two times copy gives each time a new object (as pop would else reduice the number of elements)
        # third print: use pop and check if outcome is as it should be
    #     print(route3_str, route3_str.copy(), route3_str.copy().pop())
    #     # are objects identical? as well their fields as the fact that they are objects of the class Route with the attribute fields
    #     print(route3_str == route3_str.copy(),
    #           route3_str.fields == route3_str.copy().fields)
    #     # useless as pop occurs before print in the print statement
    #     print(route3_str)

    #     # test copy
    #     print("\n test inner/intrinsic params \n")
    #     route3_str = Route(ak_array1_route3_str)
    #     print(route3_str, route3_str.fields)
    #     print("str", str(route3_str))
    #     # "official" object representation in string format
    #     print("repr", repr(route3_str))
    #     print("repr string", repr("b.bb2.bbb2.bbbb1.b_bbbb1"),
    #           repr(ak_array1_route3_str))
    #     print("hash", hash(route3_str), "hash equalities string", hash(route3_str) == hash(ak_array1_route3_str), hash(route3_str) == hash("b.bb2.bbb2.bbbb1.b_bbbb1"), "hash equalities tuple", hash(
    #         route3_str) == hash(ak_array1_route3_tuple), hash(route3_str) == hash(("b", "bb2", "bbb2", "bbbb1", "b_bbbb1")), hash(route3_str) == hash(route3_str.fields))
    #     print("len", len(route3_str))
    #     print("eq", "with route", route3_str == route3_str, "with str in dot format", route3_str == "b.bb2.bbb2.bbbb1.b_bbbb1", "with str in underscore format", route3_str == "b_bb2_bbb2_bbbb1_b_bbbb1", "with list",
    #           route3_str == ["b", "bb2", "bbb2", "bbbb1", "b_bbbb1"], "with tuple", route3_str == ("b", "bb2", "bbb2", "bbbb1", "b_bbbb1"), "with anything else, e.g. its hash", route3_str == hash(route3_str))
    #     print("bool", bool(route3_str), ", for an empty list:", bool(Route()))
    #     # == python2 version of bool no test needed, already in bool
    #     print("nonzero", "see bool")
    #     print("add", route3_str + "foo")
    #     print(route3_str)
    #     print("radd", "bar" + route3_str)
    #     print(route3_str)
    #     route3_str += "rhabarberbarbarabarbarbarenbartbarbier"
    #     print("iadd", route3_str)
    #     print("getitem item -1", route3_str[-1], "item 1", route3_str[1])
    #     route3_str[-1] = "shorter"
    #     print("setitem", route3_str)


    # def test_add(self):
    #     # extend self._fields of Route Instance with
    #     # - other Route.field entries
    #     # - str seqeuence
    #     # - str in dot sequence

    #     other_route = c_util.Route([])
    #     route4_str=Route("b")
    #     route4_str.add("bb1")
    #     print("route4_str", route4_str.fields,ak_array1[route4_str.fields])

    #     route5_str=Route("b")
    #     route5_str.add("bb2")
    #     route5_str.add("bbb2.bbbb1.b_bbbb1")
    #     print("route5_str", route5_str.fields,ak_array1[route5_str.fields])

    #     # wrongroute5_str=Route("b")
    #     # wrongroute5_str.add("bb2")
    #     # wrongroute5_str.add("bbb2.bbbb1.b_bbbb2")
    #     # print("wrongroute5_str", wrongroute5_str.fields,ak_array1[wrongroute5_str.fields])

    #     print("\n now we go to the tuples:\n ")

    #     ak_array1_route1_tuple = ("a",)
    #     ak_array1_route2_tuple = ("b", "bb1")
    #     ak_array1_wrongroute1_tuple = ("a", "b")
    #     ak_array1_wrongroute2_tuple = ("a", "bb1")
    #     ak_array1_route3_tuple = ("b", "bb2", "bbb2", "bbbb1", "b_bbbb1")
    #     route1_tuple=Route(ak_array1_route1_tuple)
    #     print("route1 tuple", route1_tuple, route1_tuple._fields, route1_tuple.fields)
    #     print("route1_tuple column and nano column", route1_tuple.column, route1_tuple.nano_column)
    #     print("route1 tuple results", ak_array1[route1_tuple.fields])

    #     route2_tuple=Route(ak_array1_route2_tuple)
    #     print("route2 tuple", route2_tuple, route2_tuple._fields, route2_tuple.fields)
    #     print("route2_tuple column and nano column", route2_tuple.column, route2_tuple.nano_column)
    #     print("route2 tuple results", ak_array1[route2_tuple.fields])

    #     wrongroute1_tuple=Route(ak_array1_wrongroute1_tuple)
    #     # print("wrongroute1 tuple", wrongroute1_tuple, wrongroute1_tuple._fields, wrongroute1_tuple.fields)
    #     # print("wrongroute1 tuple results", ak_array1[wrongroute1_tuple.fields])

    #     route3_tuple=Route(ak_array1_route3_tuple)
    #     print("route3_tuple", route3_tuple.fields,ak_array1[route3_tuple.fields])
    #     print("route3_tuple column and nano column", route3_tuple.column, route3_tuple.nano_column)

    #     #test add:
    #     route4_tuple=Route(("b",))
    #     route4_tuple.add(("bb1",))
    #     print("route4_tuple", route4_tuple.fields,ak_array1[route4_tuple.fields])

    #     route5_tuple=Route(("b",))
    #     route5_tuple.add(("bb2",))
    #     route5_tuple.add(("bbb2", "bbbb1", "b_bbbb1"))
    #     print("route5_tuple", route5_tuple.fields,ak_array1[route5_tuple.fields])

    #     # wrongroute5_tuple=Route(("b",))
    #     # wrongroute5_tuple.add(("bb2",))
    #     # wrongroute5_tuple.add(("bbb2", "bbbb1", "b_bbbb2"))
    #     # print("wrongroute5_tuple", wrongroute5_tuple.fields,ak_array1[wrongroute5_tuple.fields])

    #     print("\n now we go to the lists:\n ")

    #     ak_array1_route1_list = ["a"]
    #     ak_array1_route2_list = ["b", "bb1"]
    #     ak_array1_wrongroute1_list = ["a", "b"]
    #     ak_array1_wrongroute2_list = ["a", "bb1"]
    #     ak_array1_route3_list = ["b", "bb2", "bbb2", "bbbb1", "b_bbbb1"]
    #     route1_list=Route(ak_array1_route1_list)
    #     print("route1 list", route1_list, route1_list._fields, route1_list.fields)
    #     print("route1_list column and nano column", route1_list.column, route1_list.nano_column)
    #     print("route1 list results", ak_array1[route1_list.fields])

    #     route2_list=Route(ak_array1_route2_list)
    #     print("route2 list", route2_list, route2_list._fields, route2_list.fields)
    #     print("route2_list column and nano column", route2_list.column, route2_list.nano_column)
    #     print("route2 list results", ak_array1[route2_list.fields])

    #     wrongroute1_list=Route(ak_array1_wrongroute1_list)
    #     # print("wrongroute1 list", wrongroute1_list, wrongroute1_list._fields, wrongroute1_list.fields)
    #     # print("wrongroute1 list results", ak_array1[wrongroute1_list.fields])

    #     route3_list=Route(ak_array1_route3_list)
    #     print("route3_list", route3_list.fields,ak_array1[route3_list.fields])
    #     print("route3_list column and nano column", route3_list.column, route3_list.nano_column)

    #     #test add:
    #     route4_list=Route(["b"])
    #     route4_list.add(["bb1"])
    #     print("route4_list", route4_list.fields,ak_array1[route4_list.fields])

    #     route5_list=Route(["b"])
    #     route5_list.add(["bb2"])
    #     route5_list.add(["bbb2", "bbbb1", "b_bbbb1"])
    #     print("route5_list", route5_list.fields,ak_array1[route5_list.fields])

    #     # wrongroute5_list=Route(["b"])
    #     # wrongroute5_list.add(["bb2"])
    #     # wrongroute5_list.add(["bbb2", "bbbb1", "b_bbbb2"])
    #     # print("wrongroute5_list", wrongroute5_list.fields,ak_array1[wrongroute5_list.fields])

    #     # check if standard split function has been overwritten
    #     print("\n try with whitespaces:\n ")
    #     ak_array1_route3_whitespaces = "b bb2 bbb2 bbbb1 b_bbbb1"
    #     ak_array1_route3_newlines = "b\nbb2\nbbb2\nbbbb1\nb_bbbb1"
    #     route3_whitespaces=Route(ak_array1_route3_whitespaces)
    #     route3_newlines=Route(ak_array1_route3_whitespaces)
    #     print("route3_whitespaces", route3_whitespaces.fields)#,ak_array1[route3_whitespaces.fields])
    #     print("route3_newlines", route3_newlines.fields)#,ak_array1[route3_newlines.fields])

    #     print("\n try with weird symbols:\n ")

    #     array2_content={"/!:;{}[]\ ":{"_-+*#`?=()$%§,¿¯≠·˜^£¢¶¬":0}}
    #     ak_array2=ak.Array([array2_content])
    #     ak_array2_route1 = "/!:;{}[]\ ._-+*#`?=()$%§,¿¯≠·˜^£¢¶¬"
    #     route1_weird_symbols=Route(ak_array2_route1)
    #     print("route1 with weird symbols", route1_weird_symbols,route1_weird_symbols.fields)
    #     print("route1 with weird symbols results", ak_array2[route1_weird_symbols.fields])

        # !! Teste ob es mit is klappt!

        # + test all submethods:
        # __str__, __repr__, __hash__, __len__,
        # __eq__, __bool__, __nonzero__, __add__, __radd__, __iadd__, __getitem__, __setitem__

        # questions:
        # - funktioniert es mit str, tuple, list?
        # - teste auch str, tuple, list für die add Methode

        # für all diese:
        # - teste für strings mit underscores, punkte, whitespaces, newlines as delimiter
        # - teste some random combination von Sonderzeichen mit einem punkt irgendwo dazwischen
        # -

        # - check if you can access the arrays in a simple way using the routes and the results are accurate
        # -

        # Fragen Marcel: ist die Struktur nur Punkte als delimiter, oder werden auch whitespaces akzeptiert?
        # 2. Frage: was soll in die Tests der Klasse? e.g. keine spezifische Exception ausgegeben wenn Route nicht existiert, only in has_ak_column
        #           maybe at least to be written as comment in the tests?   -> tests machen, aber nur mit Methode aus der Klasse auflösen
        # 3. Frage: == tells you that the Route objects are the same as the corresponding field list... to change, to get info on Route/simple list?
        #          if so, how to differentiate between a Route object and a list for example?  --> type check?   -> use is, not ==
        # 4. Frage: organisation tests for a class: also in methods or all in one with comments for each section?
        # 5. frage: tests kommentieren?     -> nur wenn notwendig
        # 6. Frage what means the difference in implementation between add and iadd? simply that we use a copy?

        # define all of these routes as object of the class Route and look at the properties/attributes/methods
        # TODO


# class ColumnarUtilTest(unittest.TestCase):

# #
# a = {"a": 0, "c_1": 1, "b": {"bb1": 1,
#          "bb2": {"bbb1": 2, "bbb2": {"bbbb1": {"b_bbbb1": 4}, "bbbb2": 3}}}, "d": {"d_1": 1}}
