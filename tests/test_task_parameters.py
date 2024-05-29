# coding: utf-8


__all__ = ["TaskParametersTest"]

import unittest

from columnflow.tasks.framework.parameters import SettingsParameter, MultiSettingsParameter


class TaskParametersTest(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def test_settings_parameter(self):
        # check that the default delimiters have not been changed
        self.assertEqual(SettingsParameter.settings_delimiter, "=")
        self.assertEqual(SettingsParameter.tuple_delimiter, ";")

        # initialize a SettingParameter
        p = SettingsParameter()

        # parsing
        self.assertEqual(p.parse(""), {})
        self.assertEqual(p.parse("A"), {"A": True})
        self.assertEqual(
            p.parse("param1=10,param2,param3=text,param4=false"),
            {"param1": 10.0, "param2": True, "param3": "text", "param4": False},
        )
        self.assertEqual(
            # parsing of lists of values, separated via ";"
            p.parse("param1=1;2;3j;4j,param2=a;b;true;false"),
            {"param1": (1, 2, 3j, 4j), "param2": ("a", "b", True, False)},
        )
        self.assertEqual(
            # if a parameter is set multiple times, prioritize last one
            p.parse("A=1,B,A=2"),
            {"B": True, "A": 2.0},
        )

        # serializing
        self.assertEqual(
            p.serialize({"param1": 2, "param2": False}),
            "param1=2,param2=False",
        )
        print(p.serialize({"param1": [1, 2j, "A", True, False]}))
        self.assertEqual(
            p.serialize({"param1": [1, 2j, "A", True, False]}),
            "param1=1;2j;A;True;False",
        )

    def test_multi_settings_parameter(self):
        # initialize a MultiSettingsParameter
        p = MultiSettingsParameter()

        # parsing
        self.assertEqual(p.parse(""), {})
        self.assertEqual(p.parse("A"), {"A": {}})
        self.assertEqual(p.parse("A,B"), {"A": {"B": True}})
        self.assertEqual(
            p.parse("obj1,k1=10,k2,k3=text:obj2,k4=false"),
            {"obj1": {"k1": 10.0, "k2": True, "k3": "text"}, "obj2": {"k4": False}},
        )
        self.assertEqual(
            # parsing of lists of values, separated via ";"
            p.parse("obj1,k1=1;2;3j;4j,k2=a;b;true;false:obj2,k3=5;6;x;y"),
            {
                "obj1": {"k1": (1, 2, 3j, 4j), "k2": ("a", "b", True, False)},
                "obj2": {"k3": (5, 6, "x", "y")},
            },
        )
        self.assertEqual(
            # providing the same key twice results in once combined dict
            p.parse("tt,A=2:st,A=2:tt,B=True"),
            {"tt": {"A": 2.0, "B": True}, "st": {"A": 2.0}},
        )

        # serializing
        self.assertEqual(
            p.serialize({"obj1": {"k1": "val"}, "obj2": {"k2": 2}}),
            "obj1,k1=val:obj2,k2=2",
        )
