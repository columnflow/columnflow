# coding: utf-8

"""
Custom luigi parameters.
"""

from typing import Union

import law
from columnflow.util import test_float


class SettingsParameterBase():
    """
    Base class to implement class methods for different types of setting parameters.
    """
    @classmethod
    def parse_setting(self, setting: str) -> tuple[str, Union[float, bool, str]]:
        pair = setting.split("=", 1)
        key, value = pair if len(pair) == 2 else (pair[0], "True")
        if test_float(value):
            value = float(value)
        elif value.lower() == "true":
            value = True
        elif value.lower() == "false":
            value = False
        return (key, value)

    @classmethod
    def serialize_setting(self, name: str, value: str) -> str:
        return f"{name}={value}"


class SettingsParameter(SettingsParameterBase, law.CSVParameter):
    r"""
    Parameter that parses the input of a CSVParameter into a dictionary

    Example:

    .. code-block:: python

        p = SettingsParameter()
        p.parse("param1=10,param2,param3=text,param4=false")
        => {"param1": 10.0, "param2": True, "param3": "text", "param4": False}
        p.serialize({"param1": 2, "param2": False})
        => "param1=2.0,param2=False"
    """
    def parse(self, inp):
        inputs = super().parse(inp)

        return dict(map(self.parse_setting, inputs))

    def serialize(self, value):
        if isinstance(value, dict):
            value = tuple(self.serialize_setting(*tpl) for tpl in value.items())

        return super().serialize(value)


class MultiSettingsParameter(SettingsParameterBase, law.MultiCSVParameter):
    r"""
    Parameter that parses the input of a MultiCSVParameter into a double-dict structure.

    Example:

    .. code-block:: python

        p = MultiSettingsParameter()
        p.parse("obj1,k1=10,k2,k3=text:obj2,k4=false")
        # => {"obj1": {"k1": 10.0, "k2": True, "k3": "text"}, {"obj2": {"k4": False}}}
        p.serialize({"obj1": {"k1": "val"}, "obj2": {"k2": 2}})
        # => "obj1,k1=val:obj2,k2=2"
    """
    def parse(self, inp):
        inputs = super().parse(inp)

        outputs = {
            settings[0]: dict(map(self.parse_setting, settings[1:]))
            for settings in inputs
        }

        return outputs

    def serialize(self, value):
        if isinstance(value, dict):
            value = tuple(
                (str(k),) + tuple(self.serialize_setting(*tpl) for tpl in v.items())
                for k, v in value.items()
            )

        return super().serialize(value)
