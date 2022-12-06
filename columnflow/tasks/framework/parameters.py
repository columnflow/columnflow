# coding: utf-8

"""
Custom luigi parameters.
"""

import law
from columnflow.util import test_float


def parse_setting(setting: str):
    pair = setting.split("=", 1)
    key, value = pair if len(pair) == 2 else (pair[0], "True")
    if test_float(value):
        value = float(value)
    elif value.lower() == "true":
        value = True
    elif value.lower() == "false":
        value = False
    return (key, value)


def serialize_setting(name, value):
    return f"{name}={value}"


class SettingsParameter(law.CSVParameter):
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

        return dict(map(parse_setting, inputs))

    def serialize(self, value):
        if isinstance(value, dict):
            value = tuple(serialize_setting(*tpl) for tpl in value.items())

        return super().serialize(value)


class MultiSettingsParameter(law.MultiCSVParameter):
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
            settings[0]: dict(map(parse_setting, settings[1:]))
            for settings in inputs
        }

        return outputs

    def serialize(self, value):
        if isinstance(value, dict):
            value = tuple(
                (str(k),) + tuple(serialize_setting(*tpl) for tpl in v.items())
                for k, v in value.items()
            )

        return super().serialize(value)
