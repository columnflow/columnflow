# coding: utf-8

"""
Custom luigi parameters.
"""

from __future__ import annotations

import copy

import law
import luigi

from columnflow.util import test_float, DotDict


class PlotFunctionParameter(luigi.Parameter):
    """
    Plain parameter subclass that provides a convenience method for copying an instance, assigning
    a different default value and optionally changing the description text.
    """

    def with_default(
        self,
        default: str,
        description: str | None = None,
        amend_description: bool = False,
    ) -> PlotFunctionParameter:
        inst = copy.copy(self)
        inst._default = default
        if description is not None:
            inst.description = description
        elif amend_description:
            inst.description += f"; default: {default}"
        return inst


class SettingsParameter(law.CSVParameter):
    """
    Parameter that parses the input of a CSVParameter into a dictionary
    Example:

    .. code-block:: python

        p = SettingsParameter()

        p.parse("param1=10,param2,param3=text,param4=false")
        => {"param1": 10.0, "param2": True, "param3": "text", "param4": False}

        p.serialize({"param1": 2, "param2": False})
        => "param1=2.0,param2=False"
    """

    @classmethod
    def parse_setting(cls, setting: str) -> tuple[str, float | bool | str]:
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
    def serialize_setting(cls, name: str, value: str) -> str:
        return f"{name}={value}"

    def __init__(self, **kwargs):
        # bypass the default value
        default = kwargs.pop("default", law.no_value)

        super().__init__(**kwargs)

        if default != law.no_value:
            self._default = default

    def parse(self, inp):
        inputs = super().parse(inp)

        return DotDict(self.parse_setting(s) for s in inputs)

    def serialize(self, value):
        if isinstance(value, dict):
            value = tuple(self.serialize_setting(*tpl) for tpl in value.items())

        return super().serialize(value)


class MultiSettingsParameter(law.MultiCSVParameter):
    """
    Parameter that parses the input of a MultiCSVParameter into a double-dict structure.
    Example:

    .. code-block:: python

        p = MultiSettingsParameter()

        p.parse("obj1,k1=10,k2,k3=text:obj2,k4=false")
        # => {"obj1": {"k1": 10.0, "k2": True, "k3": "text"}, {"obj2": {"k4": False}}}

        p.serialize({"obj1": {"k1": "val"}, "obj2": {"k2": 2}})
        # => "obj1,k1=val:obj2,k2=2"
    """

    def __init__(self, **kwargs):
        # bypass the default value
        default = kwargs.pop("default", law.no_value)

        super().__init__(**kwargs)

        if default != law.no_value:
            self._default = default

    def parse(self, inp):
        inputs = super().parse(inp)

        outputs = DotDict({
            settings[0]: DotDict(SettingsParameter.parse_setting(s) for s in settings[1:])
            for settings in inputs
        })

        return outputs

    def serialize(self, value):
        if isinstance(value, dict):
            value = tuple(
                (str(k),) + tuple(SettingsParameter.serialize_setting(*tpl) for tpl in v.items())
                for k, v in value.items()
            )

        return super().serialize(value)
