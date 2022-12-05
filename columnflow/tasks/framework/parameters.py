# coding: utf-8

"""
Custom luigi parameters.
"""

import law
from columnflow.util import test_float


class SettingsParameter(law.MultiCSVParameter):
    r"""

    Example:

    .. code-block:: python

        p = SettingsParameter()
        p.parse("obj1,k1=10,k2,k3=text:obj2,k4=false")
        # => {"obj1": {"k1": 10.0, "k2": True, "k3": "text"}, {"obj2": {"k4": False}}}
        p.serialize({"obj1": {"k1": "val"}, "obj2": {"k2": 2}})
        # => "obj1,k1=val:obj2,k2=2"
    """
    def parse(self, inp):
        inputs = super().parse(inp)

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

        outputs = {
            settings[0]: dict(map(parse_setting, settings[1:]))
            for settings in inputs
        }

        return outputs

    def serialize(self, value):
        if isinstance(value, dict):
            def serialize_setting(name, value):
                return f"{name}={value}"
            value = tuple(
                (str(k),) + tuple(serialize_setting(*tpl) for tpl in v.items())
                for k, v in value.items()
            )

        return super().serialize(value)
