# coding: utf-8

"""
Inference model parameter types and helpers.
"""

from __future__ import annotations

from columnflow.util import StrEnum


class ParameterType(StrEnum):
    """
    Parameter type flag.

    :cvar rate_gauss: Gaussian rate parameter.
    :cvar rate_uniform: Uniform rate parameter.
    :cvar rate_unconstrained: Unconstrained rate parameter.
    :cvar shape: Shape parameter.
    """

    rate_gauss = "rate_gauss"
    rate_uniform = "rate_uniform"
    rate_unconstrained = "rate_unconstrained"
    shape = "shape"

    @property
    def is_rate(self) -> bool:
        """
        Checks if the parameter type is a rate type.

        :returns: *True* if the parameter type is a rate type, *False* otherwise.
        """
        return self in {
            self.rate_gauss,
            self.rate_uniform,
            self.rate_unconstrained,
        }

    @property
    def is_shape(self) -> bool:
        """
        Checks if the parameter type is a shape type.

        :returns: *True* if the parameter type is a shape type, *False* otherwise.
        """
        return self in {
            self.shape,
        }
