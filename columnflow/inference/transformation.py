# coding: utf-8

"""
Parameter transformation definitions and utilities.
"""

from __future__ import annotations

import law

from columnflow.util import StrEnum, maybe_import
from columnflow.types import TYPE_CHECKING, Sequence

if TYPE_CHECKING:
    hist = maybe_import("hist")


logger = law.logger.get_logger(__name__)


class ParameterTransformation(StrEnum):
    """
    Flags denoting transformations to be applied on parameters.

    Implementation details depend on the routines that apply these transformations, usually as part for a serialization
    processes (such as so-called "datacards" in the CMS context). As such, the exact implementation may also differ
    depending on the type of the parameter that a transformation is applied to (e.g. shape vs rate).

    The general purpose of each transformation is described below.

    :cvar none: No transformation.
    :cvar effect_from_rate: Creates shape variations for a shape-type parameter using the single- or two-valued effect
        usually attributed to rate-type parameters. Only applies to shape-type parameters.
    :cvar effect_from_shape: Derive the effect of a rate-type parameter using the overall, integral effect of shape
        variations. Only applies to rate-type parameters.
    :cvar effect_from_shape_if_flat: Same as :py:attr:`effect_from_shape`, but applies only if both shape variations are
        reasonably flat. The definition of "reasonably flat" can be subject to the serialization routine. Only applies
        to rate-type parameters.
    :cvar symmetrize: The overall (integral) effect of up and down variations is measured and centralized, updating the
        variations such that they are equidistant to the nominal one. Can apply to both rate- and shape-type parameters.
    :cvar asymmetrize: The symmetric effect on a rate-type parameter (usually given as a single value) is converted into
        an asymmetric representation (using two values). Only applies to rate-type parameters.
    :cvar asymmetrize_if_large: Same as :py:attr:`asymmetrize`, but depending on a threshold on the size of the
        symmetric effect which can be subject to the serialization routine. Only applies to rate-type parameters.
    :cvar normalize: Variations of shape-type parameters are changed such that their integral effects become identical
        to that of the nominal one. Should only apply to shape-type parameters.
    :cvar centralize: The nominal shape is moved, potentially on a bin-by-bin basis, to be right in the middle between
        the two shape variations. Should only apply to shapes subject to no (other) shape-type parameter.
    :cvar envelope: Builds an evelope of the up and down variations of a shape-type parameter, potentially on a
        bin-by-bin basis. Only applies to shape-type parameters.
    :cvar envelope_if_one_sided: Same as :py:attr:`envelope`, but only if the shape variations are one-sided following
        a definition that can be subject to the serialization routine. Only applies to shape-type parameters.
    :cvar envelope_enforce_two_sided: Same as :py:attr:`envelope`, but it enforces that the up (down) variation of the
        constructed envelope is always above (below) the nominal one. Only applies to shape-type parameters.
    :cvar flip_smaller_if_one_sided: For asymmetric rate effects (usually given by two values) that are found to be
        one-sided (e.g. after applying :py:attr:`effect_from_shape`), flips the smaller effect to the other side of the
        nominal value. Only applies to rate-type parameters.
    :cvar flip_larger_if_one_sided: Same as :py:attr:`flip_smaller_if_one_sided`, but flips the larger effect. Only
        applies to rate-type parameters.
    """

    none = "none"
    effect_from_rate = "effect_from_rate"
    effect_from_shape = "effect_from_shape"
    effect_from_shape_if_flat = "effect_from_shape_if_flat"
    symmetrize = "symmetrize"
    asymmetrize = "asymmetrize"
    asymmetrize_if_large = "asymmetrize_if_large"
    normalize = "normalize"
    centralize = "centralize"
    envelope = "envelope"
    envelope_if_one_sided = "envelope_if_one_sided"
    envelope_enforce_two_sided = "envelope_enforce_two_sided"
    flip_smaller_if_one_sided = "flip_smaller_if_one_sided"
    flip_larger_if_one_sided = "flip_larger_if_one_sided"

    # sets of instances for easier distinguishing (set below class creation)
    _ignore_ = ["_first_index_trafos", "_shape_only_trafos", "_rate_only_trafos", "_rate_and_shape_trafos"]
    first_index_trafos: set[ParameterTransformation]
    shape_only_trafos: set[ParameterTransformation]
    rate_only_trafos: set[ParameterTransformation]
    rate_and_shape_trafos: set[ParameterTransformation]

    @property
    def from_shape(self) -> bool:
        """
        Checks if the transformation is derived from shape.
        """
        return self in {
            self.effect_from_shape,
            self.effect_from_shape_if_flat,
        }

    @property
    def from_rate(self) -> bool:
        """
        Checks if the transformation is derived from rate.
        """
        return self in {
            self.effect_from_rate,
        }

    @property
    def requires_first_index(self) -> bool:
        """
        Checks if the transformation must be applied first.
        """
        return self in self.first_index_trafos

    @property
    def affects_rate(self) -> bool:
        """
        Checks if the transformation affects rate-type parameters.
        """
        return self in self.rate_only_trafos or self in self.rate_and_shape_trafos

    @property
    def affects_rate_only(self) -> bool:
        """
        Checks if the transformation affects only rate-type parameters.
        """
        return self in self.rate_only_trafos

    @property
    def affects_shape(self) -> bool:
        """
        Checks if the transformation affects shape-type parameters.
        """
        return self in self.shape_only_trafos or self in self.rate_and_shape_trafos

    @property
    def affects_shape_only(self) -> bool:
        """
        Checks if the transformation affects only shape-type parameters.
        """
        return self in self.shape_only_trafos


# fill instance groups
ParameterTransformation.first_index_trafos = {
    ParameterTransformation.effect_from_rate,
    ParameterTransformation.effect_from_shape,
    ParameterTransformation.effect_from_shape_if_flat,
}
ParameterTransformation.shape_only_trafos = {
    ParameterTransformation.effect_from_rate,
    ParameterTransformation.normalize,
    ParameterTransformation.envelope,
    ParameterTransformation.envelope_if_one_sided,
    ParameterTransformation.envelope_enforce_two_sided,
}
ParameterTransformation.rate_only_trafos = {
    ParameterTransformation.effect_from_shape,
    ParameterTransformation.effect_from_shape_if_flat,
    ParameterTransformation.asymmetrize,
    ParameterTransformation.asymmetrize_if_large,
    ParameterTransformation.flip_smaller_if_one_sided,
    ParameterTransformation.flip_larger_if_one_sided,
}
ParameterTransformation.rate_and_shape_trafos = {
    ParameterTransformation.symmetrize,
    ParameterTransformation.centralize,
}


class ParameterTransformations(tuple):
    """
    Container around a sequence of :py:class:`ParameterTransformation`'s with a few convenience methods.

    :param transformations: A sequence of :py:class:`ParameterTransformation` or their string names.
    """

    def __new__(
        cls,
        transformations: Sequence[ParameterTransformation | str],
    ) -> ParameterTransformations:
        """
        Creates a new instance of :py:class:`ParameterTransformations`.

        :param transformations: A sequence of :py:class:`ParameterTransformation` or their string names.
        :returns: A new instance of :py:class:`ParameterTransformations`.
        """
        # TODO: at this point one could object / complain in case incompatible trafos are used
        transformations = [
            (t if isinstance(t, ParameterTransformation) else ParameterTransformation[t])
            for t in transformations
        ]

        # initialize
        return super().__new__(cls, transformations)

    @property
    def any_from_shape(self) -> bool:
        """
        Checks if any transformation is derived from shape.

        :returns: *True* if any transformation is derived from shape, *False* otherwise.
        """
        return any(t.from_shape for t in self)

    @property
    def any_from_rate(self) -> bool:
        """
        Checks if any transformation is derived from rate.

        :returns: *True* if any transformation is derived from rate, *False* otherwise.
        """
        return any(t.from_rate for t in self)
