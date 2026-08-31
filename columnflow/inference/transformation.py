# coding: utf-8

"""
Parameter transformation definitions and utilities.
"""

from __future__ import annotations

import dataclasses

import law

from columnflow.inference.parameter import ParameterType
from columnflow.util import StrEnum, DotDict, maybe_import, safe_div
from columnflow.types import TYPE_CHECKING, TypeAlias, Sequence, Union

np = maybe_import("numpy")
if TYPE_CHECKING:
    hist = maybe_import("hist")

Effect: TypeAlias = Union[float, int, tuple[Union[float, int], Union[float, int]]]
FloatEffect: TypeAlias = Union[float, tuple[float, float]]
FloatsEffect: TypeAlias = tuple[float, float]
DownUpHists: TypeAlias = tuple["hist.Hist", "hist.Hist"]


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
    _ignore_ = [
        "_first_index_trafos",
        "_shape_only_trafos",
        "_rate_only_trafos",
        "_rate_and_shape_trafos",
        "_changing_rate_to_shape_trafos",
        "_changing_shape_to_rate_trafos",
        "_changing_nominal_trafos",
    ]
    first_index_trafos: set[ParameterTransformation]
    shape_only_trafos: set[ParameterTransformation]
    rate_only_trafos: set[ParameterTransformation]
    rate_and_shape_trafos: set[ParameterTransformation]
    changing_rate_to_shape_trafos: set[ParameterTransformation]
    changing_shape_to_rate_trafos: set[ParameterTransformation]
    changing_nominal_trafos: set[ParameterTransformation]

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

    @property
    def changes_rate_to_shape(self) -> bool:
        """
        Checks if the transformation changes the parameter type from rate to shape.
        """
        return self in self.changing_rate_to_shape_trafos

    @property
    def changes_shape_to_rate(self) -> bool:
        """
        Checks if the transformation changes the parameter type from shape to rate.
        """
        return self in self.changing_shape_to_rate_trafos

    @property
    def changes_type(self) -> bool:
        """
        Checks if the transformation changes the parameter type.
        """
        return self.changes_rate_to_shape or self.changes_shape_to_rate

    @property
    def changes_nominal(self) -> bool:
        """
        Checks if the transformation changes the nominal histogram.
        """
        return self in self.changing_nominal_trafos


# fill instance groups
ParameterTransformation.first_index_trafos = {
    ParameterTransformation.effect_from_rate,
    ParameterTransformation.effect_from_shape,
    ParameterTransformation.effect_from_shape_if_flat,
}
ParameterTransformation.shape_only_trafos = {
    ParameterTransformation.effect_from_shape,
    ParameterTransformation.effect_from_shape_if_flat,
    ParameterTransformation.normalize,
    ParameterTransformation.envelope,
    ParameterTransformation.envelope_if_one_sided,
    ParameterTransformation.envelope_enforce_two_sided,
}
ParameterTransformation.rate_only_trafos = {
    ParameterTransformation.effect_from_rate,
    ParameterTransformation.asymmetrize,
    ParameterTransformation.asymmetrize_if_large,
    ParameterTransformation.flip_smaller_if_one_sided,
    ParameterTransformation.flip_larger_if_one_sided,
}
ParameterTransformation.rate_and_shape_trafos = {
    ParameterTransformation.symmetrize,
    ParameterTransformation.centralize,
}
ParameterTransformation.changing_rate_to_shape_trafos = {
    ParameterTransformation.effect_from_rate,
}
ParameterTransformation.changing_shape_to_rate_trafos = {
    ParameterTransformation.effect_from_shape,
    ParameterTransformation.effect_from_shape_if_flat,
}
ParameterTransformation.changing_nominal_trafos = {
    ParameterTransformation.centralize,
}


class ParameterTransformations(tuple):
    """
    Container around a sequence of :py:class:`ParameterTransformation`'s with a few convenience methods.

    :param transformations: A sequence of :py:class:`ParameterTransformation` or their string names.
    """

    def __new__(
        cls,
        *transformations: Sequence[ParameterTransformation | str],
    ) -> ParameterTransformations:
        """
        Creates a new instance of :py:class:`ParameterTransformations`.

        :param transformations: A sequence of :py:class:`ParameterTransformation` or their string names.
        :returns: A new instance of :py:class:`ParameterTransformations`.
        """
        transformations = [
            (t if isinstance(t, ParameterTransformation) else ParameterTransformation[t])
            for t in law.util.flatten(transformations)
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

    @property
    def any_changes_type(self) -> bool:
        """
        Checks if any transformation changes the parameter type.

        :returns: *True* if any transformation changes the parameter type, *False* otherwise.
        """
        return any(t.changes_type for t in self)

    @property
    def any_changes_nominal(self) -> bool:
        """
        Checks if any transformation changes the nominal histogram.

        :returns: *True* if any transformation changes the nominal histogram, *False* otherwise.
        """
        return any(t.changes_nominal for t in self)


class ShapeTransformer:
    """
    Class that applies transformations to parameters and their associated histograms in a unified way.
    """

    default_effect_from_shape_if_flat_max_outlier: float = 0.2
    default_effect_from_shape_if_flat_max_deviation: float = 0.1
    default_asymmetrize_if_large_threshold: float = 0.2

    class OutputType(StrEnum):
        """
        Flags denoting the output type of a transformation.
        """

        # effect values should be returned (if possible), and shapes otherwise
        keep_effects = "keep_effects"
        # rates are converted into varied shapes, and returned with actual shapes
        convert_to_shapes = "convert_to_shapes"

    @dataclasses.dataclass
    class TransformationOutput:
        """
        Container object holding output of transformations in a unified format.
        """

        param_type: ParameterType
        h_nom: hist.Hist | None
        effect: Effect | None
        h_varied: DownUpHists | None
        param_type_changed: bool
        nominal_changed: bool
        variations_changed: bool
        effect_changed: bool
        output_type: ShapeTransformer.OutputType

    @dataclasses.dataclass
    class ProcessOutput:
        """
        Container object holding output of a sequence of parameters each with a potential sequence of transformations in
        a unified format.
        """

        h_nom: hist.Hist | None
        effects: dict[str, Effect | None]
        h_varied: dict[str, DownUpHists | None]
        trafo_outputs: dict[str, ShapeTransformer.TransformationOutput]
        nominal_changed: bool
        output_type: ShapeTransformer.OutputType

    def __init__(
        self,
        effect_from_shape_if_flat_max_outlier: float | None = None,
        effect_from_shape_if_flat_max_deviation: float | None = None,
        asymmetrize_if_large_threshold: float | None = None,
    ) -> None:
        super().__init__()

        # set thresholds
        self.effect_from_shape_if_flat_max_outlier = (
            self.default_effect_from_shape_if_flat_max_outlier
            if effect_from_shape_if_flat_max_outlier is None
            else effect_from_shape_if_flat_max_outlier
        )
        self.effect_from_shape_if_flat_max_deviation = (
            self.default_effect_from_shape_if_flat_max_deviation
            if effect_from_shape_if_flat_max_deviation is None
            else effect_from_shape_if_flat_max_deviation
        )
        self.asymmetrize_if_large_threshold = (
            self.default_asymmetrize_if_large_threshold
            if asymmetrize_if_large_threshold is None
            else asymmetrize_if_large_threshold
        )

    #
    # high-level user interface
    #

    def apply_parameters(
        self,
        *,
        param_objs: Sequence[DotDict],
        h_nom: hist.Hist,
        h_varied: Sequence[DownUpHists | None] | dict[str, DownUpHists | None] | None = None,
        trafos: Sequence[Sequence[ParameterTransformation]] | None = None,
        output_type: OutputType = OutputType.keep_effects,
    ) -> ProcessOutput:
        """
        Applies a sequence of transformations to a sequence of parameters and their associated histograms.

        :param param_objs: The parameter objects whose transformations are applied.
        :param h_nom: The nominal histogram.
        :param h_varied: The down/up variations of the nominal histogram, if applicable. Can be a sequence of
            :py:class:`DownUpHists` or a dictionary mapping parameter names to :py:class:`DownUpHists`.
        :param trafos: The sequence of transformations to apply for each parameter. If *None*, all transformations
            registered to each parameter are used. The length must match the number of parameters.
        :param output_type: The output type of the transformation.
        :returns: A :py:class:`ProcessOutput` object containing the results of the transformations.
        """
        # validation across parameters
        trafos = self.validate_parameters(param_objs, trafos)

        # convert h_varied to dict type
        if isinstance(h_varied, (list, tuple)):
            if len(h_varied) != len(param_objs):
                raise ValueError(
                    f"number of passes varied histograms ({len(h_varied)}) does not match number of parameters "
                    f"({len(param_objs)})",
                )
            h_varied = {param_obj.name: h for param_obj, h in zip(param_objs, h_varied)}

        # eagerly build an output object that is adjusted in the param loop
        output_type = self.OutputType(output_type)
        output = self.ProcessOutput(
            h_nom=h_nom,
            effects={},
            h_varied={},
            trafo_outputs={},
            nominal_changed=False,
            output_type=output_type,
        )

        for param_obj, _trafos in zip(param_objs, trafos):
            # apply trafos
            trafos_output = self.apply_transformations(
                param_obj=param_obj,
                h_nom=output.h_nom,
                h_varied=h_varied.get(param_obj.name, None),
                trafos=_trafos,
                output_type=output_type,
            )

            # update output fields
            if trafos_output.nominal_changed:
                output.h_nom = trafos_output.h_nom
                output.nominal_changed = True
            output.effects[param_obj.name] = trafos_output.effect
            output.h_varied[param_obj.name] = trafos_output.h_varied
            output.trafo_outputs[param_obj.name] = trafos_output

        return output

    def apply_transformations(
        self,
        *,
        param_obj: DotDict,
        h_nom: hist.Hist,
        h_varied: DownUpHists | None = None,
        trafos: Sequence[ParameterTransformation] | None = None,
        output_type: OutputType = OutputType.keep_effects,
    ) -> TransformationOutput:
        """
        Applies a sequence of transformations to a parameter and its associated histograms.

        :param param_obj: The parameter object whose transformations are applied.
        :param h_nom: The nominal histogram.
        :param h_varied: The down/up variations of the nominal histogram, if applicable.
        :param trafos: The sequence of transformations to apply. If *None*, all transformations registered to the
            parameter are used.
        :param output_type: The output type of the transformation.
        :returns: A :py:class:`TransformationOutput` object containing the results of the transformations.
        """
        # default trafos
        if trafos is None:
            trafos = list(param_obj.transformations)

        # eagerly build an output object that is adjusted in the trafo loop
        output_type = self.OutputType(output_type)
        output = self.TransformationOutput(
            param_type=param_obj.type,
            h_nom=h_nom,
            effect=param_obj.effect if param_obj.type.is_rate else None,
            h_varied=h_varied,
            nominal_changed=False,
            variations_changed=False,
            effect_changed=False,
            param_type_changed=False,
            output_type=output_type,
        )

        # loop through transformations and apply them in order, including some validation
        for i, trafo in enumerate(trafos):
            # validation
            if trafo.requires_first_index and i != 0:
                raise ValueError(
                    f"transformation '{trafo}' must be applied first, but is at sequence position {i} for parameter "
                    f"'{param_obj.name}'",
                )

            # create an altered param_obj with values from the previous iteration injected
            _param_obj = DotDict({**param_obj, "type": output.param_type, "effect": output.effect})

            # apply the trafo
            trafo_output = self.apply_transformation(
                param_obj=_param_obj,
                trafo=trafo,
                h_nom=output.h_nom,
                h_varied=output.h_varied,
                output_type=self.OutputType.keep_effects,
                _type_mismatch_postfix=f" after trafo sequence {','.join(map(str, trafos[:i]))}",
            )

            # update output fields
            output.param_type = trafo_output.param_type
            output.h_nom = trafo_output.h_nom
            output.effect = trafo_output.effect
            output.h_varied = trafo_output.h_varied
            output.nominal_changed |= trafo_output.nominal_changed
            output.variations_changed |= trafo_output.variations_changed
            output.effect_changed |= trafo_output.effect_changed
            output.param_type_changed |= trafo_output.param_type_changed

        # adjust the output format
        output = self.adjust_trafo_output(output, output_type, param_obj.name)

        return output

    def apply_transformation(
        self,
        *,
        param_obj: DotDict,
        trafo: ParameterTransformation,
        h_nom: hist.Hist,
        h_varied: DownUpHists | None = None,
        output_type: OutputType = OutputType.keep_effects,
        _type_mismatch_postfix: str = "",
    ) -> TransformationOutput:
        """
        Applies a single transformation to a parameter and the associated histograms.

        :param param_obj: The parameter object whose transformation is applied.
        :param trafo: The transformation to apply.
        :param h_nom: The nominal histogram.
        :param h_varied: The down/up variations of the nominal histogram, if applicable.
        :param output_type: The output type of the transformation.
        :returns: A :py:class:`TransformationOutput` object containing the results of the transformation.
        """
        # first check of the transormation can be applied to the parameter
        if trafo.affects_rate_only and not param_obj.type.is_rate:
            raise ValueError(
                f"transformation '{trafo}' can only be applied to rate-type parameters, but parameter "
                f"'{param_obj.name}' has type '{param_obj.type}'{_type_mismatch_postfix}",
            )
        if trafo.affects_shape_only and not param_obj.type.is_shape:
            raise ValueError(
                f"transformation '{trafo}' can only be applied to shape-type parameters, but parameter "
                f"'{param_obj.name}' has type '{param_obj.type}'{_type_mismatch_postfix}",
            )

        # helper to raise in case varied hists are required but not provided
        def require_h_varied() -> DownUpHists:
            if h_varied is None:
                raise ValueError(
                    f"transformation '{trafo}' requires varied histograms, but none were provided for parameter "
                    f"'{param_obj.name}'",
                )
            return h_varied

        # eagerly build an output object that is adjusted below
        output_type = self.OutputType(output_type)
        output = self.TransformationOutput(
            param_type=param_obj.type,
            h_nom=h_nom,
            effect=param_obj.effect if param_obj.type.is_rate else None,
            h_varied=h_varied,
            nominal_changed=False,
            variations_changed=False,
            effect_changed=False,
            param_type_changed=False,
            output_type=output_type,
        )

        # dispatch, and collect and unify output
        if trafo.from_rate:
            output.h_varied = self._apply_effect_from_rate(
                param_obj=param_obj,
                trafo=trafo,
                h_nom=h_nom,
                effect=param_obj.effect,
            )
            output.variations_changed = True
            output.effect = None
            output.effect_changed = True
            output.param_type = ParameterType.shape
            output.param_type_changed = True
        elif trafo.from_shape:
            d, u = self._apply_effect_from_shape(
                param_obj=param_obj,
                trafo=trafo,
                h_nom=h_nom,
                h_varied=require_h_varied(),
            )
            if isinstance(d, float):
                output.effect = (d, u)
                output.effect_changed = True
                output.h_varied = None
                output.variations_changed = True
                output.param_type = ParameterType.rate_gauss
                output.param_type_changed = True
        elif trafo == ParameterTransformation.symmetrize:
            if param_obj.type.is_rate:
                output.effect = self._apply_symmetrize_rate(param_obj=param_obj, trafo=trafo, effect=param_obj.effect)
                output.effect_changed = True
            else:
                output.h_varied = self._apply_symmetrize_shape(
                    param_obj=param_obj,
                    trafo=trafo,
                    h_nom=h_nom,
                    h_varied=require_h_varied(),
                )
                output.variations_changed = True
        elif trafo in {
            ParameterTransformation.asymmetrize,
            ParameterTransformation.asymmetrize_if_large,
        }:
            output.effect = self._apply_asymmetrize_rate(param_obj=param_obj, trafo=trafo, effect=param_obj.effect)
            output.effect_changed = True
        elif trafo == ParameterTransformation.normalize:
            output.h_varied = self._apply_normalize_shape(
                param_obj=param_obj,
                trafo=trafo,
                h_nom=h_nom,
                h_varied=require_h_varied(),
            )
            output.variations_changed = True
        elif trafo == ParameterTransformation.centralize:
            if param_obj.type.is_rate:
                h_nom_updated, effect_updated = self._apply_centralize_rate(
                    param_obj=param_obj,
                    trafo=trafo,
                    h_nom=h_nom,
                    effect=param_obj.effect,
                )
                if h_nom_updated is not None:
                    output.h_nom = h_nom_updated
                    output.nominal_changed = True
                    output.effect = effect_updated
                    output.effect_changed = True
            else:
                output.h_nom = self._apply_centralize_shape(
                    param_obj=param_obj,
                    trafo=trafo,
                    h_nom=h_nom,
                    h_varied=require_h_varied(),
                )
                output.nominal_changed = True
        elif trafo in {
            ParameterTransformation.envelope,
            ParameterTransformation.envelope_if_one_sided,
            ParameterTransformation.envelope_enforce_two_sided,
        }:
            output.h_varied = self._apply_envelope_shape(
                param_obj=param_obj,
                trafo=trafo,
                h_nom=h_nom,
                h_varied=require_h_varied(),
            )
            output.variations_changed = True
        elif trafo in {
            ParameterTransformation.flip_smaller_if_one_sided,
            ParameterTransformation.flip_larger_if_one_sided,
        }:
            output.effect = self._apply_flip_if_one_sided_rate(
                param_obj=param_obj,
                trafo=trafo,
                effect=param_obj.effect,
            )
            output.effect_changed = True
        else:
            raise ValueError(f"unknown transformation '{trafo}'")

        # adjust the output format
        output = self.adjust_trafo_output(output, output_type, param_obj.name)

        return output

    #
    # helpers to determine shape & effect features
    #

    @classmethod
    def get_values(cls, inp: hist.Hist | np.ndarray, flow: bool = True) -> np.ndarray:
        import hist
        if isinstance(inp, hist.Hist):
            view = inp.view(flow=flow)
            inp = (
                view.value
                if isinstance(inp.storage_type(), (hist.storage.Weight, hist.storage.WeightedMean))
                else view
            )
        if not isinstance(inp, np.ndarray):
            raise TypeError(f"cannot extract values from unknown type {type(inp)}: {inp}")
        return inp

    @classmethod
    def get_integral(cls, inp: hist.Hist | np.ndarray, flow: bool = True) -> float:
        return float(sum(cls.get_values(inp, flow=flow)))

    @classmethod
    def validate_effect(cls, effect: Effect) -> bool:
        valid_num = lambda n: isinstance(n, (float, int)) and n >= 0
        if valid_num(effect):
            return True
        if isinstance(effect, tuple) and len(effect) == 2 and all(map(valid_num, effect)):
            return True
        raise ValueError(f"invalid effect {effect}: must be single value of 2-tuple of a non-negative float, int")

    @classmethod
    def split_effect(cls, effect: Effect) -> FloatsEffect:
        cls.validate_effect(effect)
        return (
            (2.0 - float(effect), float(effect))
            if isinstance(effect, (float, int))
            else (float(effect[0]), float(effect[1]))
        )

    @classmethod
    def assert_trafo_type(cls, trafo: ParameterTransformation, expected_trafos: set[ParameterTransformation]) -> None:
        if trafo not in expected_trafos:
            raise ValueError(
                f"transformation '{trafo}' is not in the expected set of transformations: "
                f"{', '.join(map(str, expected_trafos))}",
            )

    @classmethod
    def validate_parameters(
        cls,
        param_objs: Sequence[DotDict],
        trafos: Sequence[Sequence[ParameterTransformation]] | None = None,
    ) -> Sequence[ParameterTransformations]:
        # default trafos
        if not trafos:
            trafos = [list(param_obj.transformations) for param_obj in param_objs]
        elif len(trafos) != len(param_objs):
            raise ValueError(
                f"number of transformation sequences ({len(trafos)}) does not match number of parameters "
                f"({len(param_objs)})",
            )

        # cast
        trafos = list(map(ParameterTransformations, trafos))

        # two checks:
        # 1. there can only be a single parameter with a transformation that can change the nominal shape
        # 2. when a parameter has a transformation that might change the nominal shape, all other parameters must be
        #    rate-type without any transformation that could convert them into a shape-type
        nominal_changing_params = {
            param_obj.name
            for param_obj, _trafos in zip(param_objs, trafos)
            if _trafos.any_changes_nominal
        }
        # check 1
        if len(nominal_changing_params) > 1:
            raise ValueError(
                f"multiple parameters have transformations that change the nominal shape which is not supported: "
                f"{', '.join(map(str, [p.name for p in nominal_changing_params]))}",
            )
        # check 2
        if len(nominal_changing_params) == 1:
            for param_obj, _trafos in zip(param_objs, trafos):
                # only loop over other parameters
                if param_obj.name in nominal_changing_params:
                    continue
                if param_obj.type.is_shape or (param_obj.type.is_rate and _trafos.any_changes_type):
                    raise ValueError(
                        f"parameter '{param_obj.name}' has transformations that could change its type to shape, but "
                        f"another parameter has a transformation that changes the nominal shape which is not supported",
                    )

        return trafos

    @classmethod
    def adjust_trafo_output(
        cls,
        output: TransformationOutput,
        output_type: OutputType,
        param_name: str,
    ) -> TransformationOutput:
        # output is meant to be in "keep_effects" format initially

        # do some cleaning first, saving normalization factors in the effect when variations are given
        if output.h_varied is not None and output.effect is None:
            output.effect = (
                safe_div(cls.get_integral(output.h_varied[0]), cls.get_integral(output.h_nom)),
                safe_div(cls.get_integral(output.h_varied[1]), cls.get_integral(output.h_nom)),
            )

        # nothing to be changed
        if output_type == cls.OutputType.keep_effects:
            return output

        # below this point, effects must be converted into shapes
        if output.h_varied is None:
            if output.effect is None:
                raise ValueError(
                    f"output object for parameter '{param_name}' has neither varied histograms nor effect values: "
                    f"'{output}'",
                )
            effect = cls.split_effect(output.effect)
            output.h_varied = (
                output.h_nom * effect[0],
                output.h_nom * effect[1],
            )

        return output

    def determine_shape_effect_is_flat(
        self,
        h_nom: hist.Hist | np.ndarray,
        h_var: hist.Hist | np.ndarray,
        flow: bool = True,
    ) -> bool:
        """
        Determines if the effect of a shape variation is "flat".

        :param h_nom: The nominal histogram.
        :param h_var: The varied histogram.
        :param flow: Whether to include underflow and overflow bins in the calculation.
        :returns: *True* if the effect is flat, *False* otherwise.
        """
        diffs = self.get_values(h_nom, flow=flow) - self.get_values(h_var, flow=flow)
        mean, std = abs(diffs.mean()), abs(diffs.std())
        max_rel_outlier = safe_div(max(abs(diffs)), mean)
        rel_deviation = safe_div(std, mean)
        return (
            max_rel_outlier <= self.effect_from_shape_if_flat_max_outlier and
            rel_deviation <= self.effect_from_shape_if_flat_max_deviation
        )

    def determine_effect_is_large(self, effect: Effect) -> bool:
        """
        Determines if the effect is "large" based on the configured threshold.

        :param effect: The effect to check.
        :returns: *True* if the effect is large, *False* otherwise.
        """
        self.validate_effect(effect)
        d, u = self.split_effect(effect)
        return max(abs(d), abs(u)) >= self.asymmetrize_if_large_threshold

    #
    # low-level implementations of transformations with varying inputs & outputs
    #

    def _apply_effect_from_rate(
        self,
        *,
        param_obj: DotDict,
        trafo: ParameterTransformation,
        h_nom: hist.Hist,
        effect: Effect,
    ) -> DownUpHists:
        """
        Low-level implementation of the :py:attr:`ParameterTransformation.effect_from_rate` transformation. Only
        applicable to rate-type parameters.
        """
        self.assert_trafo_type(trafo, {ParameterTransformation.effect_from_rate})
        d, u = self.split_effect(effect)
        return (
            h_nom * d,
            h_nom * u,
        )

    def _apply_effect_from_shape(
        self,
        *,
        param_obj: DotDict,
        trafo: ParameterTransformation,
        h_nom: hist.Hist,
        h_varied: DownUpHists,
    ) -> FloatsEffect | DownUpHists:
        """
        Low-level implementation of the :py:attr:`ParameterTransformation.effect_from_shape` and
        :py:attr:`ParameterTransformation.effect_from_shape_if_flat` transformations. Only applicable to
        shape-type parameters.
        """
        self.assert_trafo_type(
            trafo,
            {ParameterTransformation.effect_from_shape, ParameterTransformation.effect_from_shape_if_flat},
        )
        if trafo == ParameterTransformation.effect_from_shape_if_flat:
            # check flat-ness criteria on both variations
            d_flat = self.determine_shape_effect_is_flat(h_nom, h_varied[0])
            u_flat = self.determine_shape_effect_is_flat(h_nom, h_varied[1])
            # when any of them is not flat, return the shapes unchanged
            if not d_flat or not u_flat:
                return h_varied

        # return integral down/up ratios w.r.t. nominal
        n = self.get_integral(h_nom)
        d = self.get_integral(h_varied[0])
        u = self.get_integral(h_varied[1])
        return (safe_div(d, n), safe_div(u, n))

    def _apply_symmetrize_rate(
        self,
        *,
        param_obj: DotDict,
        trafo: ParameterTransformation,
        effect: Effect,
    ) -> Effect:
        """
        Low-level implementation of the :py:attr:`ParameterTransformation.symmetrize` transformation. Only applicable to
        rate-type parameters.
        """
        self.assert_trafo_type(trafo, {ParameterTransformation.symmetrize})
        if effect == 1:
            return effect
        d, u = self.split_effect(effect)
        # skip one sided effects
        if not (min(d, u) <= 1 <= max(d, u)):
            logger.debug(
                f"skipping rate symmetrization of parameter '{param_obj.name}' as effect '{effect}' is one-sided",
            )
            return (d, u)
        # symmetrize by taking the mean of differences to 1
        mean = 0.5 * (u + d) - 1
        return (1 - mean, 1 + mean) if u >= d else (1 + mean, 1 - mean)

    def _apply_symmetrize_shape(
        self,
        *,
        param_obj: DotDict,
        trafo: ParameterTransformation,
        h_nom: hist.Hist,
        h_varied: DownUpHists,
    ) -> DownUpHists:
        """
        Low-level implementation of the :py:attr:`ParameterTransformation.symmetrize` transformation. Only applicable to
        shape-type parameters.
        """
        self.assert_trafo_type(trafo, {ParameterTransformation.symmetrize})
        # get the absolute spread based on integrals
        n = self.get_integral(h_nom)
        d = self.get_integral(h_varied[0])
        u = self.get_integral(h_varied[1])
        # skip one sided effects
        if not (min(d, n) <= n <= max(d, n)):
            logger.info(f"skipping shape symmetrization of parameter '{param_obj.name}' as effect is one-sided")
            return h_varied
        # find the central point, compute the diff w.r.t. nominal, and shift
        diff = 0.5 * (d + u) - n
        h_d = h_varied[0] * safe_div(d - diff, d)
        h_u = h_varied[1] * safe_div(u - diff, u)
        return (h_d, h_u)

    def _apply_asymmetrize_rate(
        self,
        *,
        param_obj: DotDict,
        trafo: ParameterTransformation,
        effect: Effect,
    ) -> Effect:
        """
        Low-level implementation of the :py:attr:`ParameterTransformation.asymmetrize` and
        :py:attr:`ParameterTransformation.asymmetrize_if_large` transformations. Only applicable to rate-type
        parameters.
        """
        self.assert_trafo_type(
            trafo,
            {ParameterTransformation.asymmetrize, ParameterTransformation.asymmetrize_if_large},
        )
        d, u = self.split_effect(effect)
        # when the effect is too small, maybe return the original effect unchanged
        if trafo == ParameterTransformation.asymmetrize_if_large:
            if max(d, u) < self.asymmetrize_if_large_threshold:
                return effect
        # return the asymmetric representation
        return (d, u)

    def _apply_normalize_shape(
        self,
        *,
        param_obj: DotDict,
        trafo: ParameterTransformation,
        h_nom: hist.Hist,
        h_varied: DownUpHists,
    ) -> DownUpHists:
        """
        Low-level implementation of the :py:attr:`ParameterTransformation.normalize` transformation. Only applicable to
        shape-type parameters.
        """
        self.assert_trafo_type(trafo, {ParameterTransformation.normalize})
        n = self.get_integral(h_nom)
        d = self.get_integral(h_varied[0])
        u = self.get_integral(h_varied[1])
        # return scaled variations
        return (
            h_varied[0] * safe_div(n, d),
            h_varied[1] * safe_div(n, u),
        )

    def _apply_centralize_rate(
        self,
        *,
        param_obj: DotDict,
        trafo: ParameterTransformation,
        h_nom: hist.Hist,
        effect: Effect,
    ) -> tuple[hist.Hist | None, Effect]:
        """
        Low-level implementation of the :py:attr:`ParameterTransformation.centralize` transformation. Only applicable to
        rate-type parameters.
        """
        self.assert_trafo_type(trafo, {ParameterTransformation.centralize})
        d, u = self.split_effect(effect)
        # do nothing when the effect is symmetric
        if isinstance(effect, (float, int)) or (mean := 0.5 * (u + d)) == 1:
            return None, effect
        # simply scale histogram to mean and return adjust effects
        width = u - d
        return (
            h_nom * mean,
            (1 - 0.5 * width, 1 + 0.5 * width),
        )

    def _apply_centralize_shape(
        self,
        *,
        param_obj: DotDict,
        trafo: ParameterTransformation,
        h_nom: hist.Hist,
        h_varied: DownUpHists,
        propagate_variance: bool = False,
    ) -> hist.Hist:
        """
        Low-level implementation of the :py:attr:`ParameterTransformation.centralize` transformation. Only applicable to
        shape-type parameters.
        """
        self.assert_trafo_type(trafo, {ParameterTransformation.centralize})
        # create copy of nominal histogram change values to be the mean of the two variations
        h_central = h_nom.copy()
        v_c = h_central.view(flow=True)
        v_d = h_varied[0].view(flow=True)
        v_u = h_varied[1].view(flow=True)
        v_c.value[...] = 0.5 * (v_d.value + v_u.value)
        # optionally use the error-propagated variance of the variations
        if propagate_variance:
            v_c.variance[...] = 0.25 * (v_d.variance + v_u.variance)
        return h_central

    def _apply_envelope_shape(
        self,
        *,
        param_obj: DotDict,
        trafo: ParameterTransformation,
        h_nom: hist.Hist,
        h_varied: DownUpHists,
    ) -> DownUpHists:
        """
        Low-level implementation of the :py:attr:`ParameterTransformation.envelope`,
        :py:attr:`ParameterTransformation.envelope_if_one_sided`, and
        :py:attr:`ParameterTransformation.envelope_enforce_two_sided` transformations. Only applicable to shape-type
        parameters.
        """
        self.assert_trafo_type(
            trafo,
            {
                ParameterTransformation.envelope,
                ParameterTransformation.envelope_if_one_sided,
                ParameterTransformation.envelope_enforce_two_sided,
            },
        )
        # prepare copies of histograms and views for in-place modifications
        h_d = h_varied[0].copy()
        h_u = h_varied[1].copy()
        v_n = h_nom.view(flow=True)
        v_d = h_d.view(flow=True)
        v_u = h_u.view(flow=True)

        if trafo in {ParameterTransformation.envelope, ParameterTransformation.envelope_if_one_sided}:
            # compute masks denoting at which locations a variation is abs larger than the other
            diffs_u = v_u.value - v_n.value
            diffs_d = v_d.value - v_n.value
            mask_u = abs(diffs_u) > abs(diffs_d)
            mask_d = abs(diffs_d) > abs(diffs_u)
            # when only checking one-sided, remove True's from the masks where variations are two-sided
            if trafo == ParameterTransformation.envelope_if_one_sided:
                one_sided = (diffs_u * diffs_d) > 0
                mask_u &= one_sided
                mask_d &= one_sided
            # fill values from the larger variation
            v_u.value[mask_d] = v_n.value[mask_d] - diffs_d[mask_d]
            v_u.variance[mask_d] = v_d.variance[mask_d]
            v_d.value[mask_u] = v_n.value[mask_u] - diffs_u[mask_u]
            v_d.variance[mask_u] = v_u.variance[mask_u]

        else:  # envelope_enforce_two_sided
            # compute masks denoting at which locations a variation is abs larger than the other
            abs_diffs_u = abs(v_u.value - v_n.value)
            abs_diffs_d = abs(v_d.value - v_n.value)
            mask_u = abs_diffs_u >= abs_diffs_d
            mask_d = ~mask_u
            # fill values from the absolute larger variation
            v_u.value[mask_u] = v_n.value[mask_u] + abs_diffs_u[mask_u]
            v_u.value[mask_d] = v_n.value[mask_d] + abs_diffs_d[mask_d]
            v_u.variance[mask_d] = v_d.variance[mask_d]
            v_d.value[mask_d] = v_n.value[mask_d] - abs_diffs_d[mask_d]
            v_d.value[mask_u] = v_n.value[mask_u] - abs_diffs_u[mask_u]
            v_d.variance[mask_u] = v_u.variance[mask_u]

        return (h_d, h_u)

    def _apply_flip_if_one_sided_rate(
        self,
        *,
        param_obj: DotDict,
        trafo: ParameterTransformation,
        effect: Effect,
    ) -> Effect:
        """
        Low-level implementation of the :py:attr:`ParameterTransformation.flip_smaller_if_one_sided` and
        :py:attr:`ParameterTransformation.flip_larger_if_one_sided` transformations. Only applicable to rate-type
        parameters.
        """
        self.assert_trafo_type(
            trafo,
            {ParameterTransformation.flip_smaller_if_one_sided, ParameterTransformation.flip_larger_if_one_sided},
        )
        d, u = self.split_effect(effect)
        if isinstance(effect, (int, float)):
            return effect
        _min = min(d, u)
        _max = max(d, u)
        # handle cases
        if _min < 1 and _max <= 1:
            if trafo == ParameterTransformation.flip_smaller_if_one_sided:
                return (d, 2 - u) if d < u else (2 - d, u)
            else:  # flip_larger_if_one_sided
                return (2 - d, u) if d < u else (d, 2 - u)
        if _min >= 1 and _max > 1:
            if trafo == ParameterTransformation.flip_smaller_if_one_sided:
                return (2 - d, u) if d < u else (d, 2 - u)
            else:  # flip_larger_if_one_sided
                return (d, 2 - u) if d < u else (2 - d, u)
        # not one-sided, return unchanged
        return effect
