# coding: utf-8

"""
Helpers to write and work with datacards.
"""

from __future__ import annotations

import os
import dataclasses
import collections

import law
import order as od

from columnflow import __version__ as cf_version
from columnflow.inference import InferenceModel, FlowStrategy
from columnflow.inference.parameter import ParameterType
from columnflow.inference.transformation import ShapeTransformer
from columnflow.hist_util import sum_hists
from columnflow.util import DotDict, maybe_import, real_path, ensure_dir, safe_div, maybe_int
from columnflow.types import TYPE_CHECKING, TypeAlias, Sequence, Any, Union, Hashable

np = maybe_import("numpy")

if TYPE_CHECKING:
    hist = maybe_import("hist")

# type aliases for nested histogram structs
ShiftHists: TypeAlias = dict[Union[str, tuple[str, str]], "hist.Hist"]  # "nominal" or (param_name, "up|down") -> hists
ConfigHists: TypeAlias = dict[str, ShiftHists]  # config name -> hists
ProcHists: TypeAlias = dict[str, ConfigHists]  # process name -> hists
DatacardHists: TypeAlias = dict[str, ProcHists]  # category name -> hists


logger = law.logger.get_logger(__name__)


class DatacardWriter(object):
    """
    Generic writer for combine datacards using a instance of an :py:class:`InferenceModel` *inference_model_inst* and a
    four-fold nested dictionary "category -> process -> config -> shift -> hist".

    *rate_precision* and *effect_precision* control the number of digits of values for measured rates and parameter
    effects. They are used in case the category and parameter objects of the inference model are configured with
    non-postive values for *rate_precision* and *effect_precision*, respectively.

    .. note::

        At the moment, all shapes are written into the same root file and a shape line with wildcards for both bin and
        process resolution is created.

    As per the definition in :py:class:`ParameterTransformation`, the following parameter effect transormations are
    implemented with the following details.

        - :py:attr:`ParameterTransformation.effect_from_rate`: Creates shape variations from a rate-style effect.
            Shape-type parameters only.
        - :py:attr:`ParameterTransformation.effect_from_shape`: Converts the integral effect of shape variations to an
            asymmetric rate-style effect. Rate-type parameters only.
        - :py:attr:`ParameterTransformation.effect_from_shape_if_flat`: Same as above but only applies to cases where
            both shape variations are reasonably flat. The flatness per varied shape is determined by two criteria that
            both must be met: 1. the maximum relative outlier of bin contents with respect to their mean (defaults to
            20%, configurable via *effect_from_shape_if_flat_max_outlier*), 2. the deviation / dispersion of bin
            contents, i.e., the square root of the variance of bin contents, relative to their mean (defaults to 10%,
            configurable via *effect_from_shape_if_flat_max_deviation*). The parameter should initially be of rate-type,
            but in case the criteria are not met, the effect is interpreted as shape-type.
        - :py:attr:`ParameterTransformation.symmetrize`: Changes up and down variations of either rate effects and
            shapes to symmetrize them around the nominal value. For rate-type parameters, this has no effect if the
            effect strength was provided by a single value. There is no conversion into a single value and consequently,
            the result is always a two-valued effect.
        - :py:attr:`ParameterTransformation.asymmetrize`: Converts single-valued to two-valued effects for rate-style
            parameters.
        - :py:attr:`ParameterTransformation.asymmetrize_if_large`: Same as above, with a default threshold of 20%.
            Configurable via *asymmetrize_if_large_threshold*.
        - :py:attr:`ParameterTransformation.normalize`: Normalizes shape variations such that their integrals match that
            of the nominal shape.
        - :py:attr:`ParameterTransformation.centralize`: Moves the nominal shape right in between the up and down
            variations, both for rate- and shype-type parameters. Rate effects are updated accordingly.
        - :py:attr:`ParameterTransformation.envelope`: Takes the bin-wise maximum in each direction of the up and down
            variations of shape-type parameters and constructs new shapes.
        - :py:attr:`ParameterTransformation.envelope_if_one_sided`: Same as above, but only in bins where up and down
            contributions are one-sided.
        - :py:attr:`ParameterTransformation.envelope_enforce_two_sided`: Same as :py:attr:`envelope`, but it enforces
            that the up (down) variation of the constructed envelope is always above (below) the nominal one.
        - :py:attr:`ParameterTransformation.flip_smaller_if_one_sided`: For asymmetric (two-valued) rate effects that
            are found to be one-sided (e.g. after :py:attr:`ParameterTransformation.effect_from_shape`), flips the
            smaller effect to the other side. Rate-type parameters only.
        - :py:attr:`ParameterTransformation.flip_larger_if_one_sided`: Same as
            :py:attr:`ParameterTransformation.flip_smaller_if_one_sided`, but flips the larger effect. Rate-type
            parameters only.

    .. note::

        If used, the transformations :py:attr:`ParameterTransformation.effect_from_rate`,
        :py:attr:`ParameterTransformation.effect_from_shape`, and
        :py:attr:`ParameterTransformation.effect_from_shape_if_flat` must be the first element in the sequence of
        transformations to be applied. The remaining transformations are applied in order based on the outcome of the
        effect conversion.
    """

    # minimum separator between columns
    col_sep = "  "

    # reference to the shape transformer class to be used
    shape_transformer_cls = ShapeTransformer

    @dataclasses.dataclass
    class ShapeData:
        """
        Container object describing data returned after shape writing.
        """

        # nominal histograms in a mapping "category -> process -> histogram"
        nominal_hists: dict[str, dict[str, "hist.Hist"]]
        # the nominal rates in a mapping "category -> process -> rate"
        rates: dict[str, dict[str, float]]
        # rate-changing effects of shapes in a mapping "category -> process -> parameter -> (down effect, up effect)"
        shape_effects: dict[str, dict[str, dict[str, tuple[float, float]]]]
        # evaluated parameter types after shape writing in a mapping "category -> process -> parameter -> type"
        parameter_types: dict[str, dict[str, dict[str, ParameterType]]]
        # parameters whose transformations have been evaluated in a mapping "category -> process -> parameter names"
        evaluated_trafos: dict[str, dict[str, set[str]]]
        # the datacard pattern for extracting nominal shapes in CMS combine notation
        # (variables: $CHANNEL, $PROCESS)
        nom_pattern: str
        # the datacard pattern for extracting systematic shapes in CMS combine notation
        # (variables: $CHANNEL, $PROCESS, $SYSTEMATIC)
        syst_pattern: str

    def __init__(
        self,
        inference_model_inst: InferenceModel,
        histograms: DatacardHists,
        rate_precision: int = 4,
        effect_precision: int = 4,
        shape_transformer_kwargs: dict[str, Any] | None = None,
    ) -> None:
        super().__init__()

        # store attributes
        self.inference_model_inst = inference_model_inst
        self.histograms = histograms
        self.rate_precision = rate_precision
        self.effect_precision = effect_precision

        # create a shape transformer instance
        self.transformer = self.shape_transformer_cls(**(shape_transformer_kwargs or {}))

        # validate the inference model and histograms
        self.validate_model(self.inference_model_inst)
        self.validate_histograms(self.histograms)

    def write(
        self,
        datacard_path: str,
        shapes_path: str,
        shapes_path_ref: str | None = None,
    ) -> None:
        """
        Writes the datacard into *datacard_path* with shapes saved in *shapes_path*. When the paths
        exhibit the same directory and *shapes_path_ref* is not set, the shapes file reference is
        relative to the datacard.
        """
        # determine full paths and the shapes path reference to put into the card
        datacard_path = real_path(datacard_path)
        shapes_path = real_path(shapes_path)
        if not shapes_path_ref:
            shapes_path_ref = os.path.relpath(shapes_path, os.path.dirname(datacard_path))

        # write the shapes files
        shape_data = self.write_shapes(shapes_path)

        # get category objects
        cat_objects = [self.inference_model_inst.get_category(cat_name) for cat_name in shape_data.rates]

        # prepare blocks and lines to write
        blocks: DotDict[str, list] = DotDict()
        separators = set()
        empty_lines = set()

        # extra info
        blocks.extra = [f"# created with columnflow v{cf_version}"]
        empty_lines.add("extra")

        # counts block
        blocks.counts = [("imax", "*"), ("jmax", "*"), ("kmax", "*")]
        separators.add("counts")

        # shape lines
        blocks.shapes = [("shapes", "*", "*", shapes_path_ref, shape_data.nom_pattern, shape_data.syst_pattern)]
        separators.add("shapes")

        # store rate precisions per category
        rate_precisions = {
            cat_obj.name: self.rate_precision if cat_obj.rate_precision <= 0 else cat_obj.rate_precision
            for cat_obj in map(self.inference_model_inst.get_category, shape_data.rates.keys())
        }

        # observations
        blocks.observations = []
        if all("data" in _rates for _rates in shape_data.rates.values()):
            blocks.observations = [
                ("bin", list(shape_data.rates)),
                ("observation", [
                    maybe_int(round(_rates["data"], rate_precisions[cat_name]))
                    for cat_name, _rates in shape_data.rates.items()
                ]),
            ]
            separators.add("observations")

        # expected rates
        proc_names, s_names, b_names = [], [], []
        flat_rates = collections.OrderedDict()
        for cat_name, _rates in shape_data.rates.items():
            for proc_name, rate in _rates.items():
                if proc_name == "data":
                    continue

                # devide into signal and backgrounds
                if proc_name not in proc_names:
                    proc_obj = self.inference_model_inst.get_process(proc_name, category=cat_name)
                    (s_names if proc_obj.is_signal else b_names).append(proc_name)

                # fill flat rates
                flat_rates[(cat_name, proc_name)] = rate

        blocks.rates = [
            ("bin", [cat_name for cat_name, _ in flat_rates]),
            ("process", [proc_name for _, proc_name in flat_rates]),
            ("process", [
                (-s_names.index(proc_name) if proc_name in s_names else b_names.index(proc_name) + 1)
                for _, proc_name in flat_rates
            ]),
            ("rate", [
                round(rate, rate_precisions[cat_name])
                for (cat_name, _), rate in flat_rates.items()
            ]),
        ]
        separators.add("rates")

        # tabular-style parameters
        blocks.tabular_parameters = []
        for param_name in self.inference_model_inst.get_parameters(flat=True):
            types = set()
            effects = []
            for cat_name, proc_name in flat_rates:
                cat_obj = self.inference_model_inst.get_category(category=cat_name)
                proc_obj = self.inference_model_inst.get_process(category=cat_name, process=proc_name)
                param_obj = self.inference_model_inst.get_parameter(
                    parameter=param_name,
                    category=cat_name,
                    process=proc_name,
                    silent=True,
                )

                # skip empty effects
                if param_obj is None:
                    effects.append("-")
                    continue

                # skip line-style parameters as they are handled separately below
                param_type = shape_data.parameter_types[cat_name][proc_name][param_name]
                if param_obj and param_type == ParameterType.rate_unconstrained:
                    continue

                # compare with previously seen types as combine cannot mix arbitrary parameter types acting differently
                # on different processes
                types.add(param_type)
                if len(types) > 1 and types != {ParameterType.rate_gauss, ParameterType.shape}:
                    raise ValueError(
                        f"misconfigured parameter '{param_name}' with type '{param_type}' that was previously seen "
                        f"with incompatible type(s) '{types - {param_type}}'",
                    )

                # get the effect
                effect = param_obj.effect

                # rounding helper depending on the effect precision
                effect_precision = (
                    self.effect_precision
                    if param_obj.effect_precision <= 0
                    else param_obj.effect_precision
                )

                def rnd(f: float | int) -> float:
                    r = round(f, effect_precision)
                    # warn in case the precision is too low for the effect
                    if abs(1.0 - f) < 10**(-effect_precision):
                        logger.warning(
                            f"the effect value '{f}' is rounded to '{r}' which probably leads to loosing its intended "
                            f"impact; consider choosing an effect precision higher than the current value of "
                            f"{effect_precision} for the paremeter '{param_name}' acting on process '{proc_name}' in "
                            f"category '{cat_name}'",
                        )
                    return r

                # update and transform effects
                if param_type.is_shape:
                    # when the shape was originally constructed from a rate, reset the effect to 1
                    if param_obj.type.is_rate:
                        effect = 1.0

                elif param_type.is_rate:
                    # when the rate was originally constructed from a shape, read the effect from the shape data
                    if param_obj.type.is_shape:
                        effect = shape_data.shape_effects[cat_name][proc_name][param_name]

                    elif param_name not in shape_data.evaluated_trafos[cat_name][proc_name]:
                        # in this case, the transformation sequence was not evaluated during shape writing, so do it now
                        trafo_output: ShapeTransformer.TransormationOutput = self.transformer.apply_transformations(
                            param_obj=param_obj,
                            h_nom=shape_data.nominal_hists[cat_name][proc_name],
                        )

                        # store the effect
                        effect = trafo_output.effect

                # custom hook to modify the effect
                effect = self.modify_parameter_effect(cat_obj, proc_obj, param_obj, effect)

                # encode the effect
                encoded_effect: str
                if param_type.is_shape and effect in {None, 1}:
                    encoded_effect = "1"
                elif isinstance(effect, (int, float)):
                    if effect == 0.0:
                        encoded_effect = "-"
                    else:
                        encoded_effect = str(rnd(effect))
                elif isinstance(effect, (tuple, list)) and self.transformer.validate_effect(effect):
                    encoded_effect = f"{rnd(effect[0])}/{rnd(effect[1])}"
                else:
                    raise ValueError(
                        f"effect '{effect}' (type {type(effect)}) of parameter '{param_name}' with type "
                        f"{param_obj.type} on process '{proc_name}' in category '{cat_name}' cannot be encoded",
                    )
                effects.append(encoded_effect)

            # add the tabular line
            if types and effects:
                type_str = None
                if len(types) == 1:
                    _type = list(types)[0]
                    if _type == ParameterType.rate_gauss:
                        type_str = "lnN"
                    elif _type == ParameterType.rate_uniform:
                        type_str = "lnU"
                    elif _type == ParameterType.shape:
                        type_str = "shape"
                elif types == {ParameterType.rate_gauss, ParameterType.shape}:
                    # when mixing lnN and shape effects, combine expects the "shape?" type and makes the actual decision
                    # dependent on the presence of shape variations in the accompanying shape files, see
                    # https://cms-analysis.github.io/HiggsAnalysis-CombinedLimit/v10.2.X/part2/settinguptheanalysis/?h=shape%3F#template-shape-uncertainties # noqa: E501
                    # (this hopefully gets solved less hacky in the future)
                    type_str = "shape?"
                if not type_str:
                    raise ValueError(f"misconfigured parameter '{param_name}' with incompatible type(s) '{types}'")
                blocks.tabular_parameters.append([param_name, type_str, effects])

        # alphabetical, case-insensitive order by name
        blocks.tabular_parameters.sort(key=lambda line: line[0].lower())

        if blocks.tabular_parameters:
            empty_lines.add("tabular_parameters")

        # line-style parameters
        blocks.line_parameters = []
        for param_name in self.inference_model_inst.get_parameters(flat=True):
            for cat_name, proc_name in flat_rates:
                param_obj = self.inference_model_inst.get_parameter(
                    param_name,
                    category=cat_name,
                    process=proc_name,
                    silent=True,
                )

                # skip non-line-style parameters
                if not param_obj or param_obj.type != ParameterType.rate_unconstrained:
                    continue

                # add the line
                blocks.line_parameters.append([
                    param_name,
                    "rateParam",
                    cat_name,
                    proc_name,
                    param_obj.effect,
                ])

        if blocks.line_parameters:
            empty_lines.add("line_parameters")

        # groups
        blocks.groups = []
        for group in self.inference_model_inst.get_parameter_groups():
            blocks.groups.append([group.name, "group", "="] + group.parameter_names)

        if blocks.groups:
            empty_lines.add("groups")

        # mc stats
        blocks.mc_stats = []
        for cat_obj in cat_objects:
            mc_stats = cat_obj.mc_stats
            if mc_stats not in (None, False):
                # default value when True
                if isinstance(mc_stats, bool):
                    mc_stats = 10
                mc_stats_list = list(map(str, law.util.make_list(mc_stats)))
                blocks.mc_stats.append([cat_obj.name, "autoMCStats"] + mc_stats_list)

        # prettify blocks
        if blocks.observations:
            blocks.observations = self.align_lines(list(blocks.observations))
        if blocks.tabular_parameters:
            blocks.rates, blocks.tabular_parameters = self.align_rates_and_parameters(
                list(blocks.rates),
                list(blocks.tabular_parameters),
            )
        else:
            blocks.rates = self.align_lines(list(blocks.rates))
        if blocks.line_parameters:
            blocks.line_parameters = self.align_lines(list(blocks.line_parameters))
        if blocks.groups:
            blocks.groups = self.align_lines(list(blocks.groups), end=3)
        if blocks.mc_stats:
            blocks.mc_stats = self.align_lines(list(blocks.mc_stats))

        # allow modification before writing via hook
        blocks, separators, empty_lines = self.modify_before_write(blocks, separators, empty_lines)

        # write the blocks
        with open(datacard_path, "w") as f:
            for block_name, lines in blocks.items():
                if not lines:
                    continue

                # block lines
                for line in lines:
                    if isinstance(line, (list, tuple)):
                        line = self.col_sep.join(map(str, law.util.flatten(line)))
                    f.write(f"{line}\n")

                # block separator
                if block_name in separators:
                    f.write(100 * "-" + "\n")
                elif block_name in empty_lines:
                    f.write("\n")

    def write_shapes(
        self,
        shapes_path: str,
    ) -> ShapeData:
        """
        Create the shapes file at *shapes_path* and returns a :py:class:`ShapeData` object, containing all info from
        shape handling and serialization.
        """
        import uproot

        # create the directory
        shapes_path = real_path(shapes_path)
        shapes_dir = os.path.dirname(shapes_path)
        ensure_dir(shapes_dir)

        # define shape patterns
        data_pattern = "{category}/data_obs"
        nom_pattern = "{category}/{process}"
        nom_pattern_comb = "$CHANNEL/$PROCESS"
        syst_pattern = "{category}/{process}__{parameter}{direction}"
        syst_pattern_comb = "$CHANNEL/$PROCESS__$SYSTEMATIC"

        # prepare book-keeping dicts
        rates = collections.OrderedDict()
        effects = collections.OrderedDict()
        param_types = collections.OrderedDict()
        evaluated_trafos = collections.OrderedDict()
        nom_hists = collections.OrderedDict()
        out_hists = collections.OrderedDict()

        # helper to handle and apply flow strategy to histogram
        def handle_flow(cat_obj, h, name):
            # stop early if flow is ignored altogether
            if cat_obj.flow_strategy == FlowStrategy.ignore:
                return

            # get objects and flow contents
            ax = h.axes[0]
            view = h.view(flow=True)
            underflow = (view.value[0], view.variance[0]) if ax.traits.underflow else (0.0, 0.0)
            overflow = (view.value[-1], view.variance[-1]) if ax.traits.overflow else (0.0, 0.0)

            # nothing to do if flow bins are emoty
            if not underflow[0] and not overflow[0]:
                return

            # warn in case of flow content
            if cat_obj.flow_strategy in {FlowStrategy.warn, FlowStrategy.move}:
                move_msg = "; will be moved to first/last bin" if cat_obj.flow_strategy == FlowStrategy.move else ""
                if underflow[0]:
                    logger.warning_once(
                        f"underflow_warn_{self.inference_model_inst.cls_name}_{cat_obj.name}_{name}",
                        f"underflow content detected in category '{cat_obj.name}' for histogram "
                        f"'{name}' ({underflow[0] / view.value.sum() * 100:.1f}% of integral){move_msg}",
                    )
                if overflow[0]:
                    logger.warning_once(
                        f"overflow_warn_{self.inference_model_inst.cls_name}_{cat_obj.name}_{name}",
                        f"overflow content detected in category '{cat_obj.name}' for histogram "
                        f"'{name}' ({overflow[0] / view.value.sum() * 100:.1f}% of integral){move_msg}",
                    )

            # stop here in case of warn-only
            if cat_obj.flow_strategy == FlowStrategy.warn:
                return

            # here, we can already remove overflow values
            if underflow[0]:
                view.value[0] = 0.0
                view.variance[0] = 0.0
            if overflow[0]:
                view.value[-1] = 0.0
                view.variance[-1] = 0.0

            # finally handle move
            if cat_obj.flow_strategy == FlowStrategy.move:
                if underflow[0]:
                    view.value[1] += underflow[0]
                    view.variance[1] += underflow[1]
                if overflow[0]:
                    view.value[-2] += overflow[0]
                    view.variance[-2] += overflow[1]

        # iterate through shapes
        for cat_name, proc_hists in self.histograms.items():
            cat_obj = self.inference_model_inst.get_category(cat_name)

            _rates = rates[cat_name] = collections.OrderedDict()
            _effects = effects[cat_name] = collections.OrderedDict()
            _param_types = param_types[cat_name] = collections.OrderedDict()
            _evaluated_trafos = evaluated_trafos[cat_name] = collections.OrderedDict()
            _nom_hists = nom_hists[cat_name] = collections.OrderedDict()
            _out_hists = out_hists[cat_name] = collections.OrderedDict()

            for proc_name, config_hists in proc_hists.items():
                # skip if process is not known to category
                proc_obj = self.inference_model_inst.get_process(process=proc_name, category=cat_name, silent=True)
                if not proc_obj:
                    continue

                # defer the handling of data to the end
                if proc_name == "data":
                    continue

                # flat list of hists for configs that contribute to this category
                hists: list[dict[Hashable, hist.Hist]] = [
                    hd for config_name, hd in config_hists.items()
                    if not cat_obj.config_data or config_name in cat_obj.config_data
                ]
                if not hists:
                    continue

                # helper to sum over histograms for a given shift key and an optional fallback
                def get_hist_sum(key: Hashable, fallback_key: Hashable | None = None) -> hist.Hist:
                    def get(hd: dict[Hashable, hist.Hist]) -> hist.Hist:
                        if key in hd:
                            return hd[key]
                        if fallback_key and fallback_key in hd:
                            return hd[fallback_key]
                        raise Exception(
                            f"'{key}' shape for process '{proc_name}' in category '{cat_name}' misconfigured: {hd}",
                        )
                    return sum_hists(map(get, hists))

                # optionally skip the process under specific conditions
                if (skip_reason := self.check_skip_process(cat_obj, proc_obj, get_hist_sum(od.Shift.NOMINAL))):
                    skip_msg = f"skipping process '{proc_name}' in category '{cat_name}'"
                    if not isinstance(skip_reason, bool):
                        skip_msg += f", reason: {skip_reason}"
                    skip_msg = logger.info(skip_msg)
                    continue

                # helper to fill empty bins in-place
                def fill_empty(h: hist.Hist) -> None:
                    empty_bin_value = proc_obj.empty_bin_value
                    if empty_bin_value is None:
                        empty_bin_value = cat_obj.empty_bin_value
                    if empty_bin_value is None or empty_bin_value <= 0:
                        return
                    value = h.view().value
                    mask = value <= 0
                    value[mask] = empty_bin_value
                    h.view().variance[mask] = empty_bin_value

                # helper to extract sum of hists, apply scale, handle flow and fill empty bins
                def load(
                    hist_name: str,
                    hist_key: Hashable,
                    fallback_key: Hashable | None = None,
                    scale: float = 1.0,
                ) -> hist.Hist:
                    h = get_hist_sum(hist_key, fallback_key) * scale
                    handle_flow(cat_obj, h, hist_name)
                    fill_empty(h)
                    return h

                # get the process scale (usually 1)
                scale = proc_obj.scale

                # nominal shape
                nom_name = nom_pattern.format(category=cat_name, process=proc_name)
                h_nom = load(nom_name, od.Shift.NOMINAL, scale=scale)
                integral = lambda h: h.sum().value

                # prepare book-keeping dicts
                __effects = _effects[proc_name] = collections.OrderedDict()
                __param_types = _param_types[proc_name] = collections.OrderedDict()
                __evaluated_trafos = _evaluated_trafos[proc_name] = set()
                __out_hists = _out_hists[proc_name] = collections.OrderedDict()

                # go through all parameters and potentially handle varied shapes
                for _, _, param_obj in self.inference_model_inst.iter_parameters(category=cat_name, process=proc_name):
                    # store the initial parameter type
                    __param_types[param_obj.name] = param_obj.type

                    # the parameter can be skipped under certain conditions
                    if (
                        # initially not a shape
                        not param_obj.type.is_shape and
                        # does not change to a shape
                        not param_obj.transformations.any_changes_type and
                        # does not change nominal
                        not param_obj.transformations.any_changes_nominal
                    ):
                        continue
                    __evaluated_trafos.add(param_obj.name)

                    # prepare up/down shape names
                    down_name = syst_pattern.format(
                        category=cat_name,
                        process=proc_name,
                        parameter=param_obj.name,
                        direction=od.Shift.DOWN.capitalize(),
                    )
                    up_name = syst_pattern.format(
                        category=cat_name,
                        process=proc_name,
                        parameter=param_obj.name,
                        direction=od.Shift.UP.capitalize(),
                    )

                    # extract the varied histograms from the input when needed
                    h_varied = None
                    if param_obj.type.is_shape:
                        h_varied = (
                            load(down_name, (param_obj.name, od.Shift.DOWN), fallback_key=od.Shift.NOMINAL, scale=scale),
                            load(up_name, (param_obj.name, od.Shift.UP), fallback_key=od.Shift.NOMINAL, scale=scale),
                        )

                    # apply the sequence of transformations
                    trafo_output: ShapeTransformer.TransormationOutput = self.transformer.apply_transformations(
                        param_obj=param_obj,
                        h_nom=h_nom,
                        h_varied=h_varied,
                    )

                    # update iteration variables
                    h_nom = trafo_output.h_nom
                    __param_types[param_obj.name] = trafo_output.param_type

                    if trafo_output.param_type.is_rate:
                        # when then type changed to rate, we only need to save the converted rate effect
                        __effects[param_obj.name] = trafo_output.effect

                    elif trafo_output.param_type.is_shape:
                        # otherwise, handle shapes

                        # create a shallow copy of the parameter object with potentially updated type
                        _param_obj = DotDict({**param_obj, "type": trafo_output.param_type})

                        # unpack shapes
                        h_nom = trafo_output.h_nom
                        h_down = trafo_output.h_varied[0]
                        h_up = trafo_output.h_varied[1]

                        # custom hook to modify the shapes
                        h_nom, h_down, h_up = self.modify_parameter_shape(
                            cat_obj,
                            proc_obj,
                            _param_obj,
                            h_nom,
                            h_down,
                            h_up,
                        )

                        # fill empty bins again after all transformations
                        fill_empty(h_down)
                        fill_empty(h_up)

                        # save the effect
                        __effects[param_obj.name] = (
                            safe_div(integral(h_down), integral(h_nom)),
                            safe_div(integral(h_up), integral(h_nom)),
                        )

                        # store hists
                        __out_hists[down_name] = h_down
                        __out_hists[up_name] = h_up

                # store the nominal hist too and move it to the front
                __out_hists[nom_name] = h_nom
                __out_hists.move_to_end(nom_name, last=False)
                _nom_hists[proc_name] = h_nom
                _rates[proc_name] = h_nom.sum().value

            # data handling, first checking if data should be faked, then if real data exists
            if cat_obj.data_from_processes:
                # fake data from processes
                h_data = []
                for proc_name in cat_obj.data_from_processes:
                    if proc_name in proc_hists:
                        h_data.extend([hd[od.Shift.NOMINAL] for hd in proc_hists[proc_name].values()])
                    else:
                        logger.warning(f"process '{proc_name}' not found in histograms for creating fake data, skipping")
                if not h_data:
                    proc_str = ",".join(map(str, cat_obj.data_from_processes))
                    raise Exception(f"none of requested processes '{proc_str}' found to create fake data")
                data_name = data_pattern.format(category=cat_name)
                h_data = sum_hists(h_data)
                handle_flow(cat_obj, h_data, data_name)
                h_data.view().variance = h_data.view().value
                _out_hists[data_name] = h_data
                _out_hists.move_to_end(data_name, last=False)
                _nom_hists["data"] = h_data
                _rates["data"] = float(h_data.sum().value)

            elif proc_hists.get("data"):
                # real data
                h_data = []
                for config_name, config_hists in proc_hists["data"].items():
                    if cat_obj.config_data and config_name not in cat_obj.config_data:
                        raise Exception(
                            f"received real data in datacard category '{cat_name}' for config '{config_name}', but the "
                            f"inference model '{self.inference_model_inst.cls_name}' is not configured to use it in "
                            f"the config_data for that config; configured config_names are "
                            f"'{','.join(cat_obj.config_data.keys())}'",
                        )
                    h_data.append(config_hists["nominal"])

                # simply save the data histogram that was already built from the requested datasets
                h_data = sum_hists(h_data)
                data_name = data_pattern.format(category=cat_name)
                handle_flow(cat_obj, h_data, data_name)
                _out_hists[data_name] = h_data
                _out_hists.move_to_end(data_name, last=False)
                _nom_hists["data"] = h_data
                _rates["data"] = h_data.sum().value

            else:
                logger.warning(f"neither real data found nor fake data created in category '{cat_name}'")

        # write to file
        q = collections.deque(out_hists.items())
        with uproot.recreate(shapes_path) as out_file:
            while q:
                name, h = q.popleft()
                if isinstance(h, dict):
                    q.extendleft(reversed(h.items()))
                else:
                    out_file[name] = h

        return self.ShapeData(
            nominal_hists=nom_hists,
            rates=rates,
            shape_effects=effects,
            parameter_types=param_types,
            evaluated_trafos=evaluated_trafos,
            nom_pattern=nom_pattern_comb,
            syst_pattern=syst_pattern_comb,
        )

    def validate_model(self, inference_model_inst: InferenceModel) -> None:
        # per category and process, validate all parameters
        for cat_obj in inference_model_inst.categories:
            for proc_obj in cat_obj.processes:
                try:
                    self.transformer.validate_parameters(proc_obj.parameters)
                except Exception as e:
                    raise ValueError(
                        f"invalid parameters for process '{proc_obj.name}' in category '{cat_obj.name}': {e}",
                    ) from e

    def validate_histograms(self, histograms: DatacardHists) -> None:
        import hist

        # validate structure of histograms, shape keys and histogram types
        errors: list[str] = []
        for cat_name, proc_hists in histograms.items():
            if not isinstance(cat_name, str):
                errors.append(f"category name key '{cat_name}' is not a string")
            for proc_name, config_hists in proc_hists.items():
                if not isinstance(proc_name, str):
                    errors.append(f"process name '{proc_name}' in category '{cat_name}' is not a string")
                for config_name, shift_hists in config_hists.items():
                    if not isinstance(config_name, str):
                        errors.append(
                            f"config name '{config_name}' for process '{proc_name}' in category '{cat_name}' is not a "
                            f"string",
                        )
                    for shift_key, h in shift_hists.items():
                        # shift_key must be nominal or a tuple of (param_name, "up|down")
                        if (
                            shift_key != od.Shift.NOMINAL and
                            (
                                not isinstance(shift_key, (tuple, list)) or
                                len(shift_key) != 2 or
                                shift_key[1] not in {od.Shift.UP, od.Shift.DOWN}
                            )
                        ):
                            errors.append(
                                f"invalid shift key '{shift_key}' in config '{config_name}' for process '{proc_name}' "
                                f"in category '{cat_name}'",
                            )
                        if not isinstance(h, hist.Hist):
                            errors.append(
                                f"histogram for shift '{shift_key}' in config '{config_name}' for process "
                                f"'{proc_name}' in category '{cat_name}' is not a hist.Hist instance",
                            )

        # handle errors
        if errors:
            errors_repr = "\n  - ".join(errors)
            raise ValueError(f"datacard histograms invalid, reasons:\n  - {errors_repr}")

    @classmethod
    def align_lines(
        cls,
        lines: Sequence[Any],
        end: int = -1,
    ) -> list[str]:
        lines = [
            (line.split() if isinstance(line, str) else list(map(str, law.util.flatten(line))))
            for line in lines
        ]

        lengths = {min(len(line), 1e9 if end < 0 else end) for line in lines}
        if len(lengths) > 1:
            raise Exception(f"line alignment cannot be performed with lines of varying lengths: {lengths}")

        # convert to columns and get the maximum width per column
        n_cols = lengths.pop()
        cols = [
            [line[j] for line in lines]
            for j in range(n_cols)
        ]
        max_widths = [
            max(len(s) for s in col)
            for col in cols
        ]

        # stitch back
        return [
            cls.col_sep.join(
                f"{s: <{max_widths[j]}}" if end < 0 or j < end else s
                for j, s in enumerate(line)
            )
            for line in lines
        ]

    @classmethod
    def align_rates_and_parameters(
        cls,
        rates: Sequence[Any],
        parameters: Sequence[Any],
    ) -> tuple[list[str], list[str]]:
        rates, parameters = [
            [
                (line.split() if isinstance(line, str) else list(map(str, law.util.flatten(line))))
                for line in lines
            ]
            for lines in [rates, parameters]
        ]

        # first, align parameter names and types on their own
        param_starts = cls.align_lines([line[:2] for line in parameters])

        # prepend to parameter lines
        parameters = [([start] + line[2:]) for start, line in zip(param_starts, parameters)]

        # align in conjunction with rates
        n_rate_lines = len(rates)
        lines = cls.align_lines(rates + parameters)

        return lines[:n_rate_lines], lines[n_rate_lines:]

    def modify_before_write(
        self,
        blocks: DotDict[str, list],
        separators: set[str],
        empty_lines: set[str],
    ) -> tuple[DotDict[str, list], set[str], set[str]]:
        """
        Hook to modify the datacard blocks, empty lines and separators before they are written to the datacard file.

        :param blocks: Datacard blocks.
        :param separators: Set of block names after which a separator line should be inserted.
        :param empty_lines: Set of block names after which an empty line should be inserted.
        :returns: The modified datacard blocks, separators and empty lines.
        """
        return blocks, separators, empty_lines

    def modify_parameter_effect(
        self,
        cat_obj: DotDict,
        proc_obj: DotDict,
        param_obj: DotDict,
        effect: float | tuple[float, float],
    ) -> float | tuple[float, float]:
        """
        Custom hook to modify the effect of a parameter on a given category and process before it is encoded into the
        datacard. By default, this does nothing and simply returns the given effect.

        :param cat_obj: The category object, following :py:meth:`columnflow.inference.InferenceModel.category_spec`.
        :param proc_obj: The process object, following :py:meth:`columnflow.inference.InferenceModel.process_spec`.
        :param param_obj: The parameter object, following :py:meth:`columnflow.inference.InferenceModel.parameter_spec`.
        :param effect: The effect value(s) to be modified.
        :returns: The modified effect value(s).
        """
        return effect

    def modify_parameter_shape(
        self,
        cat_obj: DotDict,
        proc_obj: DotDict,
        param_obj: DotDict,
        h_nom: hist.Hist,
        h_down: hist.Hist,
        h_up: hist.Hist,
    ) -> tuple[hist.Hist, hist.Hist, hist.Hist]:
        """
        Custom hook to modify the nominal and varied (down, up) shapes of a parameter on a given category and process
        before they are saved to the shapes file. By default, this does nothing and simply returns the given histograms.

        :param cat_obj: The category object, following :py:meth:`columnflow.inference.InferenceModel.category_spec`.
        :param proc_obj: The process object, following :py:meth:`columnflow.inference.InferenceModel.process_spec`.
        :param param_obj: The parameter object, following :py:meth:`columnflow.inference.InferenceModel.parameter_spec`.
        :param h_nom: The nominal histogram.
        :param h_down: The down-varied histogram.
        :param h_up: The up-varied histogram.
        :returns: The modified nominal and varied (down, up) histograms.
        """
        return h_nom, h_down, h_up

    def check_skip_process(
        self,
        cat_obj: DotDict,
        proc_obj: DotDict,
        h: hist.Hist,
    ) -> bool | str:
        """
        Custom hook to check if a process in a given category should be skipped entirely based on the nominal histogram
        and the process and category objects. If a string is returned, it is added to the log message as a reason for
        skipping the process.

        :param cat_obj: The category object, following :py:meth:`columnflow.inference.InferenceModel.category_spec`.
        :param proc_obj: The process object, following :py:meth:`columnflow.inference.InferenceModel.process_spec`.
        :param h: The nominal histogram for the process in the category.
        :returns: Whether to skip the process, and optionally a reason for skipping.
        """
        if proc_obj.skip_if_empty and np.all(h.view().value == 0):
            return "nominal histogram is empty"
        return False
