# coding: utf-8

"""
Helpers to write and work with datacards.
"""

from __future__ import annotations

import os
from collections import OrderedDict

import law

from columnflow import __version__ as cf_version
from columnflow.inference import InferenceModel, ParameterType, ParameterTransformation, FlowStrategy
from columnflow.util import DotDict, maybe_import, real_path, ensure_dir, safe_div, maybe_int
from columnflow.types import Sequence, Any, Union, Hashable

hist = maybe_import("hist")


logger = law.logger.get_logger(__name__)

# type aliases for nested histogram structs
ShiftHists = dict[Union[str, tuple[str, str]], hist.Hist]  # "nominal" or (param_name, "up|down") -> hists
ConfigHists = dict[str, ShiftHists]  # config name -> hists
ProcHists = dict[str, ConfigHists]  # process name -> hists
DatacardHists = dict[str, ProcHists]  # category name -> hists


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
    """

    # minimum separator between columns
    col_sep = "  "

    def __init__(
        self,
        inference_model_inst: InferenceModel,
        histograms: DatacardHists,
        rate_precision: int = 4,
        effect_precision: int = 4,
    ) -> None:
        super().__init__()

        # store attributes
        self.inference_model_inst = inference_model_inst
        self.histograms = histograms
        self.rate_precision = rate_precision
        self.effect_precision = effect_precision

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
        rates, shape_effects, nom_pattern, syst_pattern = self.write_shapes(shapes_path)

        # get category objects
        cat_objects = [self.inference_model_inst.get_category(cat_name) for cat_name in rates]

        # prepare blocks and lines to write
        blocks = DotDict()
        separators = set()
        empty_lines = set()

        # extra info
        blocks.extra = [f"# created with columnflow v{cf_version}"]
        empty_lines.add("extra")

        # counts block
        blocks.counts = [("imax", "*"), ("jmax", "*"), ("kmax", "*")]
        separators.add("counts")

        # shape lines
        blocks.shapes = [("shapes", "*", "*", shapes_path_ref, nom_pattern, syst_pattern)]
        separators.add("shapes")

        # store rate precisions per category
        rate_precisions = {
            cat_obj.name: self.rate_precision if cat_obj.rate_precision <= 0 else cat_obj.rate_precision
            for cat_obj in map(self.inference_model_inst.get_category, rates.keys())
        }

        # observations
        blocks.observations = []
        if all("data" in _rates for _rates in rates.values()):
            blocks.observations = [
                ("bin", list(rates)),
                ("observation", [
                    maybe_int(round(_rates["data"], rate_precisions[cat_name]))
                    for cat_name, _rates in rates.items()
                ]),
            ]
            separators.add("observations")

        # expected rates
        proc_names, s_names, b_names = [], [], []
        flat_rates = OrderedDict()
        for cat_name, _rates in rates.items():
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
            param_obj = None
            effects = []
            for cat_name, proc_name in flat_rates:
                _param_obj = self.inference_model_inst.get_parameter(
                    param_name,
                    category=cat_name,
                    process=proc_name,
                    silent=True,
                )

                # skip line-style parameters as they are handled separately below
                if _param_obj and _param_obj.type == ParameterType.rate_unconstrained:
                    continue

                # empty effect
                if _param_obj is None:
                    effects.append("-")
                    continue

                # compare with previous param_obj
                if param_obj is None:
                    param_obj = _param_obj
                elif _param_obj.type != param_obj.type:
                    raise ValueError(
                        f"misconfigured parameter '{param_name}' with type '{_param_obj.type}' that was previously "
                        f"seen with incompatible type '{param_obj.type}'",
                    )

                # get the effect
                effect = _param_obj.effect

                # rounding helper depending on the effect precision
                effect_precision = (
                    self.effect_precision
                    if _param_obj.effect_precision <= 0
                    else _param_obj.effect_precision
                )
                rnd = lambda f: round(f, effect_precision)

                # update and transform effects
                if _param_obj.type.is_rate:
                    # obtain from shape effects when requested
                    if _param_obj.transformations.any_from_shape:
                        effect = shape_effects[cat_name][proc_name][param_name]

                    # apply transformations one by one
                    for trafo in _param_obj.transformations:
                        if trafo == ParameterTransformation.centralize:
                            # skip symmetric effects
                            if not isinstance(effect, tuple) and len(effect) != 2:
                                continue
                            # skip one sided effects
                            if not (min(effect) <= 1 <= max(effect)):
                                continue
                            d, u = effect
                            diff = 0.5 * (d + u) - 1.0
                            effect = (effect[0] - diff, effect[1] - diff)

                        elif trafo == ParameterTransformation.symmetrize:
                            # skip symmetric effects
                            if not isinstance(effect, tuple) and len(effect) != 2:
                                continue
                            # skip one sided effects
                            if not (min(effect) <= 1 <= max(effect)):
                                continue
                            d, u = effect
                            effect = 0.5 * (u - d) + 1.0

                        elif trafo == ParameterTransformation.asymmetrize or (
                            trafo == ParameterTransformation.asymmetrize_if_large and
                            isinstance(effect, float) and
                            abs(effect - 1.0) >= 0.2
                        ):
                            # skip asymmetric effects
                            if not isinstance(effect, float):
                                continue
                            effect = (2.0 - effect, effect)

                elif _param_obj.type.is_shape:
                    # when the shape was constructed from a rate, reset the effect to 1
                    if _param_obj.transformations.any_from_rate:
                        effect = 1.0

                # encode the effect
                if isinstance(effect, (int, float)):
                    if effect == 0.0:
                        effects.append("-")
                    elif effect == 1.0 and _param_obj.type.is_shape:
                        effects.append("1")
                    else:
                        effects.append(str(rnd(effect)))
                elif isinstance(effect, tuple) and len(effect) == 2:
                    effects.append(f"{rnd(effect[0])}/{rnd(effect[1])}")
                else:
                    raise ValueError(
                        f"effect '{effect}' of parameter '{param_name}' with type {param_obj.type} on process "
                        f"'{proc_name}' in category '{cat_name}' cannot be encoded",
                    )

            # add the tabular line
            if param_obj and effects:
                type_str = "shape"
                if param_obj.type == ParameterType.rate_gauss:
                    type_str = "lnN"
                elif param_obj.type == ParameterType.rate_uniform:
                    type_str = "lnU"
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
    ) -> tuple[
        dict[str, dict[str, float]],
        dict[str, dict[str, dict[str, tuple[float, float]]]],
        str,
        str,
    ]:
        """
        Create the shapes file at *shapes_path* and returns a tuple with four items,

            - the nominal rates in a nested mapping "category -> process -> rate",
            - rate-changing effects of shape systematics in a nested mapping
              "category -> process -> parameter -> (down effect, up effect)",
            - the datacard pattern for extracting nominal shapes, and
            - the datacard pattern for extracting systematic shapes.
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

        # prepare rates and shape effects
        rates = OrderedDict()
        effects = OrderedDict()

        # create the output file
        out_file = uproot.recreate(shapes_path)

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
            if cat_obj.flow_strategy == FlowStrategy.warn:
                if underflow[0]:
                    logger.warning(
                        f"underflow content detected in category '{cat_obj.name}' for histogram "
                        f"'{name}' ({underflow[0] / view.value.sum() * 100:.1f}% of integral)",
                    )
                if overflow[0]:
                    logger.warning(
                        f"overflow content detected in category '{cat_obj.name}' for histogram "
                        f"'{name}' ({overflow[0] / view.value.sum() * 100:.1f}% of integral)",
                    )
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

        # helper to fill empty bins in-place
        def fill_empty(cat_obj, h):
            if not cat_obj.empty_bin_value:
                return
            value = h.view().value
            mask = value <= 0
            value[mask] = cat_obj.empty_bin_value
            h.view().variance[mask] = cat_obj.empty_bin_value

        # iterate through shapes
        for cat_name, proc_hists in self.histograms.items():
            cat_obj = self.inference_model_inst.get_category(cat_name)

            _rates = rates[cat_name] = OrderedDict()
            _effects = effects[cat_name] = OrderedDict()
            for proc_name, config_hists in proc_hists.items():
                # skip if process is not known to category
                if not self.inference_model_inst.has_process(process=proc_name, category=cat_name):
                    continue

                # defer the handling of data to the end
                if proc_name == "data":
                    continue

                # flat list of hists for configs that contribute to this category
                hists: list[dict[Hashable, hist.Hist]] = [
                    hd for config_name, hd in config_hists.items()
                    if config_name in cat_obj.config_data
                ]
                if not hists:
                    continue

                # helper to sum over them for a given shift key and an optional fallback
                def sum_hists(key: Hashable, fallback_key: Hashable | None = None) -> hist.Hist:
                    def get(hd: dict[Hashable, hist.Hist]) -> hist.Hist:
                        if key in hd:
                            return hd[key]
                        if fallback_key and fallback_key in hd:
                            return hd[fallback_key]
                        raise Exception(
                            f"'{key}' shape for process '{proc_name}' in category '{cat_name}' misconfigured: {hd}",
                        )
                    return sum(map(get, hists[1:]), get(hists[0]).copy())

                # get the process scale (usually 1)
                proc_obj = self.inference_model_inst.get_process(proc_name, category=cat_name)
                scale = proc_obj.scale

                # nominal shape
                h_nom = sum_hists("nominal") * scale
                nom_name = nom_pattern.format(category=cat_name, process=proc_name)
                fill_empty(cat_obj, h_nom)
                handle_flow(cat_obj, h_nom, nom_name)
                out_file[nom_name] = h_nom
                _rates[proc_name] = h_nom.sum().value

                # prepare effects
                __effects = _effects[proc_name] = OrderedDict()

                # go through all parameters and check if varied shapes need to be processed
                for _, _, param_obj in self.inference_model_inst.iter_parameters(category=cat_name, process=proc_name):
                    # read or create the varied histograms, or skip the parameter
                    if param_obj.type.is_shape:
                        # the source of the shape depends on the transformation
                        if param_obj.transformations.any_from_rate:
                            if isinstance(param_obj.effect, float):
                                f_down, f_up = 2.0 - param_obj.effect, param_obj.effect
                            elif isinstance(param_obj.effect, tuple) and len(param_obj.effect) == 2:
                                f_down, f_up = param_obj.effect
                            else:
                                raise ValueError(
                                    f"cannot interpret effect of parameter '{param_obj.name}' to create shape: "
                                    f"{param_obj.effect}",
                                )
                            h_down = h_nom.copy() * f_down
                            h_up = h_nom.copy() * f_up
                        else:
                            # just extract the shapes
                            h_down = sum_hists((param_obj.name, "down"), "nominal") * scale
                            h_up = sum_hists((param_obj.name, "up"), "nominal") * scale

                    elif param_obj.type.is_rate:
                        if param_obj.transformations.any_from_shape:
                            # just extract the shapes
                            h_down = sum_hists((param_obj.name, "down"), "nominal") * scale
                            h_up = sum_hists((param_obj.name, "up"), "nominal") * scale
                        else:
                            # skip the parameter
                            continue

                    # apply optional transformations
                    integral = lambda h: h.sum().value
                    for trafo in param_obj.transformations:
                        if trafo == ParameterTransformation.envelope_if_one_sided:
                            n, d, u = integral(h_nom), integral(h_down), integral(h_up)
                            if (n - d) * (n - u) > 0:
                                # one-sided effect, use the larger variation
                                if abs(n - d) > abs(n - u):
                                    # use the down variation with effect flipped
                                    h_up = 2 * h_nom.copy() - h_down.view()
                                    # TODO: better estimate of the variance
                                    h_up.view().variance = h_down.variances()
                                else:
                                    # use the up variation with effect flipped
                                    h_down = 2 * h_nom.copy() - h_up.view()
                                    h_down.view().variance = h_up.variances()

                        elif trafo == ParameterTransformation.centralize:
                            # get the absolute spread based on integrals
                            n, d, u = integral(h_nom), integral(h_down), integral(h_up)
                            if not (min(d, n) <= n <= max(d, n)):
                                # skip one sided effects
                                logger.info(
                                    f"skipping shape centralization of parameter '{param_obj.name}' for process "
                                    f"'{proc_name}' in category '{cat_name}' as effect is one-sided",
                                )
                                continue
                            # find the central point, compute the diff w.r.t. nominal, and shift
                            diff = 0.5 * (d + u) - n
                            h_down *= safe_div(d - diff, d)
                            h_up *= safe_div(u - diff, u)

                        elif trafo == ParameterTransformation.normalize:
                            # normale varied hists to the nominal integral
                            h_down *= safe_div(integral(h_nom), integral(h_down))
                            h_up *= safe_div(integral(h_nom), integral(h_up))

                        else:
                            # no other transormation is applied at this point
                            continue

                    # empty bins are always filled
                    fill_empty(cat_obj, h_down)
                    fill_empty(cat_obj, h_up)

                    # save them when they represent real shapes
                    if param_obj.type.is_shape:
                        down_name = syst_pattern.format(
                            category=cat_name,
                            process=proc_name,
                            parameter=param_obj.name,
                            direction="Down",
                        )
                        up_name = syst_pattern.format(
                            category=cat_name,
                            process=proc_name,
                            parameter=param_obj.name,
                            direction="Up",
                        )
                        handle_flow(cat_obj, h_down, down_name)
                        handle_flow(cat_obj, h_up, up_name)
                        out_file[down_name] = h_down
                        out_file[up_name] = h_up

                    # save the effect
                    __effects[param_obj.name] = (
                        safe_div(integral(h_down), integral(h_nom)),
                        safe_div(integral(h_up), integral(h_nom)),
                    )

            # data handling, first checking if data should be faked, then if real data exists
            if cat_obj.data_from_processes:
                # fake data from processes
                h_data = []
                for proc_name in cat_obj.data_from_processes:
                    if proc_name in proc_hists:
                        h_data.extend([hd["nominal"] for hd in proc_hists[proc_name].values()])
                    else:
                        logger.warning(f"process '{proc_name}' not found in histograms for created fake data, skipping")
                if not h_data:
                    proc_str = ",".join(map(str, cat_obj.data_from_processes))
                    raise Exception(f"none of requested processes '{proc_str}' found to create fake data")
                h_data = sum(h_data[1:], h_data[0].copy())
                data_name = data_pattern.format(category=cat_name)
                fill_empty(cat_obj, h_data)
                handle_flow(cat_obj, h_data, data_name)
                out_file[data_name] = h_data
                _rates["data"] = float(h_data.sum().value)

            elif any(cd.data_datasets for cd in cat_obj.config_data.values()):
                h_data = []
                for config_name, config_data in cat_obj.config_data.items():
                    if "data" not in proc_hists or config_name not in proc_hists["data"]:
                        raise Exception(
                            f"the inference model '{self.inference_model_inst.cls_name}' is configured to use real "
                            f"data for config '{config_name}' in category '{cat_name}' but no histogram received at "
                            f"entry ['data']['{config_name}']: {proc_hists}",
                        )
                    h_data.append(proc_hists["data"][config_name]["nominal"])

                # simply save the data histogram that was already built from the requested datasets
                h_data = sum(h_data[1:], h_data[0].copy())
                data_name = data_pattern.format(category=cat_name)
                handle_flow(cat_obj, h_data, data_name)
                out_file[data_name] = h_data
                _rates["data"] = h_data.sum().value

        return (rates, effects, nom_pattern_comb, syst_pattern_comb)

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
