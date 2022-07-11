# coding: utf-8

"""
Helpers to write and work with datacards.
"""

import os
from collections import OrderedDict
from typing import Optional, Dict, Tuple, List, Sequence, Any

import law

from ap.inference import InferenceModel, ParameterType
from ap.util import DotDict, maybe_import, real_path, ensure_dir, safe_div


np = maybe_import("np")
hist = maybe_import("hist")
uproot = maybe_import("uproot")


class DatacardWriter(object):
    """
    Generic writer for combine datacards using a instance of an :py:class:`InferenceModel`
    *inference_model_inst* and a threefold nested dictionary "category -> process -> shift -> hist".

    *rate_precision* and *parameter_precision* control the number of digits of values for measured
    rates and parameter effects.

    .. note::

        At the moment, all shapes are written into the same root file and a shape line with
        wildcards for both bin and process resolution is created.
    """

    def __init__(
        self,
        inference_model_inst: InferenceModel,
        histograms: Dict[str, Dict[str, Dict[str, hist.Hist]]],
        rate_precision: int = 4,
        parameter_precision: int = 4,
        sep: str = "  ",
    ):
        super().__init__()

        # store attributes
        self.inference_model_inst = inference_model_inst
        self.histograms = histograms
        self.rate_precision = rate_precision
        self.parameter_precision = parameter_precision
        self.sep = sep

    def write(
        self,
        datacard_path: str,
        shapes_path: str,
        shapes_path_ref: Optional[str] = None,
    ) -> None:
        """
        Writes the datacard into *datacard_path* with shapes saved in *shapes_path*. When the paths
        exhibit the same directory and *shapes_path_ref* is not set, the shapes file reference is
        relative in the datacard.
        """
        # determine full paths and the shapes path reference to put into the card
        datacard_path = real_path(datacard_path)
        shapes_path = real_path(shapes_path)
        if not shapes_path_ref:
            shapes_path_ref = os.path.relpath(shapes_path, os.path.dirname(datacard_path))
            if shapes_path_ref.startswith(".."):
                # not relative to each other, use the absolute path
                shapes_path_ref = shapes_path

        # write the shapes files
        rates, shape_effects, nom_pattern, syst_pattern = self.write_shapes(shapes_path)

        # get category objects
        cat_objects = [self.inference_model_inst.get_category(cat_name) for cat_name in rates]

        # prepare blocks and lines to write
        blocks = DotDict()
        separators = set()
        empty_lines = set()

        # counts block
        blocks.counts = [("imax", "*"), ("jmax", "*"), ("kmax", "*")]
        separators.add("counts")

        # shape lines
        blocks.shapes = [("shapes", "*", "*", shapes_path_ref, nom_pattern, syst_pattern)]
        separators.add("shapes")
        separators.add("shapes")

        # observations
        if all("data" in _rates for _rates in rates.values()):
            blocks.observations = [
                ("bin", list(rates)),
                ("observation", [
                    round(_rates["data"], self.rate_precision)
                    for _rates in rates.values()
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
                    (s_names if proc_obj.signal else b_names).append(proc_name)
                # fill flat rates
                flat_rates[(cat_name, proc_name)] = rate

        blocks.rates = [
            ("bin", [cat_name for cat_name, _ in flat_rates]),
            ("process", [proc_name for _, proc_name in flat_rates]),
            ("process", [
                (-s_names.index(proc_name) if proc_name in s_names else b_names.index(proc_name) + 1)
                for _, proc_name in flat_rates
            ]),
            ("rate", [round(rate, self.rate_precision) for rate in flat_rates.values()]),
        ]
        separators.add("rates")

        # parameters
        blocks.parameters = []
        rnd = lambda f: round(f, self.parameter_precision)
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
                if _param_obj is None:
                    effects.append("-")
                    continue
                # compare with previous param_obj
                if param_obj is None:
                    param_obj = _param_obj
                elif _param_obj.type.is_rate != param_obj.type.is_rate:
                    raise ValueError(
                        f"misconfigured parameter '{param_name}' with type '{_param_obj.type}' "
                        f"that was previously seen with incompatible type '{param_obj.type}'")
                effect = _param_obj.effect
                if _param_obj.type == ParameterType.shape_to_rate:
                    effect = shape_effects[cat_name][proc_name][param_name]
                if isinstance(effect, tuple):
                    effects.append(f"{rnd(effect[0])}/{rnd(effect[1])}")
                elif effect == 1 and _param_obj.type.is_shape:
                    effects.append("1")
                else:
                    effects.append(str(rnd(effect)))
            # add the line
            if param_obj:
                type_str = "shape" if param_obj.type.is_shape else "lnN"
                blocks.parameters.append([param_name, type_str, effects])

        # mc stats
        blocks.mc_stats = []
        for cat_obj in cat_objects:
            if cat_obj.mc_stats:
                blocks.mc_stats.append([cat_obj.name, "autoMCStats", 10])  # TODO: configure this
                empty_lines.add("parameters")

        # prettify blocks
        blocks.observations = self.align_lines(list(blocks.observations), sep=self.sep)
        if blocks.parameters:
            blocks.rates, blocks.parameters = self.align_rates_and_parameters(
                list(blocks.rates),
                list(blocks.parameters),
                sep=self.sep,
            )
        else:
            blocks.rates = self.align_lines(list(blocks.rates), sep=self.sep)

        # write the blocks
        with open(datacard_path, "w") as f:
            for block_name, lines in blocks.items():
                if not lines:
                    continue

                # block lines
                for line in lines:
                    if isinstance(line, (list, tuple)):
                        line = self.sep.join(map(str, law.util.flatten(line)))
                    f.write(f"{line}\n")

                # block separator
                if block_name in separators:
                    f.write(100 * "-" + "\n")
                elif block_name in empty_lines:
                    f.write("\n")

    def write_shapes(
        self,
        shapes_path: str,
        fill_empty_bins: float = 1e-5,
    ) -> Tuple[
        Dict[str, Dict[str, float]],
        Dict[str, Dict[str, Dict[str, Tuple[float, float]]]],
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

        When *fill_empty_bins* is non-zero, empty (and negative!) bins are filled with this value.
        """
        # create the directory
        shapes_path = real_path(shapes_path)
        shapes_dir = os.path.dirname(shapes_path)
        ensure_dir(shapes_dir)

        # define shape patterns
        data_pattern = "{category}/data_obs"
        nom_pattern = "{category}/{process}"
        nom_pattern_comb = "$BIN/$PROCESS"
        syst_pattern = "{category}/{process}__{parameter}{direction}"
        syst_pattern_comb = "$BIN/$PROCESS__$SYSTEMATIC"

        # prepare rates and shape effects
        rates = OrderedDict()
        effects = OrderedDict()

        # create the output file
        out_file = uproot.recreate(shapes_path)

        # iterate through shapes
        for cat_name, hists in self.histograms.items():
            _rates = rates[cat_name] = OrderedDict()
            _effects = effects[cat_name] = OrderedDict()
            for proc_name, _hists in hists.items():
                __effects = _effects[proc_name] = OrderedDict()

                # defer the handling of data to the end
                if proc_name == "data":
                    continue

                # nominal shape
                h_nom = _hists["nominal"].copy()
                nom_name = nom_pattern.format(category=cat_name, process=proc_name)
                out_file[nom_name] = h_nom
                _rates[proc_name] = h_nom.sum().value

                # go through all shifted shapes
                for param_name, __hists in _hists.items():
                    if param_name == "nominal":
                        continue

                    if "up" not in __hists or "down" not in __hists:
                        raise Exception(
                            f"shapes of parameter '{param_name}' for process '{proc_name}' in "
                            f"category '{cat_name}' misconfigured: {__hists}",
                        )

                    # get the parameter object
                    param_obj = self.inference_model_inst.get_parameter(
                        param_name,
                        category=cat_name,
                        process=proc_name,
                    )

                    f = []
                    for d in ["down", "up"]:
                        h_syst = __hists[d].copy()

                        # apply optional transformations
                        if param_obj.type == ParameterType.norm_shape:
                            h_syst *= safe_div(h_nom.sum().value, h_syst.sum().value)

                        if fill_empty_bins:
                            value = h_syst.view().value
                            mask = value <= 0
                            value[mask] = fill_empty_bins
                            h_syst.view().variance[mask] = fill_empty_bins

                        # save it
                        if param_obj.type != ParameterType.shape_to_rate:
                            syst_name = syst_pattern.format(
                                category=cat_name,
                                process=proc_name,
                                parameter=param_name,
                                direction=d.capitalize(),
                            )
                            out_file[syst_name] = h_syst
                        f.append(safe_div(h_syst.sum().value, h_nom.sum().value))

                    __effects[param_name] = tuple(f)

            # dedicated data handling
            cat_obj = self.inference_model_inst.get_category(cat_name)
            if cat_obj.data_datasets:
                if "data" not in hists:
                    raise Exception(
                        f"the inference model '{self.inference_model_inst.name}' is configured to "
                        f"use real data in category '{cat_name}' but no histogram named 'data' "
                        "exists",
                    )

                # simply save the data histogram
                h_data = hists["data"]["nominal"].copy()
                data_name = data_pattern.format(category=cat_name)
                out_file[data_name] = h_data
                _rates["data"] = h_data.sum().value

            elif cat_obj.data_from_processes:
                # fake data from processes
                h_data = [hists[proc_name]["nominal"] for proc_name in cat_obj.data_from_processes]
                h_data = sum(h_data[1:], h_data[0].copy())
                data_name = data_pattern.format(category=cat_name)
                out_file[data_name] = h_data
                _rates["data"] = h_data.sum().value

        return (rates, effects, nom_pattern_comb, syst_pattern_comb)

    @classmethod
    def align_lines(
        cls,
        lines: Sequence[Any],
        sep: str = "  ",
    ) -> List[str]:
        lines = [
            (line.split() if isinstance(line, str) else list(map(str, law.util.flatten(line))))
            for line in lines
        ]

        lengths = {len(line) for line in lines}
        if len(lengths) > 1:
            raise Exception(
                f"line alignment cannot be performed with lines of varying lengths: {lengths}",
            )

        # convert to rows and get the maximum width per row
        n_rows = list(lengths)[0]
        rows = [
            [line[j] for line in lines]
            for j in range(n_rows)
        ]
        max_widths = [
            max(len(s) for s in row)
            for row in rows
        ]

        # stitch back
        return [
            sep.join((s + " " * (max_widths[j] - len(s))) for j, s in enumerate(line))
            for i, line in enumerate(lines)
        ]

    @classmethod
    def align_rates_and_parameters(
        cls,
        rates: Sequence[Any],
        parameters: Sequence[Any],
        sep: str = "  ",
    ) -> Tuple[List[str], List[str]]:
        rates, parameters = [
            [
                (line.split() if isinstance(line, str) else list(map(str, law.util.flatten(line))))
                for line in lines
            ]
            for lines in [rates, parameters]
        ]

        # first, align parameter names and types on their own
        param_starts = cls.align_lines([line[:2] for line in parameters], sep=sep)

        # prepend to parameter lines
        parameters = [([start] + line[2:]) for start, line in zip(param_starts, parameters)]

        # align in conjunction with rates
        n_rate_lines = len(rates)
        lines = cls.align_lines(rates + parameters, sep=sep)

        return lines[:n_rate_lines], lines[n_rate_lines:]
