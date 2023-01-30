# coding: utf-8

"""
Some utils for plot functions.
"""

from __future__ import annotations

import order as od

import functools
import operator

from collections import OrderedDict

from columnflow.util import maybe_import


math = maybe_import("math")
hist = maybe_import("hist")
np = maybe_import("numpy")
plt = maybe_import("matplotlib.pyplot")
mplhep = maybe_import("mplhep")


def apply_process_settings(
        hists: dict,
        process_settings: dict | None = None,
) -> dict:
    """
    applies settings from `process_settings` dictionary to the `process_insts`;
    the `scale` setting is directly applied to the histograms
    """

    if not process_settings:
        return hists

    for proc_inst, h in hists.items():
        # check if there are process settings to apply for this variable
        if proc_inst.name not in process_settings.keys():
            continue

        proc_settings = process_settings[proc_inst.name]

        # apply "scale" setting if given
        if "scale" in proc_settings.keys():
            scale_factor = proc_settings.pop("scale")
            h = h * scale_factor
            # TODO: there might be a prettier way for the label
            proc_inst.label = f"{proc_inst.label} x{scale_factor}"

        # apply all other process settings to the process_inst
        for setting_key, setting_value in proc_settings.items():
            try:
                setattr(proc_inst, setting_key, setting_value)
            except AttributeError:
                proc_inst.set_aux(setting_key, setting_value)

    return hists


def apply_variable_settings(
        hists: dict,
        variable_insts: list[od.Variable],
        variable_settings: dict | None = None,
) -> dict:
    """
    applies settings from `variable_settings` dictionary to the `variable_insts`;
    the `rebin` setting is directly applied to the histograms
    """
    # check if there are variable settings to apply
    if not variable_settings:
        return hists

    # apply all settings
    for var_inst in variable_insts:
        # check if there are variable settings to apply for this variable
        if var_inst.name not in variable_settings.keys():
            continue

        var_settings = variable_settings[var_inst.name]

        for proc_inst, h in list(hists.items()):
            # apply rebinning setting
            rebin_factor = int(var_settings.pop("rebin", 1))
            h = h[{var_inst.name: hist.rebin(rebin_factor)}]

            # override the histogram
            hists[proc_inst] = h

        # apply all other variable settings to the variable_inst
        for setting_key, setting_value in var_settings.items():
            try:
                setattr(var_inst, setting_key, setting_value)
            except AttributeError:
                var_inst.set_aux(setting_key, setting_value)

    return hists


def apply_density_to_hists(hists: dict, density: bool | None = False) -> dict:
    """
    Scales number of histogram entries to bin widths.
    """
    if not density:
        return hists

    for key, hist in hists.items():
        # bin area safe for multi-dimensional histograms
        area = functools.reduce(operator.mul, hist.axes.widths)

        # scale hist by bin area
        hists[key] = hist / area

    return hists


def remove_residual_axis(hists: dict, ax_name: str, max_bins: int = 1) -> dict:
    """
    removes axis named 'ax_name' if existing and there is only a single bin in the axis;
    raises Exception otherwise
    """
    for key, hist in list(hists.items()):
        if ax_name in hist.axes.name:
            n_bins = len(hist.axes[ax_name])
            if n_bins > max_bins:
                raise Exception(
                    f"{ax_name} axis of histogram for key {key} has {n_bins} values whereas at most "
                    f"{max_bins} is expected",
                )
            hists[key] = hist[{ax_name: sum}]

    return hists


def prepare_style_config(
    config_inst: od.Config,
    category_inst: od.Category,
    variable_inst: od.Variable,
    density: bool | None = False,
    shape_norm: bool | None = False,
    yscale: str | None = "",
) -> dict:
    """
    small helper function that sets up a default style config based on the instances
    of the config, category and variable
    """

    if not yscale:
        yscale = "log" if variable_inst.log_y else "linear"

    xlim = (variable_inst.x("x_min", variable_inst.x_min), variable_inst.x("x_max", variable_inst.x_max))

    style_config = {
        "ax_cfg": {
            "xlim": xlim,
            "ylabel": variable_inst.get_full_y_title(bin_width="" if density else None),
            "xlabel": variable_inst.get_full_x_title(),
            "yscale": yscale,
            "xscale": "log" if variable_inst.log_x else "linear",
        },
        "rax_cfg": {
            "ylabel": "Data / MC",
            "xlabel": variable_inst.get_full_x_title(),
        },
        "legend_cfg": {},
        "annotate_cfg": {"text": category_inst.label},
        "cms_label_cfg": {
            "lumi": config_inst.x.luminosity.get("nominal") / 1000,  # pb -> fb
        },
    }

    # disable minor ticks based on variable_inst
    if variable_inst.discrete_x:
        style_config["ax_cfg"]["xticks"] = range(int(xlim[0]), int(xlim[1]) + 1)
        style_config["ax_cfg"]["minorxticks"] = []
    if variable_inst.discrete_y:
        style_config["ax_cfg"]["minoryticks"] = []

    return style_config


def prepare_plot_config(
    hists: OrderedDict,
    shape_norm: bool | None = False,
) -> OrderedDict:
    """
    Prepares a plot config with one entry to create plots containing a stack of
    backgrounds with uncertainty bands, unstacked processes as lines and
    data entrys with errorbars.
    """

    # separate histograms into stack, lines and data hists
    mc_hists, mc_colors, mc_edgecolors, mc_labels = [], [], [], []
    line_hists, line_colors, line_labels = [], [], []
    data_hists = []

    for process_inst, h in hists.items():
        if process_inst.is_data:
            data_hists.append(h)
        elif process_inst.is_mc:
            if getattr(process_inst, "unstack", False):
                line_hists.append(h)
                line_colors.append(process_inst.color1)
                line_labels.append(process_inst.label)
            else:
                mc_hists.append(h)
                mc_colors.append(process_inst.color1)
                mc_edgecolors.append(process_inst.color2)
                mc_labels.append(process_inst.label)

    h_data, h_mc, h_mc_stack = None, None, None
    if data_hists:
        h_data = sum(data_hists[1:], data_hists[0].copy())
    if mc_hists:
        h_mc = sum(mc_hists[1:], mc_hists[0].copy())
        h_mc_stack = hist.Stack(*mc_hists)

    # setup plotting configs
    plot_config = OrderedDict()

    # draw stack + error bands
    if h_mc_stack:
        mc_norm = sum(h_mc.values()) if shape_norm else 1
        plot_config["mc_stack"] = {
            "method": "draw_stack",
            "hist": h_mc_stack,
            "kwargs": {
                "norm": mc_norm,
                "label": mc_labels,
                "color": mc_colors,
                "edgecolor": mc_edgecolors,
                "linewidth": [(0 if c is None else 1) for c in line_colors],
            },
        }
        plot_config["mc_uncert"] = {
            "method": "draw_error_bands",
            "hist": h_mc,
            "kwargs": {"norm": mc_norm, "label": "MC stat. unc."},
            "ratio_kwargs": {"norm": h_mc.values()},
        }
    # draw lines
    for i, h in enumerate(line_hists):
        label = line_labels[i]
        line_norm = sum(h.values()) if shape_norm else 1
        plot_config[f"line_{i}"] = {
            "method": "draw_hist",
            "hist": h,
            "kwargs": {"norm": line_norm, "label": label, "color": line_colors[i]},
            # "ratio_kwargs": {"norm": h.values(), "color": line_colors[i]},
        }

    # draw data
    if data_hists:
        data_norm = sum(h_data.values()) if shape_norm else 1
        plot_config["data"] = {
            "method": "draw_errorbars",
            "hist": h_data,
            "kwargs": {"norm": data_norm, "label": "Data"},
            "ratio_kwargs": {"norm": h_mc.values()},
        }

    return plot_config


def get_position(minimum: float, maximum: float, factor: float = 1.4, logscale: bool = False) -> float:
    """ get a relative position between a min and max value based on the scale """
    if logscale:
        value = 10 ** ((math.log10(maximum) - math.log10(minimum)) * factor + math.log10(minimum))
    else:
        value = (maximum - minimum) * factor + minimum

    return value
