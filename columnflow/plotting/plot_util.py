# coding: utf-8

"""
Some utils for plot functions.
"""

from __future__ import annotations

from collections import OrderedDict

from columnflow.util import maybe_import

hist = maybe_import("hist")
np = maybe_import("numpy")
plt = maybe_import("matplotlib.pyplot")
mplhep = maybe_import("mplhep")
od = maybe_import("order")


def prepare_plot_config(
    hists: OrderedDict,
    shape_norm: bool | None = False,
    process_settings: dict | None = None,
) -> OrderedDict:
    """
    Prepares a plot config with one entry to create plots containing a stack of
    backgrounds with uncertainty bands, unstacked processes as lines and
    data entrys with errorbars.
    `process-settings` (unstack, scale, color, label) and `shape-norm` are applied.
    """

    # process_settings
    if not process_settings:
        process_settings = {}

    # separate histograms into stack, lines and data hists
    mc_hists, mc_colors, mc_edgecolors, mc_labels = [], [], [], []
    line_hists, line_colors, line_labels = [], [], []
    data_hists = []

    for process_inst, h in hists.items():
        # get settings for this process
        settings = process_settings.get(process_inst.name, {})
        color1 = settings.get("color1", settings.get("color", process_inst.color1))
        color2 = settings.get("color2", process_inst.color2)
        label = settings.get("label", process_inst.label)

        if "scale" in settings.keys():
            h = h * settings["scale"]
            label = f"{label} x{settings['scale']}"

        if process_inst.is_data:
            data_hists.append(h)
        elif process_inst.is_mc:
            if settings.get("unstack", False):
                line_hists.append(h)
                line_colors.append(color1)
                line_labels.append(label)
            else:
                mc_hists.append(h)
                mc_colors.append(color1)
                mc_edgecolors.append(color2)
                mc_labels.append(label)

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
