# coding: utf-8

"""
Scripts to create plots using the plotter
"""

from collections import OrderedDict
from typing import Sequence, Optional

import law

from columnflow.util import maybe_import
from columnflow.plotting.plotter import plot_all

hist = maybe_import("hist")
np = maybe_import("numpy")
plt = maybe_import("matplotlib.pyplot")
od = maybe_import("order")


def plot_variable_per_process(
    hists: OrderedDict,
    config_inst: od.config,
    variable_inst: od.variable,
    style_config: Optional[dict] = None,
    **kwargs,
) -> plt.Figure:

    # create the stack and a fake data hist using the smeared sum
    data_hists = [h for process_inst, h in hists.items() if process_inst.is_data]
    mc_hists = [h for process_inst, h in hists.items() if process_inst.is_mc]
    mc_colors = [process_inst.color for process_inst in hists if process_inst.is_mc]
    mc_labels = [process_inst.label for process_inst in hists if process_inst.is_mc]

    h_data, h_mc, h_mc_stack = None, None, None
    if data_hists:
        h_data = sum(data_hists[1:], data_hists[0].copy())
    if mc_hists:
        h_mc = sum(mc_hists[1:], mc_hists[0].copy())
        h_mc_stack = hist.Stack(*mc_hists)

    # get configs from kwargs
    shape_norm = kwargs.get("shape_norm", False)

    yscale = kwargs.get("yscale")
    if not yscale:
        yscale = "log" if variable_inst.log_y else "linear"

    # setup plotting configs
    mc_norm = sum(h_mc.values()) if shape_norm else 1
    plot_config = {
        "mc_stack": {
            "method": "draw_stack",
            "hist": h_mc_stack,
            "kwargs": {"norm": mc_norm, "label": mc_labels, "color": mc_colors},
        },
        "mc_uncert": {
            "method": "draw_error_bands",
            "hist": h_mc,
            "kwargs": {"norm": mc_norm, "label": "MC stat. unc."},
            "ratio_kwargs": {"norm": h_mc.values()},
        },
    }

    # dummy since not implemented yet
    mc_lines = False
    if mc_lines:
        plot_config["mc_lines"] = {
            "method": "draw_stack",
            # "hist": h_lines_stack,
            # "kwargs": {"label": lines_label, "color": lines_colors, "stack": False, "histtype": "step"},
        }

    if data_hists:
        data_norm = sum(h_data.values()) if shape_norm else 1
        plot_config["data"] = {
            "method": "draw_errorbars",
            "hist": h_data,
            "kwargs": {"norm": data_norm, "label": "Data"},
            "ratio_kwargs": {"norm": h_mc.values()},
        }

    default_style_config = {
        "ax_cfg": {
            "xlim": (variable_inst.x_min, variable_inst.x_max),
            "ylabel": variable_inst.get_full_y_title(),
            "xlabel": variable_inst.get_full_x_title(),
            "yscale": yscale,
        },
        "rax_cfg": {
            "ylabel": "Data / MC",
            "xlabel": variable_inst.get_full_x_title(),
        },
        "legend_cfg": {},
        "cms_label_cfg": {
            "lumi": config_inst.x.luminosity.get("nominal") / 1000,  # pb -> fb
        },
    }
    style_config = law.util.merge_dicts(default_style_config, style_config, deep=True)
    if shape_norm:
        style_config["ax_cfg"]["ylabel"] = r"$\Delta N/N$"

    return plot_all(plot_config, style_config, **kwargs)


def plot_variable_variants(
    hists: OrderedDict,
    config_inst: od.config,
    variable_inst: od.variable,
    style_config: Optional[dict] = None,
    **kwargs,
) -> plt.Figure:
    plot_config = OrderedDict()

    # get configs from kwargs
    shape_norm = kwargs.get("shape_norm", False)

    yscale = kwargs.get("yscale")
    if not yscale:
        yscale = "log" if variable_inst.log_y else "linear"

    # add hists
    for label, h in hists.items():
        plot_config[f"hist_{label}"] = {
            "method": "draw_hist",
            "hist": h,
            "kwargs": {"label": label},
        }

    default_style_config = {
        "ax_cfg": {
            "xlim": (variable_inst.x_min, variable_inst.x_max),
            "ylabel": variable_inst.get_full_y_title(),
            "xlabel": variable_inst.get_full_x_title(),
            "yscale": yscale,
        },
        "legend_cfg": {},
        "cms_label_cfg": {
            "lumi": config_inst.x.luminosity.get("nominal") / 1000,  # pb -> fb
        },
    }
    style_config = law.util.merge_dicts(default_style_config, style_config, deep=True)
    if shape_norm:
        style_config["ax_cfg"]["ylabel"] = r"$\Delta N/N$"

    return plot_all(plot_config, style_config, **kwargs)


def plot_shifted_variable(
    hists: Sequence[hist.Hist],
    config_inst: od.config,
    process_inst: od.process,
    variable_inst: od.variable,
    style_config: Optional[dict] = None,
    **kwargs,
) -> plt.Figure:

    # create the stack and the sum
    h_sum = sum(list(hists.values())[1:], list(hists.values())[0].copy())
    h_stack = h_sum.stack("shift")
    label = [config_inst.get_shift(h_sum.axes["shift"][i]).label for i in range(3)]

    # get the normalization factors into the correct shape (over/underflow bins)
    norm = np.concatenate(([-1], h_sum[{"shift": hist.loc(0)}].values(), [-1]))

    # get configs from kwargs
    shape_norm = kwargs.get("shape_norm", False)

    yscale = kwargs.get("yscale")
    if not yscale:
        yscale = "log" if variable_inst.log_y else "linear"

    # setup plotting configs
    mc_norm = [sum(h_sum[{"shift": i}].values()) for i in range(3)]
    plot_config = {
        "MC": {
            "method": "draw_stack",
            "hist": h_stack,
            "kwargs": {
                "norm": mc_norm,
                "label": label,
                "color": ["black", "red", "blue"],
                "histtype": "step",
                "stack": False,
            },
            "ratio_kwargs": {"norm": norm, "color": ["black", "red", "blue"], "histtype": "step", "stack": False},
        },
    }

    default_style_config = {
        "ax_cfg": {
            "xlim": (variable_inst.x_min, variable_inst.x_max),
            "ylabel": variable_inst.get_full_y_title(),
            "yscale": yscale,
        },
        "rax_cfg": {
            "xlim": (variable_inst.x_min, variable_inst.x_max),
            "ylim": (0.25, 1.75),
            "ylabel": "Sys / Nom",
            "xlabel": variable_inst.get_full_x_title(),
        },
        "legend_cfg": {
            "title": process_inst.label,
        },
        "cms_label_cfg": {
            "lumi": config_inst.x.luminosity.get("nominal") / 1000,  # pb -> fb
        },
    }
    style_config = law.util.merge_dicts(default_style_config, style_config, deep=True)
    if shape_norm:
        style_config["ax_cfg"]["ylabel"] = r"$\Delta N/N$"

    return plot_all(plot_config, style_config, ratio=True)


def plot_cutflow(
    hists: OrderedDict,
    config_inst: od.config,
    style_config: Optional[dict] = None,
    **kwargs,
) -> plt.Figure:

    mc_hists = [h for process_inst, h in hists.items() if process_inst.is_mc]
    mc_colors = [process_inst.color for process_inst in hists if process_inst.is_mc]
    mc_labels = [process_inst.label for process_inst in hists if process_inst.is_mc]

    # create the stack
    h_mc_stack = None
    if mc_hists:
        h_mc_stack = hist.Stack(*mc_hists)

    # get configs from kwargs
    yscale = kwargs.get("yscale", "linear")

    # setup plotting configs
    plot_config = {
        "procs": {
            "method": "draw_stack",
            "hist": h_mc_stack,
            "kwargs": {
                "norm": [h[{"step": "Initial"}].value for h in mc_hists],
                "label": mc_labels,
                "color": mc_colors,
                "histtype": "step",
                "stack": False,
            },
        },
    }

    default_style_config = {
        "ax_cfg": {
            "ylabel": "Selection efficiency",
            "xlabel": "Selection steps",
            "yscale": yscale,
        },
        "legend_cfg": {
            "loc": "upper right",
        },
        "cms_label_cfg": {
            "lumi": config_inst.x.luminosity.get("nominal") / 1000,  # pb -> fb
        },
    }
    style_config = law.util.merge_dicts(default_style_config, style_config, deep=True)

    return plot_all(plot_config, style_config, ratio=False)
