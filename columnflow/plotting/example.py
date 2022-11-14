# coding: utf-8

"""
Example plot functions.
"""

from __future__ import annotations

from collections import OrderedDict
from typing import Sequence

import law

from columnflow.util import maybe_import, test_float

hist = maybe_import("hist")
np = maybe_import("numpy")
plt = maybe_import("matplotlib.pyplot")
mplhep = maybe_import("mplhep")
od = maybe_import("order")


def draw_error_bands(ax: plt.Axes, h: hist.Hist, norm: float = 1.0, **kwargs) -> None:
    # compute relative errors
    rel_error = h.variances()**0.5 / h.values()
    rel_error[np.isnan(rel_error)] = 0.0

    # compute the baseline
    # fill 1 in places where both numerator and denominator are 0, and 0 for remaining nan's
    baseline = h.values() / norm
    baseline[(h.values() == 0) & (norm == 0)] = 1.0
    baseline[np.isnan(baseline)] = 0.0

    defaults = {
        "x": h.axes[0].centers,
        "width": h.axes[0].edges[1:] - h.axes[0].edges[:-1],
        "height": baseline * 2 * rel_error,
        "bottom": baseline * (1 - rel_error),
        "hatch": "///",
        "facecolor": "none",
        "linewidth": 0,
        "color": "black",
        "alpha": 1.0,
    }
    defaults.update(kwargs)
    ax.bar(**defaults)


def draw_stack(ax: plt.Axes, h: hist.Stack, norm: float = 1.0, **kwargs) -> None:
    # check if norm is a number
    if test_float(norm):
        h = hist.Stack(*[i / norm for i in h])
    else:
        if not isinstance(norm, Sequence) and not isinstance(norm, np.ndarray):
            raise TypeError(f"norm must be either a number, sequence or np.ndarray, not a {type(norm)}")
        norm = np.array(norm)
        if len(norm) == len(h):
            # 1 normalization factor/array per histogram
            h = hist.Stack(*[h[i] / norm[i] for i in range(len(h))])
        else:
            # same normalization for each histogram
            # edge case N_bins = N_histograms not considered :(
            # solution: transform norm -> [norm]*len(h)
            h = hist.Stack(*[i / norm for i in h])

    defaults = {
        "ax": ax,
        "stack": True,
        "histtype": "fill",
    }
    defaults.update(kwargs)
    h.plot(**defaults)


def draw_hist(ax: plt.Axes, h: hist.Hist, norm: float = 1.0, **kwargs) -> None:
    h = h / norm
    defaults = {
        "ax": ax,
        "stack": False,
        "histtype": "step",
    }
    defaults.update(kwargs)
    h.plot1d(**defaults)


def draw_errorbars(ax: plt.Axes, h: hist.Hist, norm: float = 1.0, **kwargs) -> None:
    values = h.values() / norm
    variances = np.sqrt(h.variances()) / norm
    # compute asymmetric poisson errors for data
    # TODO: passing the output of poisson_interval as yerr to mpl.plothist leads to
    #       buggy error bars and the documentation is clearly wrong (mplhep 0.3.12,
    #       hist 2.4.0), so adjust the output to make up for that, but maybe update or
    #       remove the next lines if this is fixed to not correct it "twice"
    from hist.intervals import poisson_interval
    yerr = poisson_interval(values, variances)
    yerr[np.isnan(yerr)] = 0
    yerr[0] = values - yerr[0]
    yerr[1] -= values

    defaults = {
        "x": h.axes[0].centers,
        "y": values,
        "yerr": yerr,
        "color": "k",
        "linestyle": "none",
        "marker": "o",
        "elinewidth": 1,
    }
    defaults.update(kwargs)
    ax.errorbar(**defaults)


def plot_all(
    plot_config: dict,
    style_config: dict,
    skip_ratio: bool = False,
    shape_norm: bool = False,
    skip_legend: bool = False,
    skip_cms: bool = False,
    **kwargs,
) -> plt.Figure:
    """
    plot_config expects dictionaries with fields:
    "method": str, identical to the name of a function defined above,
    "hist": hist.Hist or hist.Stack,
    "kwargs": dict (optional),
    "ratio_kwargs": dict (optional),

    style_config expects fields (all optional):
    "ax_cfg": dict,
    "rax_cfg": dict,
    "legend_cfg": dict,
    "cms_label_cfg": dict,
    """
    # available plot methods mapped to their names
    plot_methods = {  # noqa
        func.__name__: func
        for func in [draw_error_bands, draw_stack, draw_hist, draw_errorbars]
    }

    plt.style.use(mplhep.style.CMS)

    rax = None
    if not skip_ratio:
        fig, (ax, rax) = plt.subplots(2, 1, gridspec_kw=dict(height_ratios=[3, 1], hspace=0), sharex=True)
    else:
        fig, ax = plt.subplots()

    for key, cfg in plot_config.items():
        if "method" not in cfg:
            raise ValueError(f"no method given in plot_cfg entry {key}")
        method = cfg["method"]

        if "hist" not in cfg:
            raise ValueError(f"no histogram(s) given in plot_cfg entry {key}")
        hist = cfg["hist"]
        kwargs = cfg.get("kwargs", {})
        plot_methods[method](ax, hist, **kwargs)

        if not skip_ratio and "ratio_kwargs" in cfg:
            # take ratio_method if the ratio plot requires a different plotting method
            method = cfg.get("ratio_method", method)
            plot_methods[method](rax, hist, **cfg["ratio_kwargs"])

    # axis styling
    ax_kwargs = {
        "ylabel": "Counts",
        "xlabel": "variable",
        "yscale": "linear",
    }

    # some default ylim settings based on yscale
    log_y = style_config.get("ax_cfg", {}).get("yscale", "linear") == "log"
    ax_ymax = ax.get_ylim()[1]
    ax_ylim = (ax_ymax / 10**4, ax_ymax * 40) if log_y else (0.00001, ax_ymax * 1.2)
    ax_kwargs.update({"ylim": ax_ylim})

    # prioritize style_config ax settings
    ax_kwargs.update(style_config.get("ax_cfg", {}))

    ax.set(**ax_kwargs)

    if not skip_ratio:
        # hard-coded line at 1
        rax.axhline(y=1.0, linestyle="dashed", color="gray")
        rax_kwargs = {
            "ylim": (0.72, 1.28),
            "ylabel": "Ratio",
            "xlabel": "Variable",
            "yscale": "linear",
        }
        rax_kwargs.update(style_config.get("rax_cfg", {}))
        rax.set(**rax_kwargs)
        fig.align_ylabels()

    # legend
    if not skip_legend:
        legend_kwargs = {
            "ncol": 1,
            "loc": "upper right",
        }
        legend_kwargs.update(style_config.get("legend_cfg", {}))
        ax.legend(**legend_kwargs)

    # cms label
    cms_label_kwargs = {
        "ax": ax,
        "llabel": "Work in progress",
        "fontsize": 22,
    }
    # TODO: set "data": False when there are no data histograms (adds 'Simulation' to CMS label)
    cms_label_kwargs.update(style_config.get("cms_label_cfg", {}))
    if skip_cms:
        cms_label_kwargs.update({"data": True, "label": ""})
    mplhep.cms.label(**cms_label_kwargs)

    plt.tight_layout()

    return fig


def plot_variable_per_process(
    hists: OrderedDict,
    config_inst: od.config,
    variable_insts: list[od.variable],
    style_config: dict | None = None,
    shape_norm: bool | None = False,
    yscale: str | None = "",
    process_settings: dict | None = None,
    **kwargs,
) -> plt.Figure:
    variable_inst = variable_insts[0]

    # process_settings
    if not process_settings:
        process_settings = {}

    # separate histograms into stack, lines and data hists
    data_hists, mc_hists, mc_colors, mc_labels = [], [], [], []
    line_hists, line_colors, line_labels = [], [], []

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
            if "unstack" in settings:
                line_hists.append(h)
                line_colors.append(color1)
                line_labels.append(label)
            else:
                mc_hists.append(h)
                mc_colors.append(color1)
                line_colors.append(color2)
                mc_labels.append(label)

    h_data, h_mc, h_mc_stack = None, None, None
    if data_hists:
        h_data = sum(data_hists[1:], data_hists[0].copy())
    if mc_hists:
        h_mc = sum(mc_hists[1:], mc_hists[0].copy())
        h_mc_stack = hist.Stack(*mc_hists)

    # setup plotting configs
    plot_config = {}

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
                "edgecolor": line_colors,
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
        plot_config[f"line_{label}"] = {
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

    # setup style config
    if not yscale:
        yscale = "log" if variable_inst.log_y else "linear"

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
    variable_insts: list[od.variable],
    style_config: dict | None = None,
    shape_norm: bool = False,
    yscale: str | None = None,
    **kwargs,
) -> plt.Figure:
    variable_inst = variable_insts[0]

    plot_config = OrderedDict()

    # for updating labels of individual selector steps
    selector_step_labels = config_inst.x("selector_step_labels", {})

    # add hists
    for label, h in hists.items():
        norm = sum(h.values()) if shape_norm else 1
        plot_config[f"hist_{label}"] = {
            "method": "draw_hist",
            "hist": h,
            "kwargs": {"norm": norm, "label": selector_step_labels.get(label, label)},
            "ratio_kwargs": {"norm": hists["Initial"].values()},
        }

    # setup style config

    if not yscale:
        yscale = "log" if variable_inst.log_y else "linear"

    default_style_config = {
        "ax_cfg": {
            "xlim": (variable_inst.x_min, variable_inst.x_max),
            "ylabel": variable_inst.get_full_y_title(),
            "xlabel": variable_inst.get_full_x_title(),
            "yscale": yscale,
        },
        "rax_cfg": {
            "xlim": (variable_inst.x_min, variable_inst.x_max),
            "ylim": (0., 1.1),
            "ylabel": "Step / Initial",
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


def plot_shifted_variable(
    hists: Sequence[hist.Hist],
    config_inst: od.config,
    variable_insts: list[od.variable],
    style_config: dict | None = None,
    shape_norm: bool = False,
    yscale: str | None = None,
    legend_title: str | None = None,
    process_settings: dict | None = None,
    **kwargs,
) -> plt.Figure:
    variable_inst = variable_insts[0]

    # create the sum of histograms over all processes
    h_sum = sum(list(hists.values())[1:], list(hists.values())[0].copy())

    # setup plotting configs
    plot_config = {}
    colors = {
        "nominal": "black",
        "up": "red",
        "down": "blue",
    }
    for i, shift_id in enumerate(h_sum.axes["shift"]):
        shift_inst = config_inst.get_shift(shift_id)

        h = h_sum[{"shift": hist.loc(shift_id)}]
        # assuming `nominal` always has shift id 0
        ratio_norm = h_sum[{"shift": hist.loc(0)}].values()

        diff = sum(h.values()) / sum(ratio_norm) - 1
        label = shift_inst.label
        if not shift_inst.is_nominal:
            label += " ({0:+.2f}%)".format(diff * 100)

        plot_config[shift_inst.name] = {
            "method": "draw_hist",
            "hist": h,
            "kwargs": {
                "norm": sum(h.values()) if shape_norm else 1,
                "label": label,
                "color": colors[shift_inst.direction],
            },
            "ratio_kwargs": {
                "norm": ratio_norm,
                "color": colors[shift_inst.direction],
            },
        }

    # setup style config
    if not process_settings:
        process_settings = {}

    # legend title setting
    if not legend_title:
        if len(hists) == 1:
            # use process label as default if 1 process
            process_inst = list(hists.keys())[0]
            legend_title = process_settings.get(process_inst.name, {}).get("label", process_inst.label)
        else:
            # default to `Background` for multiple processes
            legend_title = "Background"

    if not yscale:
        yscale = "log" if variable_inst.log_y else "linear"

    default_style_config = {
        "ax_cfg": {
            "xlim": (variable_inst.x_min, variable_inst.x_max),
            "ylabel": variable_inst.get_full_y_title(),
            "yscale": yscale,
        },
        "rax_cfg": {
            "xlim": (variable_inst.x_min, variable_inst.x_max),
            "ylim": (0.25, 1.75),
            "ylabel": "Ratio",
            "xlabel": variable_inst.get_full_x_title(),
        },
        "legend_cfg": {
            "title": legend_title,
        },
        "cms_label_cfg": {
            "lumi": config_inst.x.luminosity.get("nominal") / 1000,  # pb -> fb
        },
    }
    style_config = law.util.merge_dicts(default_style_config, style_config, deep=True)
    if shape_norm:
        style_config["ax_cfg"]["ylabel"] = r"$\Delta N/N$"

    return plot_all(plot_config, style_config, **kwargs)


def plot_cutflow(
    hists: OrderedDict,
    config_inst: od.config,
    style_config: dict | None = None,
    shape_norm: bool = False,
    yscale: str | None = None,
    process_settings: dict | None = None,
    **kwargs,
) -> plt.Figure:
    if not process_settings:
        process_settings = {}

    mc_hists, mc_colors, mc_line_colors, mc_labels, data_hists = [], [], [], [], []
    for process_inst, h in hists.items():
        # get settings for this process
        settings = process_settings.get(process_inst.name, {})
        color1 = settings.get("color1", settings.get("color", process_inst.color1))
        color2 = settings.get("color2", process_inst.color2)
        label = settings.get("label", process_inst.label)

        if process_inst.is_mc:
            mc_hists.append(h)
            mc_colors.append(color1)
            mc_line_colors.append(color2)
            mc_labels.append(label)
        else:
            data_hists.append(h)

    # create the stack
    h_mc_stack, h_data = None, None
    if mc_hists:
        h_mc_stack = hist.Stack(*mc_hists)
    if data_hists:
        h_data = sum(data_hists[1:], data_hists[0].copy())

    # setup plotting configs
    if not yscale:
        yscale = "linear"

    plot_config = {
        "procs": {
            "method": "draw_stack",
            "hist": h_mc_stack,
            "kwargs": {
                "norm": [h[{"step": "Initial"}].value for h in mc_hists] if shape_norm else 1,
                "label": mc_labels,
                "color": mc_colors,
                "edgecolor": mc_line_colors,
                "linewidth": [(0 if c is None else 1) for c in mc_line_colors],
                "histtype": "step",
                "stack": False,
            },
        },
    }
    if data_hists:
        plot_config["data"] = {
            "method": "draw_hist",
            "hist": h_data,
            "kwargs": {
                "norm": h_data[{"step": "Initial"}].value if shape_norm else 1,
                "label": "Data",
            },
        }

    # update xticklabels based on config
    xticklabels = []
    selector_step_labels = config_inst.x("selector_step_labels", {})
    for xtl in list(mc_hists[0].axes["step"]):
        xticklabels.append(selector_step_labels.get(xtl, xtl))

    default_style_config = {
        "ax_cfg": {
            "ylabel": "Selection efficiency" if shape_norm else "Selection yields",
            "xlabel": "Selection steps",
            "xticklabels": xticklabels,
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

    return plot_all(plot_config, style_config, **kwargs)
