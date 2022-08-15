# coding: utf-8

"""
Example plot functions.
"""

from collections import OrderedDict
from typing import Sequence, Optional

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

    for key in plot_config:
        cfg = plot_config[key]
        if "method" not in cfg:
            raise ValueError("No method given in plot_cfg entry {key}")
        method = cfg["method"]

        if "hist" not in cfg:
            raise ValueError("No histogram(s) given in plot_cfg entry {key}")
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
    yscale = kwargs.get("yscale") or "linear"

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
