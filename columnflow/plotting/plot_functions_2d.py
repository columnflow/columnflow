# coding: utf-8

"""
Example 2d plot functions.
"""

from __future__ import annotations

from collections import OrderedDict
from functools import partial
from unittest.mock import patch

import law

from columnflow.util import maybe_import
from columnflow.plotting.plot_util import (
    remove_residual_axis,
    apply_variable_settings,
    apply_process_settings,
    apply_density_to_hists,
    get_position,
    reduce_with,
)

hist = maybe_import("hist")
np = maybe_import("numpy")
mpl = maybe_import("matplotlib")
plt = maybe_import("matplotlib.pyplot")
mplhep = maybe_import("mplhep")
od = maybe_import("order")
mticker = maybe_import("matplotlib.ticker")


def plot_2d(
    hists: OrderedDict,
    config_inst: od.Config,
    category_inst: od.Category,
    variable_insts: list[od.Variable],
    style_config: dict | None = None,
    density: bool | None = False,
    shape_norm: bool | None = False,
    zscale: str | None = "",
    # z axis range
    zlim: tuple | None = None,
    # how to handle bins with values outside the z range
    extremes: str | None = "",
    # colors to use for marking out-of-bounds values
    extreme_colors: tuple[str] | None = None,
    colormap: str | None = "",
    skip_legend: bool = False,
    cms_label: str = "wip",
    process_settings: dict | None = None,
    variable_settings: dict | None = None,
    **kwargs,
) -> plt.Figure:
    # remove shift axis from histograms
    remove_residual_axis(hists, "shift")

    hists = apply_variable_settings(hists, variable_insts, variable_settings)

    hists = apply_process_settings(hists, process_settings)

    hists = apply_density_to_hists(hists, density)

    # use CMS plotting style
    plt.style.use(mplhep.style.CMS)
    fig, ax = plt.subplots()

    # how to handle yscale information from 2 variable insts?
    if not zscale:
        zscale = "log" if (variable_insts[0].log_y or variable_insts[1].log_y) else "linear"

    # how to handle bin values outside plot range
    if not extremes:
        extremes = "color"

    # add all processes into 1 histogram
    h_sum = sum(list(hists.values())[1:], list(hists.values())[0].copy())
    if shape_norm:
        h_sum = h_sum / h_sum.sum().value

    # mask bins without any entries (variance == 0)
    h_view = h_sum.view()
    h_view.value[h_view.variance == 0] = np.nan

    # check histogram value range
    vmin, vmax = np.nanmin(h_sum.values()), np.nanmax(h_sum.values())
    vmin, vmax = np.nan_to_num(np.array([vmin, vmax]), 0)

    # default to full z range
    if zlim is None:
        zlim = ("min", "max")

    # resolve string specifiers like "min", "max", etc.
    zlim = tuple(reduce_with(lim, h_sum.values()) for lim in zlim)

    # if requested, hide or clip bins outside specified plot range
    if extremes == "hide":
        h_view.value[h_view.value < zlim[0]] = np.nan
        h_view.value[h_view.value > zlim[1]] = np.nan
    elif extremes == "clip":
        h_view.value[h_view.value < zlim[0]] = zlim[0]
        h_view.value[h_view.value > zlim[1]] = zlim[1]

    # update histogram values from view
    h_sum[...] = h_view

    # choose appropriate colorbar normalization
    # based on scale type and histogram content

    # log scale (turning linear for low values)
    if zscale == "log":
        # use SymLogNorm to correctly handle both positive and negative values
        cbar_norm = mpl.colors.SymLogNorm(
            vmin=zlim[0],
            vmax=zlim[1],
            # TODO: better heuristics?
            linscale=1.0,
            linthresh=max(0.05 * min(abs(zlim[0]), abs(zlim[1])), 1e-3),
        )

    # linear scale
    else:
        cbar_norm = mpl.colors.Normalize(
            vmin=zlim[0],
            vmax=zlim[1],
        )

    # obtain colormap
    cmap = plt.get_cmap(colormap or "viridis")

    # use dark and light gray to mark extreme values
    if extremes == "color":
        # choose light/dark order depending on the
        # lightness of first/last colormap color
        if not extreme_colors:
            extreme_colors = ["#444444", "#bbbbbb"]
            if sum(cmap(0.0)[:3]) > sum(cmap(1.0)[:3]):
                extreme_colors = extreme_colors[::-1]

        # copy if colormap with extreme colors set
        cmap = cmap.with_extremes(
            under=extreme_colors[0],
            over=extreme_colors[1],
        )

    # setup style config
    # TODO: some kind of z-label is still missing
    default_style_config = {
        "ax_cfg": {
            "xlim": (variable_insts[0].x_min, variable_insts[0].x_max),
            "ylim": (variable_insts[1].x_min, variable_insts[1].x_max),
            "xlabel": variable_insts[0].get_full_x_title(),
            "ylabel": variable_insts[1].get_full_x_title(),
            "xscale": "log" if variable_insts[0].log_x else "linear",
            "yscale": "log" if variable_insts[1].log_x else "linear",
        },
        "legend_cfg": {
            "title": "Process" if len(hists.keys()) == 1 else "Processes",
            "handles": [mpl.lines.Line2D([0], [0], lw=0) for proc_inst in hists.keys()],  # dummy handle
            "labels": [proc_inst.label for proc_inst in hists.keys()],
            "ncol": 1,
            "loc": "upper right",
        },
        "cms_label_cfg": {
            "lumi": config_inst.x.luminosity.get("nominal") / 1000,  # pb -> fb
        },
        "plot2d_cfg": {
            "norm": cbar_norm,
            "cmap": cmap,
            # "labels": True,  # this enables displaying numerical values for each bin, but needs some optimization
            "cbar": True,
            "cbarextend": True,
        },
        "annotate_cfg": {
            "text": category_inst.label,
        },
    }
    style_config = law.util.merge_dicts(default_style_config, style_config, deep=True)

    # apply style_config
    ax.set(**style_config["ax_cfg"])
    if not skip_legend:
        ax.legend(**style_config["legend_cfg"])

    if variable_insts[0].discrete_x:
        ax.set_xticks([], minor=True)
    if variable_insts[1].discrete_x:
        ax.set_yticks([], minor=True)

    # annotation of category label
    annotate_kwargs = {
        "text": "",
        "xy": (
            get_position(*ax.get_xlim(), factor=0.05, logscale=False),
            get_position(*ax.get_ylim(), factor=0.95, logscale=False),
        ),
        "xycoords": "data",
        "color": "black",
        "fontsize": 22,
        "horizontalalignment": "left",
        "verticalalignment": "top",
    }
    annotate_kwargs.update(default_style_config.get("annotate_cfg", {}))
    plt.annotate(**annotate_kwargs)

    # cms label
    if cms_label != "skip":
        label_options = {
            "wip": "Work in progress",
            "pre": "Preliminary",
            "pw": "Private work",
            "sim": "Simulation",
            "simwip": "Simulation work in progress",
            "simpre": "Simulation preliminary",
            "simpw": "Simulation private work",
            "od": "OpenData",
            "odwip": "OpenData work in progress",
            "odpw": "OpenData private work",
            "public": "",
        }
        cms_label_kwargs = {
            "ax": ax,
            "llabel": label_options.get(cms_label, cms_label),
            "fontsize": 22,
            "data": False,
        }

        cms_label_kwargs.update(style_config.get("cms_label_cfg", {}))
        mplhep.cms.label(**cms_label_kwargs)

    # decide at which ends of the colorbar to draw symbols
    # indicating that there are values outside the range
    if extremes == "hide":
        extend = "neither"
    elif vmax > zlim[1] and vmin < zlim[0]:
        extend = "both"
    elif vmin < zlim[0]:
        extend = "min"
    elif vmax > zlim[1]:
        extend = "max"
    else:
        extend = "neither"

    # call plot method, patching the colorbar function
    # called internally by mplhep to draw the extension symbols
    with patch.object(plt, "colorbar", partial(plt.colorbar, extend=extend)):
        h_sum.plot2d(ax=ax, **style_config["plot2d_cfg"])

    # fix color bar minor ticks with SymLogNorm
    cbar = ax.collections[-1].colorbar
    if isinstance(cbar_norm, mpl.colors.SymLogNorm):
        _scale = cbar.ax.yaxis._scale
        _scale.subs = [2, 3, 4, 5, 6, 7, 8, 9]
        cbar.ax.yaxis.set_minor_locator(
            mticker.SymmetricalLogLocator(_scale.get_transform(), subs=_scale.subs),
        )
        cbar.ax.yaxis.set_minor_formatter(
            mticker.LogFormatterSciNotation(_scale.base),
        )

    plt.tight_layout()

    return fig, (ax,)
