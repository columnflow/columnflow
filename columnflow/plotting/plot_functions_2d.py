# coding: utf-8

"""
Example 2d plot functions.
"""

from __future__ import annotations

from collections import OrderedDict

import law

from columnflow.util import maybe_import
from columnflow.plotting.plot_all import make_plot_2d
from columnflow.plotting.plot_util import (
    remove_residual_axis,
    apply_variable_settings,
    apply_process_settings,
    apply_density_to_hists,
    prepare_plot_config_2d,
    prepare_style_config_2d,
)

hist = maybe_import("hist")
np = maybe_import("numpy")
mpl = maybe_import("matplotlib")
plt = maybe_import("matplotlib.pyplot")
mplhep = maybe_import("mplhep")
od = maybe_import("order")


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

    # how to handle yscale information from 2 variable insts?
    if not zscale:
        zscale = "log" if (variable_insts[0].log_y or variable_insts[1].log_y) else "linear"

    plot_config = prepare_plot_config_2d(
        hists,
        shape_norm=shape_norm,
        zscale=zscale,
        zlim=zlim,
        extremes=extremes,
        extreme_colors=extreme_colors,
        colormap=colormap,
    )

    default_style_config = prepare_style_config_2d(
        config_inst=config_inst,
        category_inst=category_inst,
        process_insts=list(hists.keys()),
        variable_insts=variable_insts,
        cms_label=cms_label,
    )

    style_config = law.util.merge_dicts(default_style_config, style_config, deep=True)

    if skip_legend:
        del style_config["legend_cfg"]

    return make_plot_2d(plot_config, style_config)
