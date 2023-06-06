# coding: utf-8

"""
Example 2d plot functions.
"""

from __future__ import annotations

from collections import OrderedDict

import law

from columnflow.util import maybe_import
from columnflow.columnar_util import EMPTY_FLOAT
from columnflow.plotting.plot_util import (
    remove_residual_axis,
    apply_variable_settings,
    apply_process_settings,
    apply_density_to_hists,
    get_position,
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
            "norm": mpl.colors.LogNorm() if zscale == "log" else None,
            # "labels": True,  # this enables displaying numerical values for each bin, but needs some optimization
            "cmin": EMPTY_FLOAT + 1,  # display zero-entries as white points
            "cbar": True,
            "cbarextend": True,
        },
        "annotate_cfg": {
            "text": category_inst.label,
        },
    }
    style_config = law.util.merge_dicts(default_style_config, style_config, deep=True)

    # add all processes into 1 histogram
    h_sum = sum(list(hists.values())[1:], list(hists.values())[0].copy())
    if shape_norm:
        h_sum = h_sum / h_sum.sum().value

    # set bins without any entries (variance == 0) to EMPTY_FLOAT
    h_view = h_sum.view()
    h_view.value[h_view.variance == 0] = EMPTY_FLOAT
    h_sum[...] = h_view

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
            "sim": "Simulation",
            "simwip": "Simulation work in progress",
            "simpre": "Simulation preliminary",
            "public": "",
        }
        cms_label_kwargs = {
            "ax": ax,
            "llabel": label_options[cms_label],
            "fontsize": 22,
            "data": False,
        }

        cms_label_kwargs.update(style_config.get("cms_label_cfg", {}))
        mplhep.cms.label(**cms_label_kwargs)

    h_sum.plot2d(ax=ax, **style_config["plot2d_cfg"])

    plt.tight_layout()

    return fig, (ax,)
