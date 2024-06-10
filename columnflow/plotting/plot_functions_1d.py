# coding: utf-8

"""
Example plot functions for one-dimensional plots.
"""

from __future__ import annotations

from collections import OrderedDict

import law

from columnflow.types import Iterable
from columnflow.util import maybe_import
from columnflow.plotting.plot_all import plot_all
from columnflow.plotting.plot_util import (
    prepare_plot_config,
    prepare_style_config,
    remove_residual_axis,
    apply_variable_settings,
    apply_process_settings,
    apply_density_to_hists,
    get_position,
    get_profile_variations,
)

hist = maybe_import("hist")
np = maybe_import("numpy")
mpl = maybe_import("matplotlib")
plt = maybe_import("matplotlib.pyplot")
mplhep = maybe_import("mplhep")
od = maybe_import("order")


def plot_variable_per_process(
    hists: OrderedDict,
    config_inst: od.Config,
    category_inst: od.Category,
    variable_insts: list[od.Variable],
    style_config: dict | None = None,
    density: bool | None = False,
    shape_norm: bool | None = False,
    yscale: str | None = "",
    hide_errors: bool | None = None,
    process_settings: dict | None = None,
    variable_settings: dict | None = None,
    **kwargs,
) -> plt.Figure:
    """
    TODO.
    """
    remove_residual_axis(hists, "shift")

    variable_inst = variable_insts[0]
    hists = apply_variable_settings(hists, variable_insts, variable_settings)
    hists = apply_process_settings(hists, process_settings)
    hists = apply_density_to_hists(hists, density)

    plot_config = prepare_plot_config(
        hists,
        shape_norm=shape_norm,
        hide_errors=hide_errors,
    )

    default_style_config = prepare_style_config(
        config_inst, category_inst, variable_inst, density, shape_norm, yscale,
    )

    style_config = law.util.merge_dicts(default_style_config, style_config, deep=True)
    if shape_norm:
        style_config["ax_cfg"]["ylabel"] = r"$\Delta N/N$"

    return plot_all(plot_config, style_config, **kwargs)


def plot_variable_variants(
    hists: OrderedDict,
    config_inst: od.Config,
    category_inst: od.Category,
    variable_insts: list[od.Variable],
    style_config: dict | None = None,
    density: bool | None = False,
    shape_norm: bool = False,
    yscale: str | None = None,
    hide_errors: bool | None = None,
    variable_settings: dict | None = None,
    **kwargs,
) -> plt.Figure:
    """
    TODO.
    """
    remove_residual_axis(hists, "shift")

    variable_inst = variable_insts[0]
    hists = apply_variable_settings(hists, variable_insts, variable_settings)
    hists = apply_density_to_hists(hists, density)

    plot_config = OrderedDict()

    # for updating labels of individual selector steps
    selector_step_labels = config_inst.x("selector_step_labels", {})

    # add hists
    for label, h in hists.items():
        norm = sum(h.values()) if shape_norm else 1
        plot_config[f"hist_{label}"] = plot_cfg = {
            "method": "draw_hist",
            "hist": h,
            "kwargs": {
                "norm": norm,
                "label": selector_step_labels.get(label, label),
            },
            "ratio_kwargs": {
                "norm": hists["Initial"].values(),
            },
        }
        if hide_errors:
            for key in ("kwargs", "ratio_kwargs"):
                if key in plot_cfg:
                    plot_cfg[key]["yerr"] = None

    # setup style config
    default_style_config = prepare_style_config(
        config_inst, category_inst, variable_inst, density, shape_norm, yscale,
    )
    # plot-function specific changes
    default_style_config["rax_cfg"]["ylim"] = (0., 1.1)
    default_style_config["rax_cfg"]["ylabel"] = "Step / Initial"

    style_config = law.util.merge_dicts(default_style_config, style_config, deep=True)
    if shape_norm:
        style_config["ax_cfg"]["ylabel"] = r"$\Delta N/N$"

    return plot_all(plot_config, style_config, **kwargs)


def plot_shifted_variable(
    hists: OrderedDict,
    config_inst: od.Config,
    category_inst: od.Category,
    variable_insts: list[od.Variable],
    style_config: dict | None = None,
    density: bool | None = False,
    shape_norm: bool = False,
    yscale: str | None = None,
    hide_errors: bool | None = None,
    legend_title: str | None = None,
    process_settings: dict | None = None,
    variable_settings: dict | None = None,
    **kwargs,
) -> plt.Figure:
    """
    TODO.
    """
    variable_inst = variable_insts[0]
    hists = apply_variable_settings(hists, variable_insts, variable_settings)
    hists = apply_process_settings(hists, process_settings)
    hists = apply_density_to_hists(hists, density)

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

        plot_config[shift_inst.name] = plot_cfg = {
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
        if hide_errors:
            for key in ("kwargs", "ratio_kwargs"):
                if key in plot_cfg:
                    plot_cfg[key]["yerr"] = None

    # legend title setting
    if not legend_title:
        if len(hists) == 1:
            # use process label as default if 1 process
            process_inst = list(hists.keys())[0]
            legend_title = process_inst.label
        else:
            # default to `Background` for multiple processes
            legend_title = "Background"

    if not yscale:
        yscale = "log" if variable_inst.log_y else "linear"

    default_style_config = prepare_style_config(
        config_inst, category_inst, variable_inst, density, shape_norm, yscale,
    )
    default_style_config["rax_cfg"]["ylim"] = (0.25, 1.75)
    default_style_config["rax_cfg"]["ylabel"] = "Ratio"
    default_style_config["legend_cfg"]["title"] = legend_title

    style_config = law.util.merge_dicts(default_style_config, style_config, deep=True)
    if shape_norm:
        style_config["ax_cfg"]["ylabel"] = r"$\Delta N/N$"

    return plot_all(plot_config, style_config, **kwargs)


def plot_cutflow(
    hists: OrderedDict,
    config_inst: od.Config,
    category_inst: od.Category,
    style_config: dict | None = None,
    density: bool | None = False,
    shape_norm: bool = False,
    yscale: str | None = None,
    hide_errors: bool | None = None,
    process_settings: dict | None = None,
    **kwargs,
) -> plt.Figure:
    """
    TODO.
    """
    remove_residual_axis(hists, "shift")

    hists = apply_process_settings(hists, process_settings)
    hists = apply_density_to_hists(hists, density)

    # setup plotting config
    plot_config = prepare_plot_config(
        hists,
        shape_norm=shape_norm,
        hide_errors=hide_errors,
    )

    if shape_norm:
        # switch normalization to normalizing to `initial step` bin
        for key in list(plot_config):
            item = plot_config[key]
            h_key = item["hist"]
            if isinstance(h_key, Iterable):
                norm = sum(h[{"step": "Initial"}].value for h in h_key)
            else:
                norm = h_key[{"step": "Initial"}].value
            item["kwargs"]["norm"] = norm

    # update xticklabels based on config
    xticklabels = []
    selector_step_labels = config_inst.x("selector_step_labels", {})

    selector_steps = list(hists[list(hists.keys())[0]].axes["step"])
    for step in selector_steps:
        xticklabels.append(selector_step_labels.get(step, step))

    # setup style config
    if not yscale:
        yscale = "linear"

    default_style_config = {
        "ax_cfg": {
            "ylabel": "Selection efficiency" if shape_norm else "Selection yield",
            "xlabel": "Selection step",
            "xticklabels": xticklabels,
            "yscale": yscale,
        },
        "legend_cfg": {
            "loc": "upper right",
        },
        "annotate_cfg": {"text": category_inst.label},
        "cms_label_cfg": {
            "lumi": round(0.001 * config_inst.x.luminosity.get("nominal"), 2),  # /pb -> /fb
            "com": config_inst.campaign.ecm,
        },
    }
    style_config = law.util.merge_dicts(default_style_config, style_config, deep=True)

    # ratio plot not used here; set `skip_ratio` to True
    kwargs["skip_ratio"] = True

    fig, (ax,) = plot_all(plot_config, style_config, **kwargs)

    ax.set_xticklabels(xticklabels, rotation=45, ha="right")

    return fig, (ax,)


def plot_profile(
    hists: OrderedDict[od.Process, hist.Hist],
    config_inst: od.Config,
    category_inst: od.Category,
    variable_insts: list[od.Variable],
    style_config: dict | None = None,
    density: bool | None = False,
    yscale: str | None = "",
    hide_errors: bool | None = None,
    process_settings: dict | None = None,
    variable_settings: dict | None = None,
    skip_base_distribution: bool = False,
    base_distribution_yscale: str = "linear",
    skip_variations: bool = False,
    **kwargs,
) -> plt.Figure:
    """
    Takes 2-dimensional histograms as an input and profiles over the second axis.

    This task adds two custom parameters, *skip_base_distribution* and *base_distribution_yscale*,
    that can be selected on command-line via the --general-settings parameter

    Exemplary task call:

    .. code-block:: bash

        law run cf.PlotVariables1D --version prod1 --processes st_tchannel_t \
            --variables jet1_pt-jet2_pt \
            --plot-function columnflow.plotting.plot_functions_1d.plot_profile

    :param skip_base_distribution: whether to skip adding distributions of the non-profiled histogram to the plot
    :param base_distribution_yscale: yscale of the base distributions
    :param skip_variations: whether to skip adding the up and down variation of the profile plot
    """
    if len(variable_insts) != 2:
        raise Exception("The plot_profile function can only be used for 2-dimensional input histograms.")

    # remove shift axis from histograms
    remove_residual_axis(hists, "shift")

    hists = apply_variable_settings(hists, variable_insts, variable_settings)
    hists = apply_process_settings(hists, process_settings)
    hists = apply_density_to_hists(hists, density)

    # process histograms to profiled and reduced histograms
    profiled_hists, reduced_hists = OrderedDict(), OrderedDict()
    for process_inst, h_in in hists.items():
        # always set "unstack" to True since we cannot stack profiled histograms
        # NOTE: to add multiple processes into one profile, you can define a new process
        process_inst.unstack = True

        profiled_hists[process_inst] = get_profile_variations(h_in, axis=1)
        reduced_hists[process_inst] = h_in[{h_in.axes[1].name: sum}]

    # setup plot config
    plot_config = OrderedDict()
    default_colors = plt.rcParams["axes.prop_cycle"].by_key()["color"]

    for proc_inst, h in profiled_hists.items():
        # set the default process color via the default matplotlib colors for consistency
        proc_inst.color = proc_inst.color or default_colors.pop(0)

        # add profiles to plot config
        plot_config[f"profile_{proc_inst.name}"] = plot_cfg = {
            "method": "draw_profile",
            "hist": h["nominal"],
            "kwargs": {
                "label": proc_inst.label,
                "color": proc_inst.color,
                "histtype": "step",
            },
        }

        if not skip_variations:
            for variation in ("up", "down"):
                plot_config[f"profile_{proc_inst.name}_{variation}"] = {
                    "method": "draw_profile",
                    "hist": h[variation],
                    "kwargs": {
                        "color": proc_inst.color,
                        "histtype": "step",
                        "linestyle": "dashed",
                        "yerr": None,  # always disable yerr
                    },
                }

        if hide_errors:
            for key in ("kwargs", "ratio_kwargs"):
                if key in plot_cfg:
                    plot_cfg[key]["yerr"] = None

    default_style_config = prepare_style_config(
        config_inst, category_inst, variable_insts[0], density=density, yscale=yscale,
    )

    default_style_config["ax_cfg"]["ylabel"] = f"profiled {variable_insts[1].x_title}"
    style_config = law.util.merge_dicts(default_style_config, style_config, deep=True)

    # ratio plot not used here; set `skip_ratio` to True
    kwargs["skip_ratio"] = True

    # create the default plot
    fig, (ax,) = plot_all(plot_config, style_config, **kwargs)

    # add base distributions only if requested
    if skip_base_distribution:
        return fig, (ax,)

    # add secondary axis for the background distribution
    ax1 = ax.twinx()

    for proc_inst, h in reduced_hists.items():
        h = h / sum(h.values())
        plot_kwargs = {
            "ax": ax1,
            "color": proc_inst.color,
            "histtype": "fill",
            "alpha": 0.25,
        }
        h.plot1d(**plot_kwargs)

    log_y = base_distribution_yscale == "log"
    ax1_ymin = ax1.get_ylim()[1] / 10**kwargs.get("magnitudes", 4) if log_y else 0.0000001
    ax1_ymax = get_position(
        ax1_ymin,
        ax1.get_ylim()[1],
        factor=1 / (1 - kwargs.get("whitespace_fraction", 0.3)),
        logscale=log_y,
    )
    ax1.set(
        ylim=(ax1_ymin, ax1_ymax),
        ylabel=r"$\Delta N/N$",
        yscale=base_distribution_yscale,
    )

    plt.tight_layout()

    return fig, (ax, ax1)
