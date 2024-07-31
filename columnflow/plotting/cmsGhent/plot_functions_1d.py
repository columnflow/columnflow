from __future__ import annotations

import order as od
import law
from collections import OrderedDict
from columnflow.util import maybe_import

from columnflow.plotting.plot_util import (
    remove_residual_axis,
    apply_variable_settings,
    apply_density_to_hists,
    prepare_style_config,
)
from columnflow.plotting.plot_all import plot_all

from columnflow.plotting.cmsGhent.plot_util import cumulate

plt = maybe_import("matplotlib.pyplot")
np = maybe_import("numpy")
mtrans = maybe_import("matplotlib.transforms")
mplhep = maybe_import("mplhep")


def plot_multi_variables(
        hists: OrderedDict,
        config_inst: od.Config,
        category_inst: od.Category,
        style_config: dict | None = None,
        density: bool | None = False,
        shape_norm: bool = False,
        yscale: str | None = None,
        hide_errors: bool | None = None,
        variable_settings: dict | None = None,
        **kwargs,
) -> plt.Figure:
    """
    Plot multiple variables as histograms with optional density, normalization, and custom styling.

    Parameters:
    -----------
    hists : OrderedDict
        A dictionary containing histograms keyed by variable instances.
    config_inst : od.Config
        Configuration instance used for making the histograms
    category_inst : od.Category
        Category instance whose label will be added to the plot
    style_config : dict, optional
        Dictionary specifying the style configuration for the plot. Default is None.
    density : bool, optional
        If True, the histograms are scaled by bin area. Default is False.
    shape_norm : bool, optional
        If True, the histograms are divided by the sum of the bin contents. Default is False.
    yscale : str, optional
        Y-axis scale type (e.g., 'linear', 'log'). Default is None.
    hide_errors : bool, optional
        If True, error bars are hidden in the plot. Default is None.
    variable_settings : dict, optional
        Dictionary specifying settings for each variable. Default is None.
    **kwargs
        Additional keyword arguments to be passed to the plot function.

    Returns:
    --------
    plt.Figure, created plt.Axes instances

    Notes:
    ------
    This function processes and plots multiple variable variants. It performs the following steps:
    1. Removes residual "shift" axes from the histograms, if it is of length one.
    2. The x label is VAR.x.x_title_multi for the first variable VAR if it is defined. If not, VAR.x_title.
    3. For the legend, each variable VAR is labeled using VAR.x_title_short
    """
    remove_residual_axis(hists, "shift")
    remove_residual_axis(hists, "process", max_bins=np.inf)

    variable_insts = list(hists)

    for variable_inst in variable_insts:
        hists |= apply_variable_settings({variable_inst: hists[variable_inst]}, [variable_inst], variable_settings)
    hists = apply_density_to_hists(hists, density)

    initial = variable_insts[0]
    plot_config = OrderedDict()

    # add hists
    for variable_inst, h in hists.items():
        norm = sum(h.values()) if shape_norm else 1
        plot_config[f"hist_{variable_inst.x_title_short}"] = plot_cfg = {
            "method": "draw_hist",
            "hist": h,
            "kwargs": {
                "norm": norm,
                "label": variable_inst.x_title_short,
            } | variable_inst.aux.get("plot_kwargs", {}),
            "ratio_kwargs": {
                "norm": hists[initial].values(),
            } | variable_inst.aux.get("plot_kwargs", {}),
        }
        if hide_errors:
            for key in ("kwargs", "ratio_kwargs"):
                if key in plot_cfg:
                    plot_cfg[key]["yerr"] = None

    # setup style config
    default_style_config = prepare_style_config(
        config_inst, category_inst, initial, density, shape_norm, yscale,
    )
    # plot-function specific changes
    default_style_config["ax_cfg"]["xlabel"] = initial.aux.get("x_title_multi", initial.x_title)
    default_style_config["rax_cfg"]["ylim"] = (0., 1.1)
    default_style_config["rax_cfg"]["ylabel"] = "ratio to " + initial.x_title_short

    style_config = law.util.merge_dicts(default_style_config, style_config, deep=True)
    if shape_norm:
        style_config["ax_cfg"]["ylabel"] = r"$\Delta N/N$"

    return plot_all(plot_config, style_config, **kwargs)


def plot_roc(
    hists: OrderedDict,
    config_inst: od.Config,
    category_inst: od.Category,
    variable_insts_groups: list[list[od.Variable]],
    style_config: dict | None = None,
    yscale: str = "linear",
    xscale: str = "linear",
    variable_settings: dict | None = None,
    switch=False,
    **kwargs,
) -> plt.Figure:
    """
    Plot ROC curves for pairs of variables (given in variable_insts_groups). Each pair of variables gives
    the score of the background and signal respectively.

    The way to cut on these variables can be specified like

    ```python

    config.add_variable(
        name="myvar",
        expression="myvar",
        aux=dict(cut_direction="below"/"above")
    )

    ```

    One can switch the role of signal and background (as well as the cut directions) by setting **switch** to True

    Parameters:
    -----------
    hists : OrderedDict
        A dictionary containing histograms keyed by variable instances.
    config_inst : od.Config
        Configuration instance used for making the histograms
    category_inst : od.Category
        Category instance whose label will be added to the plot
    variable_insts_groups: list of od.Variable pairs
        Pairs of background - signal scores (if the sublists are longer then two, all other entries are ignored)
    style_config : dict, optional
        Dictionary specifying the style configuration for the plot. Default is None.
    xscale : str, optional
        X-axis scale type (e.g., 'linear', 'log'). Default is linear.
    yscale : str, optional
        Y-axis scale type (e.g., 'linear', 'log'). Default is linear.
    variable_settings : dict, optional
        Dictionary specifying settings for each variable. Default is None.
    **kwargs
        Additional keyword arguments to be passed to the plot function.

    Returns:
    --------
    plt.Figure, created plt.Axes instances

    Notes:
    ------
    This function processes and plots multiple variable variants. It performs the following steps:
    1. Removes residual "shift" axes from the histograms, if it is of length one.
    2. The x and y label are "{VAR.x_title_short} efficiency" for the respective variables
    """

    remove_residual_axis(hists, "shift")

    plot_config = {}
    for variable_insts in variable_insts_groups:
        for variable_inst in variable_insts:
            if group_name := getattr(variable_inst, "x_title_multi", variable_inst.aux.get("x_title_multi")):
                break
        else:
            raise AssertionError("provide x_title_multi for at least one variable per pair")
        plot_config[group_name] = {
            "hist": [],
            "method": lambda ax, hist, **kwargs: ax.plot(*hist, **kwargs),
            "kwargs": dict(
                label=group_name,
            ) | variable_insts[1].aux.get("plot_kwargs", {}) | variable_insts[0].aux.get("plot_kwargs", {}),
        }
        for variable_inst in variable_insts:
            hists |= apply_variable_settings(
                {variable_inst: hists[variable_inst]},
                [variable_inst],
                variable_settings,
            )
            if dr := getattr(variable_inst, "cut_direction", variable_inst.aux.get("cut_direction")):
                break

        if dr is None:
            dr = "above"
        if switch:
            dr = "below" if dr == "above" else "above"

        for variable_inst in variable_insts[:2][::-1] if switch else variable_insts[:2]:
            cum_hist = cumulate(hists[variable_inst], direction=dr, axis=variable_inst)
            effs = cum_hist.values() / np.max(cum_hist.values())
            plot_config[group_name]["hist"].append(np.array([0, *effs, 1]))
    refvrs = variable_insts_groups[0][::-1] if switch else variable_insts_groups[0]
    default_style_config = {
        "ax_cfg": {
            "xlim": (0, 1),
            "ylim": (0, 1),
            "xlabel": f"{refvrs[0].x_title_short} efficiency",
            "ylabel": f"{refvrs[1].x_title_short} efficiency",
            "yscale": yscale,
            "xscale": xscale,
        },
        "legend_cfg": {},
        "annotate_cfg": {"text": category_inst.label},
        "cms_label_cfg": {
            "lumi": round(0.001 * config_inst.x.luminosity.get("nominal"), 2),  # /pb -> /fb
            "com": config_inst.campaign.ecm,
        },
    }

    style_config = law.util.merge_dicts(default_style_config, style_config, deep=True)
    kwargs["skip_ratio"] = True
    fig, axs = plot_all(plot_config, style_config, **kwargs)
    axs[0].axline((0, 0), slope=1, ls="--", color="k")
    return fig, axs
