# coding: utf-8

"""
Examples for custom plot functions.
"""

from __future__ import annotations

from collections import OrderedDict

from columnflow.util import maybe_import
from columnflow.plotting.plot_util import (
    remove_residual_axis,
    apply_variable_settings,
    apply_process_settings,
)

hist = maybe_import("hist")
np = maybe_import("numpy")
mpl = maybe_import("matplotlib")
plt = maybe_import("matplotlib.pyplot")
mplhep = maybe_import("mplhep")
od = maybe_import("order")


def my_plot1d_func(
    hists: OrderedDict[od.Process, hist.Hist],
    config_inst: od.Config,
    category_inst: od.Category,
    variable_insts: list[od.Variable],
    style_config: dict | None = None,
    yscale: str | None = "",
    process_settings: dict | None = None,
    variable_settings: dict | None = None,
    example_param: str | float | bool | None = None,
    **kwargs,
) -> tuple[plt.Figure, tuple[plt.Axis]]:
    """
    This is an exemplary custom plotting function.

    Exemplary task call:

    .. code-block:: bash
        law run cf.PlotVariables1D --version v1 --processes st,tt --variables jet1_pt \
            --plot-function __cf_module_name__.plotting.example.my_plot1d_func \
            --general-settings example_param=some_text
    """
    # we can add arbitrary parameters via the `general_settings` parameter to access them in the
    # plotting function. They are automatically parsed either to a bool, float, or string
    print(f"the example_param has been set to '{example_param}' (type: {type(example_param)})")

    # call helper function to remove shift axis from histogram
    remove_residual_axis(hists, "shift")

    # call helper functions to apply the variable_settings and process_settings
    variable_inst = variable_insts[0]
    hists = apply_variable_settings(hists, variable_insts, variable_settings)
    hists = apply_process_settings(hists, process_settings)

    # use the mplhep CMS stype
    plt.style.use(mplhep.style.CMS)

    # create a figure and fill it with content
    fig, ax = plt.subplots()
    for proc_inst, h in hists.items():
        h.plot1d(
            ax=ax,
            label=proc_inst.label,
            color=proc_inst.color1,
        )

    # styling and parameter implementation (e.g. `yscale`)
    ax.set(
        yscale=yscale,
        ylabel=variable_inst.get_full_y_title(),
        xlabel=variable_inst.get_full_x_title(),
        xscale="log" if variable_inst.log_x else "linear",
    )
    ax.legend()
    mplhep.cms.label(ax=ax, fontsize=22, llabel="private work")

    # task expects a figure and a tuple of axes as output
    return fig, (ax,)
