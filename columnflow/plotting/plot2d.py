# coding: utf-8

"""
Example 2d plot functions.
"""


from __future__ import annotations

from collections import OrderedDict

import law

from columnflow.util import maybe_import

hist = maybe_import("hist")
np = maybe_import("numpy")
mpl = maybe_import("matplotlib")
plt = maybe_import("matplotlib.pyplot")
mplhep = maybe_import("mplhep")
od = maybe_import("order")


def plot_2d(
    hists: OrderedDict,
    config_inst: od.config,
    variable_insts: list[od.variable],
    style_config: dict | None = None,
    shape_norm: bool | None = False,  # TODO use
    zscale: str | None = "",
    skip_legend: bool = False,
    skip_cms: bool = False,
    process_settings: dict | None = None,  # TODO use
    **kwargs,
) -> plt.Figure:

    # use CMS plotting style
    plt.style.use(mplhep.style.CMS)
    fig, ax = plt.subplots()

    # how to handle yscale information from 2 variable insts?
    if not zscale:
        zscale = "log" if (variable_insts[0].log_y or variable_insts[1].log_y) else "linear"

    # setup style config
    default_style_config = {
        "ax_cfg": {
            "xlim": (variable_insts[0].x_min, variable_insts[0].x_max),
            "ylim": (variable_insts[1].x_min, variable_insts[1].x_max),
            "xlabel": variable_insts[0].get_full_x_title(),
            "ylabel": variable_insts[1].get_full_x_title(),
            # "zlabel": variable_insts[0].get_full_y_title(),  # ?
            # "zscale": zscale,
        },
        "legend_cfg": {},
        "cms_label_cfg": {
            "lumi": config_inst.x.luminosity.get("nominal") / 1000,  # pb -> fb
        },
    }
    style_config = law.util.merge_dicts(default_style_config, style_config, deep=True)

    # NOTE: should we separate into multiple functions similar to 1d plotting?

    # add all processes into 1 histogram
    h_sum = sum(list(hists.values())[1:], list(hists.values())[0].copy())

    # apply style_config
    ax.set(**style_config["ax_cfg"])
    ax.legend(**style_config["legend_cfg"])

    # cms label (some TODOs might still be open here)
    cms_label_kwargs = {
        "ax": ax,
        "llabel": "Work in progress",
        "fontsize": 22,
    }
    cms_label_kwargs.update(style_config.get("cms_label_cfg", {}))
    if skip_cms:
        cms_label_kwargs.update({"data": True, "label": ""})
    mplhep.cms.label(**cms_label_kwargs)

    plt.tight_layout()

    h_sum.plot2d(ax=ax, norm=mpl.colors.LogNorm())

    return fig
