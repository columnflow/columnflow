# coding: utf-8

"""
unrolling a 2d histogram into 1d

Can be called as a `cf.PlotVariables1D` task with
```
--plot-function ttcc.plotting.unrolled_2d.plot_unrolled_2d
```
and two dimensional variables, e.g.
```
--variables bjet_HT-anc_4j4b__n_btagL
```
where the first variable is the one displayed on the x axis and the second one is the ancillary binning.
`x_labels` and `discrete_x=True` of the ancillary variable can be used to put labels on each of the side-by-side plots. 
The tag "Ancillary region X:" is currently pre-pended per default.
"""

from __future__ import annotations

from collections import OrderedDict
from typing import Literal
from functools import partial
from unittest.mock import patch

import law

from columnflow.util import maybe_import
from columnflow.plotting.plot_all import (
    draw_hist, draw_errorbars, draw_stack, draw_error_bands,
)
from columnflow.plotting.plot_util import (
    prepare_plot_config,
    prepare_style_config,
    remove_residual_axis,
    apply_variable_settings,
    apply_process_settings,
    apply_density_to_hists,
    get_cms_label,
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

def unroll_hists(hists):
    unrolled_hists = []
    first = True
    for process in hists:
        hist = hists[process]
        if first:
            # add the aux binning to the unrolled hist list
            # todo should also access the custom bin labels here
            n_aux = hist.shape[1]
            for iaux in range(n_aux):
                unrolled_hists.append( OrderedDict() )

            # get the aux variable for return
            aux_bins = hist.axes[1]
            first = False

        # loop over aux bins and slice histogram into 1D hists
        for iaux in range(n_aux):
            sliced_hist = hist[:, iaux]
            sliced_hist.name = hist.axes[0].name
            sliced_hist.label = hist.axes[0].label
            unrolled_hists[iaux][process] = sliced_hist
    
    return aux_bins, unrolled_hists

def plot_unrolled_2d(
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

    # remove shift axis from histograms
    remove_residual_axis(hists, "shift")

    x_variable_inst = variable_insts[0]
    y_variable_inst = variable_insts[1]

    hists = apply_variable_settings(hists, variable_insts, variable_settings)

    hists = apply_process_settings(hists, process_settings)

    hists = apply_density_to_hists(hists, density)

    aux_dimension, hists = unroll_hists(hists)

    # set up style config
    default_style_config = prepare_style_config(
        config_inst, category_inst, x_variable_inst, density, shape_norm, yscale,
    )
    style_config = law.util.merge_dicts(default_style_config, style_config, deep=True)
    skip_ratio = kwargs.get("skip_ratio", False)

    # available plot methods mapped to their names
    plot_methods = {
        func.__name__: func
        for func in [draw_error_bands, draw_stack, draw_hist, draw_errorbars]
    }

    # use CMS plotting style
    plt.style.use(mplhep.style.CMS)
    
    # create (2, n_aux) canvas
    figsize=(16,10)
    if not skip_ratio:
        fig, x = plt.subplots(2, y_variable_inst.n_bins, figsize=figsize,
                    gridspec_kw=dict(height_ratios=[3, 1], hspace=0, wspace=0), 
                    sharex="col", sharey="row")
        (axes, raxes) = x
    else:
        fig, axes = plt.subplots(1, y_variable_inst.n_bins, figsize=figsize,
                    gridspec_kw=dict(wspace=0),
                    sharey="row")
        x = (axes, )

    for i, hist in enumerate(hists):
        ax = axes[i]

        plot_config = prepare_plot_config(
            hist,
            shape_norm=shape_norm,
            hide_errors=hide_errors,
        )

        if shape_norm:
            style_config["ax_cfg"]["ylabel"] = r"$\Delta N/N$"

        for key, cfg in plot_config.items():
            if "method" not in cfg:
                raise ValueError(f"no method given in plot_cfg entry {key}")
            method = cfg["method"]

            if "hist" not in cfg:
                raise ValueError(f"no histogram(s) given in plot_cfg entry {key}")
            h = cfg["hist"]
            kw = cfg.get("kwargs", {})
            plot_methods[method](ax, h, **kw)
            if not skip_ratio:
                # take ratio_method if the ratio plot requires a different plotting method
                method = cfg.get("ratio_method", method)
                rkw = cfg.get("ratio_kwargs", {})
                plot_methods[method](raxes[i], h, **rkw)
        
    # some options to be used below
    magnitudes = kwargs.get("magnitudes", 4)
    whitespace_fraction = kwargs.get("whitespace_fraction", 0.2)
    skip_legend = kwargs.get("skip_legend", False)
    cms_label = kwargs.get("cms_label", "wip")

    # axis styling
    ax_kwargs = {
        "ylabel": "Counts",
        "xlabel": "variable",
        "yscale": "linear",
    }
    
    log_y = style_config.get("ax_cfg", {}).get("yscale", "linear") == "log"
    
    ax_ymin = ax.get_ylim()[1] / 10**magnitudes if log_y else 0.0000001
    ax_ymax = get_position(ax_ymin, ax.get_ylim()[1], 
                factor=1 / (1 - whitespace_fraction),
                logscale=log_y)
    ax_kwargs.update({"ylim": (ax_ymin, ax_ymax)})

    # prioritize style_config ax settings
    ax_kwargs.update(style_config.get("ax_cfg", {}))

    # ax configs that can not be handled by ax.set
    minorxticks = ax_kwargs.pop("minorxticks", None)
    minoryticks = ax_kwargs.pop("minoryticks", None)

    for ax in axes:
        this_kwargs = ax_kwargs.copy()

        # x label only for last ax
        if not ax == axes[-1]:
            this_kwargs["xlabel"] = None
        
        # y label only for first ax
        if not ax == axes[0]:
            this_kwargs["ylabel"] = None

        ax.set(**this_kwargs)

        if minorxticks is not None:
            ax.set_xticks(minorxticks, minor=True)
        if minoryticks is not None:
            ax.set_xticks(minoryticks, minor=True)

    if not skip_ratio:
        rax_kwargs = {
            "ylim": (0.72, 1.28),
            "ylabel": "Ratio",
            "xlabel": "Variable",
            "yscale": "linear",
        }
        rax_kwargs.update(style_config.get("rax_cfg", {}))
        for rax in raxes:
            this_kwargs = rax_kwargs.copy()

            # hard coded line at 1  
            rax.axhline(y=1.0, linestyle="dashed", color="gray")

            # x label only for last ax
            if not rax == raxes[-1]:
                this_kwargs["xlabel"] = None

            # y label only for first ax
            if not rax == raxes[0]:
                this_kwargs["ylabel"] = None

            rax.set(**this_kwargs)

        fig.align_ylabels()

    # legend
    if not skip_legend:
        # resolve legend kwargs
        legend_kwargs = {
            "borderaxespad": 0.,
            "title_fontsize": 18,
            "alignment": "left"
        }
        legend_kwargs.update(style_config.get("legend_cfg", {}))

        # overwrite some forced options for this plotting style
        legend_kwargs["ncol"] = 1
        legend_kwargs["loc"] = "upper left" 
        legend_kwargs["fontsize"] = 20
    
        # retreive legend handles and labels from last upper plot
        handles, labels = axes[-1].get_legend_handles_labels()
        
        # assime all `StepPatch` objects are part of MC stack
        in_stack = [
            isinstance(handle, mpl.patches.StepPatch)
            for handle in handles
        ]

        # reverse order of entries that are part of the stack
        if any(in_stack):
            def shuffle(entries, mask):
                entries = np.array(entries, dtype=object)
                entries[mask] = entries[mask][::-1]
                return list(entries)
    
            handles = shuffle(handles, in_stack)
            labels = shuffle(labels, in_stack)

        # make legend using ordered handles/labels
        title = style_config.get("annotate_cfg", {}).get("text", None)
        axes[-1].legend(handles, labels, title=title, 
            bbox_to_anchor=(1., 1.), **legend_kwargs)
        fig.subplots_adjust(right=0.8)

    # custom annotation
    log_x = style_config.get("ax_cfg", {}).get("xscale", "linear") == "log"
    annotate_kwargs = {
        "xycoords": "data",
        "color": "black",
        "fontsize": 20,
        "horizontalalignment": "left",
        "verticalalignment": "top",
    }
    annotate_kwargs.update(style_config.get("annotate_cfg", {}))

    # add aux binning labels to the top right of each plot
    if aux_labels := y_variable_inst.x_labels:
        if len(aux_labels) == len(axes):
            for i, aux_label in enumerate(aux_labels):
                region = f"Ancillary region {i+1}:"
                label = region+"\n"+aux_label
                this_annotation = annotate_kwargs.copy()
                this_annotation["text"] = label
                this_annotation["xy"] = (
                    get_position(*axes[i].get_xlim(), factor=0.05, logscale=log_x),
                    get_position(*axes[i].get_ylim(), factor=0.95, logscale=log_y),
                )
                axes[i].annotate(**this_annotation)
                    

    # cms label
    if cms_label != "skip":
        cms_label_kwargs = get_cms_label(axes[0], cms_label)
        cms_label_kwargs.update(style_config.get("cms_label_cfg", {}))
        
        # one label on left
        mplhep.cms.label(ax=axes[0], llabel=cms_label_kwargs["llabel"], 
                        data=cms_label_kwargs["data"], rlabel="")

        # one label on right
        mplhep.cms.label(ax=axes[-1], llabel="", label="", exp="", 
                        lumi=cms_label_kwargs["lumi"],
                        com=cms_label_kwargs["com"])

    plt.tight_layout()

    return fig, x

