# coding: utf-8

"""
Example plot function.
"""

from __future__ import annotations

__all__ = []

from columnflow.types import Sequence
from columnflow.util import maybe_import, try_float
from columnflow.plotting.plot_util import (
    get_position,
    get_cms_label,
    remove_label_placeholders,
)

hist = maybe_import("hist")
np = maybe_import("numpy")
mpl = maybe_import("matplotlib")
plt = maybe_import("matplotlib.pyplot")
mplhep = maybe_import("mplhep")
od = maybe_import("order")


def draw_error_bands(
    ax: plt.Axes,
    h: hist.Hist,
    norm: float | Sequence | np.ndarray = 1.0,
    **kwargs,
) -> None:
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


def draw_stack(
    ax: plt.Axes,
    h: hist.Stack,
    norm: float | Sequence | np.ndarray = 1.0,
    **kwargs,
) -> None:
    # check if norm is a number
    if try_float(norm):
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


def draw_hist(
    ax: plt.Axes,
    h: hist.Hist,
    norm: float | Sequence | np.ndarray = 1.0,
    **kwargs,
) -> None:
    if kwargs.get("color", "") is None:
        # when color is set to None, remove it such that matplotlib automatically chooses a color
        kwargs.pop("color")

    h = h / norm
    defaults = {
        "ax": ax,
        "stack": False,
        "histtype": "step",
    }
    defaults.update(kwargs)
    h.plot1d(**defaults)


def draw_profile(
    ax: plt.Axes,
    h: hist.Hist,
    norm: float | Sequence | np.ndarray = 1.0,
    **kwargs,
) -> None:
    """
    Profiled histograms contains the storage type "Mean" and can therefore not be normalized
    """
    if kwargs.get("color", "") is None:
        # when color is set to None, remove it such that matplotlib automatically chooses a color
        kwargs.pop("color")

    defaults = {
        "ax": ax,
        "stack": False,
        "histtype": "step",
    }
    defaults.update(kwargs)
    h.plot1d(**defaults)


def draw_errorbars(
    ax: plt.Axes,
    h: hist.Hist,
    norm: float | Sequence | np.ndarray = 1.0,
    **kwargs,
) -> None:
    values = h.values() / norm
    variances = h.variances() / norm**2
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
    yerr[yerr < 0] = 0
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
    skip_legend: bool = False,
    cms_label: str = "wip",
    whitespace_fraction: float = 0.3,
    magnitudes: float = 4,
    **kwargs,
) -> tuple(plt.Figure, tuple(plt.Axes)):
    """
    Function that calls multiple plotting methods based on two configuration dictionaries,
    *plot_config* and *style_config*.

    The *plot_config* expects dictionaries with fields:
    "method": str, identical to the name of a function defined above,
    "hist": hist.Hist or hist.Stack,
    "kwargs": dict (optional),
    "ratio_kwargs": dict (optional),

    The *style_config* expects fields (all optional):
    "gridspec_cfg": dict,
    "ax_cfg": dict,
    "rax_cfg": dict,
    "legend_cfg": dict,
    "cms_label_cfg": dict,

    :param plot_config: Dictionary that defines which plot methods will be called with which
        key word arguments.
    :param style_config: Dictionary that defines arguments on how to style the overall plot.
    :param skip_ratio: Optional bool parameter to not display the ratio plot.
    :param skip_legend: Optional bool parameter to not display the legend.
    :param cms_label: Optional string parameter to set the CMS label text.
    :param whitespace_fraction: Optional float parameter that defines the ratio of which
        the plot will consist of whitespace for the legend and labels
    :param magnitudes: Optional float parameter that defines the displayed ymin when plotting
        with a logarithmic scale.
    :return: tuple of plot figure and axes
    """
    # general mplhep style
    plt.style.use(mplhep.style.CMS)

    # setup figure and axes
    rax = None
    grid_spec = {"left": 0.15, "right": 0.95, "top": 0.95, "bottom": 0.1}
    grid_spec |= style_config.get("gridspec_cfg", {})
    if not skip_ratio:
        grid_spec |= {"height_ratios": [3, 1], "hspace": 0}
        fig, axs = plt.subplots(2, 1, gridspec_kw=grid_spec, sharex=True)
        (ax, rax) = axs
    else:
        fig, ax = plt.subplots(gridspec_kw=grid_spec)
        axs = (ax,)

    # invoke all plots methods
    plot_methods = {
        func.__name__: func
        for func in [draw_error_bands, draw_stack, draw_hist, draw_profile, draw_errorbars]
    }
    for key, cfg in plot_config.items():
        # check if required fields are present
        if "method" not in cfg:
            raise ValueError(f"no method given in plot_cfg entry {key}")
        if "hist" not in cfg:
            raise ValueError(f"no histogram(s) given in plot_cfg entry {key}")

        # invoke the method
        method = cfg["method"]
        h = cfg["hist"]
        plot_methods[method](ax, h, **cfg.get("kwargs", {}))

        # repeat for ratio axes if configured
        if not skip_ratio and "ratio_kwargs" in cfg:
            # take ratio_method if the ratio plot requires a different plotting method
            method = cfg.get("ratio_method", method)
            plot_methods[method](rax, h, **cfg.get("ratio_kwargs", {}))

    # axis styling
    ax_kwargs = {
        "ylabel": "Counts",
        "xlabel": "variable",
        "yscale": "linear",
    }

    # some default ylim settings based on yscale
    log_y = style_config.get("ax_cfg", {}).get("yscale", "linear") == "log"

    ax_ymin = ax.get_ylim()[1] / 10**magnitudes if log_y else 0.0000001
    ax_ymax = get_position(ax_ymin, ax.get_ylim()[1], factor=1 / (1 - whitespace_fraction), logscale=log_y)
    ax_kwargs.update({"ylim": (ax_ymin, ax_ymax)})

    # prioritize style_config ax settings
    ax_kwargs.update(style_config.get("ax_cfg", {}))

    # some settings cannot be handled by ax.set
    xminorticks = ax_kwargs.pop("xminorticks", ax_kwargs.pop("minorxticks", None))
    yminorticks = ax_kwargs.pop("yminorticks", ax_kwargs.pop("minoryticks", None))
    xloc = ax_kwargs.pop("xloc", None)
    yloc = ax_kwargs.pop("yloc", None)

    # set all values
    ax.set(**ax_kwargs)

    # set manual configs
    if xminorticks is not None:
        ax.set_xticks(xminorticks, minor=True)
    if yminorticks is not None:
        ax.set_xticks(yminorticks, minor=True)
    if xloc is not None:
        ax.set_xlabel(ax.get_xlabel(), loc=xloc)
    if yloc is not None:
        ax.set_ylabel(ax.get_ylabel(), loc=yloc)

    # ratio plot
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

        # some settings cannot be handled by ax.set
        xloc = rax_kwargs.pop("xloc", None)
        yloc = rax_kwargs.pop("yloc", None)

        # set all values
        rax.set(**rax_kwargs)

        # set manual configs
        if xloc is not None:
            rax.set_xlabel(rax.get_xlabel(), loc=xloc)
        if yloc is not None:
            rax.set_ylabel(rax.get_ylabel(), loc=yloc)

        # remove x-label from main axis
        if "xlabel" in rax_kwargs:
            ax.set_xlabel("")

    # label alignment
    fig.align_labels()

    # legend
    if not skip_legend:
        # resolve legend kwargs
        legend_kwargs = {
            "ncols": 1,
            "loc": "upper right",
        }
        legend_kwargs.update(style_config.get("legend_cfg", {}))

        if "title" in legend_kwargs:
            legend_kwargs["title"] = remove_label_placeholders(legend_kwargs["title"])

        # retrieve the legend handles and their labels
        handles, labels = ax.get_legend_handles_labels()

        # custom argument: entries_per_column
        n_cols = legend_kwargs.get("ncols", 1)
        entries_per_col = legend_kwargs.pop("entries_per_column", None)
        if callable(entries_per_col):
            entries_per_col = entries_per_col(ax, handles, labels, n_cols)
        if entries_per_col and n_cols > 1:
            if isinstance(entries_per_col, (list, tuple)):
                assert len(entries_per_col) == n_cols
            else:
                entries_per_col = [entries_per_col] * n_cols
            # fill handles and labels with empty entries
            max_entries = max(entries_per_col)
            empty_handle = ax.plot([], label="", linestyle="None")[0]
            for i, n in enumerate(entries_per_col):
                for _ in range(max_entries - min(n, len(handles) - sum(entries_per_col[:i]))):
                    handles.insert(i * max_entries + n, empty_handle)
                    labels.insert(i * max_entries + n, "")

        # custom hook to adjust handles and labels
        update_handles_labels = legend_kwargs.pop("update_handles_labels", None)
        if callable(update_handles_labels):
            update_handles_labels(ax, handles, labels, n_cols)

        # make legend using ordered handles/labels
        ax.legend(handles, labels, **legend_kwargs)

    # custom annotation
    log_x = style_config.get("ax_cfg", {}).get("xscale", "linear") == "log"
    annotate_kwargs = {
        "text": "",
        "xy": (
            get_position(*ax.get_xlim(), factor=0.05, logscale=log_x),
            get_position(*ax.get_ylim(), factor=0.95, logscale=log_y),
        ),
        "xycoords": "data",
        "color": "black",
        "fontsize": 22,
        "horizontalalignment": "left",
        "verticalalignment": "top",
    }
    annotate_kwargs.update(style_config.get("annotate_cfg", {}))
    ax.annotate(**annotate_kwargs)

    # cms label
    if cms_label != "skip":
        cms_label_kwargs = get_cms_label(ax, cms_label)

        cms_label_kwargs.update(style_config.get("cms_label_cfg", {}))
        mplhep.cms.label(**cms_label_kwargs)

    # finalization
    fig.tight_layout()

    return fig, axs
