# coding: utf-8

"""
Example plot function.
"""

from __future__ import annotations

__all__ = []

import order as od

from columnflow.util import maybe_import, try_float
from columnflow.config_util import group_shifts
from columnflow.plotting.plot_util import (
    get_position,
    apply_ax_kwargs,
    get_cms_label,
    remove_label_placeholders,
    apply_label_placeholders,
    calculate_stat_error,
)
from columnflow.types import TYPE_CHECKING, Sequence

np = maybe_import("numpy")
if TYPE_CHECKING:
    hist = maybe_import("hist")
    plt = maybe_import("matplotlib.pyplot")


def draw_stat_error_bands(
    ax: plt.Axes,
    h: hist.Hist,
    norm: float | Sequence | np.ndarray = 1.0,
    **kwargs,
) -> None:
    assert len(h.axes) == 1

    # compute relative statistical errors
    rel_stat_error = h.variances()**0.5 / h.values()
    rel_stat_error[np.isnan(rel_stat_error)] = 0.0

    # compute the baseline
    # fill 1 in places where both numerator and denominator are 0, and 0 for remaining nan's
    baseline = h.values() / norm
    baseline[(h.values() == 0) & (norm == 0)] = 1.0
    baseline[np.isnan(baseline)] = 0.0

    bar_kwargs = {
        "x": h.axes[0].centers,
        "bottom": baseline * (1 - rel_stat_error),
        "height": baseline * 2 * rel_stat_error,
        "width": h.axes[0].edges[1:] - h.axes[0].edges[:-1],
        "hatch": "///",
        "linewidth": 0,
        "color": "none",
        "edgecolor": "black",
        "alpha": 1.0,
        **kwargs,
    }
    ax.bar(**bar_kwargs)


def draw_syst_error_bands(
    ax: plt.Axes,
    h: hist.Hist,
    syst_hists: Sequence[hist.Hist],
    shift_insts: Sequence[od.Shift],
    norm: float | Sequence | np.ndarray = 1.0,
    method: str = "quadratic_sum",
    **kwargs,
) -> None:
    import hist

    assert len(h.axes) == 1
    assert method in ("quadratic_sum", "envelope")

    nominal_shift, shift_groups = group_shifts(shift_insts)
    if nominal_shift is None:
        raise ValueError("no nominal shift found in the list of shift instances")

    # create pairs of shifts mapping from up -> down and vice versa
    shift_pairs = {}
    shift_pairs[nominal_shift] = nominal_shift  # nominal shift maps to itself
    for up_shift, down_shift in shift_groups.values():
        shift_pairs[up_shift] = down_shift
        shift_pairs[down_shift] = up_shift

    # stack histograms separately per shift, falling back to the nominal one when missing
    shift_stacks: dict[od.Shift, hist.Hist] = {}
    for shift_inst in sum(shift_groups.values(), [nominal_shift]):
        for _h in syst_hists:
            # when the shift is present, the flipped shift must exist as well
            shift_ax = _h.axes["shift"]
            if shift_inst.name in shift_ax:
                if shift_pairs[shift_inst].name not in shift_ax:
                    raise RuntimeError(
                        f"shift {shift_inst} found in histogram but {shift_pairs[shift_inst]} is missing; "
                        f"existing shifts: {','.join(map(str, list(shift_ax)))}",
                    )
                shift_name = shift_inst.name
            else:
                shift_name = nominal_shift.name
            # store the slice
            _h = _h[{"shift": hist.loc(shift_name)}]
            if shift_inst not in shift_stacks:
                shift_stacks[shift_inst] = _h
            else:
                shift_stacks[shift_inst] += _h

    # loop over bins, subtract nominal yields from stacked yields and merge differences into
    # a systematic error per bin using the given method (quadratic sum vs. evelope)
    # note 1: if the up/down variations of the same shift source point in the same direction, a
    #         statistical combination is pointless and their minimum/maximum is selected instead
    # note 2: relative signs are consumed into the meaning of "up" and "down" here as they already
    #         are combinations evaluated for a specific direction
    syst_error_up = []
    syst_error_down = []
    for b in range(h.axes[0].size):
        up_diffs = []
        down_diffs = []
        for source, (up_shift, down_shift) in shift_groups.items():
            # get actual differences resulting from this shift
            shift_up_diff = shift_stacks[up_shift].values()[b] - shift_stacks[nominal_shift].values()[b]
            shift_down_diff = shift_stacks[down_shift].values()[b] - shift_stacks[nominal_shift].values()[b]
            # store them depending on whether they really increase or decrease the yield
            up_diffs.append(max(shift_up_diff, shift_down_diff, 0))
            down_diffs.append(min(shift_up_diff, shift_down_diff, 0))
        # combination based on the method
        if method == "quadratic_sum":
            up_diff = sum(d**2 for d in up_diffs)**0.5
            down_diff = sum(d**2 for d in down_diffs)**0.5
        else:  # envelope
            up_diff = max(up_diffs)
            down_diff = min(down_diffs)
        # save values
        syst_error_up.append(up_diff)
        syst_error_down.append(down_diff)

    # compute relative systematic errors
    rel_syst_error_up = np.array(syst_error_up) / h.values()
    rel_syst_error_up[np.isnan(rel_syst_error_up)] = 0.0
    rel_syst_error_down = np.array(syst_error_down) / h.values()
    rel_syst_error_down[np.isnan(rel_syst_error_down)] = 0.0

    # compute the baseline
    # fill 1 in places where both numerator and denominator are 0, and 0 for remaining nan's
    baseline = h.values() / norm
    baseline[(h.values() == 0) & (norm == 0)] = 1.0
    baseline[np.isnan(baseline)] = 0.0

    bar_kwargs = {
        "x": h.axes[0].centers,
        "bottom": baseline * (1 - rel_syst_error_down),
        "height": baseline * (rel_syst_error_up + rel_syst_error_down),
        "width": h.axes[0].edges[1:] - h.axes[0].edges[:-1],
        "hatch": "\\\\\\",
        "linewidth": 0,
        "color": "none",
        "edgecolor": "#30c300",
        "alpha": 1.0,
        **kwargs,
    }
    ax.bar(**bar_kwargs)


def draw_stack(
    ax: plt.Axes,
    h: hist.Stack,
    norm: float | Sequence | np.ndarray = 1.0,
    **kwargs,
) -> None:
    import hist

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

    # draw only the stack, no error bars/bands with stack = True
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
    error_type: str = "variance",
    **kwargs,
) -> None:
    import hist

    assert error_type in {"variance", "poisson_unweighted", "poisson_weighted"}

    if kwargs.get("color", "") is None:
        # when color is set to None, remove it such that matplotlib automatically chooses a color
        kwargs.pop("color")

    defaults = {
        "ax": ax,
        "stack": False,
        "histtype": "step",
    }
    defaults.update(kwargs)
    if "yerr" not in defaults:
        if h.storage_type.accumulator is not hist.accumulators.WeightedSum:
            raise TypeError(
                "Error bars calculation only implemented for histograms with storage type WeightedSum "
                "either change the Histogram storage_type or set yerr manually",
            )
        yerr = calculate_stat_error(h, error_type)
        # normalize yerr to the histogram = error propagation on standard deviation
        yerr = abs(yerr / norm)
        # replace inf with nan for any bin where norm = 0 and calculate_stat_error returns a non zero value
        if np.any(np.isinf(yerr)):
            yerr[np.isinf(yerr)] = np.nan
        defaults["yerr"] = yerr

    h = h / norm

    h.plot1d(**defaults)


def draw_profile(
    ax: plt.Axes,
    h: hist.Hist,
    norm: float | Sequence | np.ndarray = 1.0,
    error_type: str = "variance",
    **kwargs,
) -> None:
    """
    Profiled histograms contains the storage type "Mean" and can therefore not be normalized
    """
    import hist

    assert error_type in {"variance", "poisson_unweighted", "poisson_weighted"}

    if kwargs.get("color", "") is None:
        # when color is set to None, remove it such that matplotlib automatically chooses a color
        kwargs.pop("color")

    defaults = {
        "ax": ax,
        "stack": False,
        "histtype": "step",
    }
    defaults.update(kwargs)
    if "yerr" not in defaults:
        if h.storage_type.accumulator is not hist.accumulators.WeightedSum:
            raise TypeError(
                "Error bars calculation only implemented for histograms with storage type WeightedSum "
                "either change the Histogram storage_type or set yerr manually",
            )
        defaults["yerr"] = calculate_stat_error(h, error_type)
    h.plot1d(**defaults)


def draw_errorbars(
    ax: plt.Axes,
    h: hist.Hist,
    norm: float | Sequence | np.ndarray = 1.0,
    error_type: str = "poisson_unweighted",
    **kwargs,
) -> None:
    import hist

    assert error_type in {"variance", "poisson_unweighted", "poisson_weighted"}

    values = h.values() / norm

    defaults = {
        "x": h.axes[0].centers,
        "y": values,
        "color": "k",
        "linestyle": "none",
        "marker": "o",
        "elinewidth": 1,
    }
    defaults.update(kwargs)

    if "yerr" not in defaults:
        if h.storage_type.accumulator is not hist.accumulators.WeightedSum:
            raise TypeError(
                "Error bars calculation only implemented for histograms with storage type WeightedSum "
                "either change the Histogram storage_type or set yerr manually",
            )
        yerr = calculate_stat_error(h, error_type)
        # normalize yerr to the histogram = error propagation on standard deviation
        yerr = abs(yerr / norm)
        # replace inf with nan for any bin where norm = 0 and calculate_stat_error returns a non zero value
        if np.any(np.isinf(yerr)):
            yerr[np.isinf(yerr)] = np.nan
        defaults["yerr"] = yerr

    ax.errorbar(**defaults)


def draw_hist_twin(
    ax: plt.Axes,
    h: hist.Hist,
    norm: float | Sequence | np.ndarray = 1.0,
    **kwargs,
) -> None:
    """
    calls draw_hist to plot a histogram on the right y-axis
    """
    ax2 = ax.twinx()
    draw_hist(ax2, h, norm, **kwargs)
    bin_widths = h.axes[0].widths
    if norm == 1:
        ax2.set_ylabel(r"Events / {:.2f} GeV".format(bin_widths[0]))
    else:
        ax2.set_ylabel("Normalized entries")
    if bin_widths[0] > 5:
        ax2.set_ylim(0, 0.129)
    else:
        ax2.set_ylim(0, 0.072)
    # ax2.ticklabel_format(style="sci", scilimits=(2, 2), axis="y")
    # ax2.yaxis.offsetText.set_position((1.075, 0.))


def plot_all(
    plot_config: dict,
    style_config: dict,
    skip_ratio: bool = False,
    skip_legend: bool = False,
    cms_label: str = "wip",
    whitespace_fraction: float = 0.3,
    magnitudes: float = 4,
    **kwargs,
) -> tuple[plt.Figure, tuple[plt.Axes, ...]]:
    """
    Function that calls multiple plotting methods based on two configuration dictionaries, *plot_config* and
    *style_config*.

    The *plot_config* expects dictionaries with fields:

        - "method": str, identical to the name of a function defined above
        - "hist": hist.Hist or hist.Stack
        - "kwargs": dict (optional)
        - "ratio_kwargs": dict (optional)

    The *style_config* expects fields (all optional):

        - "gridspec_cfg": dict
        - "ax_cfg": dict
        - "rax_cfg": dict
        - "legend_cfg": dict
        - "cms_label_cfg": dict

    :param plot_config: Dictionary that defines which plot methods will be called with which key word arguments.
    :param style_config: Dictionary that defines arguments on how to style the overall plot.
    :param skip_ratio: Optional bool parameter to not display the ratio plot.
    :param skip_legend: Optional bool parameter to not display the legend.
    :param cms_label: Optional string parameter to set the CMS label text.
    :param whitespace_fraction: Optional float parameter that defines the ratio of which the plot will consist of
        whitespace for the legend and labels
    :param magnitudes: Optional float parameter that defines the displayed ymin when plotting with a logarithmic scale.
    :return: tuple of plot figure and axes
    """
    import matplotlib as mpl
    import matplotlib.pyplot as plt
    import mplhep

    # general mplhep style
    plt.style.use(mplhep.style.CMS)

    # use non-interactive Agg backend for plotting
    mpl.use("Agg")

    # setup figure and axes
    rax = None
    grid_spec = {"left": 0.15, "right": 0.95, "top": 0.95, "bottom": 0.1}
    grid_spec |= style_config.get("gridspec_cfg", {})

    # Get figure size from style_config, with default values
    subplots_cfg = style_config.get("subplots_cfg", {})

    if not skip_ratio:
        grid_spec = {"height_ratios": [3, 1], "hspace": 0, **grid_spec}
        fig, axs = plt.subplots(2, 1, gridspec_kw=grid_spec, sharex=True, **subplots_cfg)
        (ax, rax) = axs
    else:
        grid_spec.pop("height_ratios", None)
        fig, ax = plt.subplots(gridspec_kw=grid_spec, **subplots_cfg)
        axs = (ax,)

    # invoke all plots methods
    plot_methods = {
        func.__name__: func
        for func in [
            draw_stat_error_bands, draw_syst_error_bands, draw_stack, draw_hist, draw_profile,
            draw_errorbars,
            draw_hist_twin,
        ]
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

    # apply axis kwargs
    apply_ax_kwargs(ax, ax_kwargs)

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

        # apply axis kwargs
        apply_ax_kwargs(rax, rax_kwargs)

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
        entries_per_col = legend_kwargs.pop("cf_entries_per_column", None)
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
        update_handles_labels = legend_kwargs.pop("cf_update_handles_labels", None)
        if callable(update_handles_labels):
            update_handles_labels(ax, handles, labels, n_cols)

        # interpret placeholders
        apply = []
        if legend_kwargs.pop("cf_short_labels", False):
            apply.append("SHORT")
        if legend_kwargs.pop("cf_line_breaks", False):
            apply.append("BREAK")
        labels = [apply_label_placeholders(label, apply=apply) for label in labels]

        # drop remaining placeholders
        labels = list(map(remove_label_placeholders, labels))

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
