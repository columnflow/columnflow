# coding: utf-8

"""
Some utils for plot functions.
"""

from __future__ import annotations

import re
import operator
import functools
from collections import OrderedDict

import law
import order as od
import scinum as sn
from typing import Tuple

from columnflow.util import maybe_import, try_int, try_complex, try_float
from columnflow.types import Iterable, Any, Callable, Sequence, Union

math = maybe_import("math")
hist = maybe_import("hist")
np = maybe_import("numpy")
npt = maybe_import("numpy.typing")
np = maybe_import("numpy")
plt = maybe_import("matplotlib.pyplot")
mpl = maybe_import("matplotlib")
mplhep = maybe_import("mplhep")
mticker = maybe_import("matplotlib.ticker")


FigAxesType = Tuple[plt.Figure, Union[npt.NDArray[plt.Axes], Sequence[plt.Axes], plt.Axes]]


logger = law.logger.get_logger(__name__)


label_options = {
    "wip": "Work in progress",
    "pre": "Preliminary",
    "pw": "Private work (CMS data/simulation)",
    "pwip": "Private work in progress (CMS)",
    "sim": "Simulation",
    "simwip": "Simulation work in progress",
    "simpre": "Simulation preliminary",
    "simpw": "Private work (CMS simulation)",
    "datapw": "Private work (CMS data)",
    "od": "OpenData",
    "odwip": "OpenData work in progress",
    "odpw": "OpenData private work",
    "public": "",
}


def get_cms_label(ax: plt.Axes, llabel: str) -> dict:
    """
    Helper function to get the CMS label configuration.

    :param ax: The axis to plot the CMS label on.
    :param llabel: The left label of the CMS label.
    :return: A dictionary with the CMS label configuration.
    """
    llabel = label_options.get(llabel, llabel)
    cms_label_kwargs = {
        "ax": ax,
        "llabel": llabel,
        "fontsize": 22,
        "data": False,
    }
    if "CMS" in llabel:
        cms_label_kwargs["exp"] = ""

    return cms_label_kwargs


def round_dynamic(value: int | float) -> int | float:
    """
    Rounds a *value* at various scales to a subjective, sensible precision. Rounding rules:

        - 0 -> 0 (int)
        - (0, 1) -> round to 1 significant digit (float)
        - [1, 10) -> round to 1 significant digit (int)
        - [10, inf) -> round to 2 significant digits (int)

    :param value: The value to round.
    :return: The rounded value.
    """
    # determine significant digits
    digits = 1 if abs(value) < 10 else 2

    # split into value and magnitude
    v_str, _, mag = sn.round_value(value, method=digits)

    # recombine
    value = float(v_str) * 10**mag

    # return with proper type
    return int(value) if value >= 1 else value


def inject_label(
    label: str,
    inject: str | int | float,
    *,
    placeholder: str | None = None,
    before_parentheses: bool = False,
) -> str:
    """
    Injects a string *inject* into a *label* at a specific position, determined by different
    strategies in the following order:

        - If *placeholder* is defined, *label* should contain a substring ``"__PLACEHOLDER__"``
        which is replaced.
        - Otherwise, if *before_parentheses* is set to True, the string is inserted before the last
        pair of parentheses.
        - Otherwise, the string is appended to the label.

    :param label: The label to inject the string *inject* into.
    :param inject: The string to inject.
    :param placeholder: The placeholder to replace in the label.
    :param before_parentheses: Whether to insert the string before the parentheses in the label.
    :return: The updated label.
    """
    # replace the placeholder
    if placeholder and f"__{placeholder}__" in label:
        return label.replace(f"__{placeholder}__", inject)

    # when the label contains trailing parentheses, insert the string before them
    if before_parentheses and label.endswith(")"):
        in_parentheses = 1
        for i in range(len(label) - 2, -1, -1):
            c = label[i]
            if c == ")":
                in_parentheses += 1
            elif c == "(":
                in_parentheses -= 1
            if not in_parentheses:
                return f"{label[:i]} {inject} {label[i:]}"

    # otherwise, just append
    return f"{label} {inject}"


def apply_settings(
    instances: Iterable[od.AuxDataMixin],
    settings: dict[str, Any] | None,
    parent_check: Callable[[od.AuxDataMixin, str], bool] | None = None,
) -> None:
    """
    applies settings from `settings` dictionary to a list of order objects `containers`

    :param instances: List of order instances to apply settings to.
    :param settings: Dictionary of settings to apply on the instances. Each key should correspond
        to the name of an instance and each value should be a dictionary with attributes that will
        be set on the instance either as a attribute or as an auxiliary.
    :param parent_check: Function that checks if an instance has a parent with a given name.
    """
    if not settings:
        return

    for inst in instances:
        for name, inst_settings in (settings or {}).items():
            if inst != name and not (callable(parent_check) and parent_check(inst, name)):
                continue
            for key, value in inst_settings.items():
                # try attribute first, otherwise auxiliary entry
                try:
                    setattr(inst, key, value)
                except (AttributeError, ValueError):
                    inst.set_aux(key, value)


def apply_process_settings(
    hists: dict,
    process_settings: dict | None = None,
) -> dict:
    """
    applies settings from `process_settings` dictionary to the `process_insts`;
    the `scale` setting is directly applied to the histograms
    """
    # apply all settings on process insts
    apply_settings(
        hists.keys(),
        process_settings,
        parent_check=(lambda proc, parent_name: proc.has_parent_process(parent_name)),
    )

    # helper to compute the stack integral
    stack_integral = None

    def get_stack_integral() -> float:
        nonlocal stack_integral
        if stack_integral is None:
            stack_integral = sum(
                proc_h.sum().value
                for proc, proc_h in hists.items()
                if not hasattr(proc, "unstack") and not proc.is_data
            )
        return stack_integral

    for proc_inst, h in hists.items():
        # apply "scale" setting directly to the hists
        scale_factor = getattr(proc_inst, "scale", None) or proc_inst.x("scale", None)
        if scale_factor == "stack":
            # compute the scale factor and round
            scale_factor = round_dynamic(get_stack_integral() / h.sum().value)
        if try_float(scale_factor):
            scale_factor = float(scale_factor)
            hists[proc_inst] = h * scale_factor
            proc_inst.label = inject_label(
                proc_inst.label,
                rf"$\times${scale_factor:.2g}",
                placeholder="SCALE",
                before_parentheses=True,
            )

        # remove remaining placeholders
        proc_inst.label = re.sub("__[A-Z0-9]+__", "", proc_inst.label)

    return hists


def apply_variable_settings(
    hists: dict,
    variable_insts: list[od.Variable],
    variable_settings: dict | None = None,
) -> dict:
    """
    applies settings from *variable_settings* dictionary to the *variable_insts*;
    the *rebin*, *overflow*, *underflow*, and *slice* settings are directly applied to the histograms
    """
    # apply all settings on variable insts
    apply_settings(variable_insts, variable_settings)

    # apply certain  setting directly to histograms
    for var_inst in variable_insts:
        # rebinning
        rebin_factor = getattr(var_inst, "rebin", None) or var_inst.x("rebin", None)
        if try_int(rebin_factor):
            for proc_inst, h in list(hists.items()):
                rebin_factor = int(rebin_factor)
                h = h[{var_inst.name: hist.rebin(rebin_factor)}]
                hists[proc_inst] = h
        # overflow and underflow bins
        overflow = getattr(var_inst, "overflow", False) or var_inst.x("overflow", False)
        underflow = getattr(var_inst, "underflow", False) or var_inst.x("underflow", False)

        if overflow or underflow:
            for proc_inst, h in list(hists.items()):
                h = use_flow_bins(h, var_inst.name, underflow=underflow, overflow=overflow)
                hists[proc_inst] = h

        # slicing
        slices = getattr(var_inst, "slice", None) or var_inst.x("slice", None)
        if (
            slices and isinstance(slices, Iterable) and len(slices) >= 2 and
            try_complex(slices[0]) and try_complex(slices[1])
        ):
            slice_0 = int(slices[0]) if try_int(slices[0]) else complex(slices[0])
            slice_1 = int(slices[1]) if try_int(slices[1]) else complex(slices[1])
            for proc_inst, h in list(hists.items()):
                h = h[{var_inst.name: slice(slice_0, slice_1)}]
                hists[proc_inst] = h

    return hists


def use_flow_bins(
    h_in: hist.Hist,
    axis_name: str | int,
    underflow: bool = True,
    overflow: bool = True,
) -> hist.Hist:
    """
    Adds content of the flow bins of axis *axis_name* of histogram *h_in* to the first/last bin.

    :param h_in: Input histogram
    :param axis_name: Name or index of the axis of interest.
    :param underflow: Whether to add the content of the underflow bin to the first bin of axis *axis_name.
    :param overflow: Whether to add the content of the overflow bin to the last bin of axis *axis_name*.
    :return: Copy of the histogram with underflow and/or overflow content added to the first/last
        bin of the histogram.
    """
    # work on a copy of the histogram
    h_out = h_in.copy()

    # nothing to do if neither flag is set
    if not overflow and not underflow:
        print(f"{use_flow_bins.__name__} has nothing to do since overflow and underflow are set to False")
        return h_out

    # determine the index of the axis of interest and check if it has flow bins activated
    axis_idx = axis_name if isinstance(axis_name, int) else h_in.axes.name.index(axis_name)
    h_view = h_out.view(flow=True)
    if h_out.view().shape[axis_idx] + 2 != h_view.shape[axis_idx]:
        raise Exception(f"We expect axis {axis_name} to have assigned an underflow and overflow bin")

    # function to get slice of index *idx* from axis *axis_idx*
    slice_func = lambda idx: tuple(
        [slice(None)] * axis_idx + [idx] + [slice(None)] * (len(h_out.shape) - axis_idx - 1),
    )

    if overflow:
        # replace last bin with last bin + overflow
        h_view.value[slice_func(-2)] = h_view.value[slice_func(-2)] + h_view.value[slice_func(-1)]
        h_view.value[slice_func(-1)] = 0
        h_view.variance[slice_func(-2)] = h_view.variance[slice_func(-2)] + h_view.variance[slice_func(-1)]
        h_view.variance[slice_func(-1)] = 0

    if underflow:
        # replace last bin with last bin + overflow
        h_view.value[slice_func(1)] = h_view.value[slice_func(0)] + h_view.value[slice_func(1)]
        h_view.value[slice_func(0)] = 0
        h_view.variance[slice_func(1)] = h_view.variance[slice_func(0)] + h_view.variance[slice_func(1)]
        h_view.variance[slice_func(0)] = 0

    return h_out


def apply_density_to_hists(hists: dict, density: bool | None = False) -> dict:
    """
    Scales number of histogram entries to bin widths.
    """
    if not density:
        return hists

    for key, hist in hists.items():
        # bin area safe for multi-dimensional histograms
        area = functools.reduce(operator.mul, hist.axes.widths)

        # scale hist by bin area
        hists[key] = hist / area

    return hists


def remove_residual_axis(hists: dict, ax_name: str, max_bins: int = 1) -> dict:
    """
    removes axis named 'ax_name' if existing and there is only a single bin in the axis;
    raises Exception otherwise
    """
    for key, hist in list(hists.items()):
        if ax_name in hist.axes.name:
            n_bins = len(hist.axes[ax_name])
            if n_bins > max_bins:
                raise Exception(
                    f"{ax_name} axis of histogram for key {key} has {n_bins} values whereas at most "
                    f"{max_bins} is expected",
                )
            hists[key] = hist[{ax_name: sum}]

    return hists


def prepare_style_config(
    config_inst: od.Config,
    category_inst: od.Category,
    variable_inst: od.Variable,
    density: bool | None = False,
    shape_norm: bool | None = False,
    yscale: str | None = "",
) -> dict:
    """
    small helper function that sets up a default style config based on the instances
    of the config, category and variable
    """
    if not yscale:
        yscale = "log" if variable_inst.log_y else "linear"

    xlim = (
        variable_inst.x("x_min", variable_inst.x_min),
        variable_inst.x("x_max", variable_inst.x_max),
    )

    style_config = {
        "ax_cfg": {
            "xlim": xlim,
            "ylabel": variable_inst.get_full_y_title(bin_width="" if density else None),
            "xlabel": variable_inst.get_full_x_title(),
            "yscale": yscale,
            "xscale": "log" if variable_inst.log_x else "linear",
        },
        "rax_cfg": {
            "ylabel": "Data / MC",
            "xlabel": variable_inst.get_full_x_title(),
        },
        "legend_cfg": {},
        "annotate_cfg": {"text": category_inst.label},
        "cms_label_cfg": {
            "lumi": round(0.001 * config_inst.x.luminosity.get("nominal"), 2),  # /pb -> /fb
            "com": config_inst.campaign.ecm,
        },
    }

    # disable minor ticks based on variable_inst
    if variable_inst.discrete_x:
        # TODO: options for very large ranges, or non-uniform discrete x
        tx = np.array(variable_inst.bin_edges)
        tx = (tx[1:] + tx[:-1]) / 2
        style_config["ax_cfg"]["xticks"] = tx[::len(tx) // 10]
        style_config["ax_cfg"]["minorxticks"] = []

        # add custom bin labels if specified and same amount of x ticks
        if x_labels := variable_inst.x_labels:
            if len(x_labels) == len(tx):
                style_config["ax_cfg"]["xticklabels"] = x_labels[::len(tx) // 10]

    if variable_inst.discrete_y:
        style_config["ax_cfg"]["minoryticks"] = []

    return style_config


def prepare_plot_config(
    hists: OrderedDict,
    shape_norm: bool | None = False,
    hide_errors: bool | None = None,
) -> OrderedDict:
    """
    Prepares a plot config with one entry to create plots containing a stack of
    backgrounds with uncertainty bands, unstacked processes as lines and
    data entrys with errorbars.
    """

    # separate histograms into stack, lines and data hists
    mc_hists, mc_colors, mc_edgecolors, mc_labels = [], [], [], []
    line_hists, line_colors, line_labels, line_hide_errors = [], [], [], []
    data_hists, data_hide_errors = [], []

    for process_inst, h in hists.items():
        # if given, per-process setting overrides task parameter
        proc_hide_errors = hide_errors
        if getattr(process_inst, "hide_errors", None) is not None:
            proc_hide_errors = process_inst.hide_errors
        if process_inst.is_data:
            data_hists.append(h)
            data_hide_errors.append(proc_hide_errors)
        elif process_inst.is_mc:
            if getattr(process_inst, "unstack", False):
                line_hists.append(h)
                line_colors.append(process_inst.color1)
                line_labels.append(process_inst.label)
                line_hide_errors.append(proc_hide_errors)
            else:
                mc_hists.append(h)
                mc_colors.append(process_inst.color1)
                mc_edgecolors.append(process_inst.color2)
                mc_labels.append(process_inst.label)

    h_data, h_mc, h_mc_stack = None, None, None
    if data_hists:
        h_data = sum(data_hists[1:], data_hists[0].copy())
    if mc_hists:
        h_mc = sum(mc_hists[1:], mc_hists[0].copy())
        # reverse hists when building MC stack so that the
        # first process is on top
        h_mc_stack = hist.Stack(*mc_hists[::-1])

    # setup plotting configs
    plot_config = OrderedDict()

    # draw stack
    if h_mc_stack is not None:
        mc_norm = sum(h_mc.values()) if shape_norm else 1
        plot_config["mc_stack"] = {
            "method": "draw_stack",
            "hist": h_mc_stack,
            "kwargs": {
                "norm": mc_norm,
                "label": mc_labels[::-1],
                "color": mc_colors[::-1],
                "edgecolor": mc_edgecolors[::-1],
                "linewidth": [(0 if c is None else 1) for c in mc_colors[::-1]],
            },
        }

    # draw lines
    for i, h in enumerate(line_hists):
        line_norm = sum(h.values()) if shape_norm else 1
        plot_config[f"line_{i}"] = plot_cfg = {
            "method": "draw_hist",
            "hist": h,
            "kwargs": {
                "norm": line_norm,
                "label": line_labels[i],
                "color": line_colors[i],
            },
            # "ratio_kwargs": {
            #     "norm": h.values(),
            #     "color": line_colors[i],
            # },
        }

        # suppress error bars by overriding `yerr`
        if line_hide_errors[i]:
            for key in ("kwargs", "ratio_kwargs"):
                if key in plot_cfg:
                    plot_cfg[key]["yerr"] = False

    # draw stack error
    if h_mc_stack is not None and not hide_errors:
        mc_norm = sum(h_mc.values()) if shape_norm else 1
        plot_config["mc_uncert"] = {
            "method": "draw_error_bands",
            "hist": h_mc,
            "kwargs": {"norm": mc_norm, "label": "MC stat. unc."},
            "ratio_kwargs": {"norm": h_mc.values()},
        }

    # draw data
    if data_hists:
        data_norm = sum(h_data.values()) if shape_norm else 1
        plot_config["data"] = plot_cfg = {
            "method": "draw_errorbars",
            "hist": h_data,
            "kwargs": {
                "norm": data_norm,
                "label": "Data",
            },
        }

        if h_mc is not None:
            plot_config["data"]["ratio_kwargs"] = {
                "norm": h_mc.values() * data_norm / mc_norm,
            }

        # suppress error bars by overriding `yerr`
        if any(data_hide_errors):
            for key in ("kwargs", "ratio_kwargs"):
                if key in plot_cfg:
                    plot_cfg[key]["yerr"] = False

    return plot_config


def get_position(minimum: float, maximum: float, factor: float = 1.4, logscale: bool = False) -> float:
    """ get a relative position between a min and max value based on the scale """
    if logscale:
        value = 10 ** ((math.log10(maximum) - math.log10(minimum)) * factor + math.log10(minimum))
    else:
        value = (maximum - minimum) * factor + minimum

    return value


def reduce_with(spec: str | float | callable, values: list[float]) -> float:
    """
    Reduce an array of *values* to a single value using the function indicated
    by *spec*. Intended as a helper for resolving range specifications supplied
    as strings.

    Supported specifiers are:

      * 'min': minimum value
      * 'max': maximum value
      * 'maxabs': the absolute value of the maximum or minimum, whichever is larger
      * 'minabs': the absolute value of the maximum or minimum, whichever is smaller

    A hyphen (``-``) can be prefixed to any specifier to return its negative.

    Callables can be passed as *spec* and should take a single array-valued argument
    and return a single value. Floats passes as specifiers will be returned directly.
    """

    # if callable, apply to array
    if callable(spec):
        return spec(values)

    # if not a string, assume fixed literal and return
    if not isinstance(spec, str):
        return spec

    # determine sign
    factor = 1.
    if spec.startswith("-"):
        spec = spec[1:]
        factor = -1.

    if spec not in reduce_with.funcs:
        available = ", ".join(reduce_with.funcs)
        raise ValueError(
            f"unknown reduction function '{spec}'. "
            f"Available: {available}",
        )

    func = reduce_with.funcs[spec]
    values = np.asarray(values)

    return factor * func(values)


reduce_with.funcs = {
    "min": lambda v: np.nanmin(v),
    "max": lambda v: np.nanmax(v),
    "maxabs": lambda v: max(abs(np.nanmax(v)), abs(np.nanmin(v))),
    "minabs": lambda v: min(abs(np.nanmax(v)), abs(np.nanmin(v))),
}


def broadcast_1d_to_nd(x: np.array, final_shape: list, axis: int = 1) -> np.array:
    """
    Helper function to broadcast a 1d array *x* to an nd array with shape *final_shape*.
    The length of *x* should be the same as *final_shape[axis]*.
    """
    if len(x.shape) != 1:
        raise Exception("Only 1d arrays allowed")
    if final_shape[axis] != x.shape[0]:
        raise Exception(f"Initial shape should match with final shape in requested axis {axis}")
    initial_shape = [1] * len(final_shape)
    initial_shape[axis] = x.shape[0]
    x = np.reshape(x, initial_shape)
    x = np.broadcast_to(x, final_shape)
    return x


def broadcast_nminus1d_to_nd(x: np.array, final_shape: list, axis: int = 1) -> np.array:
    """
    Helper function to broadcast a (n-1)d array *x* to an nd array with shape *final_shape*.
    *final_shape* should be the same as *x.shape* except that the axis *axis* is missing.
    """
    if len(final_shape) - len(x.shape) != 1:
        raise Exception("Only (n-1)d arrays allowed")

    # shape comparison between x and final_shape
    _init_shape = list(final_shape)
    _init_shape.pop(axis)
    if _init_shape != list(x.shape):
        raise Exception(
            f"input shape ({x.shape}) should agree with final_shape {final_shape} "
            f"after inserting new axis at {axis}",
        )

    initial_shape = list(x.shape)
    initial_shape.insert(axis, 1)

    x = np.reshape(x, initial_shape)
    x = np.broadcast_to(x, final_shape)

    return x


def get_profile_width(h_in: hist.Hist, axis: int = 1) -> tuple[np.array, np.array]:
    """
    Function that takes a histogram *h_in* and returns the mean and width
    when profiling over the axis *axis*.
    """
    values = h_in.values()
    centers = h_in.axes[axis].centers
    centers = broadcast_1d_to_nd(centers, values.shape, axis)

    num = np.sum(values * centers, axis=axis)
    den = np.sum(values, axis=axis)

    print(num.shape)

    with np.errstate(invalid="ignore"):
        mean = num / den
        _mean = broadcast_nminus1d_to_nd(mean, values.shape, axis)
        width = np.sum(values * (centers - _mean) ** 2, axis=axis) / den

    return mean, width


def get_profile_variations(h_in: hist.Hist, axis: int = 1) -> dict[str, hist.Hist]:
    """
    Returns a profile histogram plus the up and down variations of the profile
    from a normal histogram with N-1 axes.
    The axis given is profiled over and removed from the final histograms.
    """
    # start with profile such that we only have to replace the mean
    # NOTE: how do the variances change for the up/down variations?
    h_profile = h_in.profile(axis)

    mean, variance = get_profile_width(h_in, axis=axis)

    h_nom = h_profile.copy()
    h_up = h_profile.copy()
    h_down = h_profile.copy()

    # we modify the view of h_profile -> do not use h_profile anymore!
    h_view = h_profile.view()

    h_view.value = mean
    h_nom[...] = h_view
    h_view.value = mean + np.sqrt(variance)
    h_up[...] = h_view
    h_view.value = mean - np.sqrt(variance)
    h_down[...] = h_view

    return {"nominal": h_nom, "up": h_up, "down": h_down}


def fix_cbar_minor_ticks(cbar: mpl.colorbar.Colorbar):
    if isinstance(cbar.norm, mpl.colors.SymLogNorm):
        _scale = cbar.ax.yaxis._scale
        _scale.subs = [2, 3, 4, 5, 6, 7, 8, 9]
        cbar.ax.yaxis.set_minor_locator(
            mticker.SymmetricalLogLocator(_scale.get_transform(), subs=_scale.subs),
        )
        cbar.ax.yaxis.set_minor_formatter(
            mticker.LogFormatterSciNotation(_scale.base),
        )


def prepare_plot_config_2d(
    hists: OrderedDict | dict,
    shape_norm: bool = False,
    zscale: str = "linear",
    # z axis range
    zlim: tuple | None = None,
    # how to handle bins with values outside the z range
    extremes: str = "",
    # colors to use for marking out-of-bounds values
    extreme_colors: tuple[str] | None = None,
    colormap: str = "",
):
    # add all processes into 1 histogram
    h_sum = sum(list(hists.values())[1:], list(hists.values())[0].copy())
    if shape_norm:
        h_sum = h_sum / h_sum.sum().value

    # mask bins without any entries (variance == 0)
    h_view = h_sum.view()
    h_view.value[h_view.variance == 0] = np.nan

    # check h_sum value range
    vmin, vmax = np.nanmin(h_sum.values()), np.nanmax(h_sum.values())
    vmin, vmax = [0 if np.isnan(x) else x for x in [vmin, vmax]]

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

    # update h_sum values from view
    h_sum[...] = h_view

    # choose appropriate colorbar normalization
    # based on scale type and h_sum content

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
    cmap = plt.get_cmap(colormap or "Blues")

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

    return {
        "hist": h_sum,
        "kwargs": {
            "norm": cbar_norm,
            "cmap": cmap,
            "cbar": True,
            "cbarextend": True,
            # "labels": True,  # this enables displaying numerical values for each bin, but needs some optimization
        },
        "cbar_kwargs": {
            "extend": extend,
        },
    }


def prepare_style_config_2d(
    config_inst: od.Config,
    category_inst: od.Category,
    process_insts: list[od.Process],
    variable_insts: list[od.Variable],
    cms_label: str = "",
) -> dict:
    # setup style config
    # TODO: some kind of z-label is still missing

    style_config = {
        "ax_cfg": {
            "xticks": {
                "minor": {"ticks": []} if variable_insts[0].discrete_x else {},
                "major": {},
            },
            "yticks": {
                "minor": {"ticks": []} if variable_insts[1].discrete_x else {},
                "major": {},
            },
            "xlim": (variable_insts[0].x_min, variable_insts[0].x_max),
            "ylim": (variable_insts[1].x_min, variable_insts[1].x_max),
            "xlabel": variable_insts[0].get_full_x_title(),
            "ylabel": variable_insts[1].get_full_x_title(),
            "xscale": "log" if variable_insts[0].log_x else "linear",
            "yscale": "log" if variable_insts[1].log_x else "linear",
        },
        "legend_cfg": {
            "title": "Process" if len(process_insts) == 1 else "Processes",
            "handles": [mpl.lines.Line2D([0], [0], lw=0) for _ in process_insts],  # dummy handle
            "labels": [proc_inst.label for proc_inst in process_insts],
            "ncol": 1,
            "loc": "upper right",
        },
        "annotate_cfg": {
            "text": category_inst.label,
            "xy": (0.05, 0.95),
            "xycoords": "axes fraction",
            "color": "black",
            "fontsize": 22,
            "horizontalalignment": "left",
            "verticalalignment": "top",
        },
    }

    # cms label
    if cms_label != "skip":
        style_config["cms_label_cfg"] = {
            # "ax": ax,  # need to add ax later !!
            "lumi": 0.001 * config_inst.x.luminosity.get("nominal"),  # pb -> fb
            "llabel": label_options.get(cms_label, cms_label),
            "fontsize": 22,
            "data": False,
        }

    return style_config


def blind_sensitive_bins(
    hists: dict[od.Process, hist.Hist],
    config_inst: od.Config,
    threshold: float,
) -> dict[od.Process, hist.Hist]:
    """
    Function that takes a histogram *h_in* and blinds the values of the profile
    over the axis *axis* that are below a certain threshold *threshold*.
    The function needs an entry in the process_groups key of the config auxiliary
    that is called "signals" to know, where the signal processes are defined (regex allowed).
    The histograms are not changed inplace, but copies of the modified histograms are returned.
    """
    # build the logic to seperate signal processes
    signal_procs: set[od.Process] = {
        config_inst.get_process(proc)
        for proc in config_inst.x.process_groups.get("signals", [])
    }
    check_if_signal = lambda proc: any(signal == proc or signal.has_process(proc) for signal in signal_procs)

    # separate histograms into signals, backgrounds and data hists and calculate sums
    signals = {proc: hist for proc, hist in hists.items() if proc.is_mc and check_if_signal(proc)}
    data = {proc: hist.copy() for proc, hist in hists.items() if proc.is_data}
    backgrounds = {proc: hist for proc, hist in hists.items() if proc.is_mc and proc not in signals}

    # Return hists unchanged in case any of the three dicts is empty.
    if not signals or not backgrounds or not data:
        logger.info(
            "one of the following categories: signals, backgrounds or data was not found in the given processes, "
            "returning unchanged histograms",
        )
        return hists

    signals_sum = sum(signals.values())
    backgrounds_sum = sum(backgrounds.values())

    # calculate sensitivity by S / sqrt(S + B)
    sensitivity = signals_sum.values() / np.sqrt(signals_sum.values() + backgrounds_sum.values())
    mask = sensitivity >= threshold

    # adjust the mask to blind the bins inbetween blinded ones
    if sum(mask) > 1:
        first_ind, last_ind = np.where(mask)[0][::sum(mask) - 1]
        mask[first_ind:last_ind] = True

    # set data points in masked region to zero
    for proc, hist in data.items():
        hist.values()[mask] = 0
        hist.variances()[mask] = 0

    # merge all histograms
    hists = law.util.merge_dicts(signals, backgrounds, data)

    return hists
