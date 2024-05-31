# coding: utf-8

"""
Some utils for plot functions.
"""

from __future__ import annotations

from columnflow.types import Iterable, Any

import operator
import functools
from collections import OrderedDict

import order as od

from columnflow.util import maybe_import, try_int, try_complex

math = maybe_import("math")
hist = maybe_import("hist")
np = maybe_import("numpy")
plt = maybe_import("matplotlib.pyplot")
mplhep = maybe_import("mplhep")

label_options = {
    "wip": "Work in progress",
    "pre": "Preliminary",
    "pw": "Private work",
    "sim": "Simulation",
    "simwip": "Simulation work in progress",
    "simpre": "Simulation preliminary",
    "simpw": "Simulation private work",
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
    cms_label_kwargs = {
        "ax": ax,
        "llabel": label_options.get(llabel, llabel),
        "fontsize": 22,
        "data": False,
    }

    return cms_label_kwargs


def apply_settings(containers: Iterable[od.AuxDataMixin], settings: dict[dict, Any] | None):
    """
    applies settings from `settings` dictionary to a list of order objects `containers`

    :param containers: list of order objects
    :param settings: dictionary of settings to apply on the *containers*. Each key should correspond
        to the name of a container and each value should be a dictionary. The inner dictionary contains
        keys and values that will be applied on the corresponding container either as an attribute
        or alternatively as an auxiliary.
    """
    if not settings:
        return

    for inst in containers:
        inst_settings = settings.get(inst.name, {})
        for setting_key, setting_value in inst_settings.items():
            try:
                setattr(inst, setting_key, setting_value)
            except AttributeError:
                inst.set_aux(setting_key, setting_value)


def apply_process_settings(
        hists: dict,
        process_settings: dict | None = None,
) -> dict:
    """
    applies settings from `process_settings` dictionary to the `process_insts`;
    the `scale` setting is directly applied to the histograms
    """
    # apply all settings on process insts
    process_insts = hists.keys()
    apply_settings(process_insts, process_settings)

    # apply "scale" setting directly to the hists
    for proc_inst, h in hists.items():
        scale_factor = getattr(proc_inst, "scale", None) or proc_inst.x("scale", None)
        if try_int(scale_factor):
            scale_factor = int(scale_factor)
            hists[proc_inst] = h * scale_factor
            # TODO: there might be a prettier way for the label
            proc_inst.label = f"{proc_inst.label} x{scale_factor}"

    return hists


def apply_variable_settings(
        hists: dict,
        variable_insts: list[od.Variable],
        variable_settings: dict | None = None,
) -> dict:
    """
    applies settings from `variable_settings` dictionary to the `variable_insts`;
    the `rebin` setting is directly applied to the histograms
    """
    # apply all settings on variable insts
    apply_settings(variable_insts, variable_settings)

    # apply rebinning setting directly to histograms
    for var_inst in variable_insts:
        rebin_factor = getattr(var_inst, "rebin", None) or var_inst.x("rebin", None)
        if try_int(rebin_factor):
            for proc_inst, h in list(hists.items()):
                rebin_factor = int(rebin_factor)
                h = h[{var_inst.name: hist.rebin(rebin_factor)}]
                hists[proc_inst] = h

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
            "lumi": config_inst.x.luminosity.get("nominal") / 1000,  # pb -> fb
            "com": config_inst.campaign.ecm,
        },
    }

    # disable minor ticks based on variable_inst
    if variable_inst.discrete_x:
        # TODO: find sth better than plain bin edges or possibly memory intense range(*xlim)
        style_config["ax_cfg"]["xticks"] = variable_inst.bin_edges
        style_config["ax_cfg"]["minorxticks"] = []
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

    # draw stack + error bands
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
        if not hide_errors:
            plot_config["mc_uncert"] = {
                "method": "draw_error_bands",
                "hist": h_mc,
                "kwargs": {"norm": mc_norm, "label": "MC stat. unc."},
                "ratio_kwargs": {"norm": h_mc.values()},
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
