# coding: utf-8

"""
Some utils for plot functions.
"""

from __future__ import annotations

__all__ = []

import re
import operator
import functools
from collections import OrderedDict

import law
import order as od
import scinum as sn

from columnflow.util import maybe_import, try_int, try_complex
from columnflow.types import Iterable, Any, Callable, Sequence

math = maybe_import("math")
hist = maybe_import("hist")
np = maybe_import("numpy")
plt = maybe_import("matplotlib.pyplot")
mplhep = maybe_import("mplhep")


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


def hists_merge_cutflow_steps(
    hists: dict,
) -> dict:
    """
    Make 'step' axis uniform among a set of histograms. Takes a dict of 1D histogram
    objects with a single 'step' axis of type *StrCategory*, computes the full list of possible
    'step' values across all histograms, and returns a dict of histograms whose 'step' axis
    has a corresponding, uniform structure. The values and variances inserted for missing 'step'
    are taken from the previous existing step.
    """
    # return immediately if fewer than two hists to merge
    if len(hists) < 2:
        return hists

    # get histogram instances
    hist_insts = list(hists.values())

    # validate inputs
    if any(h.ndim != 1 for h in hist_insts):
        raise ValueError(
            "cannot merge cutflow steps: histograms must be one-dimensional",
        )

    # ensure step structure is uniform by taking a linear
    # combination with only one nonzero coefficient
    hist_insts_merged = []
    for coeffs in np.eye(len(hist_insts)):
        hist_row = sum(
            h * coeff
            for h, coeff in zip(hist_insts, coeffs)
        )
        hist_insts_merged.append(hist_row)

    # fill missing entries from preceding steps
    merged_steps = list(hist_insts_merged[0].axes[0])
    for hist_inst, hist_inst_merged in zip(hist_insts, hist_insts_merged):
        last_step = merged_steps[0]
        for merged_step in merged_steps[1:]:
            if merged_step not in hist_inst.axes[0]:
                hist_inst_merged[merged_step] = hist_inst_merged[last_step]
            else:
                last_step = merged_step

    # put merged hists into dict
    hists = {
        k: h
        for k, h in zip(hists, hist_insts_merged)
    }

    # return
    return hists


def apply_process_settings(
    hists: dict,
    process_settings: dict | None = None,
) -> dict:
    """
    applies settings from `process_settings` dictionary to the `process_insts`
    """
    # apply all settings on process insts
    apply_settings(
        hists.keys(),
        process_settings,
        parent_check=(lambda proc, parent_name: proc.has_parent_process(parent_name)),
    )

    return hists


def apply_process_scaling(hists: dict) -> dict:
    # helper to compute the stack integral
    stack_integral = None

    def get_stack_integral() -> float:
        nonlocal stack_integral
        if stack_integral is None:
            stack_integral = sum(
                _remove_residual_axis(proc_h, "shift", select_value=0).sum().value
                for proc, proc_h in hists.items()
                if proc.is_mc and not getattr(proc, "unstack", False)
            )
        return stack_integral

    for proc_inst, h in hists.items():
        # apply "scale" setting directly to the hists
        scale_factor = getattr(proc_inst, "scale", None) or proc_inst.x("scale", None)
        if scale_factor == "stack":
            # compute the scale factor and round
            h_no_shift = _remove_residual_axis(h, "shift", select_value=0)
            scale_factor = round_dynamic(get_stack_integral() / h_no_shift.sum().value)
        if try_int(scale_factor):
            scale_factor = int(scale_factor)
            hists[proc_inst] = h * scale_factor
            scale_factor_str = (
                str(scale_factor)
                if scale_factor < 1e5
                else re.sub(r"e(\+?)(-?)(0*)", r"e\2", f"{scale_factor:.1e}")
            )
            proc_inst.label = apply_label_placeholders(
                proc_inst.label,
                apply="SCALE",
                scale=scale_factor_str,
            )

        # remove remaining scale placeholders
        proc_inst.label = remove_label_placeholders(proc_inst.label, drop="SCALE")

    return hists


def remove_label_placeholders(
    label: str,
    keep: str | Sequence[str] | None = None,
    drop: str | Sequence[str] | None = None,
) -> str:
    # when placeholders should be kept, determine all existing ones and identify remaining to drop
    if keep:
        keep = law.util.make_list(keep)
        placeholders = re.findall("__([^_]+)__", label)
        drop = list(set(placeholders) - set(keep))

    # drop specific placeholders or all
    if drop:
        drop = law.util.make_list(drop)
        sel = f"({'|'.join(d.upper() for d in drop)})"
    else:
        sel = "[A-Z0-9]+"

    return re.sub(f"__{sel}__", "", label)


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
        overflow = getattr(var_inst, "overflow", None)
        if overflow is None:
            overflow = var_inst.x("overflow", False)
        underflow = getattr(var_inst, "underflow", None)
        if underflow is None:
            underflow = var_inst.x("underflow", False)

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


def apply_density(hists: dict) -> dict:
    """
    Scales number of histogram entries to bin widths.
    """
    for key, hist in hists.items():
        # bin area safe for multi-dimensional histograms
        area = functools.reduce(operator.mul, hist.axes.widths)

        # scale hist by bin area
        hists[key] = hist / area

    return hists


def _remove_residual_axis(
    h: hist.Hist,
    ax_name: str,
    max_bins: int = 1,
    select_value: Any = None,
) -> hist.Hist:
    # force always returning a copy
    h = h.copy()

    # nothing to do if the axis is not present
    if ax_name not in h.axes.name:
        return h

    # when a selection is given, select the corresponding value
    if select_value is not None:
        h = h[{ax_name: [hist.loc(select_value)]}]

    # check remaining axis
    n_bins = len(h.axes[ax_name])
    if n_bins > max_bins:
        raise Exception(
            f"axis '{ax_name}' of histogram has {n_bins} bins whereas at most {max_bins} bins are "
            f"accepted for removal of residual axis",
        )

    # accumulate remaining axis
    return h[{ax_name: sum}]


def remove_residual_axis(
    hists: dict,
    ax_name: str,
    max_bins: int = 1,
    select_value: Any = None,
) -> dict:
    """
    Removes axis named 'ax_name' if existing and there is only a single bin in the axis;
    raises Exception otherwise
    """
    return {
        key: _remove_residual_axis(h, ax_name, max_bins=max_bins, select_value=select_value)
        for key, h in hists.items()
    }


def prepare_style_config(
    config_inst: od.Config,
    category_inst: od.Category,
    variable_inst: od.Variable,
    density: bool | None = False,
    shape_norm: bool | None = False,
    yscale: str | None = "",
    xtick_rotation: float | None = None,
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
    if xtick_rotation is None:
        xtick_rotation = 0

    # build the label from category and optional variable selection labels
    cat_label = join_labels(category_inst.label, variable_inst.x("selection_label", None))

    # unit format on axes (could be configurable)
    unit_format = "{title} [{unit}]"

    style_config = {
        "ax_cfg": {
            "xlim": xlim,
            # TODO: need to make bin width and unit configurable in future
            "ylabel": variable_inst.get_full_y_title(bin_width=False, unit=False, unit_format=unit_format),
            "xlabel": variable_inst.get_full_x_title(unit_format=unit_format),
            "yscale": yscale,
            "xscale": "log" if variable_inst.log_x else "linear",
            "xtick_rotation": xtick_rotation,
        },
        "rax_cfg": {
            "ylabel": "Data / MC",
            "xlabel": variable_inst.get_full_x_title(unit_format=unit_format),
            "xtick_rotation": xtick_rotation,
        },
        "legend_cfg": {},
        "annotate_cfg": {"text": cat_label or ""},
        "cms_label_cfg": {
            "lumi": round(0.001 * config_inst.x.luminosity.get("nominal"), 2),  # /pb -> /fb
            "com": config_inst.campaign.ecm,
        },
    }

    # disable minor ticks based on variable_inst
    axis_type = variable_inst.x("axis_type", "variable")
    if variable_inst.discrete_x or "int" in axis_type:
        # remove the "xscale" attribute since it messes up the bin edges
        style_config["ax_cfg"].pop("xscale")
        style_config["ax_cfg"]["minorxticks"] = []
    if variable_inst.discrete_y:
        style_config["ax_cfg"]["minoryticks"] = []

    return style_config


def prepare_stack_plot_config(
    hists: OrderedDict,
    shape_norm: bool | None = False,
    hide_stat_errors: bool | None = None,
    shift_insts: Sequence[od.Shift] | None = None,
    **kwargs,
) -> OrderedDict:
    """
    Prepares a plot config with one entry to create plots containing a stack of
    backgrounds with uncertainty bands, unstacked processes as lines and
    data entrys with errorbars.
    """
    check_nominal_shift(shift_insts)

    # separate histograms into stack, lines and data hists
    mc_hists, mc_colors, mc_edgecolors, mc_labels = [], [], [], []
    mc_syst_hists = []
    line_hists, line_colors, line_labels, line_hide_stat_errors = [], [], [], []
    data_hists, data_hide_stat_errors = [], []
    data_label = None

    for process_inst, h in hists.items():
        # if given, per-process setting overrides task parameter
        proc_hide_stat_errors = getattr(process_inst, "hide_stat_errors", hide_stat_errors)
        if process_inst.is_data:
            data_hists.append(_remove_residual_axis(h, "shift", select_value=0))
            data_hide_stat_errors.append(proc_hide_stat_errors)
            if data_label is None:
                data_label = process_inst.label
        elif getattr(process_inst, "unstack", False):
            line_hists.append(_remove_residual_axis(h, "shift", select_value=0))
            line_colors.append(process_inst.color1)
            line_labels.append(process_inst.label)
            line_hide_stat_errors.append(proc_hide_stat_errors)
        else:
            mc_hists.append(_remove_residual_axis(h, "shift", select_value=0))
            mc_colors.append(process_inst.color1)
            mc_edgecolors.append(process_inst.color2)
            mc_labels.append(process_inst.label)
            if "shift" in h.axes.name and h.axes["shift"].size > 1:
                mc_syst_hists.append(h)

    h_data, h_mc, h_mc_stack = None, None, None
    if data_hists:
        h_data = sum(data_hists[1:], data_hists[0].copy())
    if mc_hists:
        h_mc = sum(mc_hists[1:], mc_hists[0].copy())
        h_mc_stack = hist.Stack(*mc_hists)

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
                "label": mc_labels,
                "color": mc_colors,
                "edgecolor": mc_edgecolors,
                "linewidth": [(0 if c is None else 1) for c in mc_colors],
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
        if line_hide_stat_errors[i]:
            for key in ("kwargs", "ratio_kwargs"):
                if key in plot_cfg:
                    plot_cfg[key]["yerr"] = False

    # draw statistical error for stack
    if h_mc_stack is not None and not hide_stat_errors:
        mc_norm = sum(h_mc.values()) if shape_norm else 1
        plot_config["mc_stat_unc"] = {
            "method": "draw_stat_error_bands",
            "hist": h_mc,
            "kwargs": {"norm": mc_norm, "label": "MC stat. unc."},
            "ratio_kwargs": {"norm": h_mc.values()},
        }

    # draw systemetic error for stack
    if h_mc_stack is not None and mc_syst_hists:
        mc_norm = sum(h_mc.values()) if shape_norm else 1
        plot_config["mc_syst_unc"] = {
            "method": "draw_syst_error_bands",
            "hist": h_mc,
            "kwargs": {
                "syst_hists": mc_syst_hists,
                "shift_insts": shift_insts,
                "norm": mc_norm,
                "label": "MC syst. unc.",
            },
            "ratio_kwargs": {
                "syst_hists": mc_syst_hists,
                "shift_insts": shift_insts,
                "norm": h_mc.values(),
            },
        }

    # draw data
    if data_hists:
        data_norm = sum(h_data.values()) if shape_norm else 1
        plot_config["data"] = plot_cfg = {
            "method": "draw_errorbars",
            "hist": h_data,
            "kwargs": {
                "norm": data_norm,
                "label": data_label or "Data",
            },
        }

        if h_mc is not None:
            plot_config["data"]["ratio_kwargs"] = {
                "norm": h_mc.values() * data_norm / mc_norm,
            }

        # suppress error bars by overriding `yerr`
        if any(data_hide_stat_errors):
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


def join_labels(
    *labels: str | list[str | None] | None,
    inline_sep: str = ",",
    multiline_sep: str = "\n",
) -> str:
    if not labels:
        return ""

    # the first label decides whether the overall label is inline or multiline
    inline = isinstance(labels[0], str)

    # collect parts
    parts = sum(map(law.util.make_list, labels), [])

    # join and return
    return (inline_sep if inline else multiline_sep).join(filter(None, parts))


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
    signals = {proc: h for proc, h in hists.items() if proc.is_mc and check_if_signal(proc)}
    data = {proc: h.copy() for proc, h in hists.items() if proc.is_data}
    backgrounds = {proc: h for proc, h in hists.items() if proc.is_mc and proc not in signals}

    # Return hists unchanged in case any of the three dicts is empty.
    if not signals or not backgrounds or not data:
        logger.info(
            "one of the following categories: signals, backgrounds or data was not found in the given processes, "
            "returning unchanged histograms",
        )
        return hists

    # get nominal signal and background yield sums per bin
    signals_sum = sum(remove_residual_axis(signals, "shift", select_value=0).values())
    backgrounds_sum = sum(remove_residual_axis(backgrounds, "shift", select_value=0).values())

    # calculate sensitivity by S / sqrt(S + B)
    sensitivity = signals_sum.values() / np.sqrt(signals_sum.values() + backgrounds_sum.values())
    mask = sensitivity >= threshold

    # adjust the mask to blind the bins inbetween blinded ones
    if sum(mask) > 1:
        first_ind, last_ind = np.where(mask)[0][::sum(mask) - 1]
        mask[first_ind:last_ind] = True

    # set data points in masked region to zero
    for proc, h in data.items():
        h.values()[..., mask] = 0
        h.variances()[..., mask] = 0

    # merge all histograms
    hists = law.util.merge_dicts(signals, backgrounds, data)

    return hists


def apply_label_placeholders(
    label: str,
    apply: str | Sequence[str] | None = None,
    skip: str | Sequence[str] | None = None,
    **kwargs: Any,
) -> str:
    """
    Interprets placeholders in the format "__NAME__" in a label and returns an updated label.
    Currently supported placeholders are:

        - SHORT: removes everything (and including) the placeholder
        - BREAK: inserts a line break
        - SCALE: inserts a scale factor, passed as "scale" in kwargs; when "scale_format" is given
                 as well, the scale factor is formatted accordingly

    *apply* and *skip* can be used to de/select certain placeholders.
    """
    # handle apply/skip decisions
    if apply:
        _apply = set(p.upper() for p in law.util.make_list(apply))
        do_apply = lambda p: p in _apply
    elif skip:
        _skip = set(p.upper() for p in law.util.make_list(skip))
        do_apply = lambda p: p not in _skip
    else:
        do_apply = lambda p: True

    # shortening
    if do_apply("SHORT"):
        label = re.sub(r"__SHORT__.*", "", label)

    # lines breaks
    if do_apply("BREAK"):
        label = label.replace("__BREAK__", "\n")

    # scale factor
    if do_apply("SCALE") and "scale" in kwargs and "__SCALE__" in label:
        scale_str = kwargs.get("scale_format", "$\\times${}").format(kwargs["scale"])
        label = label.replace("__SCALE__", scale_str)

    return label


def check_nominal_shift(shifts: od.Shift | Sequence[od.Shift] | None) -> None:
    """
    Selects the so-called "nominal" shift from one or multiple :py:class:`order.Shift` instances
    *shifts* and checks if its id is in fact 0, which is an assumption made throughout the plotting
    helpers and functions. A ValueError is raised if the nominal shift is found but has a different
    id.
    """
    if shifts is None:
        return

    for shift in law.util.make_list(shifts):
        if shift.name == "nominal" and shift.id != 0:
            raise ValueError(f"the nominal shift should have id 0 but found {shift}")


def equal_distant_bin_width(histograms: OrderedDict, variable_inst: od.Variable) -> OrderedDict:
    """Takes an OrderedDict of histograms, and rebins to bins with equal width.
    The yield is not changed but copied to the rebinned histogram.

    :param histograms: OrderedDict of histograms
    :param variable_inst: od.Variable instance

    :return: OrderedDict of histograms with equal bin widths, but unchanged yield
    """

    hists = {}
    # take first histogram to extract old binning
    edges = list(histograms.values())[0].axes[variable_inst.name].edges
    # new bins take lower and upper edge of old bins, and are equally spaced
    bins = np.linspace(edges[0], edges[-1], len(edges))
    x_ticks = [bins, edges]

    for process, h in histograms.items():
        # create new histogram with equal bin widths
        label = h.label
        axes = (
            [h.axes[axis] for axis in h.axes.name if axis not in variable_inst.name] +
            [hist.axis.Variable(bins, name=variable_inst.name, label=label)]
        )
        new_hist = hist.Hist(*axes, storage=hist.storage.Weight())
        # copy yield to new histogram and save it
        np.copyto(dst=new_hist.view(), src=h.view(), casting="same_kind")
        hists[process] = new_hist
    return hists, x_ticks
