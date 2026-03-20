# coding: utf-8

"""
Some utils for plot functions.
"""

from __future__ import annotations

__all__ = []

import re
import math
import operator
import functools
from collections import OrderedDict

import law
import order as od
import scinum as sn

from columnflow.util import maybe_import, try_int, try_complex, safe_div, UNSET
from columnflow.hist_util import copy_axis, sum_hists
from columnflow.types import TYPE_CHECKING, Iterable, Any, Callable, Sequence, Hashable

np = maybe_import("numpy")
if TYPE_CHECKING:
    hist = maybe_import("hist")
    plt = maybe_import("matplotlib.pyplot")


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


def get_attr_or_aux(proc: od.AuxDataMixin, attr: str, default: Any) -> Any:
    if (value := getattr(proc, attr, UNSET)) != UNSET:
        return value
    if proc.has_aux(attr):
        return proc.get_aux(attr)
    return default


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
    hists: dict[Hashable, hist.Hist],
    process_settings: dict | None = None,
) -> tuple[dict[Hashable, hist.Hist], dict[str, Any]]:
    """
    applies settings from `process_settings` dictionary to the `process_insts`
    """
    # store info gathered along application of process settings that can be inserted to the style config
    process_style_config = {}

    # apply all settings on process insts
    apply_settings(
        hists.keys(),
        process_settings,
        parent_check=(lambda proc, parent_name: proc.has_parent_process(parent_name)),
    )

    return hists, process_style_config


def apply_process_scaling(hists: dict[Hashable, hist.Hist]) -> dict[Hashable, hist.Hist]:
    # helper to compute the stack integral
    stack_integral = None

    def get_stack_integral() -> float:
        nonlocal stack_integral
        if stack_integral is None:
            stack_integral = sum(
                remove_residual_axis_single(proc_h, "shift", select_value="nominal").sum().value
                for proc, proc_h in hists.items()
                if proc.is_mc and not get_attr_or_aux(proc, "unstack", False)
            )
        return stack_integral

    for proc_inst, h in hists.items():
        # apply "scale" setting directly to the hists
        scale_factor = get_attr_or_aux(proc_inst, "scale", None)
        if scale_factor == "stack":
            # compute the scale factor and round
            h_no_shift = remove_residual_axis_single(h, "shift", select_value="nominal")
            scale_factor = round_dynamic(safe_div(get_stack_integral(), h_no_shift.sum().value)) or 1
        if try_int(scale_factor):
            scale_factor = int(scale_factor)
            hists[proc_inst] = h * scale_factor
            scale_factor_str = (
                str(scale_factor)
                if scale_factor < 1e5
                else re.sub(r"e(\+?)(-?)(0*)", r"e\2", f"{scale_factor:.1e}")
            )
            if scale_factor != 1:
                proc_inst.label = apply_label_placeholders(
                    proc_inst.label,
                    apply="SCALE",
                    scale=scale_factor_str,
                )

        # remove remaining scale placeholders
        proc_inst.label = remove_label_placeholders(proc_inst.label, drop="SCALE")

    return hists


def apply_variable_settings(
    hists: dict[Hashable, hist.Hist],
    variable_insts: list[od.Variable],
    variable_settings: dict | None = None,
) -> tuple[dict[Hashable, hist.Hist], dict[od.Variable, dict[str, Any]]]:
    """
    applies settings from *variable_settings* dictionary to the *variable_insts*;
    the *rebin*, *overflow*, *underflow*, and *slice* settings are directly applied to the histograms
    """
    import hist

    # store info gathered along application of variable settings that can be inserted to the style config
    variable_style_config = {}

    # apply all settings on variable insts
    apply_settings(variable_insts, variable_settings)

    # apply certain  setting directly to histograms
    for var_inst in variable_insts:
        variable_style_config[var_inst] = {}

        # rebinning
        rebin_factor = get_attr_or_aux(var_inst, "rebin", None)
        if try_int(rebin_factor):
            for proc_inst, h in list(hists.items()):
                rebin_factor = int(rebin_factor)
                h = h[{var_inst.name: hist.rebin(rebin_factor)}]
                hists[proc_inst] = h

        # overflow and underflow bins
        overflow = get_attr_or_aux(var_inst, "overflow", False)
        underflow = get_attr_or_aux(var_inst, "underflow", False)
        if overflow or underflow:
            for proc_inst, h in list(hists.items()):
                h = use_flow_bins(h, var_inst.name, underflow=underflow, overflow=overflow)
                hists[proc_inst] = h

        # slicing
        slices = get_attr_or_aux(var_inst, "slice", None)
        if (
            slices and isinstance(slices, Iterable) and len(slices) >= 2 and
            try_complex(slices[0]) and try_complex(slices[1])
        ):
            slice_0 = int(slices[0]) if try_int(slices[0]) else complex(slices[0])
            slice_1 = int(slices[1]) if try_int(slices[1]) else complex(slices[1])
            for proc_inst, h in list(hists.items()):
                h = h[{var_inst.name: slice(slice_0, slice_1)}]
                hists[proc_inst] = h

        # additional x axis transformations
        for trafo in law.util.make_list(get_attr_or_aux(var_inst, "x_transformations", None) or []):
            # forced representation into equal bins
            if trafo in {"equal_distance_with_edges", "equal_distance_with_indices"}:
                hists, orig_edges = rebin_equal_width(hists, var_inst.name)
                new_edges = list(hists.values())[0].axes[-1].edges
                # store edge values as well as ticks if needed
                ax_cfg = {"xlim": (new_edges[0], new_edges[-1])}
                if trafo == "equal_distance_with_edges":
                    # optionally round edges
                    rnd = get_attr_or_aux(var_inst, "x_edge_rounding", (lambda e: e))
                    edge_labels = [rnd(e) for e in orig_edges]
                    ax_cfg |= {"xmajorticks": new_edges, "xmajorticklabels": edge_labels, "xminorticks": []}
                variable_style_config[var_inst].setdefault("ax_cfg", {}).update(ax_cfg)
                variable_style_config[var_inst].setdefault("rax_cfg", {}).update(ax_cfg)
            else:
                raise ValueError(f"unknown x transformation '{trafo}'")

    return hists, variable_style_config


def remove_negative_contributions(hists: dict[Hashable, hist.Hist]) -> dict[Hashable, hist.Hist]:
    _hists = hists.copy()
    for proc_inst, h in hists.items():
        h = h.copy()
        h.view().value[h.view().value < 0] = 0
        _hists[proc_inst] = h
    return _hists


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


def apply_density(hists: dict, density: bool = True) -> dict:
    """
    Scales number of histogram entries to bin widths.
    """
    if not density:
        return hists

    for key, h in hists.items():
        # bin area safe for multi-dimensional histograms
        area = functools.reduce(operator.mul, h.axes.widths)

        # scale hist by bin area
        hists[key] = h / area

    return hists


def remove_residual_axis_single(
    h: hist.Hist,
    ax_name: str,
    max_bins: int = 1,
    select_value: Any = None,
) -> hist.Hist:
    import hist

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
        key: remove_residual_axis_single(h, ax_name, max_bins=max_bins, select_value=select_value)
        for key, h in hists.items()
    }

def prepare_style_config(
    config_inst: od.Config,
    category_inst: od.Category,
    variable_inst: od.Variable,
    density: bool | None = False,
    shape_norm: bool | None = False,
    yscale: str | None = "",
    **kwargs: Any,
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

    # build the label from category and optional variable selection labels
    cat_label = join_labels(category_inst.label, variable_inst.x("selection_label", None))

    # unit format on axes (could be configurable)
    unit_format = "{title} [{unit}]"

    if density:
        ylabel = variable_inst.get_full_y_title(
            bin_width=False,
            unit=variable_inst.unit or "unit",
            unit_format="{title} / {unit}",
        )
    else:
        ylabel = variable_inst.get_full_y_title(
            bin_width=False,
            unit=False,
            unit_format=unit_format,
        )

    style_config = {
        "ax_cfg": {
            "xlim": xlim,
            "ylabel": ylabel,
            "xlabel": variable_inst.get_full_x_title(unit_format=unit_format),
            "yscale": yscale,
            "xscale": "log" if variable_inst.log_x else "linear",
            "xrotation": variable_inst.x("x_label_rotation", None),
        },
        "rax_cfg": {
            "ylabel": "Data / MC",
            "xlabel": variable_inst.get_full_x_title(unit_format=unit_format),
            "xrotation": variable_inst.x("x_label_rotation", None),
        },
        "legend_cfg": {},
        "annotate_cfg": {"text": cat_label or ""},
        "cms_label_cfg": {
            "lumi": f"{0.001 * config_inst.x.luminosity.get('nominal'):.1f}",  # /pb -> /fb
            "com": config_inst.campaign.ecm,
        },
    }

    # disable minor ticks based on variable_inst
    axis_type = variable_inst.x("axis_type", "variable")
    if variable_inst.discrete_x or "int" in axis_type:
        # remove the "xscale" attribute since it messes up the bin edges
        style_config["ax_cfg"].pop("xscale")
        style_config["ax_cfg"]["xminorticks"] = []
    if variable_inst.discrete_y:
        style_config["ax_cfg"]["yminorticks"] = []

    return style_config


def prepare_stack_plot_config(
    hists: OrderedDict,
    shape_norm: bool | None = False,
    hide_stat_errors: bool | None = None,
    draw_total_unc: bool | None = None,
    shift_insts: Sequence[od.Shift] | None = None,
    density: bool = False,
    **kwargs,
) -> OrderedDict:
    """
    Prepares a plot config with one entry to create plots containing a stack of
    backgrounds with uncertainty bands, unstacked processes as lines and
    data entrys with errorbars.

    Feature added:
      - ensure QCD nominal is present in *all* up/down shift variations used for MC systematic bands,
        by creating a QCD "syst hist" with a full shift axis and filling every shift slice with the
        QCD nominal (value+variance), then appending it to mc_syst_hists (if needed).

    Enable debug prints with:
      kwargs["debug_qcd_nominal_in_systs"] = True
    Disable the feature with:
      kwargs["include_qcd_nominal_in_systs"] = False
    """
    import numpy as np

    include_qcd_nominal_in_systs = kwargs.get("include_qcd_nominal_in_systs", True)
    debug_qcd_nominal_in_systs = kwargs.get("debug_qcd_nominal_in_systs", False)

    def _fmt(a):
        a = np.asarray(a)
        return np.array2string(a, precision=6, separator=", ", threshold=30)

    def storage_from(donor: hist.Hist):
        for attr in ("_storage_type", "storage_type"):
            if hasattr(donor, attr):
                st = getattr(donor, attr)
                try:
                    st = st() if callable(st) else st
                except TypeError:
                    st = st
                try:
                    return st() if isinstance(st, type) else st
                except Exception:
                    return st
        return hist.storage.Weight()

    def book_like_with_shift_axis(donor: hist.Hist, shift_labels) -> hist.Hist:
        axes = []
        for ax in donor.axes:
            if getattr(ax, "name", None) == "shift":
                axes.append(hist.axis.StrCategory(list(shift_labels), name="shift", growth=True))
            else:
                axes.append(ax)
        return hist.Hist(*axes, storage=storage_from(donor)).reset()

    def ensure_qcd_nominal_in_syst_hists(
        mc_syst_hists: list[hist.Hist],
        qcd_hist: hist.Hist | None,
        default_shift: str,
        shift_insts: Sequence[od.Shift] | None,
    ) -> list[hist.Hist]:
        if qcd_hist is None:
            return mc_syst_hists
        if "shift" not in qcd_hist.axes.name:
            return mc_syst_hists

        # If QCD already has >1 shift entries and is already part of mc_syst_hists, do nothing.
        # (i.e. assume QCD variations are already handled upstream)
        try:
            if qcd_hist.axes["shift"].size > 1 and any(h is qcd_hist for h in mc_syst_hists):
                return mc_syst_hists
        except Exception:
            pass

        # Determine the "target" shift labels used in syst band computation
        if mc_syst_hists:
            target_shift_labels = [str(x) for x in list(mc_syst_hists[0].axes["shift"])]
        elif shift_insts is not None:
            target_shift_labels = [s.name for s in shift_insts]
        else:
            target_shift_labels = [str(x) for x in list(qcd_hist.axes["shift"])]

        if "nominal" not in target_shift_labels:
            target_shift_labels.append("nominal")

        # Select QCD nominal slice
        qcd_shift_labels = [str(x) for x in list(qcd_hist.axes["shift"])]
        qcd_nom_shift = "nominal" if "nominal" in qcd_shift_labels else default_shift

        qcd_nom_h = remove_residual_axis_single(qcd_hist, "shift", select_value=qcd_nom_shift)
        qcd_nom_val = qcd_nom_h.values()
        qcd_nom_var = qcd_nom_h.view().variance

        # Build a QCD syst hist with full shift axis and fill every shift slice with nominal
        qcd_syst = book_like_with_shift_axis(qcd_hist, target_shift_labels)
        arr = qcd_syst.view()

        for sh in target_shift_labels:
            i = qcd_syst.axes["shift"].index(sh)

            if debug_qcd_nominal_in_systs and (sh.endswith("_up") or sh.endswith("_down")):
                print(f"[QCD syst fill] {sh} BEFORE val={_fmt(arr[i].value)} var={_fmt(arr[i].variance)}")

            arr[i].value = qcd_nom_val
            arr[i].variance = qcd_nom_var

            if debug_qcd_nominal_in_systs and (sh.endswith("_up") or sh.endswith("_down")):
                print(f"[QCD syst fill] {sh} AFTER  val={_fmt(arr[i].value)} var={_fmt(arr[i].variance)}")

        qcd_syst[...] = arr

        # Append once so that systematic-summed MC variations include QCD nominal exactly once
        return [*mc_syst_hists, qcd_syst]

    # separate histograms into stack, lines and data hists
    mc_hists, mc_colors, mc_edgecolors, mc_labels = [], [], [], []
    mc_syst_hists = []
    line_hists, line_colors, line_labels, line_hide_stat_errors = [], [], [], []
    data_hists, data_hide_stat_errors = [], []
    data_label = None

    default_shift = shift_insts[0].name if len(shift_insts) == 1 else "nominal"

    # try to identify the QCD process instance
    qcd_hist_full = None
    try:
        from cmsdb.processes.qcd import qcd as qcd_proc
    except Exception:
        qcd_proc = None

    for process_inst, h in hists.items():
        # if given, per-process setting overrides task parameter
        proc_hide_stat_errors = get_attr_or_aux(process_inst, "hide_stat_errors", hide_stat_errors)
        if process_inst.is_data:
            data_hists.append(remove_residual_axis_single(h, "shift", select_value=default_shift))
            data_hide_stat_errors.append(proc_hide_stat_errors)
            if data_label is None:
                data_label = process_inst.label
        elif get_attr_or_aux(process_inst, "unstack", False):
            line_hists.append(remove_residual_axis_single(h, "shift", select_value=default_shift))
            line_colors.append(process_inst.color1)
            line_labels.append(process_inst.label)
            line_hide_stat_errors.append(proc_hide_stat_errors)
        else:
            mc_hists.append(remove_residual_axis_single(h, "shift", select_value=default_shift))
            mc_colors.append(process_inst.color1)
            mc_edgecolors.append(process_inst.color2)
            mc_labels.append(process_inst.label)

            # keep original (with shift axis) for syst bands
            if "shift" in h.axes.name and h.axes["shift"].size > 1:
                mc_syst_hists.append(h)

            # capture QCD hist (original, with shift axis) if available
            if qcd_proc is not None and process_inst == qcd_proc:
                qcd_hist_full = h

    # ensure QCD nominal is present for all systematic variations (for uncertainty bands)
    if include_qcd_nominal_in_systs:
        mc_syst_hists = ensure_qcd_nominal_in_syst_hists(
            mc_syst_hists=mc_syst_hists,
            qcd_hist=qcd_hist_full,
            default_shift=default_shift,
            shift_insts=shift_insts,
        )

    h_data, h_mc, h_mc_stack = None, None, None
    if data_hists:
        h_data = sum_hists(data_hists)
    if mc_hists:
        h_mc = sum_hists(mc_hists)
        h_mc_stack = hist.Stack(*mc_hists)

    # setup plotting configs
    plot_config = OrderedDict()

    shape_norm_func = kwargs.get("shape_norm_func", lambda h, shape_norm: sum(h.values()) if shape_norm else 1)

    # draw stack
    if h_mc_stack is not None:
        mc_norm = shape_norm_func(h_mc, shape_norm)
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
        line_norm = shape_norm_func(h, shape_norm)
        plot_config[f"line_{i}"] = plot_cfg = {
            "method": "draw_hist",
            "hist": h,
            "kwargs": {
                "norm": line_norm,
                "label": line_labels[i],
                "color": line_colors[i],
                "error_type": "variance",
            },
        }

        # suppress error bars by overriding `yerr`
        if line_hide_stat_errors[i]:
            for key in ("kwargs", "ratio_kwargs"):
                if key in plot_cfg:
                    plot_cfg[key]["yerr"] = False

    # --- draw uncertainties for stack ---
    if h_mc_stack is not None:
        mc_norm = sum(h_mc.values()) if shape_norm else 1
        if draw_total_unc:
            print("drawing total uncertainty band")
            if mc_syst_hists:
                plot_config["mc_total_unc"] = {
                    "method": "draw_total_error_bands",
                    "hist": h_mc,
                    "kwargs": {
                        "syst_hists": mc_syst_hists,
                        "shift_insts": shift_insts,
                        "norm": mc_norm,
                        "label": "MC total unc.",
                    },
                    "ratio_kwargs": {
                        "syst_hists": mc_syst_hists,
                        "shift_insts": shift_insts,
                        "norm": h_mc.values(),
                    },
                }
            else:
                if not hide_stat_errors:
                    plot_config["mc_stat_unc"] = {
                        "method": "draw_stat_error_bands",
                        "hist": h_mc,
                        "kwargs": {"norm": mc_norm, "label": "MC stat. unc."},
                        "ratio_kwargs": {"norm": h_mc.values()},
                    }
        else:
            if not hide_stat_errors:
                plot_config["mc_stat_unc"] = {
                    "method": "draw_stat_error_bands",
                    "hist": h_mc,
                    "kwargs": {"norm": mc_norm, "label": "MC stat. unc."},
                    "ratio_kwargs": {"norm": h_mc.values()},
                }

            if mc_syst_hists:
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
        data_norm = shape_norm_func(h_data, shape_norm)
        plot_config["data"] = plot_cfg = {
            "method": "draw_errorbars",
            "hist": h_data,
            "kwargs": {
                "norm": data_norm,
                "label": data_label or "Data",
                "error_type": "poisson_unweighted",
                "density": density,
            },
        }

        if h_mc is not None:
            plot_config["data"]["ratio_kwargs"] = {
                "norm": h_mc.values() * data_norm / mc_norm,
                "error_type": "poisson_unweighted",
                "density": density,
            }

        if any(data_hide_stat_errors):
            for key in ("kwargs", "ratio_kwargs"):
                if key in plot_cfg:
                    plot_cfg[key]["yerr"] = False

    return plot_config



def split_ax_kwargs(kwargs: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    """
    Split the given dictionary into two dictionaries based on the keys that are valid for matplotlib's ``ax.set()``
    function, and all others, potentially accepted by :py:func:`apply_ax_kwargs`.
    """
    set_kwargs, other_kwargs = {}, {}
    other_keys = {
        "xmajorticks", "xminorticks", "xmajorticklabels", "xminorticklabels", "xloc", "xrotation",
        "ymajorticks", "yminorticks", "yloc", "yrotation",
    }
    for key, value in kwargs.items():
        (other_kwargs if key in other_keys else set_kwargs)[key] = value
    return set_kwargs, other_kwargs


def apply_ax_kwargs(ax: plt.Axes, kwargs: dict[str, Any]) -> None:
    """
    Apply the given keyword arguments to the given axis, splitting them into those that are valid for ``ax.set()`` and
    those that are not, and applying them separately.
    """
    # split
    set_kwargs, other_kwargs = split_ax_kwargs(kwargs)

    # apply standard ones
    ax.set(**set_kwargs)

    # apply others
    if other_kwargs.get("xmajorticks") is not None:
        ax.set_xticks(other_kwargs.get("xmajorticks"), minor=False)
    if other_kwargs.get("ymajorticks") is not None:
        ax.set_yticks(other_kwargs.get("ymajorticks"), minor=False)
    if other_kwargs.get("xminorticks") is not None:
        ax.set_xticks(other_kwargs.get("xminorticks"), minor=True)
    if other_kwargs.get("yminorticks") is not None:
        ax.set_yticks(other_kwargs.get("yminorticks"), minor=True)
    if other_kwargs.get("xmajorticklabels") is not None:
        ax.set_xticklabels(other_kwargs.get("xmajorticklabels"), minor=False)
    if other_kwargs.get("xminorticklabels") is not None:
        ax.set_xticklabels(other_kwargs.get("xminorticklabels"), minor=True)
    if other_kwargs.get("xloc") is not None:
        ax.set_xlabel(ax.get_xlabel(), loc=other_kwargs.get("xloc"))
    if other_kwargs.get("yloc") is not None:
        ax.set_ylabel(ax.get_ylabel(), loc=other_kwargs.get("yloc"))
    if other_kwargs.get("xrotation") is not None:
        ax.tick_params(axis="x", labelrotation=other_kwargs.get("xrotation"))
    if other_kwargs.get("yrotation") is not None:
        ax.tick_params(axis="y", labelrotation=other_kwargs.get("yrotation"))


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
    signals_sum = sum(remove_residual_axis(signals, "shift", select_value="nominal").values())
    backgrounds_sum = sum(remove_residual_axis(backgrounds, "shift", select_value="nominal").values())

    # calculate sensitivity by S / sqrt(S + B)
    sensitivity = signals_sum.values() / np.sqrt(signals_sum.values() + backgrounds_sum.values())
    mask = sensitivity >= threshold

    # adjust the mask to blind the bins inbetween blinded ones
    if sum(mask) > 1:
        first_ind, last_ind = np.where(mask)[0][::sum(mask) - 1]
        mask[first_ind:last_ind] = True

    # set data points in masked region to zero
    for proc, h in data.items():
        h.values()[..., mask] = -999
        h.variances()[..., mask] = 0

    # merge all histograms
    hists = law.util.merge_dicts(signals, backgrounds, data)

    return hists


def rebin_equal_width(
    hists: dict[Hashable, hist.Hist],
    axis_name: str,
) -> tuple[dict[Hashable, hist.Hist], np.ndarray]:
    """
    In a dictionary, rebins an axis named *axis_name* of all histograms to have the same amount of bins but with equal
    width. This is achieved by using integer edge values starting at 0. The original edge values are returned as well.
    Bin contents are not changed but copied to the rebinned histograms.

    :param hists: Dictionary of histograms to rebin.
    :param axis_name: Name of the axis to rebin.
    :return: Tuple of the rebinned histograms and the new bin edges.
    """
    import hist

    # get the variable axis from the first histogram
    assert hists
    for var_index, var_axis in enumerate(list(hists.values())[0].axes):
        if var_axis.name == axis_name:
            break
    else:
        raise ValueError(f"axis '{axis_name}' not found in histograms")
    assert isinstance(var_axis, (hist.axis.Variable, hist.axis.Regular))
    orig_edges = var_axis.edges

    # prepare arguments for the axis copy
    if isinstance(var_axis, hist.axis.Variable):
        axis_kwargs = {"edges": list(range(len(orig_edges)))}
    else:  # hist.axis.Regular
        axis_kwargs = {"start": orig_edges[0], "stop": orig_edges[-1]}

    # rebin all histograms
    new_hists = type(hists)()
    for key, h in hists.items():
        # create a new histogram
        new_axes = h.axes[:var_index] + (copy_axis(var_axis, **axis_kwargs),) + h.axes[var_index + 1:]
        new_h = hist.Hist(*new_axes, storage=h.storage_type())

        # copy contents and save
        new_h.view()[...] = h.view()
        new_hists[key] = new_h

    return new_hists, orig_edges


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
    if do_apply("SCALE") and "scale" in kwargs:
        scale_str = kwargs.get("scale_format", "$\\times${}").format(kwargs["scale"])
        if "__SCALE__" in label:
            label = label.replace("__SCALE__", scale_str)
        else:
            label += scale_str

    return label


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


def calculate_stat_error(h: hist.Hist, error_type: str, density: bool = True) -> np.ndarray:
    """
    Calculate the error to be plotted for the given histogram *h*.
    Supported error types are:

        - "variance": the plotted error is the square root of the variance for each bin
        - "poisson_unweighted": the plotted error is the poisson error for each bin
        - "poisson_weighted": the plotted error is the poisson error for each bin, weighted by the variance
    """
    # undo density if needed
    if density:
        area = functools.reduce(operator.mul, h.axes.widths)
        h = h * area

    # determine the error type
    if error_type == "variance":
        yerr = h.view().variance ** 0.5

    elif error_type in {"poisson_unweighted", "poisson_weighted"}:
        # compute asymmetric poisson confidence interval
        from hist.intervals import poisson_interval

        variances = h.view().variance if error_type == "poisson_weighted" else None
        values = h.view().value
        confidence_interval = poisson_interval(values, variances)

        # negative values are considerd as blinded bins -> set confidence interval to 0
        confidence_interval[:, values < 0] = 0

        if error_type == "poisson_weighted":
            # might happen if some bins are empty, see https://github.com/scikit-hep/hist/blob/5edbc25503f2cb8193cc5ff1eb71e1d8fa877e3e/src/hist/intervals.py#L74  # noqa: E501
            confidence_interval[np.isnan(confidence_interval)] = 0
        elif np.any(np.isnan(confidence_interval)):
            raise ValueError("Unweighted Poisson interval calculation returned NaN values, check Hist package")

        # calculate the error
        yerr_lower = values - confidence_interval[0]
        yerr_upper = confidence_interval[1] - values
        yerr = np.array([yerr_lower, yerr_upper])

        if np.any(yerr < 0):
            logger.warning("found yerr < 0, forcing to 0; this should not happen, please check your histogram")
            yerr[yerr < 0] = 0

    else:
        raise ValueError(f"unknown error type '{error_type}'")

    # re-apply density if needed
    if density:
        area = functools.reduce(operator.mul, h.axes.widths)
        h = h / area
        yerr = yerr / area

    return yerr
