# coding: utf-8

"""
Collection of helper functions for creating and handling histograms.
"""

from __future__ import annotations

import law
import order as od

from columnflow.util import maybe_import

hist = maybe_import("hist")
np = maybe_import("numpy")
ak = maybe_import("awkward")

logger = law.logger.get_logger(__name__)


def add_hist_axis(histogram: hist.Hist, variable_inst: od.Variable) -> hist.Hist:
    """
    Add an axis to a histogram based on a variable instance. The axis_type is chosen
    based on the variable instance's "axis_type" auxiliary.

    :param histogram: The histogram to add the axis to.
    :param variable_inst: The variable instance to use for the axis.
    :return: The histogram with the added axis.
    """
    default_kwargs = {
        "name": variable_inst.name,
        "label": variable_inst.get_full_x_title(),
    }

    axis_kwargs = law.util.merge_dicts(
        default_kwargs,
        variable_inst.x("axis_kwargs", {}),
        deep=True,
    )

    # NOTE: maybe "discrete_x" should correspond to "intcat" instead of "integer" per default
    default_axis_type = "integer" if variable_inst.discrete_x else "variable"
    axis_type = variable_inst.x("axis_type", default_axis_type).lower()

    if axis_type == "variable" or axis_type == "var":
        return histogram.Var(
            variable_inst.bin_edges,
            **axis_kwargs,
        )
    elif axis_type == "integer" or axis_type == "int":
        return histogram.Integer(
            int(variable_inst.bin_edges[0]),
            int(variable_inst.bin_edges[-1]),
            **axis_kwargs,
        )
    elif axis_type == "boolean" or axis_type == "bool":
        return histogram.Boolean(
            **axis_kwargs,
        )
    elif axis_type == "intcategory" or axis_type == "intcat":
        binning = [int(b) for b in variable_inst.binning] if isinstance(variable_inst.binning, list) else []
        axis_kwargs.setdefault("growth", True)
        return histogram.IntCat(
            binning,
            **axis_kwargs,
        )
    elif axis_type == "strcategory" or axis_type == "strcat":
        axis_kwargs.setdefault("growth", True)
        return histogram.StrCat(
            [],
            **axis_kwargs,
        )
    elif axis_type == "regular" or axis_type == "reg":
        if not variable_inst.even_binning:
            logger.warning("Regular axis with uneven binning is not supported. Using first and last bin edge instead.")
        return histogram.Regular(
            variable_inst.n_bins,
            variable_inst.bin_edges[0],
            variable_inst.bin_edges[-1],
            **axis_kwargs,
        )


def create_hist_from_variables(
    *variable_insts,
    add_default_axes: bool = False,
) -> hist.Hist:
    histogram = hist.Hist.new

    # default axes
    if add_default_axes:
        # NOTE: category axes are initialized as IntCat, but transformed to StrCat later on
        histogram = histogram.IntCat([], name="category", growth=True)
        histogram = histogram.IntCat([], name="process", growth=True)
        histogram = histogram.StrCat([], name="shift", growth=True)

    # requested axes
    for variable_inst in variable_insts:
        histogram = add_hist_axis(histogram, variable_inst)

    histogram = histogram.Weight()

    return histogram


def translate_hist_intcat_to_strcat(
    h: hist.Hist,
    axis_name: str,
    id_map: dict[int, str],
) -> hist.Hist:
    out_axes = [
        ax if ax.name != axis_name else hist.axis.StrCategory(
            [id_map[v] for v in list(ax)],
            name=ax.name,
            label=ax.label,
            growth=ax.traits.growth,
        )
        for ax in h.axes
    ]
    return hist.Hist(*out_axes, storage=h.storage_type(), data=h.view(flow=True))


def add_missing_shifts(
    h: hist.Hist,
    expected_shifts_bins: set[str],
    str_axis: str = "shift",
    nominal_bin: str = "nominal",
) -> None:
    """
    Adds missing shift bins to a histogram *h*.
    """
    # get the set of bins that are missing in the histogram
    shift_bins = set(h.axes[str_axis])
    missing_shifts = set(expected_shifts_bins) - shift_bins
    if missing_shifts:
        nominal = h[{str_axis: hist.loc(nominal_bin)}]
        for missing_shift in missing_shifts:
            # for each missing shift, create the missing shift bin with an
            # empty fill and then copy the nominal histogram into it
            dummy_fill = [
                ax[0] if ax.name != str_axis else missing_shift
                for ax in h.axes
            ]
            h.fill(*dummy_fill, weight=0)
            # TODO: this might skip overflow and underflow bins
            h[{str_axis: hist.loc(missing_shift)}] = nominal.view()
