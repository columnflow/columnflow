# coding: utf-8

"""
Collection of helper functions for creating and handling histograms.
"""

from __future__ import annotations

__all__ = []

import law
import order as od

from columnflow.columnar_util import flat_np_view
from columnflow.util import maybe_import
from columnflow.types import Any

hist = maybe_import("hist")
np = maybe_import("numpy")
ak = maybe_import("awkward")


logger = law.logger.get_logger(__name__)


def fill_hist(
    h: hist.Hist,
    data: ak.Array | np.array | dict[str, ak.Array | np.array],
    *,
    last_edge_inclusive: bool | None = None,
    fill_kwargs: dict[str, Any] | None = None,
) -> None:
    """
    Fills a histogram *h* with data from an awkward array, numpy array or nested dictionary *data*.
    The data is assumed to be structured in the same way as the histogram axes. If
    *last_edge_inclusive* is *True*, values that would land exactly on the upper-most bin edge of an
    axis are shifted into the last bin. If it is *None*, the behavior is determined automatically
    and depends on the variable axis type. In this case, shifting is applied to all continuous,
    non-circular axes.
    """
    if fill_kwargs is None:
        fill_kwargs = {}

    # helper to decide whether the variable axis qualifies for shifting the last bin
    def allows_shift(ax) -> bool:
        return ax.traits.continuous and not ax.traits.circular

    # determine the axis names, figure out which which axes the last bin correction should be done
    axis_names = []
    correct_last_bin_axes = []
    for ax in h.axes:
        axis_names.append(ax.name)
        # include values hitting last edge?
        if not len(ax.widths) or not isinstance(ax, hist.axis.Variable):
            continue
        if (last_edge_inclusive is None and allows_shift(ax)) or last_edge_inclusive:
            correct_last_bin_axes.append(ax)

    # check data
    if not isinstance(data, dict):
        if len(axis_names) != 1:
            raise ValueError("got multi-dimensional hist but only one dimensional data")
        data = {axis_names[0]: data}
    else:
        for name in axis_names:
            if name not in data and name not in fill_kwargs:
                raise ValueError(f"missing data for histogram axis '{name}'")

    # correct last bin values
    for ax in correct_last_bin_axes:
        right_egde_mask = ak.flatten(data[ax.name], axis=None) == ax.edges[-1]
        if np.any(right_egde_mask):
            data[ax.name] = ak.copy(data[ax.name])
            flat_np_view(data[ax.name])[right_egde_mask] -= ax.widths[-1] * 1e-5

    # fill
    arrays = ak.flatten(ak.cartesian(data))
    h.fill(**fill_kwargs, **{field: arrays[field] for field in arrays.fields})


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

    if axis_type in {"variable", "var"}:
        return histogram.Var(variable_inst.bin_edges, **axis_kwargs)

    if axis_type in {"integer", "int"}:
        return histogram.Integer(
            int(variable_inst.bin_edges[0]),
            int(variable_inst.bin_edges[-1]),
            **axis_kwargs,
        )

    if axis_type in {"boolean", "bool"}:
        return histogram.Boolean(**axis_kwargs)

    if axis_type in {"intcategory", "intcat"}:
        binning = (
            [int(b) for b in variable_inst.binning]
            if isinstance(variable_inst.binning, list)
            else []
        )
        axis_kwargs.setdefault("growth", True)
        return histogram.IntCat(binning, **axis_kwargs)

    if axis_type in {"strcategory", "strcat"}:
        axis_kwargs.setdefault("growth", True)
        return histogram.StrCat([], **axis_kwargs)

    if axis_type in {"regular", "reg"}:
        if not variable_inst.even_binning:
            logger.warning(
                "regular axis with uneven binning is not supported, using first and last bin edge "
                "instead",
            )
        return histogram.Regular(
            variable_inst.n_bins,
            variable_inst.bin_edges[0],
            variable_inst.bin_edges[-1],
            **axis_kwargs,
        )

    raise ValueError(f"unknown axis type '{axis_type}'")


def create_hist_from_variables(
    *variable_insts,
    int_cat_axes: tuple[str] | None = None,
    weight: bool = True,
) -> hist.Hist:
    histogram = hist.Hist.new

    # integer category axes
    if int_cat_axes:
        for name in int_cat_axes:
            histogram = histogram.IntCat([], name=name, growth=True)

    # requested axes
    for variable_inst in variable_insts:
        histogram = add_hist_axis(histogram, variable_inst)

    # weight storage
    if weight:
        histogram = histogram.Weight()

    return histogram
