# coding: utf-8

"""
Collection of helper functions for creating and handling histograms.
"""

from __future__ import annotations

__all__ = []

import functools
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
    categorical_axes: tuple[tuple[str, str]] | None = None,
    weight: bool = True,
) -> hist.Hist:
    histogram = hist.Hist.new

    # additional category axes
    if categorical_axes:
        for name, axis_type in categorical_axes:
            if axis_type in ("intcategory", "intcat"):
                histogram = histogram.IntCat([], name=name, growth=True)
            elif axis_type in ("strcategory", "strcat"):
                histogram = histogram.StrCat([], name=name, growth=True)
            else:
                raise ValueError(f"unknown axis type '{axis_type}' in argument 'categorical_axes'")

    # requested axes from variables
    for variable_inst in variable_insts:
        histogram = add_hist_axis(histogram, variable_inst)

    # weight storage
    if weight:
        histogram = histogram.Weight()

    return histogram


create_columnflow_hist = functools.partial(create_hist_from_variables, categorical_axes=(
    # axes that are used in columnflow tasks per default
    # (NOTE: "category" axis is filled as int, but transformed to str afterwards)
    ("category", "intcat"),
    ("process", "intcat"),
    ("shift", "strcat"),
))


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
