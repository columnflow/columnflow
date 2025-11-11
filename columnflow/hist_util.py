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
from columnflow.types import TYPE_CHECKING, Any, Sequence

np = maybe_import("numpy")
ak = maybe_import("awkward")
if TYPE_CHECKING:
    hist = maybe_import("hist")


logger = law.logger.get_logger(__name__)


def fill_hist(
    h: hist.Hist,
    data: ak.Array | np.array | dict[str, ak.Array | np.array],
    *,
    last_edge_inclusive: bool | None = None,
    fill_kwargs: dict[str, Any] | None = None,
) -> None:
    """
    Fills a histogram *h* with data from an awkward array, numpy array or nested dictionary *data*. The data is assumed
    to be structured in the same way as the histogram axes. If *last_edge_inclusive* is *True*, values that would land
    exactly on the upper-most bin edge of an axis are shifted into the last bin. If it is *None*, the behavior is
    determined automatically and depends on the variable axis type. In this case, shifting is applied to all continuous,
    non-circular axes.
    """
    import hist

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
            raise ValueError("got multi-dimensional hist but only one-dimensional data")
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

    # check if conversion to records is needed
    arr_types = (ak.Array, np.ndarray)
    vals = list(data.values())
    convert = (
        # values is a mixture of singular and array types
        (any(isinstance(v, arr_types) for v in vals) and not all(isinstance(v, arr_types) for v in vals)) or
        # values contain at least one array with more than one dimension
        any(isinstance(v, arr_types) and v.ndim != 1 for v in vals)
    )

    # actual conversion
    if convert:
        arrays = ak.flatten(ak.cartesian(data))
        data = {field: arrays[field] for field in arrays.fields}
        del arrays

    # fill
    h.fill(**fill_kwargs, **data)


def add_hist_axis(histogram: hist.Hist, variable_inst: od.Variable) -> hist.Hist:
    """
    Add an axis to a histogram based on a variable instance. The axis_type is chosen based on the variable instance's
    "axis_type" auxiliary.

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
            logger.warning("regular axis with uneven binning is not supported, using first and last bin edge instead")
        return histogram.Regular(
            variable_inst.n_bins,
            variable_inst.bin_edges[0],
            variable_inst.bin_edges[-1],
            **axis_kwargs,
        )

    raise ValueError(f"unknown axis type '{axis_type}'")


def get_axis_kwargs(axis: hist.axis.AxesMixin) -> dict[str, Any]:
    """
    Extract information from an *axis* instance that would be needed to create a new one.

    :param axis: The axis instance to extract information from.
    :return: The extracted information in a dict.
    """
    import hist

    axis_attrs = ["name", "label"]
    traits_attrs = []
    kwargs = {}

    if isinstance(axis, hist.axis.Variable):
        axis_attrs.append("edges")
        traits_attrs = ["underflow", "overflow", "growth", "circular"]
    elif isinstance(axis, hist.axis.Regular):
        axis_attrs = ["transform"]
        traits_attrs = ["underflow", "overflow", "growth", "circular"]
        kwargs["bins"] = axis.size
        kwargs["start"] = axis.edges[0]
        kwargs["stop"] = axis.edges[-1]
    elif isinstance(axis, hist.axis.Integer):
        traits_attrs = ["underflow", "overflow", "growth", "circular"]
        kwargs["start"] = axis.edges[0]
        kwargs["stop"] = axis.edges[-1]
    elif isinstance(axis, hist.axis.Boolean):
        # nothing to add to common attributes
        pass
    elif isinstance(axis, (hist.axis.IntCategory, hist.axis.StrCategory)):
        traits_attrs = ["overflow", "growth"]
        kwargs["categories"] = list(axis)
    else:
        raise NotImplementedError(f"axis type '{type(axis).__name__}' not supported")

    return (
        {attr: getattr(axis, attr) for attr in axis_attrs} |
        {attr: getattr(axis.traits, attr) for attr in traits_attrs} |
        kwargs
    )


def copy_axis(axis: hist.axis.AxesMixin, **kwargs: dict[str, Any]) -> hist.axis.AxesMixin:
    """
    Copy an axis with the option to override its attributes.
    """
    # create arguments for new axis from overlay with current and requested ones
    axis_kwargs = get_axis_kwargs(axis) | kwargs

    # create new instance
    return type(axis)(**axis_kwargs)


def create_hist_from_variables(
    *variable_insts,
    categorical_axes: tuple[tuple[str, str]] | None = None,
    weight: bool = True,
    storage: str | None = None,
) -> hist.Hist:
    import hist

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

    # add the storage
    if storage is None:
        # use weight value for backwards compatibility
        storage = "weight" if weight else "double"
    else:
        storage = storage.lower()
    if storage == "weight":
        histogram = histogram.Weight()
    elif storage == "double":
        histogram = histogram.Double()
    else:
        raise ValueError(f"unknown storage type '{storage}'")

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
    import hist

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
    import hist

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


def update_ax_labels(hists: list[hist.Hist], config_inst: od.Config, variable_name: str) -> None:
    """
    Helper function to update the axis labels of histograms based on variable instances from
    the *config_inst*.

    :param hists: List of histograms to update.
    :param config_inst: Configuration instance containing variable definitions.
    :param variable_name: Name of the variable to update labels for, formatted as a string
                         with variable names separated by hyphens (e.g., "var1-var2").
    :raises ValueError: If a variable name is not found in the histogram axes.
    """
    labels = {}
    for var_name in variable_name.split("-"):
        var_inst = config_inst.get_variable(var_name, None)
        if var_inst:
            labels[var_name] = var_inst.x_title

    for h in hists:
        for var_name, label in labels.items():
            ax_names = [ax.name for ax in h.axes]
            if var_name in ax_names:
                h.axes[var_name].label = label
            else:
                raise ValueError(f"variable '{var_name}' not found in histogram axes: {h.axes}")

                
def sum_hists(hists: Sequence[hist.Hist]) -> hist.Hist:
    """
    Sums a sequence of histograms into a new histogram. In case axis labels differ, which typically leads to errors
    ("axes not mergable"), the labels of the first histogram are used.

    :param hists: The histograms to sum.
    :return: The summed histogram.
    """
    hists = list(hists)
    if not hists:
        raise ValueError("no histograms given for summation")

    # copy the first histogram
    h_sum = hists[0].copy()
    if len(hists) == 1:
        return h_sum

    # store labels of first histogram
    axis_labels = {ax.name: ax.label for ax in h_sum.axes}

    for h in hists[1:]:
        # align axis labels if needed, only copy if necessary
        h_aligned_labels = None
        for ax in h.axes:
            if ax.name not in axis_labels or ax.label == axis_labels[ax.name]:
                continue
            if h_aligned_labels is None:
                h_aligned_labels = h.copy()
            h_aligned_labels.axes[ax.name].label = axis_labels[ax.name]
        h_sum = h_sum + (h if h_aligned_labels is None else h_aligned_labels)

    return h_sum
