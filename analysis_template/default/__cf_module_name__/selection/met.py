# coding: utf-8

"""
MET and MET filter selection methods.
"""

from columnflow.selection import Selector, SelectionResult, selector
from columnflow.util import maybe_import
from columnflow.columnar_util import Route


np = maybe_import("numpy")
ak = maybe_import("awkward")


@selector(
    uses={"event"},
)
def met_filter_selection(
    self: Selector,
    events: ak.Array,
    **kwargs,
) -> tuple[ak.Array, SelectionResult]:
    """
    MET filter selection.
    """
    # start with a true mask and iteratively apply filters
    mask = abs(events.event) >= 0
    for column in self.config_inst.x.met_filters[self.dataset_inst.data_source]:
        mask = mask & (Route(column).apply(events) == 1)

    return events, SelectionResult(
        steps={
            "met_filter": mask,
        },
    )


@met_filter_selection.init
def met_filter_selection_init(self: Selector) -> None:
    """
    Adds only the necessary MET filter flags to the set of used columns.
    """
    if not getattr(self, "dataset_inst", None):
        return

    self.uses |= set(self.config_inst.x.met_filters[self.dataset_inst.data_source])
