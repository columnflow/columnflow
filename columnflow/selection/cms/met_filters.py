# coding: utf-8

"""
Selector related to MET filters.
"""

from __future__ import annotations

from columnflow.selection import Selector, selector
from columnflow.util import maybe_import
from columnflow.columnar_util import Route


ak = maybe_import("awkward")


@selector(
    uses={"event", "nFlag"},
    # function to obtain met filters from the config
    get_met_filters=(lambda config_inst: config_inst.x.met_filters),
)
def met_filters(
    self: Selector,
    events: ak.Array,
    **kwargs,
) -> ak.Array:
    """
    Compute a selection mask to filter out noisy/anomalous high-MET events (MET filters).

    Individual filter decisions based on different criteria are stored as bool-valued columns
    in the input NanoAOD. The columns to apply are specified via an auxiliary config entry:

    .. code-block:: python

        cfg.x.met_filters = {
            "Flag.globalSuperTightHalo2016Filter",
            "Flag.HBHENoiseFilter",
            "Flag.HBHENoiseIsoFilter",
            "Flag.EcalDeadCellTriggerPrimitiveFilter",
            "Flag.BadPFMuonFilter",
            "Flag.BadPFMuonDzFilter",
            "Flag.eeBadScFilter",
            "Flag.ecalBadCalibFilter",
        }

    *get_met_filters* can be adapted in a subclass in case they are stored differently in the
    config.

    The specified columns are interpreted as booleans, with missing values treated as *True*,
    i.e. the event is considered to have passed the corresponding filter.

    Returns a bool array containing the logical AND of all input columns.
    """
    result = ak.ones_like(events.event, dtype=bool)

    for route in self.met_filters:
        # interpret column values as booleans
        vals = Route(route).apply(events)
        vals = ak.values_astype(vals, bool)
        # treat missing values as a "pass"
        vals = ak.fill_none(vals, True)
        # append to the result
        result = (result & vals)

    return result


@met_filters.init
def met_filters_init(self: Selector) -> None:
    """
    Read MET filters from config and add them as input columns.
    """
    met_filters = self.get_met_filters(self.config_inst)
    if isinstance(met_filters, dict):
        # do nothing when no dataset_inst is known
        if not getattr(self, "dataset_inst", None):
            return
        met_filters = met_filters[self.dataset_inst.data_source]

    # store filters as an attribute for faster lookup
    self.met_filters = set(met_filters)
    self.uses |= self.met_filters
