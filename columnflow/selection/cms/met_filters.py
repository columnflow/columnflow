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

    The specified columns are interpreted as booleans, with missing values treated as *True*,
    i.e. the event is considered to have passed the corresponding filter.

    Returns a bool array containing the logical AND of all input columns.
    """
    met_filters = set(self.config_inst.x.met_filters)

    result = ak.ones_like(events.event, dtype=bool)

    for route in met_filters:
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
    met_filters = set(self.config_inst.x.met_filters)
    self.uses |= met_filters
