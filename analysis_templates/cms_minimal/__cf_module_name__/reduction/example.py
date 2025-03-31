# coding: utf-8

"""
Exemplary reduction methods that can run on-top of columnflow's default reduction.
"""

from columnflow.reduction import Reducer, reducer
from columnflow.reduction.default import cf_default
from columnflow.util import maybe_import
from columnflow.columnar_util import set_ak_column

ak = maybe_import("awkward")


@reducer(
    uses={cf_default, "Jet.hadronFlavour"},
    produces={cf_default, "Jet.from_b_hadron"},
)
def example(self: Reducer, events: ak.Array, selection: ak.Array, **kwargs) -> ak.Array:
    # run cf's default reduction which handles event selection and collection creation
    events = self[cf_default](events, selection, **kwargs)

    # compute and store additional columns after the default reduction
    # (so only on a subset of the events and objects which might be computationally lighter)
    events = set_ak_column(events, "Jet.from_b_hadron", abs(events.Jet.hadronFlavour) == 5, value_type=bool)

    return events
