# coding: utf-8

"""
Methods for dealing with MC weights.
"""

from columnflow.production import Producer, producer
from columnflow.util import maybe_import
from columnflow.columnar_util import set_ak_column, has_ak_column, optional_column as optional

np = maybe_import("numpy")
ak = maybe_import("awkward")


@producer(
    uses={"genWeight", optional("LHEWeight.originalXWGTUP")},
    produces={"mc_weight"},
    # only run on mc
    mc_only=True,
)
def mc_weight(self: Producer, events: ak.Array, **kwargs) -> ak.Array:
    """
    Reads the genWeight and LHEWeight columns and makes a decision about which one to save. This
    should have been configured centrally [1] and stored in genWeight, but there are some samples
    where this failed.

    Strategy:

      1. Use LHEWeight.originalXWGTUP when it exists and genWeight is always 1.
      2. In all other cases, use genWeight.

    [1] https://twiki.cern.ch/twiki/bin/view/CMSPublic/WorkBookNanoAOD?rev=99#Weigths
    """
    # determine the mc_weight
    mc_weight = events.genWeight
    if has_ak_column(events, "LHEWeight.originalXWGTUP") and ak.all(events.genWeight == 1.0):
        mc_weight = events.LHEWeight.originalXWGTUP

    # store the column
    events = set_ak_column(events, "mc_weight", mc_weight, value_type=np.float32)

    return events
