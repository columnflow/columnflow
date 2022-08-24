# coding: utf-8

"""
Methods for dealing with MC weights.
"""

from columnflow.production import Producer, producer
from columnflow.util import maybe_import
from columnflow.columnar_util import set_ak_column, has_ak_column

np = maybe_import("numpy")
ak = maybe_import("awkward")


@producer(
    uses={"genWeight", "LHEWeight.originalXWGTUP"},
    produces={"mc_weight"},
)
def mc_weight(self: Producer, events: ak.Array, **kwargs) -> ak.Array:
    """
    Reads the genWeight and LHEWeight columns and makes a decision about which one to save. This
    should have been configured centrally [1] and stored in genWeight, but there are some samples
    where this failed.

    Strategy:
      1. When LHEWeight is missing, use genWeight.
      2. When both exist and they are identical, use genWeight.
      3. When both exist and genWeight is always one, use LHEWeight.
      4. Raise an exception in all other cases.

    [1] https://twiki.cern.ch/twiki/bin/view/CMSPublic/WorkBookNanoAOD?rev=99#Weigths
    """
    # skip real data
    if self.dataset_inst.is_data:
        return events

    # determine the mc_weight
    if not has_ak_column(events, "LHEWeight.originalXWGTUP"):
        mc_weight = events.genWeight
    elif ak.all(events.genWeight == events.LHEWeight.originalXWGTUP):
        mc_weight = events.genWeight
    elif ak.all(events.genWeight == 1.0):
        mc_weight = events.LHEWeight.originalXWGTUP
    else:
        raise Exception(
            f"cannot determine mc_weight from genWeight {events.genWeight} and "
            f"LHEWeight {events.LHEWeight.originalXWGTUP}",
        )

    # store the column
    events = set_ak_column(events, "mc_weight", mc_weight)

    return events
