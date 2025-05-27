# coding: utf-8

"""
Example histogram producer.
"""

from columnflow.histogramming import HistProducer
from columnflow.histogramming.default import cf_default
from columnflow.util import maybe_import
from columnflow.config_util import get_shifts_from_sources
from columnflow.columnar_util import Route

ak = maybe_import("awkward")
np = maybe_import("numpy")


# extend columnflow's default hist producer
@cf_default.hist_producer()
def example(self: HistProducer, events: ak.Array, **kwargs) -> ak.Array:
    # build the full event weight
    weight = ak.Array(np.ones(len(events), dtype=np.float32))

    if self.dataset_inst.is_mc and len(events):
        for column in self.weight_columns:
            weight = weight * Route(column).apply(events)

    return events, weight


@example.init
def example_init(self: HistProducer) -> None:
    self.weight_columns = {}

    if self.dataset_inst.is_data:
        return

    # store column names referring to weights to multiply
    self.weight_columns |= {"normalization_weight", "muon_weight"}
    self.uses |= self.weight_columns

    # declare shifts that the produced event weight depends on
    shift_sources = {"mu"}
    self.shifts |= set(get_shifts_from_sources(self.config_inst, *shift_sources))
