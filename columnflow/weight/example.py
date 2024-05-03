# coding: utf-8

"""
Exemplary event weight producer.
"""

from columnflow.weight import WeightProducer, weight_producer
from columnflow.util import maybe_import
from columnflow.columnar_util import has_ak_column, Route

np = maybe_import("numpy")
ak = maybe_import("awkward")


@weight_producer(
    # only run on mc
    mc_only=True,
)
def example(self: WeightProducer, events: ak.Array, **kwargs) -> ak.Array:
    # build the full event weight
    weight = ak.Array(np.ones(len(events)))
    if self.dataset_inst.is_mc and len(events):
        # multiply weights from global config `event_weights` aux entry
        for column in self.config_inst.x.event_weights:
            weight = weight * Route(column).apply(events)

        # multiply weights from dataset-specific `event_weights` aux entry
        for column in self.dataset_inst.x("event_weights", []):
            if has_ak_column(events, column):
                weight = weight * Route(column).apply(events)
            else:
                self.logger.warning_once(
                    f"missing_dataset_weight_{column}",
                    f"weight '{column}' for dataset {self.dataset_inst.name} not found",
                )
    return weight


@example.init
def example_init(self: WeightProducer) -> None:
    weight_columns = set()

    # add used weight columns and declare shifts that the produced event weight depends on
    if self.config_inst.has_aux("event_weights"):
        weight_columns |= {Route(column) for column in self.config_inst.x.event_weights}
        for shift_insts in self.config_inst.x.event_weights.values():
            self.shifts |= {shift_inst.name for shift_inst in shift_insts}

    # optionally also for weights defined by a dataset
    if self.dataset_inst.has_aux("event_weights"):
        weight_columns |= {Route(column) for column in self.dataset_inst.x("event_weights", [])}
        for shift_insts in self.dataset_inst.x.event_weights.values():
            self.shifts |= {shift_inst.name for shift_inst in shift_insts}

    # add weight columns to uses
    self.uses |= weight_columns
