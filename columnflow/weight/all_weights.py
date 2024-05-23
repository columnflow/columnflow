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
def all_weights(self: WeightProducer, events: ak.Array, **kwargs) -> ak.Array:
    """
    WeightProducer that combines all event weights from the *event_weights* aux entry from either
    the config or the dataset. The weights are multiplied together to form the full event weight.

    The expected structure of the *event_weights* aux entry is a dictionary with the weight column
    name as key and a list of shift sources as value. The shift sources are used to declare the
    shifts that the produced event weight depends on. Example:

    .. code-block:: python

        from columnflow.config_util import get_shifts_from_sources
        # add weights and their corresponding shifts for all datasets
        cfg.x.event_weights = {
            "normalization_weight": [],
            "muon_weight": get_shifts_from_sources(config, "mu_sf"),
            "btag_weight": get_shifts_from_sources(config, f"btag_hf", "btag_lf"),
        }
        for dataset_inst in cfg.datasets:
            # add dataset-specific weights and their corresponding shifts
            dataset.x.event_weights = {}
            if not dataset_inst.has_tag("skip_pdf"):
                dataset_inst.event_weights["pdf_weight"] = get_shifts_from_sources(config, "pdf")
    """
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
    return events, weight


@all_weights.init
def all_weights_init(self: WeightProducer) -> None:
    if not getattr(self, "dataset_inst", None):
        return

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
