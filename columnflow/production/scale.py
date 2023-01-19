# coding: utf-8

"""
Column production methods related to the renormalization and factorization scales.
"""

import law

from columnflow.production import Producer, producer
from columnflow.util import maybe_import
from columnflow.columnar_util import set_ak_column

ak = maybe_import("awkward")

logger = law.logger.get_logger(__name__)


@producer(
    uses={"LHEScaleWeight"},
    produces={
        "mur_weight", "mur_weight_up", "mur_weight_down",
        "muf_weight", "muf_weight_up", "muf_weight_down",
        "murmuf_weight", "murmuf_weight_up", "murmuf_weight_down",
    },
)
def murmuf_weights(self: Producer, events: ak.Array, **kwargs) -> ak.Array:
    """
    Producer that reads out mur and muf uncertainties on an event-by-event basis.
    This producer assumes that the nominal entry is always the 5th LHEScaleWeight entry and
    that the nominal weight is already included in the LHEWeight.
    Can only be called with MC datasets.

    Resources:
        - https://cms-nanoaod-integration.web.cern.ch/integration/master/mc94X_doc.html
    """
    # stop here for data
    if self.dataset_inst.is_data:
        raise ValueError("attempt to read out mur/muf weights in data")

    n_weights = ak.num(events.LHEScaleWeight, axis=1)
    if ak.any(n_weights != 9):
        bad_values = set(n_weights[n_weights != 9])
        raise Exception(
            "the number of LHEScaleWeights is expected to be 9, but also found values " +
            f"{bad_values} in dataset {self.dataset_inst.name}",
        )

    murf_nominal = events.LHEScaleWeight[:, 4]
    if ak.any(murf_nominal != 1):
        bad_values = set(murf_nominal[murf_nominal != 1])
        logger.debug(
            "The nominal LHEScaleWeight is expected to be 1, but also found values " +
            f"{bad_values} in dataset {self.dataset_inst.name}. All variations will be " +
            "normalized to the nominal LHEScaleWeight and it is assumed that the nominal " +
            "weight is already included in the LHEWeight.",
        )

    # normalize all weights by the nominal one, assumed to be the 5th value
    murf_weights = events.LHEScaleWeight / murf_nominal

    # decorrelated weights
    events = set_ak_column(events, "mur_weight", ak.ones_like(events.event))
    events = set_ak_column(events, "mur_weight_up", murf_weights[:, 7])
    events = set_ak_column(events, "mur_weight_down", murf_weights[:, 1])
    events = set_ak_column(events, "muf_weight", ak.ones_like(events.event))
    events = set_ak_column(events, "muf_weight_up", murf_weights[:, 5])
    events = set_ak_column(events, "muf_weight_down", murf_weights[:, 3])

    # fully correlated weights
    events = set_ak_column(events, "murmuf_weight", ak.ones_like(events.event))
    events = set_ak_column(events, "murmuf_weight_up", murf_weights[:, 0])
    events = set_ak_column(events, "murmuf_weight_down", murf_weights[:, 8])

    return events


@producer(
    uses={"LHEScaleWeight"},
    produces={"murf_envelope_weight", "murf_envelope_weight_up", "murf_envelope_weight_down"},
)
def murmuf_envelope_weights(self: Producer, events: ak.Array, **kwargs) -> ak.Array:
    """
    Producer that determines the envelope of the mur/muf up and down variations on an event-by-event basis.
    This producer assumes that the nominal entry is always the 5th LHEScaleWeight entry and
    that the nominal weight is already included in the LHEWeight.
    Can only be called with MC datasets.

    Resources:
        - https://cms-nanoaod-integration.web.cern.ch/integration/master/mc94X_doc.html
    """
    # stop here for data
    if self.dataset_inst.is_data:
        return events

    n_weights = ak.num(events.LHEScaleWeight, axis=1)
    # For now, make an exception for st_schannel_had dataset; should be fixed with NanoAODv10
    if ak.any(n_weights != 9):
        bad_values = set(n_weights[n_weights != 9])
        raise Exception(
            "the number of LHEScaleWeights is expected to be 9, but also found values " +
            f"{bad_values} in dataset {self.dataset_inst.name}",
        )

    murf_nominal = events.LHEScaleWeight[:, 4]
    if ak.any(murf_nominal != 1):
        bad_values = set(murf_nominal[murf_nominal != 1])
        logger.debug(
            "The nominal LHEScaleWeight is expected to be 1, but also found values " +
            f"{bad_values} in dataset {self.dataset_inst.name}. All variations will be " +
            "normalized to the nominal LHEScaleWeight and it is assumed that the nominal " +
            "weight is already included in the LHEWeight.",
        )

    events = set_ak_column(events, "murf_envelope_weight", ak.ones_like(events.event))

    # for the up/down variations, take the max/min value of all possible combinations
    # except mur=2, muf=0.5 (index 2) and mur=0.5, muf=2 (index 6) into account
    considered_murf_weights = (events.LHEScaleWeight / murf_nominal)[:, [0, 1, 3, 4, 5, 7, 8]]

    events = set_ak_column(events, "murf_envelope_weight_down", ak.min(considered_murf_weights, axis=1))
    events = set_ak_column(events, "murf_envelope_weight_up", ak.max(considered_murf_weights, axis=1))

    return events
