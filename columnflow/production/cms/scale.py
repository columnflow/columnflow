# coding: utf-8

"""
Column production methods related to the renormalization and factorization scales.
"""

import functools

import law

from columnflow.production import Producer
from columnflow.util import maybe_import, InsertableDict
from columnflow.columnar_util import set_ak_column
from columnflow.columnar_util import DotDict

np = maybe_import("numpy")
ak = maybe_import("awkward")


logger = law.logger.get_logger(__name__)

# helper
set_ak_column_f32 = functools.partial(set_ak_column, value_type=np.float32)


class _ScaleWeightBase(Producer):
    """
    Common base class for the scale weight producers below that join a setup function.
    """

    def setup_func(self, reqs: dict, inputs: dict, reader_targets: InsertableDict) -> None:
        # named weight indices
        self.indices_9 = DotDict(
            mur_down_muf_down=0,
            mur_down_muf_nom=1,
            mur_down_muf_up=2,
            mur_nom_muf_down=3,
            mur_nom_muf_nom=4,
            mur_nom_muf_up=5,
            mur_up_muf_down=6,
            mur_up_muf_nom=7,
            mur_up_muf_up=8,
        )

        # named weight indices for cases where only 8 of the exist
        # (expecting no nominal value and all above being shifted down by one)
        self.indices_8 = DotDict({
            key: index if index <= self.indices_9.mur_nom_muf_nom else index - 1
            for key, index in self.indices_9.items()
            if key != "mur_nom_muf_nom"
        })

        # for convenience, declare some meaningful clear names for the weights
        # here instead of the very technical names like mur_nom_muf_up
        self.clearnames = DotDict(
            # decorrelated weights
            mur_weight_up="mur_up_muf_nom",
            mur_weight_down="mur_down_muf_nom",
            muf_weight_up="mur_nom_muf_up",
            muf_weight_down="mur_nom_muf_down",
            # fully-correlated names
            murmuf_weight_up="mur_up_muf_up",
            murmuf_weight_down="mur_down_muf_down",
        )


@_ScaleWeightBase.producer(
    uses={
        "LHEScaleWeight",
    },
    produces={
        "mur_weight", "mur_weight_up", "mur_weight_down",
        "muf_weight", "muf_weight_up", "muf_weight_down",
        "murmuf_weight", "murmuf_weight_up", "murmuf_weight_down",
    },
    # only run on mc
    mc_only=True,
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
    n_weights = ak.num(events.LHEScaleWeight, axis=1)

    if ak.all(n_weights == 9):
        # if we have 9 weights, the indices above are correct, just need
        # to load the nominal weights
        indices = self.indices_9
        murf_nominal = events.LHEScaleWeight[:, indices.mur_nom_muf_nom]

        # perform an additional check to see of the nominal value is ever != 1
        if ak.any(murf_nominal != 1):
            bad_values = set(murf_nominal[murf_nominal != 1])
            logger.debug(
                "The nominal LHEScaleWeight is expected to be 1, but also found values " +
                f"{bad_values} in dataset {self.dataset_inst.name}. All variations will be " +
                "normalized to the nominal LHEScaleWeight and it is assumed that the nominal " +
                "weight is already included in the LHEWeight.",
            )
    elif ak.all(n_weights == 8):
        # if we just have 8 weights, there is no nominal LHEScale weight
        # instead, initialize the nominal weights as ones.
        # Additionally, we need to shift the last couple of weight indices
        # down by 1
        indices = self.indices_8
        murf_nominal = np.array(1, dtype=np.float32)

        # additional debug log
        logger.debug(
            f"In dataset {self.dataset_inst.name}: number of LHEScaleWeights is always " +
            "8 instead of the expected 9. It is assumed, that the missing entry is the " +
            "nominal one and all other entries are in correct order",
        )
    elif not ak.any(n_weights):
        logger.warning("No valid `LHEScaleWeight` found. For the `murmuf_weights`, a ones array is returned.")
        for shift in ["", "_up", "_down"]:
            events = set_ak_column_f32(events, f"murmuf_weight{shift}", ak.ones_like(events.event))
            events = set_ak_column_f32(events, f"mur_weight{shift}", ak.ones_like(events.event))
            events = set_ak_column_f32(events, f"muf_weight{shift}", ak.ones_like(events.event))
        return events
    else:
        bad_values = set(n_weights[any(n_weights != x for x in [8, 9])])
        raise Exception(
            "the number of LHEScaleWeights is expected to be 9, but also found values " +
            f"{bad_values} in dataset {self.dataset_inst.name}",
        )

    # normalize all weights by the nominal one, assumed to be the 5th value
    murf_weights = events.LHEScaleWeight / murf_nominal

    # setup nominal weights
    events = set_ak_column_f32(
        events,
        "mur_weight",
        ak.ones_like(events.event),
    )
    events = set_ak_column_f32(
        events,
        "muf_weight",
        ak.ones_like(events.event),
    )

    # fully correlated weights
    events = set_ak_column_f32(
        events,
        "murmuf_weight",
        ak.ones_like(events.event),
    )

    # now loop through the clear names and save the respective normalized
    # LHEScaleWeights
    for colname, indexname in self.clearnames.items():
        index = indices.get(indexname)
        # throw an error in case something went wrong with the
        # assignment of clear names for the weight columns
        if index is None:
            raise ValueError(f"Could not retrieve index for weight set {indexname}")
        events = set_ak_column_f32(
            events,
            colname,
            murf_weights[:, index],
        )

    return events


@_ScaleWeightBase.producer(
    uses={
        "LHEScaleWeight",
    },
    produces={
        "murf_envelope_weight", "murf_envelope_weight_up", "murf_envelope_weight_down",
    },
    # only run on mc
    mc_only=True,
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
    n_weights = ak.num(events.LHEScaleWeight, axis=1)

    if ak.all(n_weights == 9):
        murf_nominal = events.LHEScaleWeight[:, self.indices_9.mur_nom_muf_nom]
        envelope_indices = self.envelope_indices_9

        # perform an additional check to see of the nominal value is ever != 1
        if ak.any(murf_nominal != 1):
            bad_values = set(murf_nominal[murf_nominal != 1])
            logger.debug(
                "The nominal LHEScaleWeight is expected to be 1, but also found values " +
                f"{bad_values} in dataset {self.dataset_inst.name}. All variations will be " +
                "normalized to the nominal LHEScaleWeight and it is assumed that the nominal " +
                "weight is already included in the LHEWeight.",
            )
    elif ak.all(n_weights == 8):
        murf_nominal = np.array(1, dtype=np.float32)
        envelope_indices = self.envelope_indices_8

        # additional debug log
        logger.debug(
            f"In dataset {self.dataset_inst.name}: number of LHEScaleWeights is always " +
            "8 instead of the expected 9. It is assumed, that the missing entry is the " +
            "nominal one and all other entries are in correct order",
        )
    else:
        bad_values = set(n_weights[n_weights != 9])
        raise Exception(
            "the number of LHEScaleWeights is expected to be 9, but also found values " +
            f"{bad_values} in dataset {self.dataset_inst.name}",
        )

    # take the max/min value of all considered variations
    considered_murf_weights = (events.LHEScaleWeight / murf_nominal)[:, envelope_indices]

    # store columns
    events = set_ak_column_f32(events, "murf_envelope_weight", ak.ones_like(events.event))
    events = set_ak_column_f32(events, "murf_envelope_weight_down", ak.min(considered_murf_weights, axis=1))
    events = set_ak_column_f32(events, "murf_envelope_weight_up", ak.max(considered_murf_weights, axis=1))

    return events


@murmuf_envelope_weights.setup
def murmuf_envelope_weights_setup(self: Producer, reqs: dict, inputs: dict, reader_targets: InsertableDict) -> None:
    # call the super func
    super(murmuf_envelope_weights, self).setup_func(reqs, inputs, reader_targets)

    # create a flat list if indices, skipping those for crossed variations
    self.envelope_indices_9 = [
        index
        for name, index in self.indices_9.items()
        if name not in ["mur_down_muf_up", "mur_up_muf_down"]
    ]

    # as in the murmuf_weights_setup above, in case there are only 8 weights, the nominal one
    # is missing and the entries above are shifted down by one
    self.envelope_indices_8 = [
        index if index <= 4 else index - 1
        for name, index in self.indices_9.items()
        if name not in ["mur_down_muf_up", "mur_up_muf_down", "mur_nom_muf_nom"]
    ]
