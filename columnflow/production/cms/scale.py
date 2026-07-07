# coding: utf-8

"""
Column production methods related to the renormalization and factorization scales.
"""

import functools

import law

from columnflow.production import Producer, producer
from columnflow.util import maybe_import, DotDict, StrEnum
from columnflow.columnar_util import set_ak_column, ak_concatenate_safe

np = maybe_import("numpy")
ak = maybe_import("awkward")


logger = law.logger.get_logger(__name__)

# helper
set_ak_column_f32 = functools.partial(set_ak_column, value_type=np.float32)


class ScaleWeightOutput(StrEnum):
    """
    Flag to denote which type of weights to output in the :py:class:`murmuf_weights` producer. Options:

        - ``single``: Produces `{mur,muf}_weight{,_up,_down}`.
        - ``correlated``: Produces `murmuf_weight{,_up,_down}`.
        - ``single_correlated``: Produces `{mur,muf,murmuf}_weight{,_up,_down}`.
        - ``raw``: Produces all combinations `mur_{nom,up,down}_muf_{nom,up,down}`.
    """

    single = "single"
    correlated = "correlated"
    single_correlated = "single_correlated"
    raw = "raw"


@producer(
    # used columns
    uses={"LHEScaleWeight"},
    # which types of weights to produce (influences produced columns, defined in init)
    scale_weight_output=ScaleWeightOutput.single_correlated,
    # only run on mc
    mc_only=True,
)
def murmuf_weights(self: Producer, events: ak.Array, **kwargs) -> ak.Array:
    """
    Producer that reads out mur and muf uncertainties on an event-by-event basis. This producer assumes that the nominal
    entry is always the 5th LHEScaleWeight entry and that the nominal weight is already included in the LHEWeight. Can
    only be called with MC datasets.

    Resources:
        - https://cms-xpog.docs.cern.ch/autoDoc/NanoAODv15/2024/doc_TTH-Hto2G_Par-M-125_TuneCP5_13p6TeV_amcatnloFXFX-pythia8_RunIII2024Summer24NanoAODv15-150X_mcRun3_2024_realistic_v2-v2.html#LHEScaleWeight # noqa
    """
    n_weights = ak.num(events.LHEScaleWeight, axis=1)

    # in rare cases, some events might have 0 weights
    non_zero_mask = n_weights > 0
    murf_weights = events.LHEScaleWeight[non_zero_mask]

    # write ones in case there are no weights at all
    ones = np.ones(len(events), dtype=np.float32)
    if not ak.any(non_zero_mask):
        logger.warning(r"no valid 'LHEScaleWeight' vector found, saving ones for all output columns")
        for col in self.weight_names:
            events = set_ak_column_f32(events, col, ones)
        return events

    if ak.all(n_weights[non_zero_mask] == 9):
        indices = self.indices_9

        # perform an additional check to see of the nominal value is ever != 1
        murf_nominal = murf_weights[:, indices.mur_nom_muf_nom]
        if ak.any(murf_nominal != 1):
            bad_values = set(murf_nominal[murf_nominal != 1])
            logger.debug(
                f"the nominal LHEScaleWeight is expected to be 1, but also found values {bad_values} in dataset "
                f"{self.dataset_inst.name}; all variations will be normalized to the nominal LHEScaleWeight and it is "
                "assumed that the nominal weight is already included in the LHEWeight",
            )

        # normalize all weights by the nominal one
        murf_weights = murf_weights / murf_nominal

    elif ak.all(n_weights[non_zero_mask] == 8):
        # if we just have 8 weights, there is no nominal LHEScale weight and instead, initialize the nominal weights as
        # ones; additionally, we need to shift the last couple of weight indices down by 1
        indices = self.indices_8

        # additional debug log
        logger.debug(
            f"in dataset {self.dataset_inst.name} the number of LHEScaleWeights is always 8 instead of the expected 9, "
            "it is assumed, that the missing entry is the nominal one and all other entries are in correct order",
        )

    else:
        bad_values = set(n_weights[non_zero_mask]) - {8, 9}
        raise Exception(
            f"the number of LHEScaleWeights is expected to be 8 or 9, but also found values {bad_values} in dataset "
            f"{self.dataset_inst.name}",
        )

    # store output columns
    for weight_name in self.weight_names:
        weights = ones
        if self.scale_weight_output == ScaleWeightOutput.raw and weight_name != "mur_nom_muf_nom":
            weights = ones.copy()
            weights[non_zero_mask] = murf_weights[:, indices[weight_name]]
        elif self.scale_weight_output != ScaleWeightOutput.raw and not weight_name.endswith("_weight"):
            weights = ones.copy()
            weights[non_zero_mask] = murf_weights[:, indices[self.weight_map[weight_name]]]
        # store it
        events = set_ak_column_f32(events, weight_name, weights)

    return events


@murmuf_weights.init
def murmuf_weights_init(self: Producer, **kwargs) -> None:
    super(murmuf_weights, self).init_func(**kwargs)

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

    # named weight indices for cases where only 8 of them exist
    # (expecting no nominal value and all above being shifted down by one)
    self.indices_8 = DotDict({
        key: (index if index <= self.indices_9.mur_nom_muf_nom else index - 1)
        for key, index in self.indices_9.items()
        if key != "mur_nom_muf_nom"
    })

    # for convenience, map column names to produce to index keys
    self.weight_map = DotDict(
        mur_weight="mur_nom_muf_nom",
        muf_weight="mur_nom_muf_nom",
        murmuf_weight="mur_nom_muf_nom",
        mur_weight_up="mur_up_muf_nom",
        mur_weight_down="mur_down_muf_nom",
        muf_weight_up="mur_nom_muf_up",
        muf_weight_down="mur_nom_muf_down",
        murmuf_weight_up="mur_up_muf_up",
        murmuf_weight_down="mur_down_muf_down",
    )

    # define produced columns
    if not isinstance(self.scale_weight_output, ScaleWeightOutput):
        self.scale_weight_output = ScaleWeightOutput(self.scale_weight_output)
    self.weight_names = []
    if self.scale_weight_output in {ScaleWeightOutput.single, ScaleWeightOutput.single_correlated}:
        self.weight_names += law.util.brace_expand("{mur,muf}_weight{,_up,_down}")
    if self.scale_weight_output in {ScaleWeightOutput.correlated, ScaleWeightOutput.single_correlated}:
        self.weight_names += law.util.brace_expand("murmuf_weight{,_up,_down}")
    if self.scale_weight_output == ScaleWeightOutput.raw:
        self.weight_names += list(self.indices_9.keys())
    self.produces.update(self.weight_names)


murmuf_weights_raw = murmuf_weights.derive("murmuf_weights_raw", cls_dict={
    "scale_weight_output": ScaleWeightOutput.raw,
})


@producer(
    uses={murmuf_weights_raw},
    produces={"murmuf_envelope_weight{,_up,_down}"},
    # only run on mc
    mc_only=True,
    # which columns (produced by murmuf_weights_raw) to consider for the envelope calculation
    envelope_columns=[
        "mur_down_muf_down",
        "mur_down_muf_nom",
        "mur_nom_muf_down",
        "mur_nom_muf_nom",
        "mur_nom_muf_up",
        "mur_up_muf_nom",
        "mur_up_muf_up",
    ],
)
def murmuf_envelope_weights(self: Producer, events: ak.Array, **kwargs) -> ak.Array:
    """
    Producer that determines the envelope of the mur/muf up and down variations on an event-by-event basis. See
    :py:class:`murmuf_weights` for details on the assumptions made about the LHEScaleWeight vector.
    """
    # retrieve raw mur/muf weights
    events = self[murmuf_weights_raw](events, **kwargs)

    # compute the vector of weights per event
    murf_weights = ak_concatenate_safe([events[col][:, None] for col in self.envelope_columns], axis=1)

    # store columns
    events = set_ak_column_f32(events, "murmuf_envelope_weight", events.mur_nom_muf_nom)
    events = set_ak_column_f32(events, "murmuf_envelope_weight_down", ak.min(murf_weights, axis=1))
    events = set_ak_column_f32(events, "murmuf_envelope_weight_up", ak.max(murf_weights, axis=1))

    return events
