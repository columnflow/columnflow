# coding: utf-8

"""
Column production methods related to the renormalization and factorization scales.
"""


from columnflow.production import Producer, producer
from columnflow.util import maybe_import
from columnflow.columnar_util import set_ak_column

ak = maybe_import("awkward")


@producer(
    uses={"LHEScaleWeight"},
    produces={
        "mur_weight", "mur_weight_up", "mur_weight_down",
        "muf_weight", "muf_weight_up", "muf_weight_down",
    },
)
def murmuf_weights(self: Producer, events: ak.Array, **kwargs) -> ak.Array:
    """
    reads out mur and muf uncertainties independently; documentation of the LHEScaleWeight columns:
    https://cms-nanoaod-integration.web.cern.ch/integration/master/mc94X_doc.html
    TODO: better documentation
    """

    # stop here for data
    if self.dataset_inst.is_data:
        return events

    N_scaleweights = ak.num(events.LHEScaleWeight, axis=1)
    if ak.any(N_scaleweights != 9):
        N_scaleweights = N_scaleweights[N_scaleweights != 9]
        raise Exception(f"Number of LHEScaleWeights ({N_scaleweights}) is not "
                        f"as expected (9) in dataset {self.dataset_inst.name}")

    if ak.any(events.LHEScaleWeight[:, 4]):
        raise Exception("The nominal LHEScaleWeight should always be 1")

    events = set_ak_column(events, "mur_weight", events.LHEScaleWeight[:, 4])
    events = set_ak_column(events, "mur_weight_up", events.LHEScaleWeight[:, 7])
    events = set_ak_column(events, "mur_weight_down", events.LHEScaleWeight[:, 1])
    events = set_ak_column(events, "muf_weight", events.LHEScaleWeight[:, 4])
    events = set_ak_column(events, "muf_weight_up", events.LHEScaleWeight[:, 5])
    events = set_ak_column(events, "muf_weight_down", events.LHEScaleWeight[:, 3])

    return events


@producer(
    uses={"LHEScaleWeight"},
    produces={"scale_weight", "scale_weight_up", "scale_weight_down"},
)
def scale_weights(self: Producer, events: ak.Array, **kwargs) -> ak.Array:
    """
    determines the scale up and down variations; documentation of the LHEScaleWeight columns:
    https://cms-nanoaod-integration.web.cern.ch/integration/master/mc94X_doc.html
    TODO: better documentation
    """

    # stop here for data
    if self.dataset_inst.is_data:
        return events

    N_scaleweights = ak.num(events.LHEScaleWeight, axis=1)
    # For now, make an exception for st_schannel_had dataset; should be fixed with NanoAODv10
    if ak.any(N_scaleweights != 9):
        N_scaleweights = N_scaleweights[N_scaleweights != 9]
        raise Exception(f"Number of LHEScaleWeights ({N_scaleweights}) is not "
                        f"as expected (9) in dataset {self.dataset_inst.name}")

    if ak.any(events.LHEScaleWeight[:, 4]):
        raise Exception("The nominal LHEScaleWeight should always be 1")

    events = set_ak_column(events, "scale_weight", events.LHEScaleWeight[:, 4])

    # for the up/down variations, take the max/min value of all possible combinations
    # except mur=2, muf=0.5 (index 2) and mur=0.5, muf=2 (index 6) into account
    idx_mask = (ak.local_index(events.LHEScaleWeight) != 2) & (ak.local_index(events.LHEScaleWeight) != 6)
    considered_scale_weights = events.LHEScaleWeight[idx_mask]
    events = set_ak_column(events, "scale_weight_down", ak.min(considered_scale_weights, axis=1))
    events = set_ak_column(events, "scale_weight_up", ak.max(considered_scale_weights, axis=1))

    return events
