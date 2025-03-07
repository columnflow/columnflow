# coding: utf-8

"""
Column production methods related to higher-level features.
"""

from functools import partial

from columnflow.production import Producer, producer
from columnflow.production.categories import category_ids
from columnflow.production.normalization import normalization_weights
from columnflow.production.cms.seeds import deterministic_seeds
from columnflow.production.cms.mc_weight import mc_weight
from columnflow.production.cms.muon import muon_weights
from columnflow.selection.util import create_collections_from_masks
from columnflow.util import maybe_import
from columnflow.columnar_util import EMPTY_FLOAT, Route, set_ak_column
from columnflow.production.util import attach_coffea_behavior

np = maybe_import("numpy")
ak = maybe_import("awkward")


# helper
set_ak_f32 = partial(set_ak_column, value_type=np.float32)


@producer(
    uses={
        # nano columns
        attach_coffea_behavior, "Jet.{pt,eta,phi,mass}",
    },
    produces={
        # new columns
        attach_coffea_behavior, "ht", "n_jet", "dijet.{pt,mass,dr}",
    },
)
def jet_features(self: Producer, events: ak.Array, **kwargs) -> ak.Array:

    # event observables
    events = set_ak_column(events, "ht", ak.sum(events.Jet.pt, axis=1))
    events = set_ak_column(events, "n_jet", ak.num(events.Jet.pt, axis=1), value_type=np.int32)

    # attach coffea behaviour
    events = self[attach_coffea_behavior](events, collections={}, **kwargs)
    # object padding (Note that after padding, ak.num(events.Jet.pt, axis=1) would always be >= 2)
    events = set_ak_column(events, "Jet", ak.pad_none(events.Jet, 2))

    # dijet features
    dijet = ak.with_name(events.Jet[:, 0] + events.Jet[:, 1], "Jet")
    dijet = set_ak_f32(dijet, "dr", events.Jet[:, 0].delta_r(events.Jet[:, 1]))
    dijet_columns = ("pt", "mass", "dr")
    for col in dijet_columns:
        events = set_ak_f32(events, f"dijet.{col}", ak.fill_none(getattr(dijet, col), EMPTY_FLOAT))

    return events


@producer(
    uses={
        mc_weight, category_ids,
        # nano columns
        "Jet.pt",
    },
    produces={
        mc_weight, category_ids,
        # new columns
        "cutflow.jet1_pt",
    },
)
def cutflow_features(
    self: Producer,
    events: ak.Array,
    object_masks: dict[str, dict[str, ak.Array]],
    **kwargs,
) -> ak.Array:
    if self.dataset_inst.is_mc:
        events = self[mc_weight](events, **kwargs)

    # apply object masks and create new collections
    reduced_events = create_collections_from_masks(events, object_masks)

    # create category ids per event and add categories back to the
    events = self[category_ids](reduced_events, target_events=events, **kwargs)

    # add cutflow columns
    events = set_ak_column(
        events,
        "cutflow.jet1_pt",
        Route("Jet.pt[:,0]").apply(events, EMPTY_FLOAT),
    )

    return events


@producer(
    uses={
        jet_features, category_ids, normalization_weights, muon_weights, deterministic_seeds,
    },
    produces={
        jet_features, category_ids, normalization_weights, muon_weights, deterministic_seeds,
    },
)
def example(self: Producer, events: ak.Array, **kwargs) -> ak.Array:
    # jet_features
    events = self[jet_features](events, **kwargs)

    # category ids
    events = self[category_ids](events, **kwargs)

    # deterministic seeds
    events = self[deterministic_seeds](events, **kwargs)

    # mc-only weights
    if self.dataset_inst.is_mc:
        # normalization weights
        events = self[normalization_weights](events, **kwargs)

        # muon weights
        events = self[muon_weights](events, **kwargs)

    return events
