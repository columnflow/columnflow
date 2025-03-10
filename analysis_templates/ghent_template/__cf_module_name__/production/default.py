
"""
Column production methods related to higher-level features.
"""

from columnflow.production import Producer, producer
from columnflow.util import maybe_import, four_vec
from columnflow.columnar_util import set_ak_column

from columnflow.production.categories import category_ids

from __cf_short_name_lc__.production.weights import event_weights
from __cf_short_name_lc__.config.categories import add_categories_production

np = maybe_import("numpy")
ak = maybe_import("awkward")
coffea = maybe_import("coffea")
maybe_import("coffea.nanoevents.methods.nanoaod")


@producer(
    uses=({
        category_ids,
        event_weights,
    } | four_vec(
        {"Electron", "Muon", }
    ) | four_vec(
        {"Jet"},
        {"btagDeepFlavB"}
    )
    ),
    produces=({
        category_ids, event_weights,
        "ht", "n_jet", "n_electron", "n_muon", "n_bjet"}),
)
def default(self: Producer, events: ak.Array, **kwargs) -> ak.Array:

    # add event weights
    if self.dataset_inst.is_mc:
        events = self[event_weights](events, **kwargs)

    # (re)produce category i
    events = self[category_ids](events, **kwargs)

    events = set_ak_column(events, "ht", ak.sum(events.Jet.pt, axis=1), value_type=np.float32)
    events = set_ak_column(events, "n_jet", ak.sum(events.Jet.pt > 0, axis=1))
    events = set_ak_column(events, "n_bjet", ak.sum(events.Jet.btagDeepFlavB >=
                           self.config_inst.x.btag_working_points.deepjet.medium, axis=1))
    events = set_ak_column(events, "n_electron", ak.sum(events.Electron.pt > 0, axis=1))
    events = set_ak_column(events, "n_muon", ak.sum(events.Muon.pt > 0, axis=1))

    return events


@default.init
def default_init(self: Producer) -> None:
    # add categories to config
    add_categories_production(self.config_inst)
