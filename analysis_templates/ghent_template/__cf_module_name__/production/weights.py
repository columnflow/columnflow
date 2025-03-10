# coding: utf-8

"""
Column production methods related to generic event weights.
"""

from columnflow.util import maybe_import
from columnflow.selection import SelectionResult
from columnflow.production import Producer, producer
from columnflow.production.cms.pileup import pu_weight
from columnflow.production.normalization import normalization_weights
from columnflow.production.cms.electron import electron_weights
from columnflow.production.cms.muon import muon_weights
from columnflow.production.cms.scale import murmuf_weights, murmuf_envelope_weights
from columnflow.production.cms.pdf import pdf_weights
from __cf_short_name_lc__.production.normalized_weights import normalized_weight_factory

np = maybe_import("numpy")
ak = maybe_import("awkward")


@producer(
    uses={
        pu_weight,
        murmuf_envelope_weights,
        murmuf_weights,
        pdf_weights
    },
    produces={pu_weight},
    mc_only=True,
)
def event_weights_to_normalize(self: Producer, events: ak.Array, results: SelectionResult, **kwargs) -> ak.Array:
    """
    Wrapper of several event weight producers that are typically called as part of SelectEvents
    since it is required to normalize them before applying certain event selections.
    """

    # compute pu weights

    events = self[pu_weight](events, **kwargs)

    # skip scale/pdf weights for some datasets (missing columns)
    if not self.dataset_inst.has_tag("skip_scale"):
        # compute scale weights
        events = self[murmuf_envelope_weights](events, **kwargs)

        # read out mur and weights
        events = self[murmuf_weights](events, **kwargs)

    if not self.dataset_inst.has_tag("skip_pdf"):
        # compute pdf weights
        events = self[pdf_weights](
            events,
            outlier_action="remove",
            outlier_log_mode="warning",
            **kwargs,
        )

    return events


@event_weights_to_normalize.init
def event_weights_to_normalize_init(self) -> None:
    if not getattr(self, "dataset_inst", None):
        return

    if not self.dataset_inst.has_tag("skip_scale"):
        self.uses |= {murmuf_envelope_weights, murmuf_weights}
        self.produces |= {murmuf_envelope_weights, murmuf_weights}

    if not self.dataset_inst.has_tag("skip_pdf"):
        self.uses |= {pdf_weights}
        self.produces |= {pdf_weights}


normalized_scale_weights = normalized_weight_factory(
    producer_name="normalized_scale_weights",
    weight_producers={murmuf_envelope_weights, murmuf_weights},
)

normalized_pdf_weights = normalized_weight_factory(
    producer_name="normalized_pdf_weights",
    weight_producers={pdf_weights},
    add_uses={"pdf_weight{,_up,_down}"},
)

normalized_pu_weights = normalized_weight_factory(
    producer_name="normalized_pu_weights",
    weight_producers={pu_weight},
)


@producer(
    uses={
        normalization_weights, electron_weights, muon_weights,
        normalized_pu_weights,
    },
    produces={
        normalization_weights, electron_weights, muon_weights,
        normalized_pu_weights,
    },
    mc_only=True,
)
def event_weights(self: Producer, events: ak.Array, **kwargs) -> ak.Array:
    """
    Wrapper of several event weight producers that are typically called in ProduceColumns.
    """
    # compute normalization weights

    events = self[normalization_weights](events, **kwargs)

    # compute electron and muon SF weights
    events = self[electron_weights](events, **kwargs)
    events = self[muon_weights](events, **kwargs)

    # normalize event weights using stats
    events = self[normalized_pu_weights](events, **kwargs)

    if not self.dataset_inst.has_tag("skip_scale"):
        events = self[normalized_scale_weights](events, **kwargs)

    if not self.dataset_inst.has_tag("skip_pdf"):
        events = self[normalized_pdf_weights](events, **kwargs)

    return events


@event_weights.init
def event_weights_init(self: Producer) -> None:
    if not getattr(self, "dataset_inst", None):
        return

    if not self.dataset_inst.has_tag("skip_scale"):
        self.uses |= {normalized_scale_weights}
        self.produces |= {normalized_scale_weights}

    if not self.dataset_inst.has_tag("skip_pdf"):
        self.uses |= {normalized_pdf_weights}
        self.produces |= {normalized_pdf_weights}
