# coding: utf-8
from __future__ import annotations

from columnflow.selection import Selector, SelectionResult, selector
from columnflow.util import maybe_import

np = maybe_import("numpy")
ak = maybe_import("awkward")


def add_triggers(cfg: od.Config, campaign: od.Campaign):
    cfg.x.trigger_matrix = [
        (
            "EGamma", {
                "Ele32_WPTight_Gsf",
                "Ele115_CaloIdVT_GsfTrkIdT",
                "Ele23_Ele12_CaloIdL_TrackIdL_IsoVL",
                "DoubleEle25_CaloIdL_MW",
                "Ele16_Ele12_Ele8_CaloIdL_TrackIdL",
            },
        ),
        (
            "DoubleMuon", {
                "Mu37_TkMu27",
                "Mu17_TrkIsoVVL_Mu8_TrkIsoVVL_DZ_Mass3p8",
            },
        ),
        (
            "MuonEG", {
                "Mu23_TrkIsoVVL_Ele12_CaloIdL_TrackIdL_IsoVL_DZ",
                "Mu8_TrkIsoVVL_Ele23_CaloIdL_TrackIdL_IsoVL_DZ",
                "Mu12_TrkIsoVVL_Ele23_CaloIdL_TrackIdL_IsoVL_DZ",
                "Mu23_TrkIsoVVL_Ele12_CaloIdL_TrackIdL_IsoVL",
                "Mu27_Ele37_CaloIdL_MW",
                "Mu37_Ele27_CaloIdL_MW",
            },
        ),
        (
            "SingleMuon", {
                "IsoMu24",
                "IsoMu27",
                "Mu50",
                "OldMu100",
                "TkMu100",
            },
        ),
    ]

    cfg.x.all_triggers = {
        trigger
        for _, triggers in cfg.x.trigger_matrix
        for trigger in triggers
    }


@selector
def trigger_selection(
    self: Selector,
    events: ak.Array,
    **kwargs,
) -> tuple[ak.Array, SelectionResult]:

    # start with an all-false mask
    sel_trigger = ak.Array(np.zeros(len(events), dtype=bool))
    veto_trigger = ak.Array(np.zeros(len(events), dtype=bool))

    # pick events that passed one of the required triggers

    for trigger in self.dataset_inst.x("require_triggers", []):
        sel_trigger = sel_trigger | events.HLT[trigger]

    # but reject events that also passed one of the triggers to veto
    for trigger in self.dataset_inst.x("veto_triggers", []):
        veto_trigger = veto_trigger & ~events.HLT[trigger]
        sel_trigger = sel_trigger & ~events.HLT[trigger]

    return events, SelectionResult(
        steps={
            "Trigger": sel_trigger, "VetoTrigger": veto_trigger
        },
    )


@trigger_selection.init
def trigger_selection_init(self: Selector) -> None:
    # return immediately if config object has not been loaded yet
    if not getattr(self, "config_inst", None):
        return

    # add HLT trigger bits to uses
    self.uses |= {
        f"HLT.{trigger}"
        for trigger in self.config_inst.x.all_triggers
    }
