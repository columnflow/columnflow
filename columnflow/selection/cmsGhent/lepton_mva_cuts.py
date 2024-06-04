# coding: utf-8

"""
Selection modules for object selection of Muon, Electron, and Jet.
"""

# from collections import defaultdict
from typing import Tuple, Literal, Dict

# import law

from columnflow.util import maybe_import, four_vec
from columnflow.columnar_util import set_ak_column, optional_column
# from columnflow.production.util import attach_coffea_behavior
from columnflow.selection import Selector, SelectionResult, selector
from columnflow.selection.util import masked_sorted_indices

ak = maybe_import("awkward")


@selector(
    uses=(
        four_vec({"Electron", "Muon"}, {"dxy", "dz", "sip3d", "miniPFRelIso_all"}) |
        {"Electron.lostHits", "Electron.deltaEtaSC", "Muon.mediumId"} |
        optional_column("Electron.mvaTOP", "Muon.mvaTOP")
    ),
)
def lepton_mva_object(
        self: Selector,
        events: ak.Array,
        working_point: 'Dict[Literal["Muon", "Electron"], str] | str' = "veto",
        **kwargs,
) -> Tuple[ak.Array, SelectionResult]:
    """
    The following cuts are the cuts that are required to be able to use the lepton MVA. Leptons that are
    passing these cuts are referred to as "veto" leptons.
    No additional cuts should be applied for the available scale factors to apply, except on p_T and eta.

    :param events: Array containing events in the NanoAOD format
    :param working_point: name of the working_point or dict mapping leptons to working points to apply to the muons
    and electrons outputted in the SelectionResult
    :return: Tuple containing the events array and a :py:class:`~columnflow.selection.SelectionResult`
    with selected Muon and Electron objects passing **working_point**. The event array has extra Muon and Electron
    boolean fields for the veto definition, as well as the TOP mva working points if the mvaTOP field is present in the
    event.Muon and event.Electron fields

    """
    if isinstance(working_point, str):
        working_point = {lepton: working_point for lepton in ["Muon", "Electron"]}
    if set(working_point.values()) != {"veto"}:
        assert working_point in self.config_inst.x.top_mva_wps
        assert "mvaTOP" in events.Electron.fields
        assert "mvaTOP" in events.Muon.fields

    # conditions differing for muons and leptons
    ele, mu = events.Electron, events.Muon
    ele_absetaSC = abs(ele.eta + ele.deltaEtaSC)
    masks = {
        "Electron": (abs(ele.eta) < 2.5) & (ele.lostHits < 2) & ((ele_absetaSC > 1.5560) | (ele_absetaSC < 1.4442)),
        "Muon": (abs(mu.eta) < 2.4) & mu.mediumId,
    }

    # conditions shared for muons and leptons
    for lepton_name in masks:
        lepton = events[lepton_name]
        veto_mask = masks[lepton_name] & (
            (lepton.pt > 10) &
            (lepton.miniPFRelIso_all < 0.4) &
            (lepton.sip3d < 8) &
            (lepton.dz < 0.1) &
            (lepton.dxy < 0.05)
        )
        events = set_ak_column(events, f"{lepton_name}.veto", veto_mask)
        if "mvaTOP" in lepton.fields:
            wps = self.config_inst.x.top_mva_wps
            for wp in wps:
                events = set_ak_column(
                    events, f"{lepton_name}.{wp}",
                    events[lepton_name]["veto"] & (lepton.mvaTOP > wps[wp])
                )

    return events, SelectionResult(
        steps={},
        objects={
            lep:
                {lep: masked_sorted_indices(events[lep][working_point[lep]], events[lep].pt)}
            for lep in ["Muon", "Electron"]
        },
    )
