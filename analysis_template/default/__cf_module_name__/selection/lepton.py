# coding: utf-8

"""
Lepton selection methods.
"""

from __future__ import annotations

from columnflow.util import maybe_import
from columnflow.selection import Selector, SelectionResult, selector
from columnflow.columnar_util import set_ak_column
from columnflow.util import DotDict

from hbt.config.util import Trigger


np = maybe_import("numpy")
ak = maybe_import("awkward")


def trigger_object_matching(
    vectors1: ak.Array,
    vectors2: ak.Array,
    threshold: float = 0.25,
    axis: int = 2,
) -> ak.Array | tuple[ak.Array, ak.Array]:
    """
    Helper to check per object in *vectors1* if there is at least one object in *vectors2* that
    leads to a delta R metric below *threshold*. The final reduction is applied over *axis* of the
    resulting metric table containing the full combinatorics. When *return_all_matches* is *True*,
    the matrix with all matching decisions is returned as well.
    """
    # delta_r for all combinations
    dr = vectors1.metric_table(vectors2)

    # check per element in vectors1 if there is at least one matching element in vectors2
    any_match = ak.any(dr < threshold, axis=axis)

    return any_match


@selector(
    uses={
        # nano columns
        "nElectron", "Electron.pt", "Electron.eta", "Electron.phi", "Electron.dxy", "Electron.dz",
        "Electron.pfRelIso03_all", "Electron.mvaIso_WP80", "Electron.mvaIso_WP90", "Electron.mvaNoIso_WP90",
        "nTrigObj", "TrigObj.pt", "TrigObj.eta", "TrigObj.phi",
        # <= nano v9 names
        "Electron.mvaFall17V2Iso_WP80", "Electron.mvaFall17V2Iso_WP90", "Electron.mvaFall17V2noIso_WP90",
    },
)
def electron_selection(
    self: Selector,
    events: ak.Array,
    trigger: Trigger,
    leg_masks: list[ak.Array],
    **kwargs,
) -> tuple[ak.Array, ak.Array]:
    """
    Electron selection returning two sets of indidces for default and veto electrons.
    See https://twiki.cern.ch/twiki/bin/view/CMS/EgammaNanoAOD?rev=4
    """
    is_single = trigger.has_tag("single_e")
    is_cross = trigger.has_tag("cross_e_tau")
    is_2016 = self.config_inst.campaign.x.year == 2016

    # start per-electron mask with trigger object matching
    if is_single:
        # catch config errors
        assert trigger.n_legs == len(leg_masks) == 1
        assert abs(trigger.legs[0].pdg_id) == 11
        # match leg 0
        matches_leg0 = trigger_object_matching(events.Electron, events.TrigObj[leg_masks[0]])
    elif is_cross:
        # catch config errors
        assert trigger.n_legs == len(leg_masks) == 2
        assert abs(trigger.legs[0].pdg_id) == 11
        # match leg 0
        matches_leg0 = trigger_object_matching(events.Electron, events.TrigObj[leg_masks[0]])

    # pt sorted indices for converting masks to indices
    sorted_indices = ak.argsort(events.Electron.pt, axis=-1, ascending=False)

    # obtain mva flags, which might be located at different routes, depending on the nano version
    if "mvaIso_WP80" in events.Electron.fields:
        # >= nano v10
        mva_iso_wp80 = events.Electron.mvaIso_WP80
        mva_iso_wp90 = events.Electron.mvaIso_WP90
        mva_noniso_wp90 = events.Electron.mvaNoIso_WP90
    else:
        # <= nano v9
        mva_iso_wp80 = events.Electron.mvaFall17V2Iso_WP80
        mva_iso_wp90 = events.Electron.mvaFall17V2Iso_WP90
        mva_noniso_wp90 = events.Electron.mvaFall17V2noIso_WP90

    # default electron mask, only required for single and cross triggers with electron leg
    default_mask = None
    default_indices = None
    if is_single or is_cross:
        min_pt = 26.0 if is_2016 else (33.0 if is_single else 25.0)
        default_mask = (
            (mva_iso_wp80 == 1) &
            (abs(events.Electron.eta) < 2.1) &
            (abs(events.Electron.dxy) < 0.045) &
            (abs(events.Electron.dz) < 0.2) &
            (events.Electron.pt > min_pt) &
            matches_leg0
        )
        # convert to sorted indices
        default_indices = sorted_indices[default_mask[sorted_indices]]
        default_indices = ak.values_astype(default_indices, np.int32)

    # veto electron mask
    veto_mask = (
        (
            (mva_iso_wp90 == 1) |
            ((mva_noniso_wp90 == 1) & (events.Electron.pfRelIso03_all < 0.3))
        ) &
        (abs(events.Electron.eta) < 2.5) &
        (abs(events.Electron.dxy) < 0.045) &
        (abs(events.Electron.dz) < 0.2) &
        (events.Electron.pt > 10.0)
    )
    # convert to sorted indices
    veto_indices = sorted_indices[veto_mask[sorted_indices]]
    veto_indices = ak.values_astype(veto_indices, np.int32)

    return default_indices, veto_indices


@selector(
    uses={
        # nano columns
        "nMuon", "Muon.pt", "Muon.eta", "Muon.phi", "Muon.mediumId", "Muon.tightId",
        "Muon.pfRelIso04_all", "Muon.dxy", "Muon.dz",
        "nTrigObj", "TrigObj.pt", "TrigObj.eta", "TrigObj.phi",
    },
)
def muon_selection(
    self: Selector,
    events: ak.Array,
    trigger: Trigger,
    leg_masks: list[ak.Array],
    **kwargs,
) -> tuple[ak.Array, ak.Array]:
    """
    Muon selection returning two sets of indidces for default and veto muons.

    References:

    - Isolation working point: https://twiki.cern.ch/twiki/bin/view/CMS/SWGuideMuonIdRun2?rev=59
    - ID und ISO : https://twiki.cern.ch/twiki/bin/view/CMS/MuonUL2017?rev=15
    """
    is_single = trigger.has_tag("single_mu")
    is_cross = trigger.has_tag("cross_mu_tau")
    is_2016 = self.config_inst.campaign.x.year == 2016

    # start per-muon mask with trigger object matching
    if is_single:
        # catch config errors
        assert trigger.n_legs == len(leg_masks) == 1
        assert abs(trigger.legs[0].pdg_id) == 13
        # match leg 0
        matches_leg0 = trigger_object_matching(events.Muon, events.TrigObj[leg_masks[0]])
    elif is_cross:
        # catch config errors
        assert trigger.n_legs == len(leg_masks) == 2
        assert abs(trigger.legs[0].pdg_id) == 13
        # match leg 0
        matches_leg0 = trigger_object_matching(events.Muon, events.TrigObj[leg_masks[0]])

    # pt sorted indices for converting masks to indices
    sorted_indices = ak.argsort(events.Muon.pt, axis=-1, ascending=False)

    # default muon mask, only required for single and cross triggers with muon leg
    default_mask = None
    default_indices = None
    if is_single or is_cross:
        if is_2016:
            min_pt = 23.0 if is_single else 20.0
        else:
            min_pt = 33.0 if is_single else 25.0
        default_mask = (
            (events.Muon.tightId == 1) &
            (abs(events.Muon.eta) < 2.1) &
            (abs(events.Muon.dxy) < 0.045) &
            (abs(events.Muon.dz) < 0.2) &
            (events.Muon.pfRelIso04_all < 0.15) &
            (events.Muon.pt > min_pt) &
            matches_leg0
        )
        # convert to sorted indices
        default_indices = sorted_indices[default_mask[sorted_indices]]
        default_indices = ak.values_astype(default_indices, np.int32)

    # veto muon mask
    veto_mask = (
        (events.Muon.mediumId == 1) &
        (abs(events.Muon.eta) < 2.4) &
        (abs(events.Muon.dxy) < 0.045) &
        (abs(events.Muon.dz) < 0.2) &
        (events.Muon.pfRelIso04_all < 0.3) &
        (events.Muon.pt > 10)
    )
    # convert to sorted indices
    veto_indices = sorted_indices[veto_mask[sorted_indices]]
    veto_indices = ak.values_astype(veto_indices, np.int32)

    return default_indices, veto_indices


@selector(
    uses={
        # nano columns
        "nTau", "Tau.pt", "Tau.eta", "Tau.phi", "Tau.dz", "Tau.idDeepTau2017v2p1VSe",
        "Tau.idDeepTau2017v2p1VSmu", "Tau.idDeepTau2017v2p1VSjet",
        "nTrigObj", "TrigObj.pt", "TrigObj.eta", "TrigObj.phi",
        "nElectron", "Electron.pt", "Electron.eta", "Electron.phi",
        "nMuon", "Muon.pt", "Muon.eta", "Muon.phi",
    },
    # shifts are declared dynamically below in tau_selection_init
)
def tau_selection(
    self: Selector,
    events: ak.Array,
    trigger: Trigger,
    leg_masks: list[ak.Array],
    electron_indices: ak.Array,
    muon_indices: ak.Array,
    **kwargs,
) -> tuple[ak.Array, ak.Array]:
    """
    Tau selection returning a set of indices for taus that are at least VVLoose isolated (vs jet)
    and a second mask to select the action Medium isolated ones, eventually to separate normal and
    iso inverted taus for QCD estimations.

    TODO: there is no decay mode selection yet, but this should be revisited!
    """
    is_single_e = trigger.has_tag("single_e")
    is_single_mu = trigger.has_tag("single_mu")
    is_cross_e = trigger.has_tag("cross_e_tau")
    is_cross_mu = trigger.has_tag("cross_mu_tau")
    is_cross_tau = trigger.has_tag("cross_tau_tau")
    is_cross_tau_vbf = trigger.has_tag("cross_tau_tau_vbf")
    is_any_cross_tau = is_cross_tau or is_cross_tau_vbf
    is_2016 = self.config_inst.campaign.x.year == 2016
    # tau id v2.1 working points (binary to int transition after nano v10)
    if self.config_inst.campaign.x.version < 10:
        # https://cms-nanoaod-integration.web.cern.ch/integration/master/mc94X_doc.html
        tau_vs_e = DotDict(vvloose=2, vloose=4)
        tau_vs_mu = DotDict(vloose=1, tight=8)
        tau_vs_jet = DotDict(vvloose=2, medium=16)
    else:
        # https://cms-nanoaod-integration.web.cern.ch/integration/cms-swmaster/data106Xul17v2_v10_doc.html#Tau
        tau_vs_e = DotDict(vvloose=2, vloose=3)
        tau_vs_mu = DotDict(vloose=1, tight=4)
        tau_vs_jet = DotDict(vvloose=2, medium=5)

    # start per-tau mask with trigger object matching per leg
    if is_cross_e or is_cross_mu:
        # catch config errors
        assert trigger.n_legs == len(leg_masks) == 2
        assert abs(trigger.legs[1].pdg_id) == 15
        # match leg 1
        matches_leg1 = trigger_object_matching(events.Tau, events.TrigObj[leg_masks[1]])
    elif is_cross_tau or is_cross_tau_vbf:
        # catch config errors
        assert trigger.n_legs == len(leg_masks) >= 2
        assert abs(trigger.legs[0].pdg_id) == 15
        assert abs(trigger.legs[1].pdg_id) == 15
        # match both legs
        matches_leg0 = trigger_object_matching(events.Tau, events.TrigObj[leg_masks[0]])
        matches_leg1 = trigger_object_matching(events.Tau, events.TrigObj[leg_masks[1]])

    # determine minimum pt and maximum eta
    if is_single_e or is_single_mu:
        min_pt = 20.0
        max_eta = 2.3
    elif is_cross_e:
        # only existing after 2016, so force a failure in case of misconfiguration
        min_pt = None if is_2016 else 35.0
        max_eta = 2.1
    elif is_cross_mu:
        min_pt = 25.0 if is_2016 else 32.0
        max_eta = 2.1
    elif is_cross_tau:
        min_pt = 40.0
        max_eta = 2.1
    elif is_cross_tau_vbf:
        # only existing after 2016, so force in failure in case of misconfiguration
        min_pt = None if is_2016 else 25.0
        max_eta = 2.1

    # base tau mask for default and qcd sideband tau
    base_mask = (
        (abs(events.Tau.eta) < max_eta) &
        (events.Tau.pt > min_pt) &
        (abs(events.Tau.dz) < 0.2) &
        (events.Tau.idDeepTau2017v2p1VSe >= (tau_vs_e.vvloose if is_any_cross_tau else tau_vs_e.vloose)) &
        (events.Tau.idDeepTau2017v2p1VSmu >= (tau_vs_mu.vloose if is_any_cross_tau else tau_vs_mu.tight)) &
        (events.Tau.idDeepTau2017v2p1VSjet >= tau_vs_jet.vvloose)
    )

    # remove taus with too close spatial separation to previously selected leptons
    if electron_indices is not None:
        base_mask = base_mask & ak.all(events.Tau.metric_table(events.Electron[electron_indices]) > 0.5, axis=2)
    if muon_indices is not None:
        base_mask = base_mask & ak.all(events.Tau.metric_table(events.Muon[muon_indices]) > 0.5, axis=2)

    # add trigger object masks
    if is_cross_e or is_cross_mu:
        base_mask = base_mask & matches_leg1
    elif is_cross_tau or is_cross_tau_vbf:
        # taus need to be matched to at least one leg, but as a side condition
        # each leg has to have at least one match to a tau
        base_mask = base_mask & (
            (matches_leg0 | matches_leg1) &
            ak.any(matches_leg0, axis=1) &
            ak.any(matches_leg1, axis=1)
        )

    # indices for sorting first by isolation, then by pt
    # for this, combine iso and pt values, e.g. iso 255 and pt 32.3 -> 2550032.3
    f = 10 ** (np.ceil(np.log10(ak.max(events.Tau.pt))) + 1)
    sort_key = events.Tau.idDeepTau2017v2p1VSjet * f + events.Tau.pt
    sorted_indices = ak.argsort(sort_key, axis=-1, ascending=False)

    # convert to sorted indices
    base_indices = sorted_indices[base_mask[sorted_indices]]
    base_indices = ak.values_astype(base_indices, np.int32)

    # additional mask to select final, Medium isolated taus
    iso_mask = events.Tau[base_indices].idDeepTau2017v2p1VSjet >= tau_vs_jet.medium

    return base_indices, iso_mask


@tau_selection.init
def tau_selection_init(self: Selector) -> None:
    # register tec shifts
    self.shifts |= {
        shift_inst.name
        for shift_inst in self.config_inst.shifts
        if shift_inst.has_tag("tec")
    }


@selector(
    uses={
        electron_selection, muon_selection, tau_selection,
        # nano columns
        "event", "Electron.charge", "Muon.charge", "Tau.charge", "Electron.mass", "Muon.mass",
        "Tau.mass",
    },
    produces={
        electron_selection, muon_selection, tau_selection,
        # new columns
        "channel_id", "leptons_os", "tau2_isolated", "single_triggered", "cross_triggered",
    },
)
def lepton_selection(
    self: Selector,
    events: ak.Array,
    trigger_results: SelectionResult,
    **kwargs,
) -> tuple[ak.Array, SelectionResult]:
    """
    Combined lepton selection.
    """
    # get channels from the config
    ch_etau = self.config_inst.get_channel("etau")
    ch_mutau = self.config_inst.get_channel("mutau")
    ch_tautau = self.config_inst.get_channel("tautau")

    # prepare vectors for output vectors
    false_mask = (abs(events.event) < 0)
    channel_id = np.uint8(1) * false_mask
    tau2_isolated = false_mask
    leptons_os = false_mask
    single_triggered = false_mask
    cross_triggered = false_mask
    empty_indices = ak.zeros_like(1 * events.event, dtype=np.uint16)[..., None][..., :0]
    sel_electron_indices = empty_indices
    sel_muon_indices = empty_indices
    sel_tau_indices = empty_indices

    # perform each lepton election step separately per trigger
    for trigger, trigger_fired, leg_masks in trigger_results.x.trigger_data:
        is_single = trigger.has_tag("single_trigger")
        is_cross = trigger.has_tag("cross_trigger")

        # electron selection
        electron_indices, electron_veto_indices = self[electron_selection](
            events,
            trigger,
            leg_masks,
            call_force=True,
            **kwargs,
        )

        # muon selection
        muon_indices, muon_veto_indices = self[muon_selection](
            events,
            trigger,
            leg_masks,
            call_force=True,
            **kwargs,
        )

        # tau selection
        tau_indices, tau_iso_mask = self[tau_selection](
            events,
            trigger,
            leg_masks,
            electron_indices,
            muon_indices,
            call_force=True,
            **kwargs,
        )

        # lepton pair selecton per trigger via lepton counting
        if trigger.has_tag({"single_e", "cross_e_tau"}):
            # expect 1 electron, 1 veto electron (the same one), 0 veto muons, and at least one tau
            is_etau = (
                trigger_fired &
                (ak.num(electron_indices, axis=1) == 1) &
                (ak.num(electron_veto_indices, axis=1) == 1) &
                (ak.num(muon_veto_indices, axis=1) == 0) &
                (ak.num(tau_indices, axis=1) >= 1)
            )
            is_iso = ak.sum(tau_iso_mask, axis=1) >= 1
            # determine the os/ss charge sign relation
            e_charge = ak.firsts(events.Electron[electron_indices].charge, axis=1)
            tau_charge = ak.firsts(events.Tau[tau_indices].charge, axis=1)
            is_os = e_charge == -tau_charge
            # store global variables
            where = (channel_id == 0) & is_etau
            channel_id = ak.where(where, ch_etau.id, channel_id)
            tau2_isolated = ak.where(where, is_iso, tau2_isolated)
            leptons_os = ak.where(where, is_os, leptons_os)
            single_triggered = ak.where(where & is_single, True, single_triggered)
            cross_triggered = ak.where(where & is_cross, True, cross_triggered)
            sel_electron_indices = ak.where(where, electron_indices, sel_electron_indices)
            sel_tau_indices = ak.where(where, tau_indices, sel_tau_indices)

        elif trigger.has_tag({"single_mu", "cross_mu_tau"}):
            # expect 1 muon, 1 veto muon (the same one), 0 veto electrons, and at least one tau
            is_mutau = (
                trigger_fired &
                (ak.num(muon_indices, axis=1) == 1) &
                (ak.num(muon_veto_indices, axis=1) == 1) &
                (ak.num(electron_veto_indices, axis=1) == 0) &
                (ak.num(tau_indices, axis=1) >= 1)
            )
            is_iso = ak.sum(tau_iso_mask, axis=1) >= 1
            # determine the os/ss charge sign relation
            mu_charge = ak.firsts(events.Muon[muon_indices].charge, axis=1)
            tau_charge = ak.firsts(events.Tau[tau_indices].charge, axis=1)
            is_os = mu_charge == -tau_charge
            # store global variables
            where = (channel_id == 0) & is_mutau
            channel_id = ak.where(where, ch_mutau.id, channel_id)
            tau2_isolated = ak.where(where, is_iso, tau2_isolated)
            leptons_os = ak.where(where, is_os, leptons_os)
            single_triggered = ak.where(where & is_single, True, single_triggered)
            cross_triggered = ak.where(where & is_cross, True, cross_triggered)
            sel_muon_indices = ak.where(where, muon_indices, sel_muon_indices)
            sel_tau_indices = ak.where(where, tau_indices, sel_tau_indices)

        elif trigger.has_tag({"cross_tau_tau", "cross_tau_tau_vbf"}):
            # expect 0 veto electrons, 0 veto muons and at least two taus of which one is isolated
            is_tautau = (
                trigger_fired &
                (ak.num(electron_veto_indices, axis=1) == 0) &
                (ak.num(muon_veto_indices, axis=1) == 0) &
                (ak.num(tau_indices, axis=1) >= 2) &
                (ak.sum(tau_iso_mask, axis=1) >= 1)
            )

            # special case for cross tau vbf trigger:
            # to avoid overlap, with non-vbf triggers, only one tau is allowed to have pt > 40
            if trigger.has_tag("cross_tau_tau_vbf"):
                is_tautau = is_tautau & (ak.num(events.Tau[tau_indices].pt > 40, axis=1) <= 1)

            is_iso = ak.sum(tau_iso_mask, axis=1) >= 2
            # tau_indices are sorted by highest isolation as cond. 1 and highest pt as cond. 2, so
            # the first two indices are exactly those selected by the full-blown pairing algorithm
            # and there is no need here to apply it again :)
            # determine the os/ss charge sign relation
            tau1_charge = ak.firsts(events.Tau[tau_indices].charge, axis=1)
            tau2_charge = ak.firsts(events.Tau[tau_indices].charge[..., 1:], axis=1)
            is_os = tau1_charge == -tau2_charge
            # store global variables
            where = (channel_id == 0) & is_tautau
            channel_id = ak.where(where, ch_tautau.id, channel_id)
            tau2_isolated = ak.where(where, is_iso, tau2_isolated)
            leptons_os = ak.where(where, is_os, leptons_os)
            single_triggered = ak.where(where & is_single, True, single_triggered)
            cross_triggered = ak.where(where & is_cross, True, cross_triggered)
            sel_tau_indices = ak.where(where, tau_indices, sel_tau_indices)

    # some final type conversions
    channel_id = ak.values_astype(channel_id, np.uint8)
    leptons_os = ak.fill_none(leptons_os, False)
    sel_electron_indices = ak.values_astype(sel_electron_indices, np.int32)
    sel_muon_indices = ak.values_astype(sel_muon_indices, np.int32)
    sel_tau_indices = ak.values_astype(sel_tau_indices, np.int32)

    # save new columns
    events = set_ak_column(events, "channel_id", channel_id)
    events = set_ak_column(events, "leptons_os", leptons_os)
    events = set_ak_column(events, "tau2_isolated", tau2_isolated)
    events = set_ak_column(events, "single_triggered", single_triggered)
    events = set_ak_column(events, "cross_triggered", cross_triggered)

    return events, SelectionResult(
        steps={
            "lepton": channel_id != 0,
        },
        objects={
            "Electron": {
                "Electron": sel_electron_indices,
            },
            "Muon": {
                "Muon": sel_muon_indices,
            },
            "Tau": {
                "Tau": sel_tau_indices,
            },
        },
        aux={
            # save the selected lepton pair for the duration of the selection
            # multiplication of a coffea particle with 1 yields the lorentz vector
            "lepton_pair": ak.concatenate(
                [
                    events.Electron[sel_electron_indices] * 1,
                    events.Muon[sel_muon_indices] * 1,
                    events.Tau[sel_tau_indices] * 1,
                ],
                axis=1,
            ),
        },
    )
