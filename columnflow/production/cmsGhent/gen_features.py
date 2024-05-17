from collections import defaultdict
from typing import Tuple

import law

from columnflow.util import maybe_import, four_vec
from columnflow.columnar_util import set_ak_column
from columnflow.production import Producer, producer
from columnflow.columnar_util_Ghent import TetraVec

np = maybe_import("numpy")
ak = maybe_import("awkward")
coffea = maybe_import("coffea")


def _geometric_matching(particles1: ak.Array, particles2: ak.Array) -> (ak.Array, ak.Array):
    """
    Returns two awkward arrays.
    First contains that for each particle in **particles** the closest particle in the same event in **particles2**.
    Second tells you whether the found closest particle is contained within a cone of 0.2.
    """
    particles1, particles2 = ak.unzip(ak.cartesian([particles1, particles2], axis=1, nested=True))
    dr = particles1.delta_r(particles2)
    drmin_idx = ak.argmin(dr, axis=-1, keepdims=True)
    drmin = ak.flatten(dr[drmin_idx], axis=2)
    closest_match = ak.flatten(particles2[drmin_idx], axis=2)
    return closest_match, ak.fill_none(drmin < 0.2, False)


# map of the status flag name to the corresponding bit in statusFlags
_statusmap = ({
        "isPrompt": 0,
        "isDecayedLeptonHadron": 1,
        "isTauDecayProduct": 2,
        "isPromptTauDecayProduct": 3,
        "isDirectTauDecayProduct": 4,
        "isDirectPromptTauDecayProduct": 5,
        "isDirectHadronDecayProduct": 6,
        "isHardProcess": 7,
        "fromHardProcess": 8,
        "isHardProcessTauDecayProduct": 9,
        "isDirectHardProcessTauDecayProduct": 10,
        "fromHardProcessBeforeFSR": 11,
        "isFirstCopy": 12,
        "isLastCopy": 13,
        "isLastCopyBeforeFSR": 14,
    })

# status flags that should be present for a prompt genparticle
_prompt_status = ["isPrompt", "isDirectPromptTauDecayProduct", "isHardProcess",
                 "fromHardProcess", "fromHardProcessBeforeFSR"]


@producer(
    uses=four_vec(
        ("Electron", "Muon"),
        ("pdgId", "genPartIdx")) |
    four_vec(
        ("GenPart"),
        ("pdgId", "status", "statusFlags")
    ),
    produces=four_vec(
        {"Electron", "Muon"},
        {"isPrompt", "matchPdgId", "isChargeFlip"}
    ),
    mc_only=True,
    exposed=False,
)
def lepton_gen_features(
    self: Producer,
    events: ak.Array,
    **kwargs,
) -> ak.Array:

    genpart = events.GenPart

    for name, abs_pdgId in (("Electron", 11), ("Muon", 13)):

        lepton = events[name]

        # first check if already has a matched gen particle (include charge matching)
        is_nanoAOD_matched = (lepton.genPartIdx >= 0)
        is_nanoAOD_charge_matched = is_nanoAOD_matched & (lepton.pdgId == genpart.pdgId[lepton.genPartIdx])
        matched_genpart = genpart[lepton.genPartIdx]

        # if this fails apply geometric matching to stable leptons and photons

        # select stable gen particles
        stable_genpart = genpart[genpart.status == 1]

        # first look for closest mathing generator lepton within cone of 0.2
        gen_abs_pdgId = abs(stable_genpart.pdgId)
        geom_match_lepton, lepton_within_cone = _geometric_matching(lepton, stable_genpart[gen_abs_pdgId == abs_pdgId])

        # if not within cone of 0.2, allow for a photon match
        geom_match_photon, photon_within_cone = _geometric_matching(lepton, stable_genpart[gen_abs_pdgId == 22])

        # finally apply hierarchy to determine matched gen particle
        match = ak.Array(ak.zeros_like(geom_match_photon))
        match = ak.where(photon_within_cone, geom_match_photon, match)
        match = ak.where(lepton_within_cone, geom_match_lepton, match)
        match = ak.where(is_nanoAOD_charge_matched, matched_genpart, match)

        # check for matched gen particle if it fulfills all status flags for being prompt
        match_isPrompt = False
        for status in _prompt_status:
            match_isPrompt = match_isPrompt | (match.statusFlags & (1 << _statusmap[status]) != 0)

        valid_match = is_nanoAOD_matched | lepton_within_cone | photon_within_cone
        match_pdgId = (match.pdgId == lepton.pdgId) & valid_match
        is_chargeflip = (match.pdgId == -lepton.pdgId) & valid_match

        events = set_ak_column(events, f"{name}.isPrompt", ak.fill_none(match_isPrompt, False, axis=-1))
        events = set_ak_column(events, f"{name}.matchPdgId", ak.fill_none(match_pdgId, False, axis=-1))
        events = set_ak_column(events, f"{name}.isChargeFlip", ak.fill_none(is_chargeflip, False, axis=-1))

    return events


@producer(
    uses=four_vec(
        ("Electron", "Muon"),
        ("pdgId", "genPartIdx")) |
    four_vec(
        ("GenPart"),
        ("pdgId", "status", "statusFlags")
    ),
    produces=four_vec(
        {"Electron", "Muon"},
        {"isPromptJules", "matchPdgIdJules", "isChargeFlipJules"}
    ),
    mc_only=True,
    exposed=False,
)
def lepton_gen_features_jules(
    self: Producer,
    events: ak.Array,
    **kwargs,
) -> ak.Array:

    electron = (events.Electron)
    muon = (events.Muon)
    genpart = (events.GenPart)

    statusmap = ({
        "isPrompt": 0,
        "isDecayedLeptonHadron": 1,
        "isTauDecayProduct": 2,
        "isPromptTauDecayProduct": 3,
        "isDirectTauDecayProduct": 4,
        "isDirectPromptTauDecayProduct": 5,
        "isDirectHadronDecayProduct": 6,
        "isHardProcess": 7,
        "fromHardProcess": 8,
        "isHardProcessTauDecayProduct": 9,
        "isDirectHardProcessTauDecayProduct": 10,
        "fromHardProcessBeforeFSR": 11,
        "isFirstCopy": 12,
        "isLastCopy": 13,
        "isLastCopyBeforeFSR": 14,
    })

    def has_statusFlag(gen, statusFlag):
        return (gen.statusFlags & (1 << statusFlag) != 0)

    for name, lepton, in (("Electron", electron), ("Muon", muon)):
        # leptons in [x,y,:] are identical, genparts i, [x,y,:] are all genparts (needed to remove genparts that do not have the same pdgId)
        _lepton, _genpart = ak.unzip(ak.cartesian([lepton, genpart], axis=1, nested=True))

        # mask to match lepton pdgId with gen particle before looking at nearest generator particle
        pdgId_mask = (abs(_lepton.pdgId) == abs(_genpart.pdgId))

        status_mask = ak.where(abs(_genpart.pdgId) == 15, (_genpart.status == 2) &
                            (has_statusFlag(_genpart, 13)), _genpart.status == 1)

        # reduced gen particle list of possible matching candidates
        _genpart_allowphoton = _genpart[(pdgId_mask | (abs(_genpart.pdgId) == 22)) & status_mask]
        _genpart = _genpart[pdgId_mask & status_mask]

        dr = ak.min(lepton.delta_r(_genpart), axis=-1)
        dr_allowphoton = ak.min(lepton.delta_r(_genpart_allowphoton), axis=-1)

        # take closest gen particle as match (with pdgId and status mask aplied on the gen particle)
        custom_match = ak.flatten(_genpart[ak.argmin(lepton.delta_r(_genpart), axis=-1, keepdims=True)], axis=2)
        custom_allowphoton = ak.flatten(_genpart_allowphoton[ak.argmin(
            lepton.delta_r(_genpart_allowphoton), axis=-1, keepdims=True)], axis=2)

        # if delta r > 0.2, check for match with gen photons
        cond = ak.fill_none((dr > 0.2), True)
        custom_match = ak.where(cond, custom_allowphoton, custom_match)

        # First check if lepton ahs a designated gen particle, if not use custom match
        cond = ak.fill_none((lepton.genPartIdx >= 0) & (
            genpart[lepton.genPartIdx].pdgId == lepton.pdgId), True)
        gen_match = ak.where(cond, genpart[lepton.genPartIdx], custom_match)
        cond = ak.fill_none((lepton.genPartIdx >= 0) & (
            genpart[lepton.genPartIdx].pdgId == lepton.pdgId), True)
        valid_match = ak.where(cond, True, dr_allowphoton < 0.2)

        # if delta r still > 0.2, there is no valid custom match!
        match_isPrompt = (
            (has_statusFlag(gen_match, statusmap["isPrompt"])) |
            (has_statusFlag(gen_match, statusmap["isDirectPromptTauDecayProduct"])) |
            (has_statusFlag(gen_match, statusmap["isHardProcess"])) |
            (has_statusFlag(gen_match, statusmap["fromHardProcess"])) |
            (has_statusFlag(gen_match, statusmap["fromHardProcessBeforeFSR"]))
        ) & (valid_match) 

        matchPdgId = (gen_match.pdgId == lepton.pdgId) & valid_match

        is_chargeflip = (gen_match.pdgId == -lepton.pdgId) & valid_match

        events = set_ak_column(events, f"{name}.isPromptJules", ak.fill_none(match_isPrompt, 0, axis=-1))
        events = set_ak_column(events, f"{name}.matchPdgIdJules", ak.fill_none(matchPdgId, 0, axis=-1))
        events = set_ak_column(events, f"{name}.isChargeFlipJules", ak.fill_none(is_chargeflip, 0, axis=-1))

    return events
