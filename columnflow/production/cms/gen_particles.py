# coding: utf-8

"""
Producers that determine the generator-level particles and bring them into a structured format. This is most likely
useful for generator studies and truth definitions of physics objects.
"""

from __future__ import annotations

import law

from columnflow.production import Producer, producer
from columnflow.columnar_util import set_ak_column, ak_concatenate_safe
from columnflow.util import UNSET, maybe_import

np = maybe_import("numpy")
ak = maybe_import("awkward")


logger = law.logger.get_logger(__name__)

_keep_gen_part_fields = {
    "pt": np.float32,
    "eta": np.float32,
    "phi": np.float32,
    "mass": np.float32,
    "pdgId": np.int32,
}


# helper to transform generator particles by dropping / adding fields
def transform_gen_part(gen_parts: ak.Array, *, depth_limit: int, optional: bool = False) -> ak.Array:
    # reduce down to relevant fields
    arr = {}
    for f, dtype in _keep_gen_part_fields.items():
        if optional:
            if (v := getattr(gen_parts, f, UNSET)) is not UNSET:
                arr[f] = ak.values_astype(v, dtype)
        else:
            arr[f] = ak.values_astype(getattr(gen_parts, f), dtype)
    arr = ak.zip(arr, depth_limit=depth_limit)

    # remove parameters and add Lorentz vector behavior
    arr = ak.without_parameters(arr)
    arr = ak.with_name(arr, "PtEtaPhiMLorentzVector")

    return arr


@producer(
    uses={
        "GenPart.{genPartIdxMother,status,statusFlags}",  # required by the gen particle identification
        f"GenPart.{{{','.join(_keep_gen_part_fields)}}}",  # additional fields that should be read and added to gen_top
    },
    produces={"gen_top.*.*"},
)
def gen_top_lookup(self: Producer, events: ak.Array, strict: bool = True, **kwargs) -> ak.Array:
    """
    Creates a new ragged column "gen_top" containing information about generator-level top quarks and their decay
    products in a structured array with the following fields:

        - ``t``: list of all top quarks in the event, sorted such that top quarks precede anti-top quarks
        - ``b``: list of bottom quarks from top quark decays, consistent ordering w.r.t. ``t`` (note that, in rare
            cases, the decay into charm or down quarks is realized, and therefore stored in this field)
        - ``w``: list of W bosons from top quark decays, consistent ordering w.r.t. ``t``
        - ``w_children``: list of W boson decay products, consistent ordering w.r.t. ``w``, the first entry is the
            down-type quark or charged lepton, the second entry is the up-type quark or neutrino, and additional decay
            products (e.g photons) are appended afterwards
        - ``w_tau_children``: list of decay products from tau lepton decays stemming from W boson decays, however,
            skipping the W boson from the tau lepton decay itself; the first entry is the tau neutrino, the second and
            third entries are either the charged lepton and neutrino, or quarks or hadrons sorted by ascending absolute
            pdg id; additional decay products (e.g photons) are appended afterwards
    """
    # helper to extract unique values
    unique_set = lambda a: set(np.unique(ak.flatten(a, axis=None)))

    # find hard top quarks
    t = events.GenPart[abs(events.GenPart.pdgId) == 6]
    t = t[t.hasFlags("isLastCopy")]  # they are either fromHardProcess _or_ isLastCopy

    # sort them so that that top quarks come before anti-top quarks
    t = t[ak.argsort(t.pdgId, axis=1, ascending=False)]

    # distinct top quark children
    # (asking for isLastCopy leads to some tops that miss children, usually b's)
    t_children = ak.drop_none(t.distinctChildren[t.distinctChildren.hasFlags("fromHardProcess", "isFirstCopy")])

    # strict mode: check that there are exactly two children that are b and w
    if strict:
        if (tcn := unique_set(ak.num(t_children, axis=2))) != {2}:
            raise Exception(f"found top quarks that have != 2 children: {tcn - {2}}")
        if (tci := unique_set(abs(t_children.pdgId))) - {1, 3, 5, 24}:
            raise Exception(f"found top quark children with unexpected pdgIds: {tci - {1, 3, 5, 24}}")

    # store b's (or s/d) and w's
    abs_tc_ids = abs(t_children.pdgId)
    b = ak.drop_none(ak.firsts(t_children[(abs_tc_ids == 1) | (abs_tc_ids == 3) | (abs_tc_ids == 5)], axis=2))
    w = ak.drop_none(ak.firsts(t_children[abs(t_children.pdgId) == 24], axis=2))

    # distinct w children
    w_children = ak.drop_none(w.distinctChildrenDeep)

    # distinguish into "hard" and additional ones
    w_children_hard = w_children[(hard_mask := w_children.hasFlags("fromHardProcess"))]
    w_children_rest = w_children[~hard_mask]

    # strict: check that there are exactly two hard children
    if strict:
        if (wcn := unique_set(ak.num(w_children_hard, axis=2))) != {2}:
            raise Exception(f"found W bosons that have != 2 children: {wcn - {2}}")

    # sort them so that down-type quarks and charged leptons (odd pdgIds) come first, followed by up-type quarks and
    # neutrinos (even pdgIds), then add back the remaining ones
    w_children_hard = w_children_hard[ak.argsort(-(w_children_hard.pdgId % 2), axis=2)]
    w_children = ak_concatenate_safe([w_children_hard, w_children_rest], axis=2)

    # further distinguish tau decays in w_children
    w_tau_children = ak.drop_none(w_children[abs(w_children.pdgId) == 15].distinctChildrenDeep)
    # sort: nu tau first, photons last, rest in between sorted by ascending absolute pdgId
    w_tau_nu_mask = abs(w_tau_children.pdgId) == 16
    w_tau_photon_mask = w_tau_children.pdgId == 22
    w_tau_rest = w_tau_children[~(w_tau_nu_mask | w_tau_photon_mask)]
    w_tau_rest = w_tau_rest[ak.argsort(abs(w_tau_rest.pdgId), axis=3, ascending=True)]
    w_tau_children = ak_concatenate_safe(
        [w_tau_children[w_tau_nu_mask], w_tau_rest, w_tau_children[w_tau_photon_mask]],
        axis=3,
    )

    # zip into a single array with named fields
    gen_top = ak.zip(
        {
            "t": transform_gen_part(t, depth_limit=2),
            "b": transform_gen_part(b, depth_limit=2),
            "w": transform_gen_part(w, depth_limit=2),
            "w_children": transform_gen_part(w_children, depth_limit=3),
            "w_tau_children": transform_gen_part(w_tau_children, depth_limit=4),
        },
        depth_limit=1,
    )

    # save the column
    events = set_ak_column(events, "gen_top", gen_top)

    return events


@producer(
    uses={
        "GenPart.{genPartIdxMother,status,statusFlags}",  # required by the gen particle identification
        f"GenPart.{{{','.join(_keep_gen_part_fields)}}}",  # additional fields that should be read and added to gen_top
    },
    produces={"gen_higgs.*.*"},
)
def gen_higgs_lookup(self: Producer, events: ak.Array, strict: bool = True, **kwargs) -> ak.Array:
    """
    Creates a new ragged column "gen_higgs" containing information about generator-level Higgs bosons and their decay
    products in a structured array with the following fields:

        - ``h``: list of all Higgs bosons in the event, sorted by the pdgId of their decay products such that Higgs
            bosons decaying to quarks (b's) come first, followed by leptons, and then gauge bosons
        - ``h_children``: list of direct Higgs boson children, consistent ordering w.r.t. ``h``, with the first entry
            being the particle and the second one being the anti-particle; for Z bosons and (effective) gluons and
            photons, no ordering is applied
        - ``tau_children``: list of decay products from tau lepton decays coming from Higgs bosons, with the first entry
            being the neutrino and the second one being the W boson
        - ``tau_w_children``: list of the decay products from W boson decays from tau lepton decays, with the first
            entry being the down-type quark or charged lepton, the second entry being the up-type quark or neutrino, and
            additional decay products (e.g photons) are appended afterwards
        - ``w_children``: list of decay products from W boson decays coming from Higgs bosons, with the first entry
                being the down-type quark or charged lepton, the second entry being the up-type quark or neutrino, and
                additional decay products (e.g photons) are appended afterwards
        - ``z_children``: list of decay products from Z boson decays coming from Higgs bosons, with the first entry
                being the particle and the second entry being the anti-particle
    """
    # helper to extract unique values
    unique_set = lambda a: set(np.unique(ak.flatten(a, axis=None)))

    # find higgs
    h = events.GenPart[events.GenPart.pdgId == 25]
    h = h[h.hasFlags("fromHardProcess", "isLastCopy")]

    # sort them by increasing pdgId of their children (quarks, leptons, Z, W, effective gluons/photons)
    h = h[ak.argsort(abs(ak.drop_none(ak.min(h.children.pdgId, axis=2))), axis=1, ascending=True)]

    # get distinct children
    h_children = ak.drop_none(h.distinctChildren[h.distinctChildren.hasFlags("fromHardProcess", "isFirstCopy")])

    # strict mode: check that there are exactly two children
    if strict:
        if (hcn := unique_set(ak.num(h_children, axis=2))) != {2}:
            raise Exception(f"found Higgs bosons that have != 2 children: {hcn - {2}}")

    # sort them by decreasing pdgId
    h_children = h_children[ak.argsort(h_children.pdgId, axis=2, ascending=False)]
    # in strict mode, fix the children dimension to 2
    if strict:
        h_children = h_children[:, :, [0, 1]]

    # h -> tautau -> children
    tau_mask = abs(h_children.pdgId[:, :, 0]) == 15
    tau = ak.fill_none(h_children[ak.mask(tau_mask, tau_mask)], [], axis=1)
    tau_children = tau.distinctChildrenDeep[tau.distinctChildrenDeep.hasFlags("isFirstCopy", "isTauDecayProduct")]
    tau_children = ak.drop_none(tau_children)
    # prepare neutrino and W boson handling
    tau_nu_mask = abs(tau_children.pdgId) == 16
    tau_w_mask = abs(tau_children.pdgId) == 24
    tau_rest_mask = ~(tau_nu_mask | tau_w_mask)
    tau_has_rest = ak.any(tau_rest_mask, axis=3)
    # strict mode: there should always be a neutrino, and _either_ a W and nothing else _or_ no W at all
    if strict:
        if not ak.all(ak.any(tau_nu_mask[tau_mask], axis=3)):
            raise Exception("found tau leptons without a tau neutrino among their children")
        tau_has_w = ak.any(tau_w_mask, axis=3)
        if not ak.all((tau_has_w ^ tau_has_rest)[tau_mask]):
            raise Exception("found tau leptons with both W bosons and other decay products among their children")
    # get the tau neutrino
    tau_nu = tau_children[tau_nu_mask].sum(axis=3)
    tau_nu = set_ak_column(tau_nu, "pdgId", ak.values_astype(16 * np.sign(tau.pdgId), np.int32))
    # get the W boson in case it is part of the tau children, otherwise build it from the sum of children
    tau_w = tau_children[tau_w_mask].sum(axis=3)
    if ak.any(tau_has_rest):
        tau_w_rest = tau_children[tau_rest_mask].sum(axis=-1)
        tau_w = ak.where(tau_has_rest, tau_w_rest, tau_w)
    tau_w = set_ak_column(tau_w, "pdgId", ak.values_astype(-24 * np.sign(tau.pdgId), np.int32))
    # combine nu and w again
    tau_nuw = ak_concatenate_safe([tau_nu[..., None], tau_w[..., None]], axis=3)
    # define w children
    tau_w_children = ak_concatenate_safe(
        [tau_children[tau_rest_mask], ak.drop_none(ak.firsts(tau_children[tau_w_mask], axis=3).children)],
        axis=2,
    )

    # h -> ww -> children
    w_mask = abs(h_children.pdgId[:, :, 0]) == 24
    w = ak.fill_none(h_children[ak.mask(w_mask, w_mask)], [], axis=1)
    w_children = w.distinctChildrenDeep[w.distinctChildrenDeep.hasFlags("fromHardProcess", "isFirstCopy")]
    w_children = ak.drop_none(w_children)

    # h -> zz -> children
    z_mask = abs(h_children.pdgId[:, :, 0]) == 23
    z = ak.fill_none(h_children[ak.mask(z_mask, z_mask)], [], axis=1)
    z_children = z.distinctChildrenDeep[z.distinctChildrenDeep.hasFlags("fromHardProcess", "isFirstCopy")]
    z_children = ak.drop_none(z_children)

    # children for decays other than taus are not yet implemented, so show a warning in case they are found
    unhandled_ids = unique_set(abs(h_children.pdgId)) - set(range(1, 6 + 1)) - set(range(11, 16 + 1)) - {23, 24}
    if unhandled_ids:
        logger.warning_once(
            f"gen_higgs_undhandled_children_{'_'.join(map(str, sorted(unhandled_ids)))}",
            f"found Higgs boson decays in the {self.cls_name} producer with pdgIds {unhandled_ids}, for which the "
            "lookup of children is not yet implemented",
        )

    # zip into a single array with named fields
    gen_higgs = ak.zip(
        {
            "h": transform_gen_part(h, depth_limit=2),
            "h_children": transform_gen_part(h_children, depth_limit=3),
            "tau_children": transform_gen_part(tau_nuw, depth_limit=4),
            "tau_w_children": transform_gen_part(tau_w_children, depth_limit=4),
            "w_children": transform_gen_part(w_children, depth_limit=4),
            "z_children": transform_gen_part(z_children, depth_limit=4),
        },
        depth_limit=1,
    )

    # save the column
    events = set_ak_column(events, "gen_higgs", gen_higgs)

    return events


@producer(
    uses={
        "GenPart.{genPartIdxMother,status,statusFlags}",  # required by the gen particle identification
        f"GenPart.{{{','.join(_keep_gen_part_fields)}}}",  # additional fields that should be read and added to gen_top
    },
    produces={"gen_dy.*.*"},
)
def gen_dy_lookup(self: Producer, events: ak.Array, strict: bool = True, **kwargs) -> ak.Array:
    """
    Creates a new ragged column "gen_dy" containing information about generator-level Z/g bosons and their decay
    products in a structured array with the following fields:

        - ``z``: list of all Z/g bosons in the event, sorted by the pdgId of their decay products
        - ``lep``: list of direct Z/g boson children, consistent ordering w.r.t. ``z``, with the first entry being the
            lepton and the second one being the anti-lepton
        - ``tau_children``: list of decay products from tau lepton decays coming from Z/g bosons, with the first entry
            being the neutrino and the second one being the W boson
        - ``tau_w_children``: list of the decay products from W boson decays from tau lepton decays, with the first
            entry being the down-type quark or charged lepton, the second entry being the up-type quark or neutrino, and
            additional decay products (e.g photons) are appended afterwards
        - ``decay_type``: integer value describing the following cases:
            - ee: 1
            - mm: 2
            - tt:
                - ee: 113
                - em: 123
                - eh: 133
                - me: 213
                - mm: 223
                - mh: 233
                - he: 313
                - hm: 323
                - hh: 333
    """
    # note: in about 4% of DY events, the Z/g boson is missing, so this lookup starts at lepton level, see
    # https://indico.cern.ch/event/1495537/contributions/6359516/attachments/3014424/5315938/HLepRare_25.02.14.pdf

    # helper to extract unique values
    unique_set = lambda a: set(np.unique(ak.flatten(a, axis=None)))

    # get the e/mu and tau masks
    abs_id = abs(events.GenPart.pdgId)
    emu_mask = (
        ((abs_id == 11) | (abs_id == 13)) &
        (events.GenPart.status == 1) &
        events.GenPart.hasFlags("fromHardProcess")
    )
    # taus need to have status == 2
    tau_mask = (
        (abs_id == 15) &
        (events.GenPart.status == 2) &
        events.GenPart.hasFlags("fromHardProcess")
    )
    lep_mask = emu_mask | tau_mask

    # strict mode: there must be exactly two charged leptons per event
    if strict:
        if (nl := unique_set(ak.num(events.GenPart[lep_mask], axis=1))) - {2}:
            raise Exception(f"found events that have != 2 charged leptons: {nl - {2}}")

    # get the leptons and sort by decreasing pdgId (lepton before anti-lepton)
    lep = events.GenPart[lep_mask]
    lep = lep[ak.argsort(lep.pdgId, axis=1, ascending=False)]

    # in strict mode, fix the lep dimension to 2
    if strict:
        lep = lep[:, [0, 1]]

    # build the z from them
    z = lep.sum(axis=-1)
    z = set_ak_column(z, "pdgId", np.int32(23))

    # further treatment of tau decays
    tau = events.GenPart[tau_mask]
    tau = tau[ak.argsort(tau.pdgId, axis=1, ascending=False)]
    tau_children = tau.distinctChildren[tau.distinctChildren.hasFlags("isFirstCopy", "isTauDecayProduct")]
    tau_children = ak.drop_none(tau_children)
    # prepare neutrino and W boson handling
    tau_nu_mask = abs(tau_children.pdgId) == 16
    tau_w_mask = abs(tau_children.pdgId) == 24
    tau_rest_mask = ~(tau_nu_mask | tau_w_mask)
    tau_has_rest = ak.any(tau_rest_mask, axis=2)
    # strict mode: there should always be a neutrino, and _either_ a W and nothing else _or_ no W at all
    if strict:
        if not ak.all(ak.any(tau_nu_mask, axis=2)):
            raise Exception("found tau leptons without a tau neutrino among their children")
        tau_has_w = ak.any(tau_w_mask, axis=2)
        if not ak.all(tau_has_w ^ tau_has_rest):
            raise Exception("found tau leptons with both W bosons and other decay products among their children")
    # get the tau neutrino
    tau_nu = tau_children[tau_nu_mask].sum(axis=2)
    tau_nu = set_ak_column(tau_nu, "pdgId", ak.values_astype(16 * np.sign(tau.pdgId), np.int32))
    # get the W boson in case it is part of the tau children, otherwise build it from the sum of children
    tau_w = tau_children[tau_w_mask].sum(axis=2)
    if ak.any(tau_has_rest):
        tau_w_rest = tau_children[tau_rest_mask].sum(axis=-1)
        tau_w = ak.where(tau_has_rest, tau_w_rest, tau_w)
    tau_w = set_ak_column(tau_w, "pdgId", ak.values_astype(-24 * np.sign(tau.pdgId), np.int32))
    # combine nu and w again
    tau_nuw = ak_concatenate_safe([tau_nu[..., None], tau_w[..., None]], axis=2)
    # define w children
    tau_w_children = ak_concatenate_safe(
        [tau_children[tau_rest_mask], ak.drop_none(ak.firsts(tau_children[tau_w_mask], axis=2).children)],
        axis=1,
    )

    # construct the decay type integer
    first_lep_id = abs(lep.pdgId[:, 0])
    t_mask = first_lep_id == 15
    min_tau_1_w_children_id = ak.fill_none(ak.min(ak.firsts(abs(tau_w_children.pdgId)[:, 0:1]), axis=1), np.int32(0))
    min_tau_2_w_children_id = ak.fill_none(ak.min(ak.firsts(abs(tau_w_children.pdgId)[:, 1:2]), axis=1), np.int32(0))
    decay_type = (
        np.zeros(len(events), np.uint16) +
        # add values for overall z decay: e=1, m=2, t=3
        ak.where(first_lep_id == 11, 1, 0) +
        ak.where(first_lep_id == 13, 2, 0) +
        ak.where(t_mask, 3, 0) +
        # add values for the first tau decay into: e=100, m=200, h=300
        ak.where(t_mask & (min_tau_1_w_children_id == 11), 100, 0) +
        ak.where(t_mask & (min_tau_1_w_children_id == 13), 200, 0) +
        ak.where(t_mask & (min_tau_1_w_children_id > 16), 300, 0) +
        # add values for the second tau decay into: e=10, m=20, h=30
        ak.where(t_mask & (min_tau_2_w_children_id == 11), 10, 0) +
        ak.where(t_mask & (min_tau_2_w_children_id == 13), 20, 0) +
        ak.where(t_mask & (min_tau_2_w_children_id > 16), 30, 0)
    )

    # zip into a single array with named fields
    gen_dy = ak.zip(
        {
            "z": transform_gen_part(z, depth_limit=1),
            "lep": transform_gen_part(lep, depth_limit=2),
            "tau_children": transform_gen_part(tau_nuw, depth_limit=3),
            "tau_w_children": transform_gen_part(tau_w_children, depth_limit=3),
            "decay_type": ak.values_astype(decay_type, np.uint16),
        },
        depth_limit=1,
    )

    # save the column
    events = set_ak_column(events, "gen_dy", gen_dy)

    return events


@producer(
    uses={
        gen_dy_lookup,
        "GenPart.{genPartIdxMother,status,statusFlags}",  # required by the gen particle identification
        f"GenPart.{{{','.join(_keep_gen_part_fields)}}}",  # additional fields that should be read and added to gen_top
    },
    produces={"gen_dy_hepmc_filters"},
)
def gen_dy_hepmc_filters(self: Producer, events: ak.Array, **kwargs) -> ak.Array:
    """
    Evaluates certain HepMC filters for DY events. The result is stored as a bit mask for specific filter
    implementations.
    """
    # add dy indexing
    events = self[gen_dy_lookup](events, **kwargs)

    # start with zeros
    hepmc_filters = np.zeros(len(events), np.uint8)

    # set bits for events passing the respective filters
    hepmc_filters += np.asarray(hepmc_filter_1(events), dtype=np.uint8) << 0
    # no others yet to be placed on higher bits

    events = set_ak_column(events, "gen_dy_hepmc_filters", hepmc_filters)

    return events


def hepmc_filter_1(events: ak.Array) -> np.ndarray:
    """
    Checks if the event contains a di-tau system and applies kinematic cuts.

    Example    : DYto2Tau-2Jets_M-50_2J_Filtered_TuneCP5_13p6TeV_amcatnloFXFX-pythia8
    HepMCFilter: https://github.com/cms-sw/cmssw/blob/master/GeneratorInterface/Core/src/EmbeddingHepMCFilter.cc
    Filter cuts: https://cms-pdmv-prod.web.cern.ch/mcm/public/restapi/requests/get_fragment/HIG-Run3Summer22EEwmLHEGS-01476/0 # noqa: E501
    """
    # get taus with "isHardProcess" flag
    tau = events.GenPart[(abs(events.GenPart.pdgId) == 15) & (events.GenPart.hasFlags("isHardProcess"))]

    # when there are no taus, stop early
    if not ak.any(ak.num(tau, axis=1)):
        return np.zeros(len(events), dtype=np.bool_)

    # sort tau before anti-tau to be consistent with decay_type definition in gen_dy_lookup
    tau = tau[ak.argsort(tau.pdgId, axis=1, ascending=False)]  # sort by tau before anti-tau

    # iterate as long as there are children, starting with first level of tau children
    visible = []
    children = tau.distinctChildrenDeep
    while (ak.max(ak.num(children, axis=2), axis=None) or 0) > 0:
        # remove nus
        abs_id = abs(children.pdgId)
        nu_mask = (abs_id == 12) | (abs_id == 14) | (abs_id == 16)
        children = children[~nu_mask]
        # save visible particles to book keeping sums
        status_mask = children.status == 1
        visible.append(children[status_mask])
        # continue with concated rest
        children = ak.flatten(children[~status_mask].children, axis=3)

    # combine to visible tau momenta and extract pt and eta
    vis_tau = ak.concatenate(visible, axis=2).sum(axis=2)
    pt = ak.fill_none(ak.pad_none(vis_tau.pt, 2, axis=1, clip=True), 0.0)
    eta = ak.fill_none(ak.pad_none(abs(vis_tau.eta), 2, axis=1, clip=True), 5.0)

    # apply channel dependent cuts, without ordering but repeating orientations
    dt = events.gen_dy.decay_type
    decision = (
        (dt % 10 == 3) &  # tautau
        ak.all(eta < 3.0, axis=1) &  # all eta's below 3.0
        (
            # em
            ((dt == 123) & (pt[:, 0] > 11.0) & (pt[:, 1] > 8.0)) |
            # eh
            ((dt == 133) & (pt[:, 0] > 22.0) & (pt[:, 1] > 16.0)) |
            # me
            ((dt == 213) & (pt[:, 0] > 8.0) & (pt[:, 1] > 11.0)) |
            # mh
            ((dt == 233) & (pt[:, 0] > 19.0) & (pt[:, 1] > 16.0)) |
            # he
            ((dt == 313) & (pt[:, 0] > 16.0) & (pt[:, 1] > 22.0)) |
            # hm
            ((dt == 323) & (pt[:, 0] > 16.0) & (pt[:, 1] > 19.0)) |
            # hh
            ((dt == 333) & (pt[:, 0] > 20) & (pt[:, 1] > 20))  # order not important as cuts are the same
        )
    )

    return decision
