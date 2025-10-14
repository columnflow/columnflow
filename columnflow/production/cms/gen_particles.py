# coding: utf-8

"""
Producers that determine the generator-level particles and bring them into a structured format. This is most likely
useful for generator studies and truth definitions of physics objects.
"""

from __future__ import annotations

import law

from columnflow.production import Producer, producer
from columnflow.util import maybe_import
from columnflow.columnar_util import set_ak_column

np = maybe_import("numpy")
ak = maybe_import("awkward")


logger = law.logger.get_logger(__name__)

_keep_gen_part_fields = ["pt", "eta", "phi", "mass", "pdgId"]


# helper to transform generator particles by dropping / adding fields
def transform_gen_part(gen_parts: ak.Array) -> ak.Array:
    # reduce down to relevant fields
    arr = ak.zip(
        {f: getattr(gen_parts, f) for f in _keep_gen_part_fields},
        depth_limit=1,
    )
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
    w_children = ak.concatenate([w_children_hard, w_children_rest], axis=2)

    # zip into a single array with named fields
    gen_top = ak.zip(
        {
            "t": transform_gen_part(t),
            "b": transform_gen_part(b),
            "w": transform_gen_part(w),
            "w_children": transform_gen_part(w_children),
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
            being the particle and the second one being the anti-particle
        - ``tau_children``: list of decay products from tau lepton decays coming from Higgs bosons, with the first entry
            being the neutrino and the second one being the W boson
        - ``tau_w_children``: list of the decay products from W boson decays from tau lepton decays, with the first
            entry being the down-type quark or charged lepton, the second entry being the up-type quark or neutrino, and
            additional decay products (e.g photons) are appended afterwards
        - ``z_children``: not yet implemented
        - ``w_children``: not yet implemented
    """
    # helper to extract unique values
    unique_set = lambda a: set(np.unique(ak.flatten(a, axis=None)))

    # find higgs
    h = events.GenPart[events.GenPart.pdgId == 25]
    h = h[h.hasFlags("fromHardProcess", "isLastCopy")]

    # sort them by increasing pdgId if their children (quarks, leptons, Z, W)
    h = h[ak.argsort(abs(ak.drop_none(ak.min(h.children.pdgId, axis=2))), axis=1, ascending=True)]

    # get distinct children
    h_children = ak.drop_none(h.distinctChildren[h.distinctChildren.hasFlags("fromHardProcess", "isFirstCopy")])

    # strict mode: check that there are exactly two children with opposite pdg ids
    if strict:
        if (hcn := unique_set(ak.num(h_children, axis=2))) != {2}:
            raise Exception(f"found Higgs bosons that have != 2 children: {hcn - {2}}")
        if ak.any((hcm := ak.sum(h_children.pdgId, axis=-1) != 0)):
            raise Exception(f"found Higgs boson children with non-matching pdgIds: {unique_set(h_children.pdgId[hcm])}")

    # sort them by decreasing pdgId
    h_children = h_children[ak.argsort(h_children.pdgId, axis=2, ascending=False)]
    # in strict mode, fix the children dimension to 2
    if strict:
        h_children = h_children[:, :, [0, 1]]

    # further treatment of tau decays
    tau_mask = h_children.pdgId[:, :, 0] == 15
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
    tau_nuw = ak.concatenate([tau_nu[..., None], tau_w[..., None]], axis=3)
    # define w children
    tau_w_children = ak.concatenate(
        [tau_children[tau_rest_mask], ak.drop_none(ak.firsts(tau_children[tau_w_mask], axis=3).children)],
        axis=2,
    )

    # children for decays other than taus are not yet implemented, so show a warning in case they are found
    unhandled_ids = unique_set(abs(h_children.pdgId)) - set(range(1, 6 + 1)) - set(range(11, 16 + 1))
    if unhandled_ids:
        logger.warning_once(
            f"gen_higgs_undhandled_children_{'_'.join(map(str, sorted(unhandled_ids)))}",
            f"found Higgs boson decays in the {self.cls_name} producer with pdgIds {unhandled_ids}, for which the "
            "lookup of children is not yet implemented",
        )

    # zip into a single array with named fields
    gen_higgs = ak.zip(
        {
            "h": transform_gen_part(h),
            "h_children": transform_gen_part(h_children),
            "tau_children": transform_gen_part(tau_nuw),
            "tau_w_children": transform_gen_part(tau_w_children),
            # "z_children": None,  # not yet implemented
            # "w_children": None,  # not yet implemented
        },
        depth_limit=1,
    )

    # save the column
    events = set_ak_column(events, "gen_higgs", gen_higgs)

    return events
