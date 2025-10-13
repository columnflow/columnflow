# coding: utf-8

"""
Producers that determine the generator-level particles and bring them into a structured format. This is most likely
useful for generator studies and truth definitions of physics objects.
"""

from __future__ import annotations

from columnflow.production import Producer, producer
from columnflow.util import maybe_import
from columnflow.columnar_util import set_ak_column, remove_ak_column

np = maybe_import("numpy")
ak = maybe_import("awkward")


keep_gen_part_fields = {"pt", "eta", "phi", "mass", "pdgId", "status", "statusFlags"}


# helper to drop fields
def drop_gen_part_fields(gen_parts: ak.Array) -> ak.Array:
    """
    Takes a GenParticleArray *gen_parts* and drops all fields except for those in `keep_gen_part_fields`.
    """
    for field in gen_parts.fields:
        if field not in keep_gen_part_fields:
            gen_parts = remove_ak_column(gen_parts, field)
    return gen_parts


@producer(
    uses={"GenPart.{genPartIdxMother,status,statusFlags}", f"GenPart.{{{','.join(keep_gen_part_fields)}}}"},
    produces={"gen_top"},
)
def gen_top_lookup(self: Producer, events: ak.Array, strict: bool = True, **kwargs) -> ak.Array:
    """
    Creates a new ragged column "gen_top" containing information about generator-level top quarks and their decay
    products in a structured array with the following fields:

        - ``t``: list of all top quarks in the event, sorted such that top quarks precede anti-top quarks
        - ``b``: list of bottom quarks from top quark decays, consistent ordering w.r.t. ``t``
        - ``w``: list of W bosons from top quark decays, consistent ordering w.r.t. ``t``
        - ``wDecay``: list of W boson decay products, consistent ordering w.r.t. ``w``, the first entry is the down-type
            quark or charged lepton, the second entry is the up-type quark or neutrino, and additional decay products
            (e.g photons) are appended afterwards
    """
    # helper to extract unique values
    unique_set = lambda a: set(np.unique(ak.flatten(a, axis=None)))

    # find hard top quarks
    t = events.GenPart[abs(events.GenPart.pdgId) == 6]
    t = t[t.hasFlags("isLastCopy")]  # they are either fromHardProcess _or_ isLastCopy

    # sort them so that that top quarks come before anti-top quarks
    t = t[ak.argsort(t.pdgId, axis=1, ascending=False)]

    # distinct top quark children
    # (asking for isLastCopy leads to some tops that miss chidlren)
    t_children = ak.drop_none(t.distinctChildren[t.distinctChildren.hasFlags("fromHardProcess", "isFirstCopy")])

    # strict mode: check that there are exactly two children that are b and w
    if strict:
        if (tcn := unique_set(ak.num(t_children, axis=2))) != {2}:
            raise Exception(f"found top quarks that have != 2 children: {tcn - {2}}")
        if (tci := unique_set(abs(t_children.pdgId))) != {5, 24}:
            raise Exception(f"found top quark children with unexpected pdgIds: {tci - {5, 24}}")

    # store b's and W's
    b = ak.drop_none(ak.firsts(t_children[abs(t_children.pdgId) == 5], axis=2))
    w = ak.drop_none(ak.firsts(t_children[abs(t_children.pdgId) == 24], axis=2))

    # distinct W children
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
    w_children_hard = w_children_hard[ak.argsort(1 - (w_children_hard.pdgId % 2), axis=2)]
    w_children = ak.concatenate([w_children_hard, w_children_rest], axis=2)

    # zip into a single array with named fields
    gen_top = ak.zip(
        {
            "t": drop_gen_part_fields(t),
            "b": drop_gen_part_fields(b),
            "w": drop_gen_part_fields(w),
            "wDecay": drop_gen_part_fields(w_children),
        },
        depth_limit=1,
    )

    # save the column
    events = set_ak_column(events, "gen_top", gen_top)

    return events
