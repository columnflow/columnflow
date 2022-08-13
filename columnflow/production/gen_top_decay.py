# coding: utf-8

"""
Producers that determine the generator-level particles related to a top quark decay.
"""

from columnflow.production import Producer, producer
from columnflow.util import maybe_import
from columnflow.columnar_util import set_ak_column


ak = maybe_import("awkward")


@producer
def gen_top_decay_products(self: Producer, events: ak.Array, **kwargs) -> ak.Array:
    """
    Creates a new ragged column "gen_top_decay" with one element per hard top quark. Each element is
    a GenParticleArray with five or more objects in a distinct order: top quark, bottom quark,
    W boson, down-type quark or charged lepton, up-type quark or neutrino, and any additional decay
    produces of the W boson (if any, then most likly photon radiations). Per event, the structure
    will be similar to:

    .. code-block:: python

        [[t1, b1, W1, q1/l, q2/n(, additional_w_decay_products)], [t2, ...], ...]
    """
    if self.dataset_inst.is_data or not self.dataset_inst.x("has_top", False):
        return events

    # find hard top quarks
    abs_id = abs(events.GenPart.pdgId)
    t = events.GenPart[abs_id == 6]
    t = t[t.hasFlags("isHardProcess")]
    t = t[~ak.is_none(t, axis=1)]
    nt = ak.num(t, axis=1)

    # bottoms from top decays
    b = events.GenPart[abs_id == 5]
    b = b[b.hasFlags("isHardProcess") & (abs(b.distinctParent.pdgId) == 6)]
    b = b[~ak.is_none(b, axis=1)]
    nb = ak.num(b, axis=1)

    # Ws from top decays
    w = events.GenPart[abs_id == 24]
    w = w[w.hasFlags("isHardProcess") & (abs(w.distinctParent.pdgId) == 6)]
    w = w[~ak.is_none(w, axis=1)]
    nw = ak.num(w, axis=1)

    # non-top quarks from W decays
    qs = events.GenPart[(abs_id >= 1) & (abs_id <= 5)]
    qs = qs[qs.hasFlags("isHardProcess") & (abs(qs.distinctParent.pdgId) == 24)]
    qs = qs[~ak.is_none(qs, axis=1)]
    nqs = ak.num(qs, axis=1)

    # leptons from W decays
    ls = events.GenPart[(abs_id >= 11) & (abs_id <= 16)]
    ls = ls[ls.hasFlags("isHardProcess") & (abs(ls.distinctParent.pdgId) == 24)]
    ls = ls[~ak.is_none(ls, axis=1)]
    nls = ak.num(ls, axis=1)

    # some checks
    # TODO: the checks are somewhat strict right now, but as generators and gen particle filtering
    #       ocassionally produce weird cases, we might use some fallbacks instead in the future
    def all_or_raise(arr, msg):
        if not ak.all(arr):
            raise Exception(f"{msg} in {100 * ak.mean(~arr):.3f}% of cases")

    all_or_raise(nt == nb, "number of top quarks != number of bottom quarks")
    all_or_raise(nt == nw, "number of top quarks != number of W bosons")
    all_or_raise((nqs % 2) == 0, "number of quarks from W decays is not dividable by 2")
    all_or_raise((nls % 2) == 0, "number of leptons from W decays is not dividable by 2")
    all_or_raise(nqs + nls == 2 * nw, "number of W decay products invalid")

    # build top decay groups of five gen particles
    # strategy: handle cases with different amounts of top quarks per event differently, and per
    # amount, create indices for each type of gen particle and do one large concatenation; example:
    #
    # groups = ak.concatenate((
    #     t[[[0, 1], [0], [0, 1, 2], ...]][:, :, None],
    #     b[[[1, 0], [0], [0, 1, 2], ...]][:, :, None],
    #     w[[[1, 0], [0], [0, 2, 1], ...]][:, :, None],
    #     ...
    # ), axis=2)
    #
    # the object slices, e.g. [[1, 0], [0], [0, 2, 1], ...] for w, account for the mapping between
    # the objects and for that matter, t dictate the order and therefore actually have no slicing
    # the other particles, w, b, qs and ls, however, use them to match "their" top and
    # the [:, :, None] just adds a new axis that is required for concatenation
    # no grouping algorithm implemented yet for 3 or more t/W/b or for 2 with same charge
    # note: in case we can verify that gen particles are stored depth-first, then any matching of
    #       b/w/qs/ls to t would be trivial as they would be intrinsically in the same order
    sign = lambda part: (part.pdgId > 0) * 2 - 1
    t_sign = sign(t)
    all_or_raise((nt != 2) | (ak.sum(t_sign, axis=1) == 0), "grouping not implemented for 2 ss tops, but found")
    all_or_raise(nt <= 2, "grouping not implemented for 3 or more tops, but found")
    mask2 = nt == 2

    # for w, start with the local index as is and for events with 2 objects, order using sign of the
    # pdg id so that it matches that of the top quark
    w_idxs = ak.local_index(w, axis=1)
    w_sign = sign(w)
    w_flip = mask2 & (ak.sum(t_sign * w_sign, axis=1) < 0)
    if ak.any(w_flip):
        w_idxs = ak.where(w_flip, w_idxs[:, ::-1], w_idxs)
    # apply the indices and rebuilt the sign
    w = w[w_idxs]
    w_sign = sign(w)

    # for b, do the same as for w
    b_idxs = ak.local_index(b, axis=1)
    b_flip = mask2 & (ak.sum(t_sign * sign(b), axis=1) < 0)
    if ak.any(b_flip):
        b_idxs = ak.where(b_flip, b_idxs[:, ::-1], b_idxs)
    # apply the indices
    b = b[b_idxs]

    # for quarks and leptons, first build pairs under the assumption that two consecutive
    # objects make up a W (checked later)
    qs_pairs = ak.concatenate([qs[:, ::2, None], qs[:, 1::2, None]], axis=2)
    ls_pairs = ak.concatenate([ls[:, ::2, None], ls[:, 1::2, None]], axis=2)

    # reorder within pairs so that d-type quark, or charged lepton is at the front (odd pdg number)
    def idxs_odd_pdg_id_first(pairs):
        pairs_idxs = ak.local_index(pairs, axis=2)
        pairs_flip = abs(pairs.pdgId)[:, :, 0] % 2 == 0
        if ak.any(pairs_flip):
            pairs_idxs = ak.where(pairs_flip, pairs_idxs[:, :, ::-1], pairs_idxs)
        return pairs_idxs

    qs_pairs = qs_pairs[idxs_odd_pdg_id_first(qs_pairs)]
    ls_pairs = ls_pairs[idxs_odd_pdg_id_first(ls_pairs)]

    # merge pairs and then order them to match the correct W per pair, again using a sign comparison
    w_prod = ak.concatenate([qs_pairs, ls_pairs], axis=1)
    w_prod_sign = ak.flatten(sign(w_prod[(w_prod.pdgId % 2 == 0)]), axis=2)
    w_prod_flip = (w_sign != w_prod_sign)[:, 0]
    if ak.any(w_prod_flip):
        w_prod = ak.where(w_prod_flip, w_prod[:, ::-1], w_prod)

    # cross check w decay product pairing by comparing masses
    # note: some w's appear to have photon radiations that are not covered by the decay products yet
    #       and that affect the mass comparison; we should wait for the proper children lookup
    #       behavior in coffea before revisting this
    # w_prod_mass = (w_prod[:, :, ::2] + w_prod[:, :, 1::2])[:, :, 0].mass
    # all_or_raise(
    #     ak.isclose(w_prod_mass, w.mass, rtol=0.01, atol=0.0),
    #     "grouping of W decay products leads to wrong masses",
    # )

    # create the groups
    groups = ak.concatenate(
        [
            t[:, :, None],
            b[:, :, None],
            w[:, :, None],
            w_prod,
        ],
        axis=2,
    )

    # save the column
    set_ak_column(events, "gen_top_decay", groups)

    return events


@gen_top_decay_products.init
def gen_top_decay_products_init(self: Producer) -> None:
    """
    Ammends the set of used and produced columns of :py:class:`gen_top_decay_products` in case
    a dataset including top decays is processed.
    """
    if getattr(self, "dataset_inst", None) and self.dataset_inst.x("has_top", False):
        self.uses |= {"nGenPart", "GenPart.*"}
        self.produces |= {"gen_top_decay"}
