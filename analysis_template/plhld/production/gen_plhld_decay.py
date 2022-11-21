# coding: utf-8

"""
Producers that determine the generator-level particles of a HH->bbWW decay.
"""

from columnflow.production import Producer, producer
from columnflow.util import maybe_import
from columnflow.columnar_util import set_ak_column, EMPTY_FLOAT


ak = maybe_import("awkward")
np = maybe_import("numpy")


@producer
def gen_plhld_decay_products(self: Producer, events: ak.Array, **kwargs) -> ak.Array:
    """
    Creates column 'gen_plhld_decay', which includes the most relevant particles of a HH->bbWW(qqlnu) decay.
    All sub-fields correspond to individual GenParticles with fields pt, eta, phi, mass and pdgId.
    """

    if self.dataset_inst.is_data or not self.dataset_inst.x("is_plhld", False):
        return events

    # for quick checks
    def all_or_raise(arr, msg):
        if not ak.all(arr):
            raise Exception(f"{msg} in {100 * ak.mean(~arr):.3f}% of cases")

    # TODO: for now, this only works for HH->bbWW(qqlnu), but could maybe be generalized to all HH->bbWW decays

    # only consider hard process genparticles
    gp = events.GenPart
    gp["index"] = ak.local_index(gp, axis=1)
    gp = gp[events.GenPart.hasFlags("isHardProcess")]
    gp = gp[~ak.is_none(gp, axis=1)]
    abs_id = abs(gp.pdgId)

    # find initial-state particles
    isp = gp[ak.is_none(gp.parent.pdgId, axis=1)]

    # find all non-Higgs daughter particles from inital state
    sec = ak.flatten(isp.children, axis=2)
    sec = sec[abs(sec.pdgId) != 25]
    sec = ak.pad_none(sec, 2)  # TODO: Not all initial particles are gluons
    gp_ghost = ak.zip({f: EMPTY_FLOAT for f in sec.fields}, with_name="GenParticle")  # TODO: avoid union type
    sec = ak.fill_none(sec, gp_ghost, axis=1)  # axis=1 necessary

    # find hard Higgs bosons
    h = gp[abs_id == 25]
    nh = ak.num(h, axis=1)
    all_or_raise(nh == 2, "number of Higgs != 2")

    # bottoms from H decay
    b = gp[abs_id == 5]
    b = b[(abs(b.distinctParent.pdgId) == 25)]
    b = b[~ak.is_none(b, axis=1)]
    nb = ak.num(b, axis=1)
    all_or_raise(nb == 2, "number of bottom quarks from Higgs decay != 2")

    # Ws from H decay
    w = gp[abs_id == 24]
    w = w[(abs(w.distinctParent.pdgId) == 25)]
    w = w[~ak.is_none(w, axis=1)]
    nw = ak.num(w, axis=1)
    all_or_raise(nw == 2, "number of Ws != 2")

    # non-top quarks from W decays
    qs = gp[(abs_id >= 1) & (abs_id <= 5)]
    qs = qs[(abs(qs.distinctParent.pdgId) == 24)]
    qs = qs[~ak.is_none(qs, axis=1)]
    nqs = ak.num(qs, axis=1)
    all_or_raise((nqs % 2) == 0, "number of quarks from W decays is not dividable by 2")
    all_or_raise(nqs == 2, "number of quarks from W decays != 2")

    # leptons from W decays
    ls = gp[(abs_id >= 11) & (abs_id <= 16)]
    ls = ls[(abs(ls.distinctParent.pdgId) == 24)]
    ls = ls[~ak.is_none(ls, axis=1)]
    nls = ak.num(ls, axis=1)
    all_or_raise((nls % 2) == 0, "number of leptons from W decays is not dividable by 2")
    all_or_raise(nls == 2, "number of leptons from W decays != 2")

    all_or_raise(nqs + nls == 2 * nw, "number of W decay products invalid")

    # check if decay product charges are valid
    sign = lambda part: (part.pdgId > 0) * 2 - 1
    all_or_raise(ak.sum(sign(b), axis=1) == 0, "two ss bottoms")
    all_or_raise(ak.sum(sign(w), axis=1) == 0, "two ss Ws")
    all_or_raise(ak.sum(sign(qs), axis=1) == 0, "sign-imbalance for quarks")
    all_or_raise(ak.sum(sign(ls), axis=1) == 0, "sign-imbalance for leptons")

    # identify decay products of W's
    lepton = ls[abs(ls.pdgId) % 2 == 1][:, 0]
    neutrino = ls[abs(ls.pdgId) % 2 == 0][:, 0]
    q_dtype = qs[abs(qs.pdgId) % 2 == 1][:, 0]
    q_utype = qs[abs(qs.pdgId) % 2 == 0][:, 0]

    # identify the leptonically and hadronically decaying W
    wlep = w[sign(w) == sign(lepton)][:, 0]
    whad = w[sign(w) != sign(lepton)][:, 0]

    # identify b1 as particle, b2 as antiparticle
    b1 = b[sign(b) == 1][:, 0]
    b2 = b[sign(b) == -1][:, 0]

    # TODO: identify H->bb and H->WW and switch from h1/h2 to hbb/hww
    # TODO: most fields have type='len(events) * ?genParticle' -> get rid of the '?'

    hhgen = {
        "h1": h[:, 0],
        "h2": h[:, 1],
        "b1": b1,
        "b2": b2,
        "wlep": wlep,
        "whad": whad,
        "l": lepton,
        "nu": neutrino,
        "q1": q_dtype,
        "q2": q_utype,
        "sec1": sec[:, 0],
        "sec2": sec[:, 1],
    }
    gen_plhld_decay = ak.Array({
        gp: {f: np.float32(hhgen[gp][f]) for f in ["pt", "eta", "phi", "mass", "pdgId"]} for gp in hhgen.keys()
    })
    events = set_ak_column(events, "gen_plhld_decay", gen_plhld_decay)

    return events


@gen_plhld_decay_products.init
def gen_plhld_decay_products_init(self: Producer) -> None:
    """
    Ammends the set of used and produced columns of :py:class:`gen_plhld_decay_products` in case
    a dataset including top decays is processed.
    """
    if getattr(self, "dataset_inst", None) and self.dataset_inst.x("is_plhld", False):
        self.uses |= {"nGenPart", "GenPart.*"}
        self.produces |= {"gen_plhld_decay"}
