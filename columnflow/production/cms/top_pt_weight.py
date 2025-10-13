# coding: utf-8

"""
Column producers related to top quark pt reweighting.
"""

from __future__ import annotations

import dataclasses

import law

from columnflow.production import Producer, producer
from columnflow.util import maybe_import
from columnflow.columnar_util import set_ak_column, full_like

ak = maybe_import("awkward")
np = maybe_import("numpy")


logger = law.logger.get_logger(__name__)


@dataclasses.dataclass
class TopPtWeightFromDataConfig:
    """
    Container to configure the top pt reweighting parameters for the method based on fits to data. For more info, see
    https://twiki.cern.ch/twiki/bin/viewauth/CMS/TopPtReweighting?rev=31#TOP_PAG_corrections_based_on_dat
    """
    params: dict[str, float] = dataclasses.field(default_factory=lambda: {
        "a": 0.0615,
        "a_up": 0.0615 * 1.5,
        "a_down": 0.0615 * 0.5,
        "b": -0.0005,
        "b_up": -0.0005 * 1.5,
        "b_down": -0.0005 * 0.5,
    })
    pt_max: float = 500.0


@dataclasses.dataclass
class TopPtWeightFromTheoryConfig:
    """
    Container to configure the top pt reweighting parameters for the theory-based method. For more info, see
    https://twiki.cern.ch/twiki/bin/viewauth/CMS/TopPtReweighting?rev=31#TOP_PAG_corrections_based_on_the
    """
    params: dict[str, float] = dataclasses.field(default_factory=lambda: {
        "a": 0.103,
        "b": -0.0118,
        "c": -0.000134,
        "d": 0.973,
    })


# for backward compatibility
class TopPtWeightConfig(TopPtWeightFromDataConfig):

    def __init__(self, *args, **kwargs):
        logger.warning_once(
            "TopPtWeightConfig is deprecated and will be removed in future versions, please use "
            "TopPtWeightFromDataConfig instead to keep using the data-based method, or TopPtWeightFromTheoryConfig to "
            "use the theory-based method",
        )
        super().__init__(*args, **kwargs)


@producer(
    uses={"gen_top.t.pt"},
    produces={"top_pt_weight{,_up,_down}"},
    get_top_pt_weight_config=(lambda self: self.config_inst.x.top_pt_weight),
)
def top_pt_weight(self: Producer, events: ak.Array, **kwargs) -> ak.Array:
    r"""
    Compute SF to be used for top pt reweighting, either with information from a fit to data or from theory.

    See https://twiki.cern.ch/twiki/bin/view/CMS/TopPtReweighting?rev=31 for more information.

    The method to be used depends on the config entry obtained with *get_top_pt_config* which should either be of
    type :py:class:`TopPtWeightFromDataConfig` or :py:class:`TopPtWeightFromTheoryConfig`.

        - data-based: $SF(p_T)=e^{a + b \cdot p_T}$
        - theory-based: $SF(p_T)=a \cdot e^{b \cdot p_T} + c \cdot p_T + d$

    The *gen_top.t.pt* column can be produced with the :py:class:`gen_top_lookup` producer. The SF should *only be
    applied in ttbar MC* as an event weight and is computed based on the gen-level top quark transverse momenta.

    The top pt weight configuration should be given as an auxiliary entry "top_pt_weight" in the config.
    *get_top_pt_config* can be adapted in a subclass in case it is stored differently in the config.
    """
    # check the number of gen tops
    if ak.any((n_tops := ak.num(events.gen_top.t, axis=1)) != 2):
        raise Exception(
            f"{self.cls_name} can only run on events with two generator top quarks, but found counts of "
            f"{','.join(map(str, sorted(set(n_tops))))}",
        )

    # get top pt
    top_pt = events.gen_top.t.pt
    if not self.theory_method and self.cfg.pt_max >= 0.0:
        top_pt = ak.where(top_pt > self.cfg.pt_max, self.cfg.pt_max, top_pt)

    for variation in ["", "_up", "_down"]:
        # evaluate SF function, implementation is method dependent
        if self.theory_method:
            # up variation: apply twice the effect
            # down variation: no weight at all
            if variation != "_down":
                sf = (
                    self.cfg.params["a"] * np.exp(self.cfg.params["b"] * top_pt) +
                    self.cfg.params["c"] * top_pt +
                    self.cfg.params["d"]
                )
            if variation == "_up":
                sf = 1.0 + 2.0 * (sf - 1.0)
            elif variation == "_down":
                sf = full_like(top_pt, 1.0)
        else:
            sf = np.exp(self.cfg.params[f"a{variation}"] + self.cfg.params[f"b{variation}"] * top_pt)

        # compute weight from SF product for top and anti-top
        weight = np.sqrt(np.prod(sf, axis=1))

        # write out weights
        events = set_ak_column(events, f"top_pt_weight{variation}", ak.fill_none(weight, 1.0))

    return events


@top_pt_weight.init
def top_pt_weight_init(self: Producer) -> None:
    # store the top pt weight config
    self.cfg = self.get_top_pt_weight_config()
    if not isinstance(self.cfg, (TopPtWeightFromDataConfig, TopPtWeightFromTheoryConfig)):
        raise Exception(
            f"{self.cls_name} expects the config entry obtained with get_top_pt_weight_config to be of type "
            f"TopPtWeightFromDataConfig or TopPtWeightFromTheoryConfig, but got {type(self.cfg)}",
        )
    self.theory_method = isinstance(self.cfg, TopPtWeightFromTheoryConfig)
