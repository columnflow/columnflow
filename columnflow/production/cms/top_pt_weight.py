# coding: utf-8

"""
Column producers related to top quark pt reweighting.
"""

from __future__ import annotations

from dataclasses import dataclass

import law

from columnflow.production import Producer, producer
from columnflow.util import maybe_import
from columnflow.columnar_util import set_ak_column

ak = maybe_import("awkward")
np = maybe_import("numpy")


logger = law.logger.get_logger(__name__)


@dataclass
class TopPtWeightConfig:
    params: dict[str, float]
    pt_max: float = 500.0

    @classmethod
    def new(cls, obj: TopPtWeightConfig | dict[str, float]) -> TopPtWeightConfig:
        # backward compatibility only
        if isinstance(obj, cls):
            return obj
        return cls(params=obj)


def get_top_pt_weight_config(self: Producer) -> TopPtWeightConfig:
    if self.config_inst.has_aux("top_pt_reweighting_params"):
        logger.info_once(
            "deprecated_top_pt_weight_config",
            "the config aux field 'top_pt_reweighting_params' is deprecated and will be removed in "
            "a future release, please use 'top_pt_weight' instead",
        )
        params = self.config_inst.x.top_pt_reweighting_params
    else:
        params = self.config_inst.x.top_pt_weight

    return TopPtWeightConfig.new(params)


@producer(
    uses={"gen_top.t.pt"},
    produces={"top_pt_weight{,_up,_down}"},
    get_top_pt_weight_config=get_top_pt_weight_config,
)
def top_pt_weight(self: Producer, events: ak.Array, **kwargs) -> ak.Array:
    """
    Compute SF to be used for top pt reweighting.

    See https://twiki.cern.ch/twiki/bin/view/CMS/TopPtReweighting?rev=31 for more information.

    The *gen_top.t.pt* column can be produced with the :py:class:`gen_top_lookup` producer. The SF should *only be
    applied in ttbar MC* as an event weight and is computed based on the gen-level top quark transverse momenta.

    The top pt reweighting parameters should be given as an auxiliary entry in the config:

    .. code-block:: python

        cfg.x.top_pt_reweighting_params = {
            "a": 0.0615,
            "a_up": 0.0615 * 1.5,
            "a_down": 0.0615 * 0.5,
            "b": -0.0005,
            "b_up": -0.0005 * 1.5,
            "b_down": -0.0005 * 0.5,
        }

    *get_top_pt_config* can be adapted in a subclass in case it is stored differently in the config.

    :param events: awkward array containing events to process
    """
    # check the number of gen tops
    if ak.any((n_tops := ak.num(events.gen_top.t, axis=1)) != 2):
        raise Exception(
            f"{self.cls_name} can only run on events with two generator top quarks, but found counts of "
            f"{','.join(map(str, sorted(set(n_tops))))}",
        )

    # clamp top pt
    top_pt = events.gen_top.t.pt
    if self.cfg.pt_max >= 0.0:
        top_pt = ak.where(top_pt > self.cfg.pt_max, self.cfg.pt_max, top_pt)

    for variation in ["", "_up", "_down"]:
        # evaluate SF function
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
