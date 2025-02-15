# coding: utf-8

"""
Module to calculate Photon super cluster eta.
Source: https://twiki.cern.ch/twiki/bin/view/CMS/EgammaNanoAOD#How_to_get_photon_supercluster_e
"""

from __future__ import annotations

import law

from columnflow.production import producer
from columnflow.util import maybe_import
from columnflow.columnar_util import set_ak_column

np = maybe_import("numpy")
ak = maybe_import("awkward")


logger = law.logger.get_logger(__name__)


@producer(
    uses={"Electron.{pt,phi,eta,deltaEtaSC}"},
    produces={"Electron.superclusterEta"},
)
def electron_sceta(self, events: ak.Array, **kwargs) -> ak.Array:
    """
    Returns the electron super cluster eta.
    """
    sc_eta = events.Electron.eta + events.Electron.deltaEtaSC
    events = set_ak_column(events, "Electron.superclusterEta", sc_eta, value_type=np.float32)
    return events
