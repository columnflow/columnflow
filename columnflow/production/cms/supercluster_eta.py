"""
Module to calculate Photon super cluster eta.
Source: https://twiki.cern.ch/twiki/bin/view/CMS/EgammaNanoAOD#How_to_get_photon_supercluster_e
"""

import law  # type: ignore
import functools

from columnflow.production import Producer, producer
from columnflow.util import maybe_import
from columnflow.columnar_util import set_ak_column
from columnflow.types import Callable

np = maybe_import("numpy")  # type: ignore
ak = maybe_import("awkward")  # type: ignore

logger = law.logger.get_logger(__name__)

set_ak_column_f32: Callable = functools.partial(set_ak_column, value_type=np.float32)  # type: ignore


@producer(
    uses={"Photon.{phi,eta,isScEtaEB,isScEtaEE}", "PV.{x,y,z}"},
    produces={"Photon.superclusterEta"},
)
def photon_sceta(self: Producer, events: ak.Array, **kwargs) -> ak.Array:  # type: ignore
    """
    Returns the photon super cluster eta.
    
    Adapted from https://twiki.cern.ch/twiki/bin/view/CMS/EgammaNanoAOD#How_to_get_photon_supercluster_e
    Original authors: Peter Major et. al.
    
    Context
    In nanoAOD, `photon_eta` is `photon.eta()` which is different from
    `photon.superCluster()->eta()`.
    Photon supercluster eta is w.r.t (0,0,0), while photon eta is w.r.t.
    primary vertex. 
    """  # noqa

    # tan(theta/2)
    tg_theta_over_2 = np.exp(-events.Photon.eta)

    # tan(atan(tg_theta_over_2)*2)
    tg_theta = 2 * tg_theta_over_2 / (1 - tg_theta_over_2 * tg_theta_over_2)

    pv_x = events.PV.x
    pv_y = events.PV.y
    pv_z = events.PV.z

    # array for the super cluster eta
    # if nothing is to be done, just copy original eta
    tg_sctheta = events.Photon.eta
    # barrel region
    if ak.any(events.Photon.isScEtaEB):
        R = 130

        # calculate the angle in the x-y plane
        # use numpy to account for the right quadrant
        angle_x0_y0 = np.arctan2(pv_y, pv_x)

        alpha = angle_x0_y0 + (np.pi - events.Photon.phi)
        sin_beta = np.sqrt(np.square(pv_x) + np.square(pv_y)) / R * np.sin(alpha)
        beta = abs(np.asin(sin_beta))
        gamma = np.pi / 2 - alpha - beta
        l = np.sqrt((
            np.square(R) + np.square(pv_x) + np.square(pv_y) -
            2 * R * np.sqrt(np.square(pv_x) + np.square(pv_y)) * np.cos(gamma)
        ))

        z0_zSC = l / tg_theta
        tg_sctheta = ak.where(
            events.Photon.isScEtaEB, R / (pv_z + z0_zSC), tg_sctheta,
        )

    # endcap
    if ak.any(events.Photon.isScEtaEE):
        intersection_z = ak.where(events.Photon.eta > 0, 310, -310)
        base = intersection_z - pv_z
        r = base * tg_theta

        crystalX = pv_x + r * np.cos(events.Photon.phi)
        crystalY = pv_y + r * np.sin(events.Photon.phi)
        tg_sctheta = ak.where(
            events.Photon.isScEtaEE,
            np.sqrt(np.square(crystalX) + np.square(crystalY)) / intersection_z,  # type: ignore
            tg_sctheta,
        )

    sctheta = np.atan(tg_sctheta)
    sctheta = ak.where(sctheta < 0, sctheta + np.pi, sctheta)
    tg_sctheta_over_2 = np.tan(sctheta / 2)
    events = set_ak_column_f32(events, "Photon.superclusterEta", -np.log(tg_sctheta_over_2))

    return events


@producer(
    uses={"Electron.{eta,deltaEtaSC}"},
    produces={"Photon.superclusterEta"},
)
def electron_sceta(self: Producer, events: ak.Array, **kwargs) -> ak.Array:  # type: ignore
    """
    Returns the electron super cluster eta.
    """

    events = set_ak_column_f32(
        events, "Electron.superclusterEta",
        events.Electron.eta + events.Electron.deltaEtaSC,
    )
    return events
