# coding: utf-8

"""
Calibration methods for testing purposes.
"""

from columnflow.calibration import Calibrator, calibrator
from columnflow.production.seeds import deterministic_seeds
from columnflow.util import maybe_import
from columnflow.columnar_util import set_ak_column

np = maybe_import("numpy")
ak = maybe_import("awkward")


@calibrator(
    uses={
        "nJet", "Jet.pt", "Jet.mass",
    },
    produces={
        "Jet.pt", "Jet.mass",
        "Jet.pt_jec_up", "Jet.mass_jec_up",
        "Jet.pt_jec_down", "Jet.mass_jec_down",
    },
)
def jec_test(self: Calibrator, events: ak.Array, **kwargs) -> ak.Array:
    # a) "correct" Jet.pt by scaling four momenta by 1.1 (pt<30) or 0.9 (pt<=30)
    # b) add 4 new columns representing the effect of JEC variations

    # a)
    a_mask = ak.flatten(events.Jet.pt < 30)
    n_jet_pt = np.asarray(ak.flatten(events.Jet.pt))
    n_jet_mass = np.asarray(ak.flatten(events.Jet.mass))
    n_jet_pt[a_mask] *= 1.1
    n_jet_pt[~a_mask] *= 0.9
    n_jet_mass[a_mask] *= 1.1
    n_jet_mass[~a_mask] *= 0.9

    # b)
    set_ak_column(events, "Jet.pt_jec_up", events.Jet.pt * 1.05)
    set_ak_column(events, "Jet.mass_jec_up", events.Jet.mass * 1.05)
    set_ak_column(events, "Jet.pt_jec_down", events.Jet.pt * 0.95)
    set_ak_column(events, "Jet.mass_jec_down", events.Jet.mass * 0.95)

    return events


@calibrator(
    uses={jec_test, deterministic_seeds},
    produces={jec_test, deterministic_seeds},
)
def test(self: Calibrator, events: ak.Array, **kwargs) -> ak.Array:
    self[jec_test](events, **kwargs)

    self[deterministic_seeds](events, **kwargs)

    return events
