# coding: utf-8

"""
Calibration methods for testing purposes.
"""

from ap.util import maybe_import
from ap.calibration import calibrator

np = maybe_import("numpy")
ak = maybe_import("awkward")


@calibrator(
    uses={"nJet", "Jet_pt", "Jet_mass"},
    produces={
        "Jet_pt", "Jet_mass",
        "Jet_pt_jec_up", "Jet_mass_jec_up",
        "Jet_pt_jec_down", "Jet_mass_jec_down",
    },
)
def jec_test(events):
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
    events["Jet", "pt_jec_up"] = events.Jet.pt * 1.05
    events["Jet", "mass_jec_up"] = events.Jet.mass * 1.05
    events["Jet", "pt_jec_down"] = events.Jet.pt * 0.95
    events["Jet", "mass_jec_down"] = events.Jet.mass * 0.95

    return events


@calibrator(uses={jec_test}, produces={jec_test})
def calib_test(events):
    events = jec_test(events)

    return events
