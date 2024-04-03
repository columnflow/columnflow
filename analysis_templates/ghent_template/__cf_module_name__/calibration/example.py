# coding: utf-8

"""
Exemplary calibration methods.
"""

from columnflow.calibration import Calibrator, calibrator
from columnflow.production.cms.seeds import deterministic_seeds
from columnflow.util import maybe_import
from columnflow.columnar_util import set_ak_column

np = maybe_import("numpy")
ak = maybe_import("awkward")


@calibrator(
    uses={
        deterministic_seeds,
        "Jet.pt", "Jet.mass",
    },
    produces={
        deterministic_seeds,
        "Jet.pt", "Jet.mass",
        "Jet.pt_jec_up", "Jet.mass_jec_up",
        "Jet.pt_jec_down", "Jet.mass_jec_down",
    },
)
def example(self: Calibrator, events: ak.Array, **kwargs) -> ak.Array:
    # a) "correct" Jet.pt by scaling four momenta by 1.1 (pt<30) or 0.9 (pt<=30)
    # b) add 4 new columns faking the effect of JEC variations

    # add deterministic seeds that could (e.g.) be used for smearings
    events = self[deterministic_seeds](events, **kwargs)

    # a)
    pt_mask = ak.flatten(events.Jet.pt < 30)
    n_jet_pt = np.asarray(ak.flatten(events.Jet.pt))
    n_jet_mass = np.asarray(ak.flatten(events.Jet.mass))
    n_jet_pt[pt_mask] *= 1.1
    n_jet_pt[~pt_mask] *= 0.9
    n_jet_mass[pt_mask] *= 1.1
    n_jet_mass[~pt_mask] *= 0.9

    # b)
    events = set_ak_column(events, "Jet.pt_jec_up", events.Jet.pt * 1.05)
    events = set_ak_column(events, "Jet.mass_jec_up", events.Jet.mass * 1.05)
    events = set_ak_column(events, "Jet.pt_jec_down", events.Jet.pt * 0.95)
    events = set_ak_column(events, "Jet.mass_jec_down", events.Jet.mass * 0.95)

    return events
