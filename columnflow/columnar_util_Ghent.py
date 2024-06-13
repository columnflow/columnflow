# coding: utf-8

"""
Helpers and utilities for working with columnar libraries (Ghent cms group)
"""

from __future__ import annotations

__all__ = [
    "TetraVec", "safe_concatenate",
]

from columnflow.util import maybe_import

ak = maybe_import("awkward")
coffea = maybe_import("coffea")


def TetraVec(arr: ak.Array) -> ak.Array:
    """
    create a Lorentz for fector from an awkward array with pt, eta, phi, and mass fields
    """
    for field in ["pt", "eta", "phi", "mass"]:
        assert hasattr(arr, field), f"Provided array is missing {field} field"
    TetraVec = ak.zip({"pt": arr.pt, "eta": arr.eta, "phi": arr.phi, "mass": arr.mass},
    with_name="PtEtaPhiMLorentzVector",
    behavior=coffea.nanoevents.methods.vector.behavior)
    return TetraVec


def safe_concatenate(arrays, *args, **kwargs):
    n = len(arrays)
    if n > 2 ** 7:
        c1 = safe_concatenate(arrays[:n // 2], *args, **kwargs)
        c2 = safe_concatenate(arrays[n // 2:], *args, **kwargs)
        return ak.concatenate([c1, c2], *args, **kwargs)
    return ak.concatenate(arrays, *args, **kwargs)
