# coding: utf-8

"""
Helpers and utilities for working with columnar libraries (Ghent cms group)
"""

from __future__ import annotations
from typing import Literal

__all__ = [
    "TetraVec", "safe_concatenate",
]

from columnflow.util import maybe_import
from columnflow.types import Sequence

ak = maybe_import("awkward")
coffea = maybe_import("coffea")


def TetraVec(arr: ak.Array, keep: Sequence | str | Literal[-1] = -1) -> ak.Array:
    """
    create a Lorentz for fector from an awkward array with pt, eta, phi, and mass fields
    """
    mandatory_fields = ("pt", "eta", "phi", "mass")
    exclude_fields = ("x", "y", "z", "t")
    for field in mandatory_fields:
        assert hasattr(arr, field), f"Provided array is missing {field} field"
    if isinstance(keep, str):
        keep = [keep]
    elif keep == -1:
        keep = arr.fields
    keep = [*keep, *mandatory_fields]
    return ak.zip(
        {p: getattr(arr, p) for p in keep if p not in exclude_fields},
        with_name="PtEtaPhiMLorentzVector",
        behavior=coffea.nanoevents.methods.vector.behavior,
    )


def safe_concatenate(arrays, *args, **kwargs):
    n = len(arrays)
    if n > 2 ** 7:
        c1 = safe_concatenate(arrays[:n // 2], *args, **kwargs)
        c2 = safe_concatenate(arrays[n // 2:], *args, **kwargs)
        return ak.concatenate([c1, c2], *args, **kwargs)
    return ak.concatenate(arrays, *args, **kwargs)
