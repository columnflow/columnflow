# coding: utf8

"""
Utility script for quickly loading event arrays and related objects for interactive processing.
"""

from __future__ import annotations

__all__ = ["load"]

import os
import json
import pickle

import awkward as ak
import coffea.nanoevents
import uproot


def _load_json(fname: str):
    with open(fname, "r") as fobj:
        return json.load(fobj)


def _load_pickle(fname: str):
    with open(fname, "rb") as fobj:
        return pickle.load(fobj)


def _load_parquet(fname: str):
    return ak.from_parquet(fname)


def _load_nano_root(fname: str):
    source = uproot.open(fname)
    return coffea.nanoevents.NanoEventsFactory.from_root(
        source,
        runtime_cache=None,
        persistent_cache=None,
    ).events()


def load(fname: str):
    """
    Load file contents based on file extension.
    """
    basename, ext = os.path.splitext(fname)
    if ext == ".pickle":
        return _load_pickle(fname)
    if ext == ".parquet":
        return _load_parquet(fname)
    if ext == ".root":
        return _load_nano_root(fname)
    if ext == ".json":
        return _load_json(fname)
    raise NotImplementedError(f"no loader implemented for extension '{ext}'")


if __name__ == "__main__":
    import argparse

    ap = argparse.ArgumentParser(
        description=(
            "Utility script for quickly loading event arrays, histograms or other supported "
            "objects from files for interactive processing.\n\n"
            "File contents are identified by the file extension. Supported formats are:\n"
            "    - 'root' (NanoAOD ROOT files)\n"
            "    - 'parquet' (columnflow array outputs)\n"
            "    - 'pickle' (columnflow histogram outputs)\n"
            "    - 'json'"
        ),
    )

    ap.add_argument("files", metavar="FILE", nargs="+", help="one or more supported files")

    args = ap.parse_args()

    objects = [
        load(fname) for fname in args.files
    ]
