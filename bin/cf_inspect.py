# coding: utf8
"""
utility script for quickly loading event arrays and related objects for interactive processing
"""
import awkward as ak
import coffea.nanoevents
import hist  # noqa
import json
import numpy as np  # noqa
import os
import pickle
import uproot


__all__ = ["load"]


def _load_json(fname):
    with open(fname, "r") as fobj:
        return json.load(fobj)


def _load_pickle(fname):
    with open(fname, "rb") as fobj:
        return pickle.load(fobj)


def _load_parquet(fname):
    return ak.from_parquet(fname)


def _load_nano_root(fname):
    source = uproot.open(fname)
    return coffea.nanoevents.NanoEventsFactory.from_root(
        source,
        runtime_cache=None,
        persistent_cache=None,
    ).events()


def load(fname):
    """
    Load file contents based on file extension.
    """
    basename, ext = os.path.splitext(fname)
    if ext == ".pickle":
        return _load_pickle(fname)
    elif ext == ".parquet":
        return _load_parquet(fname)
    elif ext == ".root":
        return _load_nano_root(fname)
    elif ext == ".json":
        return _load_json(fname)
    else:
        raise NotImplementedError(
            f"no loader implemented for extension '{ext}'",
        )


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

    if not args.files:
        raise ValueError("at least one file must be provided")

    objects = [
        load(fname) for fname in args.files
    ]
