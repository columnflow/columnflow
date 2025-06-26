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

from columnflow.util import ipython_shell
from columnflow.types import Any


def _load_json(fname: str) -> Any:
    with open(fname, "r") as fobj:
        return json.load(fobj)


def _load_pickle(fname: str) -> Any:
    with open(fname, "rb") as fobj:
        return pickle.load(fobj)


def _load_parquet(fname: str) -> ak.Array:
    return ak.from_parquet(fname)


def _load_nano_root(fname: str) -> ak.Array:
    source = uproot.open(fname)
    return coffea.nanoevents.NanoEventsFactory.from_root(
        source,
        runtime_cache=None,
        persistent_cache=None,
    ).events()


def load(fname: str) -> Any:
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


def list_content(data: Any) -> None:
    if isinstance(data, ak.Array):
        from columnflow.columnar_util import get_ak_routes
        routes = get_ak_routes(data)
        print(f"found {len(routes)} routes:")
        print("  - " + "\n  - ".join(map(str, routes)))

    else:
        raise NotImplementedError(f"content listing not implemented for '{type(data)}'")


if __name__ == "__main__":
    import argparse

    ap = argparse.ArgumentParser(
        add_help=False,
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

    ap.register("action", "help", argparse._HelpAction)
    ap.add_argument("files", metavar="FILE", nargs="+", help="one or more supported files")
    ap.add_argument("--events", "-e", action="store_true", help="assume files to contain event info")
    ap.add_argument("--hists", "-h", action="store_true", help="assume files to contain histograms")
    ap.add_argument("--list", "-l", action="store_true", help="list contents of the loaded file")
    ap.add_argument("--help", action="help", help="show this help message and exit")

    args = ap.parse_args()

    objects = [load(fname) for fname in args.files]
    if len(objects) == 1:
        objects = objects[0]
    print("file content loaded into variable 'objects'")

    # interpret data
    interpreted = objects
    if args.events:
        # preload common packages
        import awkward as ak  # noqa
        import numpy as np   # noqa

        events = interpreted = objects
        print("events loaded from objects[0] into variable 'events'")

    elif args.hists:
        # preload common packages
        import hist  # noqa

        if isinstance(objects, hist.Hist):
            h = interpreted = objects
            print("histogram loaded from objects[0] into variable 'h'")
        else:
            hists = interpreted = objects
            print("histograms loaded from objects[0] into variable 'hists'")

    # list content
    if args.list:
        list_content(interpreted)

    # start the ipython shell
    ipython_shell()()
