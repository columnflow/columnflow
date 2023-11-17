# coding: utf8

"""
Utility script for checking the differences betreen multiple CF output files.
"""

from __future__ import annotations

import itertools

import awkward as ak

from columnflow.columnar_util import get_ak_routes
from cf_inspect import load


def diff(*arrays: ak.Array):
    assert len(arrays) >= 2, "need at least two arrays to compare"
    route_sets = [
        {route for route in get_ak_routes(arr)}
        for arr in arrays
    ]
    others_route_sets = [
        {
            route
            for other_route_set in itertools.chain(route_sets[:i], route_sets[i + 1:])
            for route in other_route_set
        }
        for i in range(len(arrays))
    ]
    unique_routes = [
        route_set - others_route_set
        for route_set, others_route_set in zip(route_sets, others_route_sets)
    ]
    common_routes = set.intersection(*route_sets)

    results = {
        "diff_global": False,
        "diff_route_ravel_lengths": False,
        "diff_route_outer_lengths": False,
        "diff_values": False,
        "diff_details": {},
        "unique_routes": unique_routes,
    }
    for route in common_routes:
        d = results["diff_details"][route] = {}

        # check if columns are the same length
        contents = [route.apply(arr) for arr in arrays]
        d["outer_lengths"] = [len(content) for content in contents]
        d["ravel_lengths"] = [len(ak.ravel(content)) for content in contents]
        d["all_outer_lengths_equal"] = (len(set(d["outer_lengths"])) <= 1)
        d["all_ravel_lengths_equal"] = (len(set(d["ravel_lengths"])) <= 1)

        # set flag and skip remaining checks for route
        if not d["all_outer_lengths_equal"]:
            results["diff_global"] = True
            results["diff_route_outer_lengths"] = True

        if not d["all_ravel_lengths_equal"]:
            results["diff_global"] = True
            results["diff_route_ravel_lengths"] = True
            continue

        # check if column values are equal
        same = ak.ones_like(contents[0], dtype=bool)
        for c in contents[1:]:
            same = same & (c == contents[0])

        same_toplevel = same
        while same_toplevel.ndim > 1:
            same_toplevel = ak.all(same_toplevel, axis=-1)

        all_same = ak.all(same)
        if not all_same:
            d["diff_mask"] = (~same)
            d["diff_where"] = ak.where(~same_toplevel)[0]
            results["diff_global"] = True
            results["diff_values"] = True

    return results


def main(files: list[str], objects: list[ak.Array]):
    diff_results = diff(*objects)

    # objects are identical: do noting
    if not diff_results["diff_global"]:
        return

    # objects are identical: do nothing
    print("Showing difference of arrays in files:")
    for i, fname in enumerate(files):
        print(f"  #{i}: {fname}")

    # show uniquely occurring routes
    for i, unique_routes in enumerate(diff_results["unique_routes"]):
        if unique_routes:
            print(f"\nThe following routes are only present in array #{i}:")
        for unique_route in sorted(unique_routes, key=lambda x: x.column):
            print(f"  - {unique_route.column}")

    diff_details = diff_results["diff_details"]

    # objects have no common routes: nothing more to do
    if not diff_details:
        return

    # show diff of common routes lengths
    if diff_results["diff_route_outer_lengths"] or diff_results["diff_route_ravel_lengths"]:
        print("\nOf the routes common to all files, the following have mismatched lengths:")
        for route, route_details in sorted(diff_details.items(), key=lambda k_v: k_v[0].column):
            if not route_details["all_outer_lengths_equal"]:
                print(f"  - {route.column}: {route_details['outer_lengths']}")
            elif not route_details["all_ravel_lengths_equal"]:
                print(f"  - {route.column}: {route_details['ravel_lengths']} (unraveled)")

    # show diff of common routes contents
    if diff_results["diff_values"]:
        print("\nThe following common routes have different values at these positions:")
        for route, route_details in sorted(diff_details.items(), key=lambda k_v: k_v[0].column):
            diff_where = route_details.get("diff_where", None)
            if diff_where is None:
                continue

            n_diff = len(diff_where)
            print(f"  - {route.column}: {diff_where} ({n_diff} entries)")
            for i, arr in enumerate(objects):
                contents_where_different = route.apply(arr)[diff_where]
                print(f"      - #{i}: {contents_where_different}")


if __name__ == "__main__":
    import argparse

    ap = argparse.ArgumentParser(
        description=(
            "Utility script for checking multiple CF output files for equality.\n\n"
            "File contents are identified by the file extension. Supported formats are:\n"
            "    - 'parquet' (columnflow array outputs)\n"
        ),
    )

    ap.add_argument("files", metavar="FILE", nargs="+", help="one or more supported files")

    args = ap.parse_args()

    objects = [
        load(fname) for fname in args.files
    ]

    main(args.files, objects)
