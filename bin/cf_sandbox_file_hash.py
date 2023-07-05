#!/usr/bin/env python
# coding: utf-8

"""
Standalone script that takes the path to a sandbox file and returns a unique hash based on its path
relative to certain environment variables.

- If it is found to be relative to CF_BASE, the path will be relative to $CF_BASE.
- If it is found to be relative to CF_REPO_BASE, the path to hash will be relative to $CF_REPO_BASE.
  In case an alias is defined (usually by an upstream analysis repository), this alias is used
  instead.
- If none of the above applies, the requestd path is used unchanged.


Example 1:

> cf_sandbox_file_hash /path/to/cf/sandboxes/venv_columnar.sh --path
> $CF_BASE/sandboxes/venv_columnar.sh

> cf_sandbox_file_hash /path/to/cf/sandboxes/venv_columnar.sh
> 4ef45674

The above creates the hash for $CF_BASE/sandboxes/venv_columnar.sh since CF_BASE is identical to
/path/to/cf.


Example 2:

> cf_sandbox_file_hash /path/to/my_analysis/sandboxes/venv_columnar.sh
> 7e853f05

The above creates the hash for $CF_REPO_BASE/sandboxes/venv_columnar.sh if CF_REPO_BASE_ALIAS is
_not_ set.


Example 3:

> cf_sandbox_file_hash /path/to/my_analysis/sandboxes/venv_columnar.sh
> 76463f9e

The above creates the hash for $MY_ANALYSIS_BASE/sandboxes/venv_columnar.sh if
CF_REPO_BASE_ALIAS=MY_ANALYSIS_BASE.
"""

import os
import hashlib


def create_sandbox_file_hash(
    sandbox_file: str,
    return_path: bool = False,
) -> str:
    """
    Creates a hash of a *sandbox_file*, relative to either CF_REPO_BASE if set, or CF_BASE
    otherwise. When it is found to be relative to CF_REPO_BASE and a CF_REPO_BASE_ALIAS is defined,
    CF_REPO_BASE_ALIAS is used instead.

    If *return_path* is *True*, this function does not return the hash but rather the path
    determined by the mechanism above.

    The hash itself is based on the first eight characters of the sha1sum of the relative sandbox
    file.
    """
    # prepare paths
    abs_sandbox_file = real_path(sandbox_file)
    cf_repo_base = real_path(os.environ["CF_REPO_BASE"])
    cf_base = real_path(os.environ["CF_BASE"])

    # determine the base paths
    rel_to_repo_base = is_relative_to(abs_sandbox_file, cf_repo_base)
    rel_to_base = is_relative_to(abs_sandbox_file, cf_base)

    # relative to CF_BASE or CF_REPO_BASE?
    if rel_to_base:
        path_to_hash = os.path.join("$CF_BASE", os.path.relpath(abs_sandbox_file, cf_base))
    elif rel_to_repo_base:
        # use the CF_REPO_BASE variable as the base path, optionally replaced by an alias
        base = os.getenv("CF_REPO_BASE_ALIAS", "CF_REPO_BASE")
        path_to_hash = os.path.join(f"${base}", os.path.relpath(abs_sandbox_file, cf_repo_base))
    else:
        # use the file as is
        path_to_hash = sandbox_file

    if return_path:
        return path_to_hash

    return hashlib.sha1(path_to_hash.encode("utf-8")).hexdigest()[:8]


def real_path(*path: str) -> str:
    """
    Takes *path* fragments and returns the real, absolute location with all variables expanded.
    """
    path = os.path.join(*map(str, path))
    while "$" in path or "~" in path:
        path = os.path.expandvars(os.path.expanduser(path))

    return os.path.realpath(path)


def is_relative_to(path: str, base: str) -> bool:
    """
    Returns *True* if a *path* is contained at any depth inside a *base* path, and *False*
    otherwise.
    """
    return not os.path.relpath(real_path(path), real_path(base)).startswith("..")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="creates a hash of a sandbox file",
    )
    parser.add_argument(
        "sandbox_file",
        help="the sandbox file",
    )
    parser.add_argument(
        "--path",
        "-p",
        action="store_true",
        help="print the path determined for hashing rather than the hash itself",
    )
    args = parser.parse_args()

    print(create_sandbox_file_hash(args.sandbox_file, return_path=args.path))
