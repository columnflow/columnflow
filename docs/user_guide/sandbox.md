
# Sandboxes and Their Use in Columnflow

Columnflow is a backend framework designed to orchestrate columnar analyses entirely with Python.
Various steps of an analysis necessitate distinct software environments.
To maintain flexibility and lightweight configurations [venv](https://docs.python.org/3/library/venv.html) (virtual environments) are employed.

The following guide explains how to define, update, and utilize a sandbox within Columnflow.

## Whats needs to be defined

A Columnflow environment consists of two files:

1. **setup.sh:**, sets up a virtual environment in `$CF_VENV_PATH`.
2. **requirement.txt:** , defines the required modules and their versions.

## Where to Store the Corresponding Files

For organizational purposes, it is recommended to store your setup and requirement files in `$ANALYSIS_BASE/sandboxes/`, where `$ANALYSIS_BASE` is the root directory for your analysis.
The latter is usually the abbreviated form you specified for your analysis in upper case, e.g. the root directory for analysis `hbt` is `$HBT_BASE`.
It is of good practice to follow the naming conventions used by Columnflow.
Begin your setup file with "venv," for example, `venv_YOUR_ENVIRONMENT.sh`, and use only the environment name (without "venv") for your requirement file, e.g., `YOUR_ENVIRONMENT.txt`.

## The Setup File

Begin your setup file by referencing an existing setup file within the `$ANALYSIS_BASE/sandboxes/` directory.
In this example, we start from a copy of `venv_columnar.sh`:
We start from `venv_columnar.sh`

```{literalinclude} ../../sandboxes/venv_columnar.sh
:language: bash
```

You only need to change `CF_VENV_REQUIREMENTS` to point to your new requirement file.

## The requirement file

The requirement.txt uses pip notation.
A quick overview is given of commonly used notation:

```bash
# version 1
# Get Coffea from a GitHub repository with a specific commit/tag (@9ecdee5) as an egg file named coffea (#egg=coffea).
git+https://github.com/riga/coffea.git@9ecdee5#egg=coffea

# Get the latest version of dask-awkward, but do not exceed version 2023.2.
dask-awkward~=2023.2
```

For more information, refer to the official [pip documentation](https://pip.pypa.io/en/stable/reference/requirements-file-format/).

Columnflow manages sandboxes by using a version number at the very first line (`# version ANY_FLOAT`).
This version defines a software package, and it is good practice to change the version number whenever an environment is altered.

## How to Use the Namespace

One may wonder how to work with namespaces of certain modules when it is not guaranteed that this module is available.
For this case, the {py:meth}`~columnflow.util.maybe_import` function is there.
This utility function handles the import for you and makes the namespace available.

The input of {py:meth}`~columnflow.util.maybe_import` is the name of the module, provided as a string.

```python
# maybe_import version of:
# import tensorflow as tf
tf = maybe_import("tensorflow")
```

It is good practice to use `maybe_import` within the local namespace if you use the module once, and at the global namespace level if you intend to use multiple times.
