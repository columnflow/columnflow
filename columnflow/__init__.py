# coding: utf-8

"""
Main entry point for top-level settings and fixes before anything else is imported.
"""

import logging

import law


logger = logging.getLogger(__name__)


# package infos
from columnflow.__version__ import (  # noqa
    __doc__, __author__, __email__, __copyright__, __credits__, __contact__, __license__,
    __status__, __version__,
)


# load contrib packages
law.contrib.load(
    "arc", "awkward", "cms", "git", "htcondor", "numpy", "pyarrow", "telegram", "root", "tasks",
    "wlcg", "matplotlib",
)

# initialize wlcg file systems once so that their cache cleanup is triggered if configured
if law.config.has_option("outputs", "wlcg_file_systems"):
    wlcg_file_systems = [
        law.wlcg.WLCGFileSystem(fs.strip())
        for fs in law.config.get_expanded("outputs", "wlcg_file_systems", split_csv=True)
    ]


# initialize producers, calibrators, selectors, ml and stat models
from columnflow.util import maybe_import

import columnflow.production  # noqa
if law.config.has_option("analysis", "production_modules"):
    for mod in law.config.get_expanded("analysis", "production_modules", split_csv=True):
        logger.debug(f"loading production module '{mod}'")
        maybe_import(mod.strip())

import columnflow.calibration  # noqa
if law.config.has_option("analysis", "calibration_modules"):
    for mod in law.config.get_expanded("analysis", "calibration_modules", split_csv=True):
        logger.debug(f"loading calibration module '{mod}'")
        maybe_import(mod.strip())

import columnflow.selection  # noqa
if law.config.has_option("analysis", "selection_modules"):
    for mod in law.config.get_expanded("analysis", "selection_modules", split_csv=True):
        logger.debug(f"loading selection module '{mod}'")
        maybe_import(mod.strip())

import columnflow.ml  # noqa
if law.config.has_option("analysis", "ml_modules"):
    for mod in law.config.get_expanded("analysis", "ml_modules", split_csv=True):
        logger.debug(f"loading ml module '{mod}'")
        maybe_import(mod.strip())

import columnflow.inference  # noqa
if law.config.has_option("analysis", "inference_modules"):
    for mod in law.config.get_expanded("analysis", "inference_modules", split_csv=True):
        logger.debug(f"loading inference module '{mod}'")
        maybe_import(mod.strip())


# preload all task modules so that task parameters are globally known and accepted
if law.config.has_section("modules"):
    for mod in law.config.options("modules"):
        logger.debug(f"loading task module '{mod}'")
        maybe_import(mod.strip())
