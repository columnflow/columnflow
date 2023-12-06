# coding: utf-8

"""
Main entry point for top-level settings and fixes before anything else is imported.
"""

import os
import re
import logging

import law

# package infos
from columnflow.__version__ import (  # noqa
    __doc__, __author__, __email__, __copyright__, __credits__, __contact__, __license__,
    __status__, __version__,
)


logger = logging.getLogger(__name__)

# version info
m = re.match(r"^(\d+)\.(\d+)\.(\d+)(-.+)?$", __version__)
version = tuple(map(int, m.groups()[:3])) + (m.group(4),)

#: Boolean denoting whether the environment is in a remote job (based on ``CF_REMOTE_JOB``).
env_is_remote = law.util.flag_to_bool(os.getenv("CF_REMOTE_JOB", "0"))

#: Boolean denoting whether the environment is in a remote job on the WLCG (based on ``CF_ON_GRID``).
env_is_grid = law.util.flag_to_bool(os.getenv("CF_ON_GRID", "0"))

#: Boolean denoting whether the environment is used for development (based on ``CF_DEV``).
env_is_dev = not env_is_remote and law.util.flag_to_bool(os.getenv("CF_DEV", "0"))

#: Boolean denoting whether the environment is in a readthedocs env (based on ``CF_RTD_JOBS``, or `READTHEDOCS`).
env_is_rtd = law.util.flag_to_bool(os.getenv("CF_RTD_JOB" if "CF_RTD" in os.environ else "READTHEDOCS", "0"))

#: String refering to the "flavor" of the cf setup.
flavor = os.getenv("CF_FLAVOR")
if isinstance(flavor, str):
    flavor = flavor.lower()

# load contrib packages
law.contrib.load(
    "arc", "awkward", "git", "htcondor", "numpy", "pyarrow", "telegram", "root", "slurm", "tasks",
    "wlcg", "matplotlib",
)

# load flavor specific contrib packages
# if flavor == "cms":
#     law.contrib.load("cms")
# some core tasks (BundleCMSSW) need the cms contrib package, to be refactored, see #155
law.contrib.load("cms")

# initilize various objects
if not env_is_rtd:
    # initialize wlcg file systems once so that their cache cleanup is triggered if configured
    if law.config.has_option("outputs", "wlcg_file_systems"):
        try:
            wlcg_file_systems = [
                law.wlcg.WLCGFileSystem(fs.strip())
                for fs in law.config.get_expanded("outputs", "wlcg_file_systems", [], split_csv=True)
            ]
        except:
            print("Prints work!")
            raise Exception(f"""
CF_RDT_JOB: {os.getenv('CF_RDT_JOB')}
READTHEDOCS: {os.getenv('READTHEDOCS')}
CF_BBASE: {os.getenv('CF_BBASE')}
            """)

    # initialize producers, calibrators, selectors, categorizers, ml models and stat models
    from columnflow.util import maybe_import

    import columnflow.production  # noqa
    if law.config.has_option("analysis", "production_modules"):
        for m in law.config.get_expanded("analysis", "production_modules", [], split_csv=True):
            logger.debug(f"loading production module '{m}'")
            maybe_import(m.strip())

    import columnflow.calibration  # noqa
    if law.config.has_option("analysis", "calibration_modules"):
        for m in law.config.get_expanded("analysis", "calibration_modules", [], split_csv=True):
            logger.debug(f"loading calibration module '{m}'")
            maybe_import(m.strip())

    import columnflow.selection  # noqa
    if law.config.has_option("analysis", "selection_modules"):
        for m in law.config.get_expanded("analysis", "selection_modules", [], split_csv=True):
            logger.debug(f"loading selection module '{m}'")
            maybe_import(m.strip())

    import columnflow.categorization  # noqa
    if law.config.has_option("analysis", "categorization_modules"):
        for m in law.config.get_expanded("analysis", "categorization_modules", [], split_csv=True):
            logger.debug(f"loading categorization module '{m}'")
            maybe_import(m.strip())

    import columnflow.ml  # noqa
    if law.config.has_option("analysis", "ml_modules"):
        for m in law.config.get_expanded("analysis", "ml_modules", [], split_csv=True):
            logger.debug(f"loading ml module '{m}'")
            maybe_import(m.strip())

    import columnflow.inference  # noqa
    if law.config.has_option("analysis", "inference_modules"):
        for m in law.config.get_expanded("analysis", "inference_modules", [], split_csv=True):
            logger.debug(f"loading inference module '{m}'")
            maybe_import(m.strip())

    # preload all task modules so that task parameters are globally known and accepted
    if law.config.has_section("modules"):
        for m in law.config.options("modules"):
            logger.debug(f"loading task module '{m}'")
            maybe_import(m.strip())

    # cleanup
    del m
