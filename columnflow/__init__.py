# coding: utf-8

"""
Main entry point for top-level settings and fixes before anything else is imported.
"""

import os
import re
import time
import logging

import law

# package infos
from columnflow.__version__ import (  # noqa
    __doc__, __author__, __email__, __copyright__, __credits__, __contact__, __license__,
    __status__, __version__,
)


logger = logging.getLogger(f"{__name__}_module_loader")

# version info
m = re.match(r"^(\d+)\.(\d+)\.(\d+)(-.+)?$", __version__)
version = tuple(map(int, m.groups()[:3])) + (m.group(4),)

#: Location of the documentation.
docs_url = os.getenv("CF_DOCS_URL", "https://columnflow.readthedocs.io/en/latest")

#: Location of the repository on github.
github_url = os.getenv("CF_GITHUB_URL", "https://github.com/columnflow/columnflow")

#: Boolean denoting whether the environment is in a local environment (based on ``CF_LOCAL_ENV``).
env_is_local = law.util.flag_to_bool(os.getenv("CF_LOCAL_ENV", "false"))

#: Boolean denoting whether the environment is in a remote job (based on ``CF_REMOTE_ENV``).
env_is_remote = law.util.flag_to_bool(os.getenv("CF_REMOTE_ENV", "false"))

#: Boolean denoting whether the environment is in a remote job on the WLCG (based on ``CF_ON_GRID``).
env_is_grid = law.util.flag_to_bool(os.getenv("CF_ON_GRID", "false"))

#: Boolean denoting whether the environment is in a remote job on a HTCondor cluster (based on ``CF_ON_HTCONDOR``).
env_is_htcondor = law.util.flag_to_bool(os.getenv("CF_ON_HTCONDOR", "false"))

#: Boolean denoting whether the environment is in a remote job on a Slurm cluster (based on ``CF_ON_SLURM``).
env_is_slurm = law.util.flag_to_bool(os.getenv("CF_ON_SLURM", "false"))

#: Boolean denoting whether the environment is in a CI env (based on ``CF_CI_ENV``).
env_is_ci = law.util.flag_to_bool(os.getenv("CF_CI_ENV", "false"))

#: Boolean denoting whether the environment is in a readthedocs env (based on ``CF_RTD_ENV``, or ``READTHEDOCS``).
env_is_rtd = law.util.flag_to_bool(os.getenv("CF_RTD_ENV" if "CF_RTD" in os.environ else "READTHEDOCS", "false"))

#: Boolean denoting whether the environment is used for development (based on ``CF_DEV``).
env_is_dev = not env_is_remote and law.util.flag_to_bool(os.getenv("CF_DEV", "false"))

#: String refering to the "flavor" of the cf setup.
flavor = os.getenv("CF_FLAVOR")
if isinstance(flavor, str):
    flavor = flavor.lower()

# load contrib packages
law.contrib.load(
    "arc", "awkward", "git", "htcondor", "numpy", "pyarrow", "telegram", "root", "slurm", "tasks",
    "wlcg", "matplotlib", "slack", "mattermost",
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
        wlcg_file_systems = [
            law.wlcg.WLCGFileSystem(fs.strip())
            for fs in law.config.get_expanded("outputs", "wlcg_file_systems", [], split_csv=True)
        ]

    # initialize producers, calibrators, selectors, reducers, categorizers, ml models, hist producers and stat models
    from columnflow.util import maybe_import

    def load(module, group):
        t0 = time.perf_counter()
        maybe_import(module)
        duration = law.util.human_duration(seconds=time.perf_counter() - t0)
        logger.debug(f"loaded {group} module '{module}', took {duration}")

    import columnflow.calibration  # noqa
    if law.config.has_option("analysis", "calibration_modules"):
        for m in law.config.get_expanded("analysis", "calibration_modules", [], split_csv=True):
            load(m.strip(), "calibration")

    import columnflow.selection  # noqa
    if law.config.has_option("analysis", "selection_modules"):
        for m in law.config.get_expanded("analysis", "selection_modules", [], split_csv=True):
            load(m.strip(), "selection")

    import columnflow.reduction  # noqa
    if law.config.has_option("analysis", "reduction_modules"):
        for m in law.config.get_expanded("analysis", "reduction_modules", [], split_csv=True):
            load(m.strip(), "reduction")

    import columnflow.production  # noqa
    if law.config.has_option("analysis", "production_modules"):
        for m in law.config.get_expanded("analysis", "production_modules", [], split_csv=True):
            load(m.strip(), "production")

    import columnflow.histogramming  # noqa
    if law.config.has_option("analysis", "hist_production_modules"):
        for m in law.config.get_expanded("analysis", "hist_production_modules", [], split_csv=True):
            load(m.strip(), "hist production")

    import columnflow.categorization  # noqa
    if law.config.has_option("analysis", "categorization_modules"):
        for m in law.config.get_expanded("analysis", "categorization_modules", [], split_csv=True):
            load(m.strip(), "categorization")

    import columnflow.ml  # noqa
    if law.config.has_option("analysis", "ml_modules"):
        for m in law.config.get_expanded("analysis", "ml_modules", [], split_csv=True):
            load(m.strip(), "ml")

    import columnflow.inference  # noqa
    if law.config.has_option("analysis", "inference_modules"):
        for m in law.config.get_expanded("analysis", "inference_modules", [], split_csv=True):
            load(m.strip(), "inference")

    # preload all task modules so that task parameters are globally known and accepted
    if law.config.has_section("modules"):
        for m in law.config.options("modules"):
            load(m.strip(), "task")

    # cleanup
    del m
