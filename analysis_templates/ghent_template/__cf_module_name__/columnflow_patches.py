# coding: utf-8

"""
Collection of patches of underlying columnflow tasks.
"""

import os

import law
from columnflow.util import memoize


logger = law.logger.get_logger(__name__)


@memoize
def patch_bundle_repo_exclude_files():
    from columnflow.tasks.framework.remote import BundleRepo

    # get the relative path to CF_BASE
    cf_rel = os.path.relpath(os.environ["CF_BASE"], os.environ["__cf_short_name_uc___BASE"])

    # amend exclude files to start with the relative path to CF_BASE
    exclude_files = [os.path.join(cf_rel, path) for path in BundleRepo.exclude_files]

    # add additional files
    exclude_files.extend([
        "docs", "tests", "data", "assets", ".law", ".setups", ".data", ".github",
    ])

    # overwrite them
    BundleRepo.exclude_files[:] = exclude_files

    logger.debug("patched exclude_files of cf.BundleRepo")


@memoize
def patch_htcondor_workflow():
    from columnflow.tasks.framework.remote import HTCondorWorkflow

    # change the max_runtime parameter default
    HTCondorWorkflow.max_runtime._default = 0
    logger.debug("patched max_runtime of cf.HTCondorWorkflow")

    HTCondorWorkflow.htcondor_flavor._default = 'NO_STR'
    logger.debug("patched flavor of cf.HTCondorWorkflow")


@memoize
def patch_all():
    patch_bundle_repo_exclude_files()
    patch_htcondor_workflow()
