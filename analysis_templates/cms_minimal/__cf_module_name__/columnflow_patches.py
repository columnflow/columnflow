# coding: utf-8

"""
Collection of patches of underlying columnflow tasks.
"""

import law
from columnflow.util import memoize


logger = law.logger.get_logger(__name__)


@memoize
def patch_bundle_repo_exclude_files():
    from columnflow.tasks.framework.remote import BundleRepo

    # add additional files to exclude
    BundleRepo.exclude_files += ["docs"]

    logger.debug("patched exclude_files of cf.BundleRepo")


@memoize
def patch_all():
    patch_bundle_repo_exclude_files()
