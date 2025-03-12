# coding: utf-8

"""
Definition of groups in the Config.
"""

import order as od


def add_groups(config: od.Config) -> None:

    # process groups for conveniently looping over certain processs
    # (used in wrapper_factory and during plotting)
    config.x.process_groups = {
        "test": ["tt_dl"],
        "all": ["tt_dl", "dy", "data"],
        "sim": ["tt_dl", "dy"],
    }

    # dataset groups for conveniently looping over certain datasets
    # (used in wrapper_factory and during plotting)
    config.x.dataset_groups = {
        "test": ["tt_dl_powheg"],
        "all": ["tt_dl_powheg", "dy*", "data*"],
        "sim": ["tt_dl_powheg", "dy*"],
    }

    config.x.variable_groups = {
        "default": ["n_jet"],
    }

    # category groups for conveniently looping over certain categories
    # (used during plotting)
    config.x.category_groups = {
        "default": ["incl"],
    }

    config.x.shift_groups = {
        "jer": ["nominal", "jer_up", "jer_down"],
        "btag": ["nominal", "btag*"],
        "all": config.shifts.names(),
    }

    return
