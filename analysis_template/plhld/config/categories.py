# coding: utf-8

"""
Definition of categories.
"""

import order as od


def add_categories(config: od.Config) -> None:
    """
    Adds all categories to a *config*.
    """
    config.add_category(
        name="incl",
        id=1,
        selection="sel_incl",
        label="inclusive",
    )

    cat_e = config.add_category(  # noqa
        name="1e",
        id=100,
        selection="sel_1e",
        label="1 Electron",
    )

    cat_mu = config.add_category( # noqa
        name="1mu",
        id=200,
        selection="sel_1mu",
        label="1 Muon",
    )
