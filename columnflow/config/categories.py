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
        id=99,
        selection="sel_incl",
        label="inclusive",
    )
    cat_e = config.add_category(
        name="1e",
        id=1,
        selection="sel_1e",
        label="1 electron",
    )
    cat_mu = config.add_category(
        name="1mu",
        id=2,
        selection="sel_1mu",
        label="1 muon",
    )
    cat_e.add_category(
        name="1e_eq1b",
        id=3,
        selection="sel_1e_eq1b",
        label="1e, 1 b-tag",
    )
    cat_e.add_category(
        name="1e_ge2b",
        id=4,
        selection="sel_1e_ge2b",
        label=r"1e, $\geq$ 2 b-tags",
    )
    cat_mu.add_category(
        name="1mu_eq1b",
        id=5,
        selection="sel_1mu_eq1b",
        label="1mu, 1 b-tag",
    )
    cat_mu_bb = cat_mu.add_category(
        name="1mu_ge2b",
        id=6,
        selection="sel_1mu_ge2b",
        label=r"1mu, $\geq$ 2 b-tags",
    )
    cat_mu_bb.add_category(
        name="1mu_ge2b_lowHT",
        id=7,
        selection="sel_1mu_ge2b_lowHT",
        label=r"1mu, $\geq$ 2 b-tags, HT<=300 GeV",
    )
    cat_mu_bb.add_category(
        name="1mu_ge2b_highHT",
        id=8,
        selection="sel_1mu_ge2b_highHT",
        label=r"1mu, $\geq$ 2 b-tags, HT>300 GeV",
    )
