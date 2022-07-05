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
        expression="sel_incl",
        id=99,
        label="inclusive",
        selection="sel_incl",
    )
    cat_e = config.add_category(
        name="1e",
        expression="sel_1e",
        id=1,
        label="1 electron",
        selection="sel_1e",
    )
    cat_mu = config.add_category(
        name="1mu",
        expression="sel_1mu",
        id=2,
        label="1 muon",
        selection="sel_1mu",
    )
    cat_e.add_category(
        name="1e_eq1b",
        expression="sel_1e_eq1b",
        id=3,
        label="1e, 1 b-tag",
        selection="sel_1e_eq1b",
    )
    cat_e.add_category(
        name="1e_ge2b",
        expression="sel_1e_ge2b",
        id=4,
        label=r"1e, $\geq$ 2 b-tags",
        selection="sel_1e_ge2b",
    )
    cat_mu.add_category(
        name="1mu_eq1b",
        expression="sel_1mu_eq1b",
        id=5,
        label="1mu, 1 b-tag",
        selection="sel_1mu_eq1b",
    )
    cat_mu_bb = cat_mu.add_category(
        name="1mu_ge2b",
        expression="sel_1mu_ge2b",
        id=6,
        label=r"1mu, $\geq$ 2 b-tags",
        selection="sel_1mu_ge2b",
    )
    cat_mu_bb.add_category(
        name="1mu_ge2b_lowHT",
        expression="sel_1mu_ge2b_lowHT",
        id=7,
        label=r"1mu, $\geq$ 2 b-tags, HT<=300 GeV",
        selection="sel_1mu_ge2b_lowHT",
    )
    cat_mu_bb.add_category(
        name="1mu_ge2b_highHT",
        expression="sel_1mu_ge2b_highHT",
        id=8,
        label=r"1mu, $\geq$ 2 b-tags, HT>300 GeV",
        selection="sel_1mu_ge2b_highHT",
    )
