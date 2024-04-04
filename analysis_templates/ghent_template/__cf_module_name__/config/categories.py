# coding: utf-8

"""
Definition of categories.
"""

from collections import OrderedDict

import law

from columnflow.config_util import create_category_combinations
from columnflow.util import call_once_on_config

import order as od

logger = law.logger.get_logger(__name__)


@call_once_on_config()
def add_categories_selection(config: od.Config) -> None:
    """
    Adds categories to a *config*, that are typically produced in `SelectEvents`.
    """

    config.x.regions = ("incl", "CR_WZ")
    config.x.lepton_channels = ("2e", "1e1mu", "2mu")
    config.x.lepton_channel_labels = {"2e": "$ee$", "1e1mu": "$e\mu$", "2mu": "$\mu\mu$"}

    config.add_category(
        name="incl",
        id=1,
        selection="catid_selection_incl",
        label="Inclusive",
    )

    # add lepton categories defined in ___cf_short_name_lc__.selection.categories to the config
    for lepton_channel in config.x.lepton_channels:

        config.add_category(
            name=lepton_channel,
            selection=["catid_selection_{}".format(lepton_channel)],
            label=config.x.lepton_channel_labels[lepton_channel],
        )


@call_once_on_config()
def add_categories_production(config: od.Config) -> None:
    """
    Adds categories to a *config*, that are typically produced in `ProduceColumns`.
    """

    #
    # switch existing categories to different production module
    #

    for lepton_channel in config.x.lepton_channels:

        cat_lepton = config.get_category(lepton_channel)
        cat_lepton.selection = ["catid_{}".format(lepton_channel)]
