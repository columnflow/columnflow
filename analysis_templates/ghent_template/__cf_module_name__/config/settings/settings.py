# coding: utf-8

"""
Definition of b-tagging settings.
"""


import law

from columnflow.util import call_once_on_config

from __cf_short_name_lc__.config.settings.btagging_settings import add_btagging_settings
from __cf_short_name_lc__.config.settings.jerc_settings import add_jerc_settings
from __cf_short_name_lc__.config.settings.lepton_settings import add_lepton_settings

import order as od


logger = law.logger.get_logger(__name__)


@call_once_on_config()
def add_settings(config: od.Config) -> None:

    add_jerc_settings(config)
    add_btagging_settings(config)
    add_lepton_settings(config)

    return
