# coding: utf-8

"""
Configuration of the Run 3 ttZ processes.
"""

import order as od
from columnflow.config_util import get_root_processes_from_campaign


def add_processes(config: od.Config, campaign: od.Campaign):
    # get all root processes
    procs = get_root_processes_from_campaign(campaign)

    config.add_process(procs.n.data)

    config.add_process(procs.n.tt)

    config.add_process(procs.n.dy)

    # How to add new processes:
    # Add custom process to encapsulate all background processes:
    bg_processes = ['dy']
    background = config.add_process(
        name="background",
        id=9999,  # cannot collide with ids defined in cmsdb though
        label="Background",
        xsecs = {campaign.ecm: sum([config.get_process(bg).get_xsec(campaign.ecm) for bg in bg_processes])}
    )
    for bg in bg_processes:
        background.add_process(config.get_process(bg))
