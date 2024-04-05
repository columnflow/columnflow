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

    bg = config.add_process(
        name="background",
        id=9999,
        label="Background",

    )
    bg.add_process(config.get_process("dy"))
