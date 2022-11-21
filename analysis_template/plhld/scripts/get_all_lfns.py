# coding: utf-8

"""
Minimal script to run GetDatasetLFNs for all datasets of the 2017 config
"""

from columnflow.tasks.external import GetDatasetLFNs
from plhld.config.config_2017 import config_2017

for dataset in config_2017.datasets:
    task = GetDatasetLFNs(dataset=dataset.name)
    task.law_run()
