# coding: utf-8

"""
Some functions that seemed useful in general.

Note: This file might be removed soon once we resolve
      https://github.com/uhh-cms/analysis_playground/issues/22.
"""

""" inputs list of process names, outputs list of dataset names """


def getDatasetNamesFromProcesses(config, processes):
    if not processes:
        raise ValueError("From getDatasetNamesFromProcesses: given list of processes is empty.")
    datasets = []
    for p in processes:
        datasets += getDatasetNamesFromProcess(config, p)
    return datasets


""" inputs name of a single process, outputs list of dataset names that exist for this process """


def getDatasetNamesFromProcess(config, process):
    datasets = []
    proc = config.get_process(process)
    if proc.is_leaf_process:
        datasets.append(config.get_dataset(process).name)
    else:
        for leaf in proc.get_leaf_processes():
            print(leaf.name)
            if leaf.name in config.analysis.get_datasets(config).names():
                datasets.append(config.get_dataset(leaf.name).name)
            else:
                print("Warning: no dataset for process_leaf %s" % leaf.name)
    return datasets
