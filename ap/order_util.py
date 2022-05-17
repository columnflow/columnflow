# coding: utf-8

"""
Some functions that seemed useful in general
"""

""" inputs list of process names, outputs list of dataset names """
def getDatasetNamesFromProcesses(config, processes):
    if not processes:
        raise ValueError("From getDatasetNamesFromProcesses: given list of processes is empty.")
    datasets=[]
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
        for l in proc.get_leaf_processes():
            print(l.name)
            if l.name in config.analysis.get_datasets(config).names():
                datasets.append(config.get_dataset(l.name).name)
            else:
                print("Warning: no dataset for process_leaf %s" % l.name)
    return datasets
