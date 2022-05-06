# coding: utf-8

"""
Some functions that seemed useful in general
"""

""" inputs list of process names, outputs list of dataset names """
def getDatasetNamesFromProcesses(config, processes):
    if processes:
        procs = [config.get_process(p) for p in processes]
    else:
        print("From getDatasetNamesFromProcesses: given list of processes is empty. Take all processes instead.")
        procs = config.analysis.get_processes(p)
    datasets = []
    for p in procs:
        datasets += getDatasetNamesFromProcess(config, p.name)
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
