# coding: utf-8

"""
Main entry point for top-level settings and fixes before anything else is imported.
"""

import law


# load contrib packages
law.contrib.load(
    "arc", "awkward", "cms", "coffea", "git", "htcondor", "numpy", "pyarrow", "telegram", "root",
    "tasks", "wlcg",
)

# initialize wlcg file systems once so that their cache cleanup is triggered if configured
wlcg_file_systems = law.config.get_expanded("target", "wlcg_file_systems", split_csv=True)
wlcg_file_systems = list(map(law.wlcg.WLCGFileSystem, wlcg_file_systems))
