"""
Collection of helpers for styling, e.g.
- dicitonaries of defaults for variable definition, colors, labels, etc.
- functions to quickly create variable insts in a predefined way
"""

import order as od


#
# Processes
#

default_process_colors = {
    "data": "#000000",  # black
    "tt": "#cf9fff",  # green
    "dy_lep": "#377eb8",  # blue
}


def stylize_processes(config: od.Config) -> None:
    """
    Small helper that sets the process insts to analysis-appropriate defaults
    For now: only colors and unstacking
    Could also include some more defaults (labels, unstack, ...)
    """

    for proc in config.processes:
        # set default colors
        if color := default_process_colors.get(proc.name, None):
            proc.color1 = color
            proc.color2 = "#000000"

    config.x.default_legend_cfg = {
        "ncol": 2,
        "loc": "upper right",
        "fontsize": 15,
    }
