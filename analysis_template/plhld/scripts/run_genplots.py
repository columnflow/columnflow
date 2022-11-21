# coding: utf-8

"""
A python script to quickly plot generator-level variables.
"""

from columnflow.tasks.cutflow import PlotCutflowVariables
version = "v1"
# workers = 6
variables = ["gen_*"]
processes = ["ggHH_*", "qqHH*"]
categories = ["incl"]
selector_steps = ["Lepton"]

plot_cutflow_vars = PlotCutflowVariables(
    version=version, walltime="5h", per_plot="processes",
    selector="gen_plhld",
    variables=variables,
    processes=processes,
    categories=categories,
    selector_steps=selector_steps,
    process_settings=[["."]],
    yscale="linear",
    skip_ratio="True",
    shape_norm="True",
    skip_cms="False",
    # remove_output="3ay",  # remove outputs starting from SelectEvents
)
plot_cutflow_vars.law_run()
