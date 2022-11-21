# coding: utf-8

"""
A python script to quickly run tasks for creating plots and datacards with an exemplary set of parameters.
"""

from columnflow.tasks.selection import SelectEvents
from columnflow.tasks.reduction import ReduceEventsWrapper
from columnflow.tasks.cutflow import PlotCutflow, PlotCutflowVariables
from columnflow.tasks.plotting import PlotVariables, PlotShiftedVariables
from columnflow.tasks.inference import CreateDatacards

#
# Setup parameters
#

# General
version = "v1"
workers = 4

# Event processing
"""
These parameters should be fixed in general, so it should be fine to just use the defaults (skip on command line)
The input value of these parameters define which function is used in the calibration/selection/production etc.
If you write a new file containing such a function, you need to include it in the 'law.cfg'
"""
calibrators = ["skip_jecunc"]
selector = "default"
producers = ["features"]
ml_model = "simple"
inference_model = "test"

# config, dataset, processes, shifts
config = "run2_2017_nano_v9"
dataset = "ggHH_kl_1_kt_1_sl_hbbhww_powheg"
processes = ["default"]
shift = "nominal"
shift_sources = ["minbias_xs"]

# for wrappers
configs = ["run2_2017_nano_v9"]
# datasets = ["all"]
datasets = ["ggHH*"]
shifts = ["nominal"]

# Histogramming, Plotting
variables = ["m_bb", "deltaR_bb"]
cutflow_vars = ["cf_n_jet"]
selector_steps = ["Trigger", "Lepton", "Jet", "Bjet"]
categories = ["incl"]

# Plot styling
plot_suffix = ""
process_settings = [["ggHH_kl_1_kt_1_sl_hbbhww", "unstack"], ["dy_lep", "color=#377eb8"]]
yscale = "linear"
skip_cms = "False"
skip_ratio = "False"
shape_norm = "False"
# rebin = 1  # TODO include in CF

# TODO: find out how to include the "workers" parameter in such a python script
selection = SelectEvents(
    version=version, walltime="5h",  # workers=workers,
    # calibrators=calibrators, selector=selector,
    dataset=dataset,
)
selection.law_run()

#
# run calibration/selection/production for a set of datasets, configs and shifts
#
reduction = ReduceEventsWrapper(
    version=version, walltime="5h",  # workers=workers,
    # calibrators=calibrators, selector=selector,
    datasets=datasets,
    # configs=configs,
    shifts=shifts,
)
# reduction.law_run()

#
# create cutflow plots
#

plot_cutflow = PlotCutflow(
    version=version, walltime="5h",
    calibrators=calibrators, selector=selector,
    shift=shift,
    processes=processes,
    categories=categories,
    selector_steps=selector_steps,
    process_settings=process_settings,
    yscale="linear",
    shape_norm="True",
    skip_cms=skip_cms,
)
# plot_cutflow.law_run()

#
# create plots using cutflow variables
#
for per_plot in ["processes", "steps"]:
    plot_cutflow_vars = PlotCutflowVariables(
        version=version, walltime="5h", per_plot=per_plot,
        calibrators=calibrators, selector=selector,
        shift=shift,
        variables=cutflow_vars,
        processes=processes,
        categories=categories,
        selector_steps=selector_steps,
        process_settings=process_settings,
        yscale="linear",
        skip_ratio="True",
        shape_norm="True",
        skip_cms=skip_cms,
    )
    # plot_cutflow_vars.law_run()

# create plots after the full workflow (with full eventweights applied)
plot_variables = PlotVariables(
    version=version, walltime="5h",
    calibrators=calibrators, selector=selector, producers=producers,
    shift=shift,
    variables=variables,
    processes=processes,
    categories=categories,
    process_settings=process_settings,
    yscale="linear",
    skip_ratio="False",
    shape_norm="True",
    skip_cms=skip_cms,
)
# plot_variables.law_run()

#
# create plots for nominal + up/down variations
#
plot_shifted_variables = PlotShiftedVariables(
    version=version, walltime="5h",
    calibrators=calibrators, selector=selector, producers=producers,
    shift_sources=shift_sources,
    variables=variables,
    processes=processes,
    categories=categories,
    process_settings=process_settings,
    yscale="linear",
    skip_ratio="False",
    shape_norm="True",
    skip_cms=skip_cms,
)
# plot_shifted_variables.law_run()

#
# create datacards
#
create_datacards = CreateDatacards(
    version=version, walltime="5h",
    inference_model=inference_model,
    calibrators=calibrators, selector=selector, producers=producers,
)
# create_datacards.law_run()
