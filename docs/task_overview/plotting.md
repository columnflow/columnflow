# Plotting tasks

The following tasks are dedicated to plotting.
For more informaiton, see {doc}`"Plotting <plotting documentation>"`.

(PlotVariablesTasks)=
- ```PlotVariables*```, ```PlotShiftedVariables*``` (e.g.
{py:class}`~columnflow.tasks.plotting.PlotVariables1D`,
{py:class}`~columnflow.tasks.plotting.PlotVariables2D`,
{py:class}`~columnflow.tasks.plotting.PlotShiftedVariables1D`): Tasks to plot the histograms created by
{py:class}`~columnflow.tasks.histograms.CreateHistograms` using the python package
[matplotlib](https://matplotlib.org/) with [mplhep](https://mplhep.readthedocs.io/en/latest/) style.
Several plot types are possible, including
plots of variables for different physical processes or plots of variables for a single physical
process but different shifts (e.g. jet-energy correction variations). The argument ```--variables```
followed by the name of the variables defined in the analysis config, separated by a comma, is
needed for these tasks to run. It is also possible to replace the ```--datasets``` argument
for these tasks by the ```--processes``` argument followed by the name of the physical processes to
be plotted, as defined in the analysis config. For the ```PlotShiftedVariables*``` plots, the
argument ```shift-sources``` is needed and replaces the argument ```shift```. The output format for
these plots can be given with the ```--file-types``` argument. It is possible to set a default for the
variables in the analysis config.


- ```PlotCutflow*``` (e.g. {py:class}`~columnflow.tasks.cutflow.PlotCutflow`,
{py:class}`~columnflow.tasks.cutflow.PlotCutflowVariables1D`): Tasks to plot the histograms created
by {py:class}`~columnflow.tasks.cutflow.CreateCutflowHistograms`. The
{py:class}`~columnflow.tasks.cutflow.PlotCutflowVariables1D` are plotted in a similar way to the
["PlotVariables*"](PlotVariablesTasks) tasks. The difference is that these plots show the selection
yields of the different selection steps defined in
{py:class}`~columnflow.tasks.selection.SelectEvents` instead of only after the
{py:class}`~columnflow.tasks.reduction.ReduceEvents` procedure. The selection steps to be shown
can be chosen with the ```--selector-steps``` argument. Without further argument, the outputs are
as much plots as the number of selector steps given. On the other hand, the
PlotCutflow task gives a single histograms containing only
the total event yields for each selection step given.
