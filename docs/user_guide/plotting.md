# Plotting

In columnflow, there are multiple tasks to create plots. This section showcases, how to create and
customize a plot based on the {py:class}`~columnflow.tasks.plotting.PlotVariables1D` task. A
detailed overview of all plotting tasks is given in the [Plotting tasks](../task_overview/plotting.md) section.

## Creating your first plot

Assuming you used the analysis template to setup your analysis, you can create a first plot by running
```
law run cf.PlotVariables1D --version v1 --calibrators example --selector example --producer example --processes data,tt,st --variables n_jet
```
This will run the full analysis chain for the given processes (data, tt, st) and should create a plot looking like this:

(Include plot?)

The PlotVariables1D task is located at the bottom of our
[task graph](https://github.com/columnflow/columnflow/wiki#default-task-graph), which means that
all tasks leading to PlotVariables1D will be run for all datasets corresponding to the
```--processes``` we requested using the Calibrators, Selector, and Producers
(often referred to as CSPs) as requested. Examples on how to
implement your own CSPs can be found in the [calibrators](building_blocks/calibrators),
[selectors](building_blocks/selectors), and [producers](building_blocks/producers) sections of the
user guide. The ```--variables``` parameter defines, for which variables we want to create histograms
and plots. Variables are order objects that need to be defined in the config as shown in the
[config objects](building_blocks/config_objects) section and the column corresponding to the expression
statement needs to be stored either after the {py:class}`~columnflow.tasks.reduction.ReduceEvents`
task or by a requested Producer.


## Customization of plots

There are many different parameters implemented that allow customizing the style of a plot. A short
description to all plotting parameters is given in the [Plotting tasks](../task_overview/plotting.md).
In the following, a few exemplary task calls are given to present the usage of our plotting parameters.

Per default, the PlotVariables1D task creates one plot per variable with all Monte Carlo backgrounds being included
in a stack and data being shown as separate points. The bottom subplot shows the ratio between signal
and all processes included in the stack and can be disabled via the ```--skip_ratio``` parameter.
To change the text next to the label, you can add the ```--cms-label``` parameter.
To compare shapes of multiple processes, you might want to plot each process separately as one line.
To achieve this, you can use the ```unstack``` option of the ```--process-settings``` parameter. This
parameter can also be used to change other attributes of your process instances, such as color, label,
and the scale. To better compare shapes of processes, we can normalize each line with the
```--shape-norm``` parameter. Combining all the previously discussed parameters might lead to a task
call such as

```
law run cf.PlotVariables1D --version v1 --processes tt,st --variables n_jet,jet1_pt \
    --skip-ratio --shape-norm --cms-label simpw \
    --process-settings "tt,unstack,color=#e41a1c:st,unstack,label=Single Top"
```

to produce the following plot:

(TODO)


Parameters that only contain a single value can also be passed via the ```--general-settings```.
We can also change the y-scale of the plot to a log scale by adding ```--yscale log``` and change some
properties of specific variables via the ```variable-settings``` parameter. An exemplary task call
might be

```
law run cf.PlotVariables1D --version v1 --processes tt,st --variables n_jet,jet1_pt \
    --general-settings "skip_ratio,shape_norm,yscale=log,cms-label=simpw" \
    --variable-settings "n_jet,y_title=Events,x_title=N jets:jet1_pt,rebin=10,x_title=Leading jet \$p_{T}\$"
```

(TODO: dropdown not working)
::{dropdown} Limitations of the ```variable_settings```
While in theory, we can change anything inside the variable and process instances via the
```variable_settings```, there are certain attributes that are already used during the creation
of the histograms (e.g. the ```expression``` and the ```binning```) and since our ```variable_settings```
parameter only modifies these attributes during the runtime of our plotting task, this will not
impact our final results.
::


For the ```general_settings```, ```process_settings```, and ```variable_settings``` you can define
defaults and groups in the config, e.g. via

```
config_inst.x.default_variable_settings = {"jet1_pt": {"rebin": 4, "x_title": r"Leading jet $p_{T}$"}}
config_inst.x.process_settings_groups = {
    "unstack_processes": {proc: {"unstack": True} for proc in ("tt", "st")},
}
config_inst.x.general_settings_groups = {
    "compare_shapes": {"skip_ratio": True, "shape_norm": True, "yscale": "log", "cms_label": "simpw"},
}
```
The default is automatically used when no parameter is given in the task call, and the groups can
be used directly on the command line and will be resolved automatically. Our previously defined
defaults and groups will be used e.g. by the following task call:
```
law run cf.PlotVariables1D --version v1 --processes tt,st --variables n_jet,jet1_pt \
    --process-settings unstack_processes --general-settings compare_shapes
```


## Creating 2D plots

TODO


## Creating cutflow plots

The previously discussed plotting functions only create plots after applying the full event selection.
To allow inspecting and optimizing an event and object selection, Columnflow also includes plotting
tasks that can be run before applying any event selections.

To create a simple cutflow plots, displaying event yields after each individual selection step,
you can use the {py:class}`~columnflow.tasks.cutflow.PlotCutflow` task, e.g. via calling
```
law run cf.PlotCutflow --version v1 --calibrators example --selector example --processes tt,st --selector-steps jet,muon
```
This will produce a plot with three bins, containing the event yield before applying any selection
and after each selector step, where we always apply the locical and of all previous selector steps.

To create plots of variables as part of the cutflow, we also provide the
{py:class}`~columnflow.tasks.cutflow.PlotCutflowVariables1D`, which mostly behaves the same as the
PlotVariables1D task.

The main difference is that it also includes the ```--selector-steps``` parameter.... (TODO)


## Creating plots for different shifts

Like most tasks, our plotting tasks also contain the ```--shift``` parameter that allows requesting
the outputs for a certain type of systematic variation. Per default, the ```shift```parameter is set
to "nominal", but you could also produce your plot with a certain systematic uncertainty varied
up or down, e.g. via running
```
law run cf.PlotVariables1D --version v1 --processes tt,st --variables n_jet --shift mu_up
```
If you already ran the same task call with ```--shift nominal``` before, this will only require to
produce new histograms and plots, as a shift such as the ```mu_up``` is typically implemented as an
event weight and therefore does not require to reproduce any columns. Other shifts such as ```jec_up```
also impact our event selection and therefore also need to re-run anything starting from
{py:class}`~columnflow.tasks.selection.SelectEvents`. A detailed overview on how to implement different
types of systematic uncertainties is given in the [systematics](systematics)
section (TODO: not existing).

For directly comparing differences introduced by one shift source, we provide the
{py:class}`~columnflow.tasks.plotting.PlotShiftedVariables1D` task. Instead of the ```--shift```
parameter, this task implements the ```--shift-sources``` task and creates one plot per shift source
displaying the nominal distribution (black) compared to the shift source varied up (red) and down (blue).
The task can be called e.g. via
```
law run cf.PlotShiftedVariables1D --version v1 --processes tt,st --variables n_jet --shift-sources mu
```
and produces the following plot:

(TODO: include plot?)

This produces per default only one plot containing the sum of all processes. To produce this plot
per process, you can use the {py:class}`~columnflow.tasks.plotting.PlotShiftedVariablesPerProcess1D`
task


## Directly displaying plots in the terminal

All plotting tasks also include a ```--view-cmd``` parameter that allows directly printing the plot
during the runtime of the task:
```
law run cf.PlotVariables1D --version v1 --processes tt,st --variables n_jet --view-cmd evince-previewer
```


## Using you own plotting function

While all plotting tasks provide default plotting functions, which implement many parameters to
customize the plot, it might be necessary to write your own plotting functions if you want to create
a specific type of plot. In that case, you can simply write a function that follows the signature
of all other plotting functions and call a plotting task with this function using the
`--plot-function` parameter.

An example on how to implement such a plotting function is shown in the following:


```{literalinclude} ../../analysis_templates/cms_minimal/__cf_module_name__/plotting/example.py
:language: python
:start-at: def my_plot1d_func(
:end-at: return fig, (ax,)
```
