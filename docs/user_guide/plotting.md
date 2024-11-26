# Plotting

In columnflow, there are multiple tasks to create plots.
This section showcases how to create and customize a plot based on the {py:class}`~columnflow.tasks.plotting.PlotVariables1D` task.
The usage of other plotting tasks is mostly analogous to the ```PlotVariables1D``` task.
The most important differences compared to ```PlotVariables1D``` are presented in a separate section for each of the other plotting tasks.
An overview of all plotting tasks is given in the [Plotting tasks](../task_overview/plotting_tasks.md) section.

## Creating your first plot

Assuming you used the analysis template to setup your analysis, you can create a first plot by running

```shell
law run cf.PlotVariables1D --version v1 \
    --calibrators example --selector example --producer example \
    --processes data,tt,st --variables n_jet --categories incl,2j
```

This will run the full analysis chain for the given processes (data, tt, st) and should create plots looking like this:

::::{grid} 1 1 2 2
:::{figure} ../plots/cf.PlotVariables1D_tpl_config_analy__1__12dfac316a__plot__proc_3_7727a49dc2__cat_incl__var_n_jet.pdf
:width: 100%
:::

:::{figure} ../plots/cf.PlotVariables1D_tpl_config_analy__1__12dfac316a__plot__proc_3_7727a49dc2__cat_2j__var_n_jet.pdf
:width: 100%
:::
::::

:::{dropdown} Where do I find that plot?
You can add ```--print-output 0``` to every task call, which will print the full filename of all outputs of the requested task.
Alternatively, you can add ```--fetch-output 0,a``` to directly copy all outputs of this task into the directory you are currently in.
Finally, there is the ```--view-cmd``` parameter you can add to directly display the plot during the runtime of the task, e.g. via ```--view-cmd evince-previewer``` or ```--view-cmd imgcat```.
:::

The ```PlotVariables1D``` task is located at the bottom of our [task graph](https://github.com/columnflow/columnflow/wiki#default-task-graph), which means that all tasks leading to ```PlotVariables1D``` will be run for all datasets corresponding to the ```--processes``` we requested using the {py:class}`~columnflow.calibration.Calibrator`s, {py:class}`~columnflow.selection.Selector`, and {py:class}`~columnflow.production.Producer`s (often referred to as CSPs) as requested.
In the following examples, we will skip the ```--calibrators```, ```--selector``` and ```--producers``` parameters, which means that the defaults defined in the config will be used automatically.
Examples on how to implement your own CSPs can be found in the [calibrators](building_blocks/calibrators), [selectors](building_blocks/selectors), and [producers](building_blocks/producers) sections of the user guide.
The ```--variables``` parameter defines for which variables we want to create histograms and plots.
Variables are [order](https://github.com/riga/order) objects that need to be defined in the config as shown in the [config objects](building_blocks/config_objects) section.
The column corresponding to the expression statement needs to be stored either after the {py:class}`~columnflow.tasks.reduction.ReduceEvents` task or as part of a ```Producer``` used in the {py:class}`~columnflow.tasks.production.ProduceColumns` task.
For each of the category given with the ```--categories``` parameter, one plot will be produced.
A detailed guide on how to implement categories in Columnflow is given in the [categories](building_blocks/categories) section.

To define which processes and datasets to consider when plotting, you can use the ```--processes``` and ```--datasets``` parameter.
When only processes are given, all datasets corresponding to the requested processes will be considered.
When only datasets are given, all processes in the config will be considered.
The ```--processes``` parameter can be used to change the order of processes in the stack and the legend (try for example ```--processes st,tt``` instead) and to further distinguish between sub-processes (e.g. via ```--processes tt_sl,tt_dl,tt_fh```).

:::{dropdown} IMPORTANT! Do not add the same dataset via multiple processes!
At the time of writing this documentation, there is still an issue present that histograms corresponding to a dataset can accidentally be used multiple times.
For example, when adding ```--processes tt,tt_sl```, the events corresponding to the dataset ```tt_sl_powheg``` will be displayed twice in the resulting plot.
:::

## Customization of plots

There are many different parameters implemented that allow customizing the style of a plot.
A short overview to all plotting parameters is given in the [Plotting tasks](../task_overview/plotting_tasks.md).
In the following, a few exemplary task calls are given to present the usage of our plotting parameters, using the {py:class}`~columnflow.tasks.plotting.PlotVariables1D` task.
Most parameters are shared between the different plotting tasks.
The most important changes regarding the task parameters are discussed in separate sections for each type of plotting task.

Per default, the ```PlotVariables1D``` task creates one plot per variable with all Monte Carlo processes being included in a stack and data being shown as separate points.
The bottom subplot shows the ratio between signal and all processes included in the stack and can be disabled via the ```--skip-ratio``` parameter.
To change the text next to the label, you can add the ```--cms-label``` parameter.
:::{dropdown} What are the ```cms-label``` options?
In general, this parameter accepts all types of strings, but there is a set of shortcuts for commonly used labels that will automatically be resolved:

```{literalinclude} ../../columnflow/plotting/plot_all.py
:language: python
:start-at: label_options
:end-at: "}"
```

:::

To compare shapes of multiple processes, you might want to plot each process separately as one line.
To achieve this, you can use the ```unstack``` option of the ```--process-settings``` parameter.
This parameter can also be used to change other attributes of your process instances, such as color, label, and the scale.
To better compare shapes of processes, we can normalize each line with the ```--shape-norm``` parameter.
Combining all the previously discussed parameters might lead to a task call such as

```shell
law run cf.PlotVariables1D --version v1 --processes tt,st --variables n_jet,jet1_pt \
    --skip-ratio --shape-norm --cms-label simpw \
    --process-settings "tt,unstack,color=#e41a1c:st,unstack,label=Single Top"
```

to produce the following plot:

::::{grid} 1 1 2 2
:::{figure} ../plots/cf.PlotVariables1D_tpl_config_analy__1__0191de868f__plot__proc_2_a2211e799f__cat_incl__var_jet1_pt__c1.pdf
:width: 100%
:::

:::{figure} ../plots/cf.PlotVariables1D_tpl_config_analy__1__0191de868f__plot__proc_2_a2211e799f__cat_incl__var_n_jet__c1.pdf
:width: 100%
:::
::::

Another option to compare shapes, which can be very helpful when comparing unstacked low statistics processes (usually signal processes) with the rest of the stack, is to use ``scale=stack`` in the ```--process-settings``` parameter.
This will automatically scale the process to the integral of all processes in the stack (the value of the integral is rounded to a reasonable number).
In the task call this would look like

```shell
law run cf.PlotVariables1D --version v1 --processes tt,st,hh --variables n_jet,jet1_pt \
    --process-settings "hh,unstack,scale=stack"
```

Parameters that only contain a single value can also be passed via the ```--general-settings```, which is a single comma-separated list of parameters, where the name and the value are separated via a `=`.
The value of each parameter is automatically resolved to either a float, bool, or a string.
When no `=` is present, the parameter is automatically set to True.
:::{dropdown} What is the advantage of setting parameters via the ```--general-settings``` parameter?
While there is no direct advantage of setting parameters via the ```--general-settings```, this parameter provides some convenience by allowing you to define defaults and groups in the config (will be discussed later in the guide).

Additionally, this parameter allows you to set parameters on the command line that are not directly implemented as task parameters.
This is especially helpful when you want to parametrize
{ref}`custom plotting functions <custom_plot_function>`.
:::

We can also change the y-scale of the plot to a log scale by adding ```--yscale log``` and change some properties of specific variables via the ```variable-settings``` parameter.
For example, we might want to create the plots of our two observables in one call, but would like to try out a rebinned version of `jet1_pt` that merges bins by a factor of 10.
A corresponding task call might be

```shell
law run cf.PlotVariables1D --version v1 --processes tt,st --variables n_jet,jet1_pt \
    --general-settings "skip_ratio,shape_norm,yscale=log,cms-label=simpw" \
    --variable-settings "n_jet,y_title=Events,x_title=N jets:jet1_pt,rebin=10,x_title=Leading jet \$p_{T}\$"
```

::::{grid} 1 1 2 2
:::{figure} ../plots/cf.PlotVariables1D_tpl_config_analy__1__c80529af83__plot__proc_2_a2211e799f__cat_incl__var_jet1_pt__c2.pdf
:width: 100%
:::

:::{figure} ../plots/cf.PlotVariables1D_tpl_config_analy__1__c80529af83__plot__proc_2_a2211e799f__cat_incl__var_n_jet__c2.pdf
:width: 100%
:::
::::

:::{dropdown} Slicing, Overflows and Underflows

On a variable level, slicing a histogram during the plotting task can be achieved by using the ```slice``` option in the ```--variable-settings``` parameter.
A task call that selects the 5th to 15th bin of the variable ```jet1_pt``` might look like

```shell
law run cf.PlotVariables1D --variable-settings "jet1_pt,slice=5;15" --version prod1 \
    --variables jet1_pt  --processes tt
```

For slicing ranges instead of bin numbers, you can append a "j" to the values, which are then interpreted as ```x_min``` and ```x_max``` values.
This only slices the histogram itself but does not modify the ```x_min``` and ```x_max``` range of the plot since they are taken from the variable inst, but this can be modified via `--variable-settings "jet1_pt,x_min=50,x_max=200"`.

Moving entries from the overflow (underflow) bin into the last (first) visible bin of the histogram during plotting, can be enabled by adding the `overflow` (`underflow`) auxiliary via ```--variable-settings```, e.g.

```shell
law run cf.PlotVariables1D --variable-settings "jet1_pt,overflow" --...
```

:::

:::{dropdown} Limitations of the ```variable_settings```
While in theory we can change anything inside the variable and process instances via the ```variable_settings``` parameter, there are certain attributes that are already used during the creation of the histograms (e.g. the ```expression``` and the ```binning```).
Since our ```variable_settings``` parameter only modifies these attributes during the runtime of our plotting task, this will not impact our final results.
:::

For the ```general_settings```, ```process_settings```, and ```variable_settings``` you can define defaults and groups in the config, e.g. via

```python
config_inst.x.default_variable_settings = {"jet1_pt": {"rebin": 4, "x_title": r"Leading jet $p_{T}$"}}
config_inst.x.process_settings_groups = {
    "unstack_processes": {proc: {"unstack": True} for proc in ("tt", "st")},
}
config_inst.x.general_settings_groups = {
    "compare_shapes": {"skip_ratio": True, "shape_norm": True, "yscale": "log", "cms_label": "simpw"},
}
```

The default is automatically used when no parameter is given in the task call, and the groups can be used directly on the command line and will be resolved automatically.
Our previously defined defaults and groups will be used e.g. by the following task call:

```shell
law run cf.PlotVariables1D --version v1 --processes tt,st --variables n_jet,jet1_pt \
    --process-settings unstack_processes --general-settings compare_shapes
```

::::{grid} 1 1 2 2
:::{figure} ../plots/cf.PlotVariables1D_tpl_config_analy__1__be60d3bca7__plot__proc_2_a2211e799f__cat_incl__var_jet1_pt__c3.pdf
:width: 100%
:::

:::{figure} ../plots/cf.PlotVariables1D_tpl_config_analy__1__be60d3bca7__plot__proc_2_a2211e799f__cat_incl__var_n_jet__c3.pdf
:width: 100%
:::
::::

## Creating 2D plots

Columnflow also provides the {py:class}`~columnflow.tasks.plotting.PlotVariables2D` task to create two-dimensional plots.
Two-dimensional histograms are created by passing two variables to the ```--variables``` parameter, separated by a ```-```. Here is an exemplary task call and their outputs.

```shell
law run cf.PlotVariables2D --version v1 \
    --processes tt,st --variables n_jet-jet1_pt,jet1_pt-n_jet
```

::::{grid} 1 1 2 2
:::{figure} ../plots/cf.PlotVariables2D_tpl_config_analy__1__b27b994979__plot__proc_2_a2211e799f__cat_incl__var_jet1_pt-n_jet.pdf
:width: 100%
:::

:::{figure} ../plots/cf.PlotVariables2D_tpl_config_analy__1__b27b994979__plot__proc_2_a2211e799f__cat_incl__var_n_jet-jet1_pt.pdf
:width: 100%
:::
::::

While most of the plotting parameters used in the ```PlotVariables1D``` task can be reused for this task, there are also some additional parameters only available for 2D plotting tasks.
For more information on the task parameters of the ```PlotVariables2D``` task, take a look into the [plotting task overview](../task_overview/plotting_tasks.md).

## Creating cutflow plots

The previously discussed plotting functions only create plots after applying the full event selection.
To allow inspecting and optimizing an event and object selection, Columnflow also includes plotting tasks that can produce plots after each individual selection step.

To create a simple cutflow plot, displaying event yields after each individual selection step, you can use the {py:class}`~columnflow.tasks.cutflow.PlotCutflow` task, e.g. via calling

```shell
law run cf.PlotCutflow --version v1 \
    --calibrators example --selector example --categories incl,2j \
    --shape-norm --process-settings tt,unstack:st,unstack \
    --processes tt,st --selector-steps jet,muon
```

::::{grid} 1 1 2 2
:::{figure} ../plots/cf.PlotCutflow_tpl_config_analy__1__12a17bf79c__cutflow__cat_incl.pdf
:width: 100%
:::

:::{figure} ../plots/cf.PlotCutflow_tpl_config_analy__1__12a17bf79c__cutflow__cat_2j.pdf
:width: 100%
:::
::::

This will produce a plot with three bins, containing the event yield before applying any selection and after each selector step, where we always apply the logical ```and``` of all previous selector steps.

:::{dropdown} What are the options for the ```--selector-steps```? Can I customize the step labels?
The steps listed in the ```--selector-steps``` parameter need to be defined by the {py:class}`~columnflow.selection.Selector` that has been used.
A detailed guide on how to implement your own selector can be found in the [Selections](building_blocks/selectors) guide.

Per default, the name of the selector step is used on the x-axis, but you can also provide custom step labels via the config:

```python
config_inst.x.selector_step_labels = {
    "muon": r"$N_{muon} = 1$",
    "jet": r"$N_{jets}^{AK4} \geq 1$",
}
```

:::

To create plots of variables as part of the cutflow, we also provide the {py:class}`~columnflow.tasks.cutflow.PlotCutflowVariables1D`, which mostly behaves the same as the {py:class}`~columnflow.tasks.plotting.PlotVariables1D` task.

```shell
law run cf.PlotCutflowVariables1D --version v1 \
    --calibrators example --selector example \
    --processes tt,st --variables cf_jet1_pt --categories incl \
    --selector-steps jet,muon --per-plot processes
```

::::{grid} 1 1 3 3
:::{figure} ../plots/cf.PlotCutflowVariables1D_tpl_config_analy__1__d8a37d3da9__plot__step0_Initial__proc_2_a2211e799f__cat_incl__var_cf_jet1_pt.pdf
:width: 100%
:::

:::{figure} ../plots/cf.PlotCutflowVariables1D_tpl_config_analy__1__d8a37d3da9__plot__step1_jet__proc_2_a2211e799f__cat_incl__var_cf_jet1_pt.pdf
:width: 100%
:::

:::{figure} ../plots/cf.PlotCutflowVariables1D_tpl_config_analy__1__d8a37d3da9__plot__step2_muon__proc_2_a2211e799f__cat_incl__var_cf_jet1_pt.pdf
:width: 100%
:::
::::

The ```--per-plot``` parameter defines whether to produce one plot per selector step (```--per-plot processes```) or one plot per process (```--per-plot steps```).
For the ```--per-plot steps``` option, try the following task call:

```shell
law run cf.PlotCutflowVariables1D --version v1 \
    --calibrators example --selector example \
    --processes tt,st --variables cf_jet1_pt --categories incl \
    --selector-steps jet,muon --per-plot steps
```

::::{grid} 1 1 2 2
:::{figure} ../plots/cf.PlotCutflowVariables1D_tpl_config_analy__1__c3947accbb__plot__proc_st__cat_incl__var_cf_jet1_pt.pdf
:width: 100%
:::

:::{figure} ../plots/cf.PlotCutflowVariables1D_tpl_config_analy__1__c3947accbb__plot__proc_tt__cat_incl__var_cf_jet1_pt.pdf
:width: 100%
:::
::::

## Creating plots for different shifts

Like most tasks, our plotting tasks also contain the ```--shift``` parameter that allows requesting the outputs for a certain type of systematic variation.
Per default, the ```--shift``` parameter is set to "nominal", but you could also produce your plot with a certain systematic uncertainty varied up or down, e.g. via running

```shell
law run cf.PlotVariables1D --version v1 \
    --processes tt,st --variables n_jet --shift mu_up
```

If you already ran the same task call with ```--shift nominal``` before, this will only require to produce new histograms and plots, as a shift such as the ```mu_up``` is typically implemented as an event weight and therefore does not require to reproduce any columns.
Other shifts such as ```jec_up``` also impact our event selection and therefore also need to re-run anything starting from {py:class}`~columnflow.tasks.selection.SelectEvents`.
A detailed overview on how to implement different types of systematic uncertainties is given in the [systematics](systematics) section (TODO: not existing).

For directly comparing differences introduced by one shift source, we provide the {py:class}`~columnflow.tasks.plotting.PlotShiftedVariables1D` task.
Instead of the ```--shift``` parameter, this task implements the ```--shift-sources``` option and creates one plot per shift source displaying the nominal distribution (black) compared to the shift source varied up (red) and down (blue).
The task can be called e.g. via

```shell
law run cf.PlotShiftedVariables1D --version v1 \
    --processes tt,st --variables jet1_pt,n_jet --shift-sources mu
```

and produces the following plot:

::::{grid} 1 1 2 2
:::{figure} ../plots/cf.PlotShiftedVariables1D_tpl_config_analy__1__42b45aba89__plot__proc_2_a2211e799f__unc_mu__cat_incl__var_jet1_pt.pdf
:width: 100%
:::

:::{figure} ../plots/cf.PlotShiftedVariables1D_tpl_config_analy__1__42b45aba89__plot__proc_2_a2211e799f__unc_mu__cat_incl__var_n_jet.pdf
:width: 100%
:::
::::

This produces per default only one plot containing the sum of all processes.
To produce this plot per process, you can use the {py:class}`~columnflow.tasks.plotting.PlotShiftedVariablesPerProcess1D` task

## Directly displaying plots in the terminal

All plotting tasks also include a ```--view-cmd``` parameter that allows directly printing the plot during the runtime of the task:

```shell
law run cf.PlotVariables1D --version v1 \
    --processes tt,st --variables n_jet --view-cmd evince-previewer
```

(custom_plot_function)=

## Using your own plotting function

While all plotting tasks provide default plotting functions which implement many parameters to customize the plot, it might be necessary to write your own plotting functions if you want to create a specific type of plot.
In that case, you can simply write a function that follows the signature of all other plotting functions and call a plotting task with this function using the `--plot-function` parameter.

An example on how to implement such a plotting function is shown in the following:

```{literalinclude} ../../analysis_templates/cms_minimal/__cf_module_name__/plotting/example.py
:language: python
:start-at: def my_plot1d_func(
:end-at: return fig, (ax,)
```

## Applying a selection to a variable

In some cases, you might want to apply a selection to a variable before plotting it.
Instead of creating a new column with the selection applied, columnflow provides the possibility to apply a selection to a variable directly when histograming it.
For this purpose, the `selection` parameter can be added in the variable definition in the config.
This may look as follows:

```python
config.add_variable(
    name="jet_pt",
    expression="Jet.pt",
    binning=(50, 0, 300.0),
    selection=(lambda events: events.Jet.mass > 30.0),  # Select only jets with a mass larger than 30 GeV
    null_value=EMPTY_FLOAT,  # Set the value of the variable to EMPTY_FLOAT if the selection is not passed
    unit="GeV",
    x_title=r"all Jet $p_{T}$",
    aux={"inputs": ["Jet.mass"]},  # Add the needed selection columns to the auxiliary of the variable instance
)
```

It is important to provide the `null_value` parameter, when using the `selection` parameter, as the variable will be set to this value if the selection is not passed.
The `selection` parameter only supports functions / lambda expressions for now.
The function itself can be as complex as needed, but its signature needs to match `def my_selection(events: ak.Array) -> ak.Array[bool]` where the variable array is passed to the function and the returned value is a boolean array of the same length as the input array.

The used columns in the selection function are not automatically added to the required routes of the workflow.
For this reason, it is necessary to add the columns used in the selection function to variable instance auxiliary and to make sure that the columns are produced at the time of creating the histograms.

:::{dropdown} An other examble with a more complex selection:

```python

config.add_variable(
    name="jet_pt",
    expression="Jet.pt",
    binning=(50, 0, 300.0),
    selection=(lambda events: abs(events.Jet.eta) ** 2 + abs(events.Jet.phi) ** 2 < 0.4),
    null_value=EMPTY_FLOAT,
    unit="GeV",
    x_title=r"all Jet $p_{T}$",
    aux={"inputs": ["Jet.eta", "Jet.phi"]},
)

```

:::
