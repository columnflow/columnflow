# Columnflow Structure


In this section, an overview to the structure of Columnflow is provided, starting with
a general introduction, followed by a description of the various tasks implemented in
columnflow and ending with an introduction on how to configure your analysis on top
of columnflow with the analysis and config object from the
[order](https://github.com/riga/order) package.


## General introduction

Columnflow is a fully orchestrated columnar analysis tool for HEP analyses with Python. The
workflow orchestration is managed by [law](https://github.com/riga/law) and the meta data and
configuration is managed by [order](https://github.com/riga/order). A short introduction to law is
given in the {doc}`law section <law>`. If you have never used law before, this section is highly
recommended as a few very convenient commands are presented there.


The data processing in columnflow is based on columns in [awkward arrays](https://awkward-array.org/doc/main/)
with [coffea](https://coffeateam.github.io/coffea/)-generated behaviour. Fields like "Jet" exist
too, they contain columns with the same first dimension (the parameters of the field, e.g. Jet.pt).
A few additional functions for simplified handling of columns were defined in
{py:mod}`~columnflow.columnar_util`.

As most of the information is conserved in the form of columns, it would be very inefficient
(and might not even fit in the memory) to use all columns and all events from a dataset at once for
each task. Therefore, in order to reduce the impact on the memory:
- a chunking of the datasets is implemented using [dask](https://www.dask.org/): not all events
from a dataset are inputed in a task at once, but only chunked in groups of events.
(100 000 events max per group is default as of 05.2023, default is set in the law.cfg file).
- the user needs to define for each {py:class}`~columnflow.production.Producer`,
{py:class}`~columnflow.calibration.Calibrator` and {py:class}`~columnflow.selection.Selector` which
columns are to be loaded (this happens by defining the ```uses``` set in the header of the
decorator of the class) and which new columns/fields are to be saved in parquet files after the
respective task (this happens by defining the ```produces``` set in the header of the decorator of
the class). The exact implementation for this feature is further detailed in
{doc}`building_blocks/selectors` and {doc}`building_blocks/producers`

## Tasks in columnflow

Tasks are [law](https://github.com/riga/law) objects allowing to control a workflow. All the tasks
presented below are proposed by columnflow and allow for a fairly complete analysis workflow.
However, as analyses are very diverse, it is possible that a specific analysis will need more
stearing options or even completely new tasks. Thankfully, columnflow is not a fixed structure and
you will be able to create new tasks in such a case, following the corresponding example in the
{doc}`examples` section of this documentation.
The full task tree of general columnflow tasks can be seen in
[this wikipage](https://github.com/columnflow/columnflow/wiki#default-task-graph). Additionally, the
CMS-specific {py:class}`~columnflow.tasks.cms.external.CreatePileUpWeights` task is implemented,
but not present in the graph, as it is experiment specific.

Further informations about tasks and [law](https://github.com/riga/law) can be found in
{doc}`"Law Introduction" <law>` section of this documentation or in the
[example section](https://github.com/riga/law#examples) of the law
Github repository. In general (at least for cms), using columnflow requires a grid proxy, as the
dataset files are accessed through it. The grid proxy should be activated after the setup of the
default environment. It is however possible to create a custom law config file to be used by people
without a grid certificate, although someone with a grid proxy must run the tasks
{py:class}`~columnflow.tasks.external.GetDatasetLFNs` (and potentially
{py:class}`~columnflow.tasks.cms.external.CreatePileUpWeights`) for them. Once these
tasks are done, the local task outputs can be used without grid certificate by other users if
they are able to access them with the storage location declared in the custom law config file.
An example for such a custom config file can be found int he {doc}`examples` section of this
documentation.

While the name of each task is fairly descriptive of its purpose, a short introduction of the most
important facts and parameters about each task group is provided below. As some tasks require
others to run, the arguments for a task higher in the tree will also be required for tasks below
in the tree (sometimes in a slightly different version, e.g. with an "s" if the task allows several
instances of the parameter to be given at once (e.g. several dataset**s**)):

- {py:class}`~columnflow.tasks.external.GetDatasetLFNs`: This task looks for the logical file names
of the datasets to be used and saves them in a json file. The argument ```--dataset``` followed by
the name of the dataset to be searched for, as defined in the analysis config is needed for this
task to run. TODO: more infos?

- {py:class}`~columnflow.tasks.calibration.CalibrateEvents`: Task to implement corrections to be
applied on the datasets, e.g. jet-energy corrections. This task uses objects of the
{py:class}`~columnflow.calibration.Calibrator` class to apply the calibration. The argument
```--calibrator``` followed by the name of the {py:class}`~columnflow.calibration.Calibrator`
object to be run is needed for this task to run. A default value for this argument can be set in
the analysis config. Similarly, the ```--shift``` argument can be given, in order to choose which
corrections are to be used, e.g. which variation (up, down, nominal) of the jet-energy corrections
are to be used. TODO: more infos, e.g. output type of task?

- {py:class}`~columnflow.tasks.selection.SelectEvents`: Task to implement selections to be applied
on the datssets. This task uses objects of the {py:class}`~columnflow.selection.Selector` class to
apply the selection. The output are masks for the events and objects to be selected, saved in a
parquet file, and some additional parameters stored in a dictionary format, like the statistics of
the selection (which are needed for the plotting tasks further down the task tree), saved in a json
file. The mask are not applied to the columns during this task.
The argument ```--selector``` followed by the name of the
{py:class}`~columnflow.selection.Selector` object to be run is needed for this task to run.
A default value for this argument can be set in the analysis config. From this task on, the
```--calibrator``` argument is replaced by ```--calibrators```.

- {py:class}`~columnflow.tasks.reduction.ReduceEvents`: Task to apply the masks created in
{py:class}`~columnflow.tasks.selection.SelectEvents` on the datasets. All
tasks below {py:class}`~columnflow.tasks.reduction.ReduceEvents` in the task graph use the parquet
file resulting from {py:class}`~columnflow.tasks.reduction.ReduceEvents` to work on, not the
original dataset. The columns to be conserved after
{py:class}`~columnflow.tasks.reduction.ReduceEvents` are to be given in the analysis config under
the ```config.x.keep_columns``` argument in a ```DotDict``` structure
(from {py:mod}`~columnflow.util`).

- {py:class}`~columnflow.tasks.production.ProduceColumns`: Task to produce additional columns for
the reduced datasets, e.g. for new high level variables. This task uses objects of the
{py:class}`~columnflow.production.Producer` class to create the new columns. The new columns are
saved in a parquet file that can be used by the task below on the task graph. The argument
```--producer``` followed by the name of the {py:class}`~columnflow.production.Producer` object
to be run is needed for this task to run. A default value for this argument can be set in the
analysis config.

- {py:class}`~columnflow.tasks.ml.PrepareMLEvents`, {py:class}`~columnflow.tasks.ml.MLTraining`,
{py:class}`~columnflow.tasks.ml.MLEvaluation`: Tasks to
train, evaluate neural networks and plot (to be implemented) their results.
TODO: more informations? output type?
all tf based? or very general and almost everything must be set in the training scripts?
and if general, what is taken care of?

- {py:class}`~columnflow.tasks.histograms.CreateHistograms`: Task to create histograms with the
python package [Hist](https://hist.readthedocs.io/en/latest/) which can be used by the tasks below
in the task graph. From this task on, the ```--producer``` argument is replaced by
```--producers```. The histograms are saved in a pickle file.
TODO: more informations?

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
be plotted, as defined in the analysis config. For the ```PlotShiftedVariables*```plots, the
argument ```shift-sources``` is needed and replaces the argument ```shift```. The output format for
these plots can be given with the ```--file-types``` argument. It is possible to set a default for the
variables in the analysis config.

- {py:class}`~columnflow.tasks.cms.inference.CreateDatacards`: TODO

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
{py:class}`~columnflow.tasks.cutflow.PlotCutflow` task gives a single histograms containing only
the total event yields for each selection step given.

- ```Merge``` tasks (e.g. {py:class}`~columnflow.tasks.reduction.MergeReducedEvents`,
{py:class}`~columnflow.tasks.histograms.MergeHistograms`): Tasks to merge the local outputs from
the various occurences of the corresponding tasks. TODO: details? why needed? Only convenience?
Or I/O?

- {py:class}`~columnflow.tasks.cms.external.CreatePileUpWeights`: TODO

maybe TODO: each task separately?

also, maybe interesting to have examples e.g. for the usage of the
parameters for the 2d plots. Maybe in the example section, or after the next subsection, such that
all parameters are explained? If so, at least to be mentioned here.

## Important note on required parameters

It should also be added that there are additional parameters specific for the tasks in columnflow,
required by the fact that columnflow's purpose is for HEP analysis. These are the ```--analysis```
and ```-config``` parameters, which defaults can be set in the law.cfg. These two parameters
respectively define the config file for the different analyses to be used (where the different
analyses and their parameters should be defined) and the name of the config file for the specific
analysis to be used.

Similarly the ```--version``` parameter, which purpose is explained in the {doc}`law` section of
this documentation, is required to start a task.


## Important modules and configs

The standard syntax to access objects in columnflow is the dot syntax, usable for the
[order](https://github.com/riga/order) metavariables (e.g. campaign.x.year) as well as the
[awkward arrays](https://awkward-array.org/doc/main/) (e.g. events.Jet.pt).

TODO

here mention the analysis template

### Campaigns


### Law config


### Analysis config

The analysis config defines all analysis specific variables and objects that need to be defined for
the analysis to run. Some of them are required for columnflow to run, some are additional and can
be useful, depending on the analysis.

TODO: explain difference od.Analysis, od.Config and how they are related in the usage.
-> What I have seen: analysis_hbt.py defines the analysis and afterward declares the config, which
needs the analysis object. The behaviour of the config is defined in configs_run2ul.py.

Variables defined in config:

Obtained from the campaign:

year: the year of the measurement, obtained from the campaign

year2: the last two numbers of the year

corr_postfix: postfix added to the year, if several campaigns happened in a year, obtained fromn the campaign.

procs: root processes, obtained from campaign. WHAT IS THAT? TODO


new creations:

cfg: add analysis config????? how is that not redundant to the call in analysis_hbt? TODO

process_names: declare the names of the processes we are interested in. Must correspond to the
processes defined for the datasets.
