# Columnflow Structure


In this section, an overview to the structure of Columnflow is provided, starting with a general introduction, followed by a description of the various tasks implemented in columnflow and ending with an introduction on how to configure your analysis on top of columnflow with the analysis and config object from the [order](https://github.com/riga/order) package.


## General introduction

Columnflow is a fully orchestrated columnar analysis tool for HEP analyses with Python.
The workflow orchestration is managed by [law](https://github.com/riga/law) and the meta data and configuration is managed by [order](https://github.com/riga/order).
A short introduction to law is given in the {doc}`law section <law>`.
If you have never used law before, this section is highly recommended as a few very convenient commands are presented there.


The data processing in columnflow is based on columns in [awkward arrays](https://awkward-array.org/doc/main/) with [coffea](https://coffeateam.github.io/coffea/)-generated behaviour.
Fields like "Jet" exist too, they contain columns with the same first dimension (the parameters of the field, e.g. Jet.pt). A few additional functions for simplified handling of columns were defined in {py:mod}`~columnflow.columnar_util`.

As most of the information is conserved in the form of columns, it would be very inefficient (and might not even fit in the memory) to use all columns and all events from a dataset at once for each task.
Therefore, in order to reduce the impact on the memory:
- a chunking of the datasets is implemented using [dask](https://www.dask.org/): not all events from a dataset are inputed in a task at once, but only chunked in groups of events. (100 000 events max per group is default as of 05.2023, default is set in the law.cfg file).
- the user needs to define for each {py:class}`~columnflow.production.Producer`, {py:class}`~columnflow.calibration.Calibrator` and {py:class}`~columnflow.selection.Selector` which columns are to be loaded (this happens by defining the ```uses``` set in the header of the decorator of the class) and which new columns/fields are to be saved in parquet files after the respective task (this happens by defining the ```produces``` set in the header of the decorator of the class).
The exact implementation for this feature is further detailed in {doc}`building_blocks/selectors` and {doc}`building_blocks/producers`.

## Tasks in columnflow

Tasks are [law](https://github.com/riga/law) objects allowing to control a workflow. All the tasks
presented below are proposed by columnflow and allow for a fairly complete analysis workflow.
However, as analyses are very diverse, it is possible that a specific analysis will need more
stearing options or even completely new tasks. Thankfully, columnflow is not a fixed structure and
you will be able to create new tasks in such a case, following the corresponding example in the
{doc}`examples` section of this documentation.
The full task tree of general columnflow tasks can be seen in
[this wikipage](https://github.com/columnflow/columnflow/wiki#default-task-graph). There are also
experiment-specific tasks which are not present in this graph. However, these are introduced in the
{ref}`CMS specializations section <cms_specializations_section>`.

Further informations about tasks and law can be found in the
{doc}`"Law Introduction" <law>` section of this documentation or in the
[example section](https://github.com/riga/law#examples) of the law
Github repository. In general (at least for people working in the CMS experiment),
using columnflow requires a grid proxy, as the
dataset files are accessed through it. The grid proxy should be activated after the setup of the
default environment. It is however possible to create a custom law config file to be used by people
without a grid certificate, although someone with a grid proxy must run the tasks
{py:class}`~columnflow.tasks.external.GetDatasetLFNs` (and potentially the cms-specific
{py:class}`~columnflow.tasks.cms.external.CreatePileUpWeights`) for them. Once these
tasks are done, the local task outputs can be used without grid certificate by other users if
they are able to access them with the storage location declared in the custom law config file.
An example for such a custom config file can be found in the {doc}`examples` section of this
documentation.


While the name of each task is fairly descriptive of its purpose, a short introduction of the most
important facts and parameters about each task group is provided below. As some tasks require
others to run, the arguments for a task higher in the tree will also be required for tasks below
in the tree (sometimes in a slightly different version, e.g. with an "s" if the task allows several
instances of the parameter to be given at once (e.g. several dataset**s**)).

It should be mentioned that in your analysis, the command line argument for running the columnflow
tasks described below will contain an additional "cf."-prefix
before the name of the task, as these are columnflow tasks and not new tasks created explicitely
for your analysis. For example, running the {py:class}`~columnflow.tasks.selection.SelectEvents`
task will require the following syntax:
```shell
law run cf.SelectEvents --your-parameters parameter_value
```

- {py:class}`~columnflow.tasks.external.GetDatasetLFNs`: This task looks for the logical file names
of the datasets to be used and saves them in a json file. The argument ```--dataset``` followed by
the name of the dataset to be searched for, as defined in the analysis config is needed for this
task to run. TODO: more infos?

- {py:class}`~columnflow.tasks.calibration.CalibrateEvents`: Task to implement corrections to be
applied on the datasets, e.g. jet-energy corrections. This task uses objects of the
{py:class}`~columnflow.calibration.Calibrator` class to apply the calibration. The argument
```--calibrator``` followed by the name of the Calibrator
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
Selector object to be run is needed for this task to run.
A default value for this argument can be set in the analysis config. From this task on, the
```--calibrator``` argument is replaced by ```--calibrators```.

- {py:class}`~columnflow.tasks.reduction.ReduceEvents`: Task to apply the masks created in
{py:class}`~columnflow.tasks.selection.SelectEvents` on the datasets. All
tasks below ReduceEvents in the task graph use the parquet
file resulting from ReduceEvents to work on, not the
original dataset. The columns to be conserved after
ReduceEvents are to be given in the analysis config under
the ```config.x.keep_columns``` argument in a ```DotDict``` structure
(from {py:mod}`~columnflow.util`).

- {py:class}`~columnflow.tasks.production.ProduceColumns`: Task to produce additional columns for
the reduced datasets, e.g. for new high level variables. This task uses objects of the
{py:class}`~columnflow.production.Producer` class to create the new columns. The new columns are
saved in a parquet file that can be used by the task below on the task graph. The argument
```--producer``` followed by the name of the Producer object
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
be plotted, as defined in the analysis config. For the ```PlotShiftedVariables*``` plots, the
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
PlotCutflow task gives a single histograms containing only
the total event yields for each selection step given.

- ```Merge``` tasks (e.g. {py:class}`~columnflow.tasks.reduction.MergeReducedEvents`,
{py:class}`~columnflow.tasks.histograms.MergeHistograms`): Tasks to merge the local outputs from
the various occurences of the corresponding tasks. TODO: details? why needed? Only convenience?
Or I/O?


There are also CMS-specialized tasks, like
{py:class}`~columnflow.tasks.cms.external.CreatePileUpWeights`, which are described in the
{ref}`CMS specializations section <cms_specializations_section>`. As a note, the CreatePileUpWeights task is
interesting from a workflow point of view as it is an example of a task required through an object
of the {py:class}`~columnflow.production.Producer` class. This behaviour can be observed in the
{py:meth}`~columnflow.production.cms.pileup.pu_weight_requires` method.

TODO: maybe interesting to have examples e.g. for the usage of the
parameters for the 2d plots. Maybe in the example section, or after the next subsection, such that
all parameters are explained? If so, at least to be mentioned here.

## Important note on required parameters

It should also be added that there are additional parameters specific for the tasks in columnflow, required by the fact that columnflow's purpose is for HEP analysis.
These are the ```--analysis``` and ```-config``` parameters, which defaults can be set in the law.cfg.
These two parameters respectively define the config file for the different analyses to be used (where the different analyses and their parameters should be defined) and the name of the config file for the specific analysis to be used.

Similarly the ```--version``` parameter, which purpose is explained in the {doc}`law` section of this documentation, is required to start a task.


## Important modules and configs

The standard syntax to access objects in columnflow is the dot syntax, usable for the [order](https://github.com/riga/order) metavariables (e.g. campaign.x.year) as well as the [awkward arrays](https://awkward-array.org/doc/main/) (e.g. events.Jet.pt).

TODO

here mention the analysis template


### Law config

(analysis_campaign_config)=
### Analysis, Campaign and Config

Columnflow uses the {external+order:py:class}`order.analysis.Analysis` class from the [order](https://github.com/riga/order) package to define a specific analysis.
This object does not contain most of the analysis information by itself.
It is to be linked to objects of the {external+order:py:class}`order.config.Campaign` and {external+order:py:class}`order.config.Config` classes, as described in [the Analysis, Campaign and Config section](https://python-order.readthedocs.io/en/latest/quickstart.html#analysis-campaign-and-config) of the Quickstart section of the order documentation.
An example of an analysis with its full Analysis, Campaign and Config definitions in the same directory is given in the [Analysis Grand Challenge Columnflow repository](https://github.com/columnflow/agc_cms_ttbar/) repository and [its config directory](https://github.com/columnflow/agc_cms_ttbar/tree/master/agc/config).

A Campaign object contains the analysis-independent information related to a specific and well-defined experimental campaign.
We define an experimental campaign as a set of fixed specific conditions for the data-taking or simulations (for example, a period of data-taking for which no significant change to the detector setup or operation was made, like the data-taking period of the year 2017 for the CMS detector at CERN).
This means general information like the center of mass energy of the collisions (argument `ecm`) as well as the datasets created during/for the specific campaign.
An example of a Campaign declaration (from the AGC Columnflow repository linked above) might be:

```python
from order import Campaign

campaign_cms_opendata_2015_agc = cpn = Campaign(
    name="cms_opendata_2015_agc",  # the name of the campaign
    id=1,  # a unique id for this campaign
    ecm=13,  # center of mass energy
    aux={  # additional, arbitrary information
        "tier": "NanoAOD",  # data format, e.g. NanoAOD
        "year": 2015,  # year of data-taking
        "location": "https://xrootd-local.unl.edu:1094//store/user/AGC/nanoAOD",  # url to base path of the nanoAODs
        "wlcg_fs": "wlcg_fs_unl",  # file system to use on the WLCG
    },
)
```

In order to define a {external+order:py:class}`order.dataset.Dataset`, which may be included in the Campaign object, an associated {external+order:py:class}`order.process.Process` object must be defined first.
An example of such a Process, describing the physical process of the top-antitop production associated with jets in a pp-collision is defined in `agc.config.processes` (in the AGC Columnflow repository linked above) as:

```python
from scinum import Number
from order import Process

tt = Process(
    name="tt",
    id=1000,
    label=r"$t\bar{t}$ + Jets",
    color=(128, 76, 153),
    xsecs={
        13: Number(831.76, {
            "scale": (19.77, 29.20),
            "pdf": 35.06,
            "mtop": (23.18, 22.45),
        }),
    },
)
```

Using the physical Process defined above, one may now create a dataset, which can be added to the Campaign object.
An example of dataset definition with scale variations for this campaign would then be (values taken from [the analysis-grand-challenge GitHub repository](https://github.com/iris-hep/analysis-grand-challenge/blob/be91d2c80225b7a91ce6b153591f8605167bf555/analyses/cms-open-data-ttbar/nanoaod_inputs.json)):
```python
import agc.config.processes as procs
from order import DatasetInfo

# cpn is the Campaign object defined in the previous code block
# the following shows an example for a simulated tt dataset generated with the Powheg package
cpn.add_dataset(
    name="tt_powheg",  # name of the dataset
    id=1,  # unique id for this dataset
    processes=[procs.tt],  # link this dataset to physics processes (which are also order objects in this case)
    info={  # add additional information to this dataset
        # information regarding the 'nominal' generation of the dataset,
        # i.e. with the recommended central values of the MC parameters
        "nominal": DatasetInfo(
            # identifier for this dataset in a meta information database (e.g. DAS at CMS)
            keys=["TT_TuneCUETP8M1_13TeV-powheg-pythia8"],
            n_files=242,  # how many files to process, important for book keeping
            n_events=276079127),  # total number of events
        # you can also add information about additional samples that belong to this dataset
        # for example, add samples where a set of MC parameters (tune) is varied within uncertainties
        "scale_down": DatasetInfo(
            keys=["TT_TuneCUETP8M1_13TeV-powheg-scaledown-pythia8"],
            n_files=32,
            n_events=39329663),
        "scale_up": DatasetInfo(
            keys=["TT_TuneCUETP8M1_13TeV-powheg-scaleup-pythia8"],
            n_files=33,
            n_events=38424467),
    },
    aux={  # additional, general information about this dataset
        "agc_process": "ttbar",
        "agc_shifts": {
            "scale_down": "scaledown",
            "scale_up": "scaleup",
        },
    },
)
```

A Config object is saving the variables specific to the analysis.
It is associated to an Analysis and a Campaign object.
It is to be created using the {external+order:py:meth}`order.analysis.Analysis.add_config` method on the analysis object, with the associated Campaign object as argument.
An example would be:
```python
cfg = analysis.add_config(campaign, name=config_name, id=config_id)
```

In this way, one Analysis can be tied to different Campaigns (e.g. corresponding to different years of data-taking) very naturally.

Several classes of the order library are used for the organization and use of the metavariables.
A more detailed description of the most important objects to be defined in the Config object is presented in the {doc}`building_blocks/config_objects` section of this documentation.

