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


### Law config

### Analysis

### Campaigns

### Config

#### Generalities

The [order](https://github.com/riga/order) package defines several classes to implement the
metavariables of an Analysis. The order documentation and its {external+order:doc}`quickstart` section
provide an introduction to these different classes. In this section we will concentrate on the use
of the order classes to define your analysis.

The three classes main classes needed to define your analysis are
{external+order:py:class}`order.analysis.Analysis`, {external+order:py:class}`order.config.Campaign`
and {external+order:py:class}`order.config.Config`. Their purpose and definition can be found in
[the Analysis, Campaign and Config section](https://python-order.readthedocs.io/en/latest/quickstart.html#analysis-campaign-and-config)
of the Quickstart section of the order documentation.

After defining your Analysis object and your Campaign object(s), you can use the command
```cfg = analysis.add_config(campaign, name=your_config_name, id=your_config_id)``` to create the
new Config object `cfg`, which will be
associated to both the Analysis object and the Campaign object needed for its creation. As the
Config object should contain the analysis-dependent information related to a certain campaign, it
should contain most of the information needed for running your analysis. Therefore, in this section,
the Config parameters required by Columnflow and some convenience parameters will be presented.

To start your analysis, do not forget to use the already existing analysis template in the
`analysis_templates/cms_minimal` Git directory and its
[config](https://github.com/columnflow/columnflow/blob/master/analysis_templates/cms_minimal/__cf_module_name__/config/analysis___cf_short_name_lc__.py).


The Config saves informations under two general formats: the objects
from the order package, which are necessary for your analysis to run in columnflow, and the
additional parameters, which are saved under the auxiliary key, accessible through the "x" key.
Unlike the general order package objects and while several names for parameters in the auxiliary
field do already have a meaning in columnflow and should respect the format used in columnflow,
the auxiliary field can contain any kind of
parameter the user wants to save and reuse for parts of the analysis. These two general formats are
presented below.


#### Parameters from the order package (required)

##### Processes

The physical processes to be investigated in the analysis. These should be saved as objects of the
{external+order:py:class}`order.process.Process` class and added to the Config object using its
{external+order:py:meth}`order.config.Config.add_process()` method. The processes added to the
Config must correspond to the
processes defined for the datasets and added to the Campaign object associated to the Config.
It is possible to get all root processes from a specific campaign using the
{py:func}`~columnflow.config_util.get_root_processes_from_campaign()` function from columnflow.
As an example of information carried by a process, a color for the plotting scripts can be set using
the {external+order:py:attr}`order.mixins.ColorMixin.color1` attribute of the process.
More informations about processes can be found in the
{external+order:py:class}`order.process.Process` and the
{external+order:doc}`quickstart` sections of the order documentation.


##### Datasets

The actual datasets to be processed in the analysis. These should be saved as objects of the
{external+order:py:class}`order.dataset.Dataset` class and added to the Config object using its
{external+order:py:meth}`order.config.Config.add_dataset()` method. The datasets added to the
Config object must correspond to the
datasets added to the Campaign object associated to the Config object. They are accessible through
the {external+order:py:meth}`order.config.Campaign.get_dataset()` method of the Campaign class. The
Dataset objects should contain for example informations on the number of files and number of events
present in a Dataset as well as its key and if it is data or a Monte-Carlo dataset. It is also
possible to change informations of a dataset in the config script. An example would be reducing the
number of files to process for test purposes in a specific test config. This could be done with the
following lines of code:
e.g.
```python
n_files_max = 5
for info in dataset.info.values():
    info.n_files = min(info.n_files, n_files_max)
```

Once the processes and datasets have both been added to the config, one can check that the root
process of all datasets is part of any of the registered processes, using the columnflow function
{py:func}`~columnflow.config_util.verify_config_processes()`.

##### Versions

TODO?

-> cfg.versions -> not to be explained as it is disabled for now?


##### Variables

In order to create histograms out of the processed datasets, columnflow uses
{external+order:py:class}`order.variable.Variable`s. These
Variables need to be added to the config using the
function {external+order:py:meth}`order.config.Config.add_variable`. The standard syntax is as
follows for `cfg` the Config object:

```python
cfg.add_variable(
    name=variable_name,  # this is to be given to the "--variables" argument for the plotting task
    expression=content_of_the_variable,
    null_value=value_to_be_given_if_content_not_available_for_event,
    binning=(bins, lower_edge, upper_edge),
    unit=unit_of_the_variable_if_any,
    x_title=x_title_of_histogram_when_plotted,
)
```

An example with the transverse momentum of the first jet would be:
```python
cfg.add_variable(
    name="jet1_pt",
    expression="Jet.pt[:,0]",
    null_value=EMPTY_FLOAT,
    binning=(40, 0.0, 400.0),
    unit="GeV",
    x_title=r"Jet 1 $p_{T}$",
)
```

In histogramming tasks such as
{py:class}`~columnflow.tasks.histograms.CreateHistograms`, one histogram is created per Variable
given via the ```--variables``` argument, accessing information from columns based on
the `expression` of the Variable and storing them in histograms with binning defined
via the `binning` argument of the Variable.

The list of possible keyword arguments can be found in the order documentation for the class
{external+order:py:class}`order.variable.Variable`. The values in the ```expression``` argument can
be either a one-dimensional or a more dimensional array. In this second case the information is
flattened before plotting. It is to be mentioned that
{py:attr}`~columnflow.columnar_util.EMPTY_FLOAT` is a columnflow internal null value and
corresponds to the value ```-9999.0```.

##### Category

Categories built to investigate specific parts of the phase-space, for example for plotting.
These objects are described in [the Channel and Category](https://python-order.readthedocs.io/en/latest/quickstart.html#channel-and-category)
part of the Quickstart section of the order documentation. A specificity of columnflow regarding
this class is the fact that the `selection` argument of this object is expected to take the name of
an object of the {py:class}`~columnflow.categorization.Categorizer` class instead of a boolean
expression in a string format.
Adding a category to the Config object is possible through the
{py:func}`~columnflow.config_util.add_category()` method. An example for an inclusive category with
the Categorizer `cat_incl` defined in the cms_minimal analysis template is given below:

```python
add_category(
    cfg,
    id=1,
    name="incl",
    selection="cat_incl",
    label="inclusive",
)
```

It is recommended to always add an inclusive category with id=1 or name="incl" which is used
in various places, e.g. for the inclusive cutflow plots and the "empty" selector

##### Channel

Similarly to categories, Channels are built to investigate specific parts of the phase space and are
described in the [Channel and Category](https://python-order.readthedocs.io/en/latest/quickstart.html#channel-and-category)
part of the Quickstart section of the order documentation. They can be added to the Config object using
{external+order:py:meth}`order.config.Config.add_channel()`.

##### Shift

In order to implement systematic variations in the Config object, the
{external+order:py:class}`order.shift.Shift` class can be used. Implementing systematic variations
using shifts can take different forms depending on the kind of systematic variation involved,
therefore a complete section specialized in the description of these implementations is to be found
in (TODO: add link Shift section). Adding a Shift object to the Config object happens through the
{external+order:py:meth}`order.config.Config.add_shift()` function.

Often, shifts are related to auxiliary parameters of the Config, like the name of the scale
factors involved, or in the case of corrections accessible through external files, the paths of
these files.


Notes for the Shift section:

-> present examples auxiliary parameters calibrations: cfg.x.jec, cfg.x.muon_sf_names

-> present shift types -> use whole new NanoAODs with specific tune key defined in dataset, or different
calibrations e.g. cfg.x.jec, or weights through producer e.g. scale factors

-> mention external files

#### Auxiliary Parameters (optional)

In principle, the auxiliaries of the Config may contain any kind of variables, but there are
several keys with a special meaning for columnflow, for which you would need to respect the expected
format. These are presented below at first, followed by a few examples of the kind of information
you might want to save in the auxiliary part of the config on top of these.

##### Keep_columns

During the Task {py:class}`~columnflow.tasks.reduction.ReduceEvents` new files in parquet format
containing all remaining events and objects after the selections are created. If the auxiliary
argument `keep_columns`, accessible through `cfg.x.keep_columns`, exists in the Config object, only
the columns declared explicitely will be kept after the reduction. Actually, several tasks can make
use of such an argument in the Config object for the reduction of their output. Therefore, the
`keep_columns` argument expects a {py:class}`~columnflow.util.DotDict` containing the name of the
tasks (with the `cf.` prefix) for which such a reduction should be applied as keys and the set of
columns to be kept in the output of this task as values.

An example is given below:
```python
cfg.x.keep_columns = DotDict.wrap({
    "cf.ReduceEvents": {
        # general event info
        "run", "luminosityBlock", "event",
        # object info
        "Jet.pt", "Jet.eta", "Jet.phi", "Jet.mass", "Jet.btagDeepFlavB", "Jet.hadronFlavour",
        "Muon.pt", "Muon.eta", "Muon.phi", "Muon.mass", "Muon.pfRelIso04_all",
        "MET.pt", "MET.phi", "MET.significance", "MET.covXX", "MET.covXY", "MET.covYY",
        "PV.npvs",
        # columns added during selection
        "deterministic_seed", "process_id", "mc_weight", "cutflow.*",
    },
    "cf.MergeSelectionMasks": {
        "cutflow.*",
    },
    "cf.UniteColumns": {
        "*",
    },
})
```



##### Custom functions

-> custom get_dataset_lfns -> also for other tasks?


-> 3 get_dataset_lfns parameters auxiliary -> required

##### External_files

-> cfg.x.external_files : required for lumi and calibrations, can also take stuff like TF model;
can also be updated (e.g. add something dependent on year)

##### Weights

normalization weights:
-> luminosity -> required
-> cfg.x.external_files : required for lumi and calibrations, can also take stuff like TF model;
can also be updated (e.g. add something dependent on year)

-> cfg.x.event_weights -> required, which weights should be applied -> take shifts????????????????




##### Defaults

-> define default arguments -> convenience?

-> # target file size after MergeReducedEvents in MB
    cfg.x.reduced_file_size = 512.0    -> convenience -> can be given as argument, else default value 512

cfg.x.default_calibrator = "default"
cfg.x.default_selector = "default"
cfg.x.default_producer = "default"
cfg.x.default_ml_model = None
cfg.x.default_inference_model = "test_no_shifts"
cfg.x.default_categories = ("incl",)
cfg.x.default_variables = ("n_jet", "n_btag")

##### Groups

-> define groups -> convenience?

-> # process groups for conveniently looping over certain processs
-> # (used in wrapper_factory and during plotting)
cfg.x.process_groups = {}

-> # dataset groups for conveniently looping over certain datasets
-> # (used in wrapper_factory and during plotting)
cfg.x.dataset_groups = {}

-> # category groups for conveniently looping over certain categories
-> # (used during plotting)
cfg.x.category_groups = {}

-> # variable groups for conveniently looping over certain variables
-> # (used during plotting)
cfg.x.variable_groups = {}

-> # shift groups for conveniently looping over certain shifts
-> # (used during plotting)
cfg.x.shift_groups = {}

-> # selector step groups for conveniently looping over certain steps
-> # (used in cutflow tasks)
cfg.x.selector_step_groups = {
    "default": ["met_filter", "trigger", "lepton", "jet", "bjet"],
    "new_selection_example": ["selection_example_not_exposed"],
    "boosted": ["trigger", "lepton", "boosted_jet"],
}

##### Other useful examples



-> config.x.triggers (add_triggers) -> why some triggers only for data, why some for rest?

-> btag_working_points



-> config.x.met_filters  (add_met_filters)







TODO: explain difference od.Analysis, od.Config and how they are related in the usage.
-> What I have seen: analysis_hbt.py defines the analysis and afterward declares the config, which
needs the analysis object. The behaviour of the config is defined in configs_run2ul.py.
-> not really to do, is already in order docu.


grep -nr "text" directory
here : text= self.config_inst.x

1) really Needed for workflow

2) only paragraph-> needed for specific modules -> more information in API documentation from module, no exhaustive list

3) other convenient parameters


-> # minimum bias cross section in mb (milli) for creating PU weights, values from
    # https://twiki.cern.ch/twiki/bin/view/CMS/PileupJSONFileforData?rev=45#Recommended_cross_section
    cfg.x.minbias_xs = Number(69.2, 0.046j)?



For marcel: cfg.x.jec = DotDict.wrap({
        "campaign": f"Summer19UL{year2}{jerc_postfix}",??????????????????????????



We want the config parameters, so here:

Variables defined in config:

Obtained from the campaign:

campaign.x.year: the year of the measurement, obtained from the campaign

year2: the last two numbers of the year -> needed in docu?

corr_postfix: postfix added to the year, if several campaigns happened in a year, obtained from the
campaign. -> needed in docu? maybe just for explaining that you can add funny arguments?

procs: root processes, obtained from campaign. -> What are they used for? needed in docu?


new creations:

cfg: add analysis config????? how is that not redundant to the call in analysis_hbt? TODO










