# Best practices

Here general discussion of columnflow: columns (and fields, like Jet), awkward arrays, general
structure of cf tree and the purpose of every task


columnflow is a fully orchestrated columnar analysis tool for HEP analyses with pure Python. The
workflow orchestration is managed by [law](https://github.com/riga/law) and
[order](https://github.com/riga/order) for the metavariables. A short introduction to law is
given below.

The data processing is based on columns in [awkward arrays](https://awkward-array.org/doc/main/)
with [coffea](https://coffeateam.github.io/coffea/)-generated behaviour. Fields like "Jet" exist
too, they contain columns with the same first dimension (the parameters of the field, e.g. Jet.pt).
A few additional functions for simplified handling of columns were defined in
{py:mod}`~columnflow.columnar_util`.

As most of the information is conserved in the form of columns, it would be very inefficient
(and might not even fit in the memory) to use all columns and all events from a dataset at once for
each task. Therefore, in order to reduce the impact on the memory:
- a chunking of the datasets is implemented using [dask](https://www.dask.org/): not all events
from a dataset are inputed in a task at once, but only chunked in groups of events.
(100 000 events max per group is default as of 05.2023).
- the user needs to define for each {py:class}`~columnflow.production.Producer`,
{py:class}`~columnflow.calibration.Calibrator` and {py:class}`~columnflow.selection.Selector` which
columns are to be loaded (this happens by defining the ```uses``` set in the header of the
decorator of the class) and which new columns/fields are to be saved in parquet files after the
respective task (this happens by defining the ```produces``` set in the header of the decorator of
the class).

## Tasks in columnflow

The full task tree of columnflow can be seen in
[this issue](https://github.com/columnflow/columnflow/issues/25).

General informations about tasks and [law](https://github.com/riga/law) can be found below in
"Law introduction" or in the [example section](https://github.com/riga/law#examples) of the law
Github repository. In general (at least for cms), using columnflow requires a grid proxy, as the
dataset files are accessed through it. The grid proxy should be activated after the setup of the
default environment. It is however possible to create a custom law config file to be used by people
without a grid certificate, although someone must run the tasks ```GetDatasetLFNs``` and
```CreatePileUpWeights``` (```CreatePileUpWeights``` is an internal task, not to be accessed directly from
the command line, this will be run automatically e.g. for plots) for them, which output can then be
used be used without grid certificate.

While the name of each task is fairly descriptive of its purpose, a short introduction of the most
important facts and parameters about each task group are presented below. As some tasks require
others to run, the arguments for a task higher in the tree will also be required for tasks below
in the tree (sometimes in a slightly different version, e.g. with an "s" if the task allows several
instances of the parameter to be given at once (e.g. several dataset**s**)):

- ```GetDatasetLFNs```: This task looks for the logical file names of the datasets to be used and
saves them in a json file. The argument ```--dataset``` followed by the name of the dataset to be
searched for, as defined in the analysis config is needed for this task to run. TODO: more infos?

- ```CalibrateEvents```: Task to implement corrections to be applied on the datasets, e.g. jet-energy
corrections. This task uses objects of the {py:class}`~columnflow.calibration.Calibrator` class to
apply the calibration. The argument ```--calibrator``` followed by the name of the
{py:class}`~columnflow.calibration.Calibrator` object to be run is needed for this task to run.
A default value for this argument can be set in the analysis config. Similarly the ```--shift```
argument can be given, in order to choose which corrections are to be
used, e.g. which variation (up, down, nominal) of the jet-energy corrections are to be used.
TODO: more infos, e.g. output type of task?

- ```SelectEvents```: Task to implement selections to be applied on the datssets. This task uses
objects of the {py:class}`~columnflow.selection.Selector` class to apply the selection. The output
are masks for the events and objects to be selected saved in a parquet file and the statistics of
the selection saved in a json file. The mask are not applied to the columns during this task.
The argument ```--selector``` followed by the name of the
{py:class}`~columnflow.selection.Selector` object to be run is needed for this task to run.
A default value for this argument can be set in the analysis config. From this task on, the
```--calibrator``` argument is replaced by ```--calibrators```.

- ```ReduceEvents```: Task to apply the masks created in ```SelectEvents``` on the datasets. All
tasks below ```ReduceEvents``` in the task graph use the parquet file resulting from
```ReduceEvents``` to work on, not the original dataset. The columns to be conserved after
```ReduceEvents``` are to be given in the analysis config under the ```config.x.keep_columns```
argument in a ```DotDict``` structure (from {py:mod}`~columnflow.util`).

- ```ProduceColumns```: Task to produce additional columns for the reduced datasets, e.g. for new
high level variables. This task uses objects of the {py:class}`~columnflow.production.Producer`
class to create the new columns. The new columns are saved in a parquet file that can be used by
the task below on the task graph. The argument ```--producer``` followed by the name of the
{py:class}`~columnflow.production.Producer` object to be run is needed for this task to run.
A default value for this argument can be set in the analysis config.

- ```PrepareMLEvents```, ```MLTraining```, ```MLEvaluation``` and ```PlotMLResults```: Tasks to
train, evaluate neural networks and plot their results. TODO: more informations? output type?
all tf based?

- ```CreateHistograms```: Task to create histograms with the python package
[Hist](https://hist.readthedocs.io/en/latest/) which can be used by the tasks below in the task
graph. From this task on, the ```--producer``` argument is replaced by ```--producers```. The
histograms are saved in a pickle file.
TODO: more informations?

- ```PlotVariables*```, ```PlotShiftedVariables*```: Tasks to plot the histograms created by
```CreateHistograms``` using the python package [matplotlib](https://matplotlib.org/) with
[mplhep](https://mplhep.readthedocs.io/en/latest/) style. Several plots are possible, including
plots of variables for different physical processes or plots of variables for a single physical
process but different shifts (e.g. jet-energy correction variations). The argument ```--variables```
followed by the name of the variables defined in the analysis config, separated by a comma, is
needed for these tasks to run. It is also possible to replace the ```--datasets``` argument
for these tasks by the ```--processes``` argument followed by the name of the physical processes to
be plotted, as defined in the analysis config. For the ```PlotShiftedVariables*```plots, the
argument ```shift-sources``` is needed and replaces the argument ```shift```. The output format for
these plots can be given with the ```--file-types``` argument. It is possible to set a default for the
variables in the analysis config.

- ```WriteDatacards```: TODO

- ```CutflowPlots```: Task to plot the histograms created by ```CreateCutflowHistograms```, in a
similar way to the ```PlotVariables*``` tasks. The difference is that these plots show the selection
yields of the different selection steps defined in ```SelectEvents``` instead of only after the
```ReduceEvents``` procedure. The selection steps to be shown can be chosen with the
```--selector-steps``` argument.




It should also be added that there are additional parameters specific for the tasks in columnflow,
required by the fact that columnflow's purpose is for HEP analysis, these are the ```--analysis```
and ```-config``` parameters, which defaults can be set in the law.cfg. These two parameters
respectively define the config file for the different analyses to be used (where the different
analyses and their parameters should be defined) and the name of the config file for the specific
analysis to be used.




## Law introduction

This analysis tool uses [law](https://github.com/riga/law) for the workflow orchestration.
Therefore, a short introduction to the most essential functions of law you should be
aware of when using this tool are provided here. More informations are available for example in the
"[Examples]((https://github.com/riga/law#examples))" section of this
[Github repository](https://github.com/riga/law). This section can be ignored if you are already
familiar with law.

In [law](https://github.com/riga/law), tasks are defined and separated by purpose and may have
dependencies to each other. As an example, columnflow defines a task for the creation of histograms
 and a different task to make a plot of these histograms. The plotting task requires the
histogram task to have already run, in order to have data to plot. This is checked
by the presence or absence of the corresponding output file from the required task. If the required
file is not present, the required task will be automatically started with the corresponding
parameters before the called task.

The full task tree of columnflow can be seen in
[this issue](https://github.com/columnflow/columnflow/issues/25).

A task is run with the command ```law run``` followed by the name of the task.
A version, given by the argument ```--version```, followed by the name of the version, is required.

In law, the intermediate results (=the outputs to the different tasks) are saved locally in the
corresponding directory (given in the setup, the arguments to run the task are also used for the path).
The name of the version also appears in the path and should therefore be selected to match your
purpose, for example ```--version selection_with_gen_matching```.


Tasks in law are organized as a graph with dependencies. Therefore a "depth" for the different
required tasks exists, depending on which task required which other task. In order to see the
different required tasks for a single task, you might use the argument ```--print-status -1```,
which will show all required tasks and the existence or absence of their output for the given input
parameters up to depth "-1", hence the deepest one. The called task with ```law run``` will have
depth 0. You might check the output path of a task with the argument ```--print-output```,
followed by the depth of the task. If you want a finished task to be run anew without changing
the version (e.g. do a new histogram with different binning), you might remove the previous
outputs with the ```--remove-output``` argument, followed by the depth up to which to remove the
outputs. There are three removal modes:
- ```a``` (all: remove all outputs of the different tasks up to the given depth),
- ```i``` (interactive: prompt a selection of the tasks to remove up to the given depth)
- ```d``` (dry: show which files might be deleted with the same selection options, but do not remove
        the outputs).

The ```--remove-output``` argument does not allow the depth "-1", check the task
tree with ```--print-output``` before selecting the depth you want. The removal mode
can be already selected in the command, e.g. with ```--remove-output 1,a``` (remove all outputs up
to depth 1).

Once the output has been removed, it is possible to run the task again. It is also possible to
rerun the task in the same command as the removal by adding the ```y``` argument at the end.
Therefore, removing all outputs of a selected task (but not its dependencies) and running it again
at once would correspond to the following command:

```shell
law run name_of_the_task --version name_of_the_version --remove-output 0,a,y
```

An example command to see the location of the output file after running a 1D plot of a variable
with columnflow using only law functions and the default arguments for the tasks would be:

```shell
law run PlotVariables1D --version test_plot --print-output 0
```




## Variables creation


In order to plot something out of the processed datasets, columnflow uses
{external+order:py:class}`order.variable.Variable`s. It is therefore not enough to create a new
column in a {py:class}`~columnflow.production.Producer` for the plotting tasks, it must also be
translated in a {external+order:py:class}`order.variable.Variable`, which name can be given
to the argument ```--variables``` for the plotting/histograming task. These
{external+order:py:class}`order.variable.Variable`s need to be added to the config using the
function {external+order:py:meth}`order.config.Config.add_variable`. The standard syntax is as
follows:
```python
config.add_variable(
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
config.add_variable(
    name="jet1_pt",
    expression="Jet.pt[:,0]",
    null_value=EMPTY_FLOAT,
    binning=(40, 0.0, 400.0),
    unit="GeV",
    x_title=r"Jet 1 $p_{T}$",
)
```

The list of possible keyword arguments can be found in
{external+order:py:class}`order.variable.Variable`. The values in ```expression``` can be either a
one-dimensional or a more dimensional array. In this second case the information is flattened before
plotting. It is to be mentioned that {py:attribute}`~columnflow.columnar_util.EMPTY_FLOAT` is a
columnflow internal null value and corresponds to the value ```-9999```.


## Calibrators

TODO

## Production of columns

### Introduction

In columnflow, event/object based information (weights, properties, ...) is stored in columns.
The creation of new columns is managed by instances of the
{py:class}`~columnflow.production.Producer` class. {py:class}`~columnflow.production.Producer`s can
be called in other classes ({py:class}`~columnflow.calibration.Calibrator` and
{py:class}`~columnflow.selection.Selector`), or directly through the ```ProduceColumns``` task. It
is also possible to create new columns directly within
{py:class}`~columnflow.calibration.Calibrator`s and {py:class}`~columnflow.selection.Selector`s,
without using instances of the {py:class}`~columnflow.production.Producer` class, but the process is
the same as for the {py:class}`~columnflow.production.Producer` class. Therefore, the
{py:class}`~columnflow.production.Producer` class, which sole purpose is the creation of new
columns, will be used to describe the process. The new columns are saved in a parquet file. If the
column were created before the ```ReduceEvents``` task and are still needed afterwards, it should
not be forgotten to include them in the ```keep_columns``` auxiliary of the config, as they would
otherwise not be saved in the output file of the task. If the columns are created further down
the task tree, e.g. in ```ProduceColumns```, they will be stored in an other parquet file, namely as
the output of the corresponding task, but these parquet files will be loaded similarly to the
outputs from ```ReduceEvents```.

### Usage

To create new columns, the {py:class}`~columnflow.production.Producer` instance will need to load
the columns needed for the production of the new columns from the dataset/parquet files. This is
given by the ```uses``` set of the instance of the {py:class}`~columnflow.production.Producer`
class. Similarly, the newly created columns within the producer need to be declared in the
```produces``` set of the instance of the {py:class}`~columnflow.production.Producer` class to be
stored in the output parquet file. The {py:class}`~columnflow.production.Producer` instance only
needs to return the ```events``` array with the additional columns. New columns can be set using
the function {py:func}`~columnflow.columnar_util.set_ak_column`.

An example of a {py:class}`~columnflow.production.Producer` for the ```HT```variable is given below:

```python
from columnflow.production import Producer, producer
from columnflow.util import maybe_import
from columnflow.columnar_util import set_ak_column

np = maybe_import("numpy")
ak = maybe_import("awkward")


@producer(
    uses={"Jet.pt"},
    produces={"HT"},
)
def features(self: Producer, events: ak.Array, **kwargs) -> ak.Array:
    # reconstruct HT and write in the events
    events = set_ak_column(events, "HT", ak.sum(events.Jet.pt, axis=1))

    return events
```

To call a {py:class}`~columnflow.production.Producer` in an other
{py:class}`~columnflow.production.Producer`/{py:class}`~columnflow.calibration.Calibrator`/
{py:class}`~columnflow.selection.Selector`, the following expression might be used:
```python
events = self[producer_name](arguments_of_the_producer, **kwargs)
```

Hence, a complete example would be:
```python
from columnflow.production import Producer, producer
from columnflow.util import maybe_import
from columnflow.columnar_util import set_ak_column

np = maybe_import("numpy")
ak = maybe_import("awkward")


@producer(
    uses={"Jet.pt"},
    produces={"HT"},
)
def HT_feature(self: Producer, events: ak.Array, **kwargs) -> ak.Array:
    # reconstruct HT and write in the events
    events = set_ak_column(events, "HT", ak.sum(events.Jet.pt, axis=1))

    return events


@producer(
    uses={HT_feature},
    produces={HT_feature, "Jet.pt_squared"},
)
def all_features(self: Producer, events: ak.Array, **kwargs) -> ak.Array:
    # use other producer to create HT column
    events = self[HT_feature](events, **kwargs)

    # create for all jets a column containing the square of the transverse momentum
    events = set_ak_column(events, "Jet.pt_squared", events.Jet.pt * events.Jet.pt)

    return events
```

The ```all_features``` producer creates therefore two new columns, the ```HT``` column on event
level, and the ```pt_squared``` column for each object of the ```Jet``` collection.

Notes:
- If one wants to index the ```events``` array in a way that the index does not exist for every
event (e.g. ```events.Jet.pt[10]``` does only work for events with 11 or more jets), it is possible
to use the {py:class}`~columnflow.columnar_util.Route` class and its
{py:meth}`~columnflow.columnar_util.Route.apply` function, which allows to give a default value to
the returned array in the case of a missing element without throwing an error.

- If the {py:class}`~columnflow.production.Producer` is built in a new file and to be used directly
by ```ProduceColumns```, you will still need to put the name of the new file along with its path in
the ```law.cfg``` file under the ```production_modules``` argument for law to be able to find the
file.

- If you want to use some fields, like the ```Jet``` field, as a Lorentz vector to apply operations
on, you might use the {py:func}`~columnflow.production.util.attach_coffea_behavior` function. This
function can be applied on the ```events``` array using
```python
events = self[attach_coffea_behavior](events, **kwargs)
```
If the name of the field does not correspond to a standard field name, e.g. "BtaggedJets", which
should provide the same behaviour as a normal jet, the behaviour can still be set, using
```python
collections = {x: {"type_name": "Jet"} for x in ["BtaggedJets"]}
events = self[attach_coffea_behavior](events, collections=collections, **kwargs)
```

- The weights for plotting should ideally be created in the ```ProduceColumns``` task, after the
selection and reduction of the data.


### ProduceColumns task

The ```ProduceColumns``` task runs a specific instance of the
{py:class}`~columnflow.production.Producer` class and stores the additional columns created in a
parquet file.

While it is possible to see all the arguments and their explanation for this task using
```law run ProduceColumns --help```, the only argument created specifically for this task is the
```--producer``` argument, through which the {py:class}`~columnflow.production.Producer` to be used
can be chosen.

An example of how to run this task for an analysis with several datasets and configs is given below:

```shell
law run SelectEvents --version name_of_your_version \
                     --config name_of_your_config \
                     --producer name_of_the_producer \
                     --dataset name_of_the_dataset_to_be_run
```

It is to be mentioned that this task is run after the ```SelectEvents``` and ```CalibrateEvents```
tasks and therefore uses the default arguments for the ```--calibrators``` and the ```--selector```
if not specified otherwise.

## Selections

### Introduction

In columnflow, selections are defined through the {py:class}`~columnflow.selection.Selector` class.
This class allows for arbitrary selection criteria on event level as well as object level using masks.
The results of the selection (which events or objects are to be conserved) are saved in an instance
of the {py:class}`~columnflow.selection.SelectionResult` class. Similar to
{py:class}`~columnflow.production.Producer`s, it is possible to create new columns in
{py:class}`~columnflow.selection.Selector`s. In the original columnflow setup,
{py:class}`~columnflow.selection.Selector`s are being run in the ```SelectEvents``` task.

### Create an instance of the Selector class

Similar to {py:class}`~columnflow.production.Producer`s, {py:class}`~columnflow.selection.Selector`s
need to declare which columns are to be used (produced) by the
{py:class}`~columnflow.selection.Selector` instance in order for them to taken out of the parquet files
(saved in the new parquet files). An example for this structure is given below (partially taken
from the {py:class}`~columnflow.selection.Selector` documentation.):

```python

# import the Selector class and the selector method
from columnflow.selection import Selector, selector

# also import the SelectionResult class
from columnflow.selection import SelectionResult

# maybe import awkward in case this Selector is actually run
from columnflow.util import maybe_import
ak = maybe_import("awkward")


# now wrap any function with a selector
@selector(
    # define some additional information here, e.g.
    # what columns are needed for this Selector?
    uses={
        "Jet.pt", "Jet.eta"
    },
    # does this Selector produce any columns?
    produces=set(),

    # pass any other variable to the selector class
    some_auxiliary_variable=True,

    # ...
)
def jet_selection(self: Selector, events: ak.Array, **kwargs) -> tuple[ak.Array, SelectionResult]:
    # do something ...
    return events, SelectionResult()
```

The structure of the arguments for the returned {py:class}`~columnflow.selection.SelectionResult`
instance are discussed below. (Input the internal link here)

#### Exposed and internal Selectors

{py:class}`~columnflow.selection.Selector`s can be either available directly from the command line
or only internally, through other selectors. To make a {py:class}`~columnflow.selection.Selector`
available from the command line, it should be declared with the ```exposed=True``` argument.
To call a fully functional {py:class}`~columnflow.selection.Selector` (in the following refered
as Selector_int) from an other {py:class}`~columnflow.selection.Selector` (in the following refered
to as Selector_ext), several steps are required:
- If defined in an other file, Selector_int should be imported in the Selector_ext script,
- The columns needed for Selector_int should be declared in the ```uses``` argument of Selector_ext
(it is possible to simply write the name of the Selector_int in the ```uses``` set, the content of
the ```uses``` set from Selector_int will be added to the ```uses``` set of Selector_ext, see below)
- Selector_int must be run in Selector_ext, e.g. with the
```self[Selector_int](events, **kwargs)``` call.

An example of an exposed Selector_ext with the ```jet_selection```
{py:class}`~columnflow.selection.Selector` defined above as Selector_int, assuming the
```jet_selection``` exists in ```analysis/selection/jet.py``` is given below. It should be mentioned
that a few details must be changed for this selector to work within the worklow, the full version
can be found in the ```Complete example``` section.

```python

# import the Selector class and the selector method
from columnflow.selection import Selector, selector

# also import the SelectionResult class
from columnflow.selection import SelectionResult

# maybe import awkward in case this Selector is actually run
from columnflow.util import maybe_import
ak = maybe_import("awkward")

# import the used internal selector
from analysis.selection.jet import jet_selection


@selector(
    # some information for Selector
    # e.g., if we want to use some internal Selector, make
    # sure that you have all the relevant information
    uses={
        jet_selection,
    },
    produces={
        jet_selection,
    },

    # this is our top level Selector, so we need to make it reachable
    # for the SelectEvents task
    exposed=True,
)
def Selector_ext(self: Selector, events: ak.Array, **kwargs) -> tuple[ak.Array, SelectionResult]:
    results = SelectionResult()
    # do something here

    # e.g., call the internal Selector
    events, sub_result = self[jet_selection](events)
    results += sub_result

    return events, results
```

#### SelectionResult

The result of a {py:class}`~columnflow.selection.Selector` is propagated through an instance of the
{py:class}`~columnflow.selection.SelectionResult` class. The
{py:class}`~columnflow.selection.SelectionResult` object is instantiated using a dictionary for each
argument. There are four arguments that may be set, which contain:
- Boolean masks to select the events to be kept in the analysis, which is saved under the
```steps``` argument. Several selection steps may be defined in a single Selector, each with a unique
name being the key of the dictionary for the corresponding mask.
- Several index masks for specific objects in a double dictionary structure, saved under the
```objects``` argument. The double dictionary structure in the ```objects``` defines the source
column/field from which the indices are to be taken (first dimension of the dictionary) and the name of
the new column/field to be created with only these objects (second dimension of the dictionary). If the
name of the column/field to be created is the same as the name of an already existing column/field, the original
column/field will be overwritten by the new one!
- Additional informations to be used by other {py:class}`~columnflow.selection.Selector`s, saved
under the ```aux``` argument.
- A combined boolean mask of all steps used, which is saved under the ```main``` argument, with the
```"event"``` key. An example with this argument will be shown in the section
```Complete example```. The final
{py:class}`~columnflow.selection.SelectionResult` object to be returned by the exposed selector must
have this field.

While the arguments in the ```aux``` dictionary are discarded after the ```ReduceEvents``` task and are
only used for short-lived saving of internal information that might be needed by other
{py:class}`~columnflow.selection.Selector`s, the ```steps``` and ```objects``` arguments are
specifically used by the ```ReduceEvents``` task to apply the given masks to the nanoAOD files
(potentially with additional columns). As described above, the ```steps``` argument is used to reduce
the number of events to be processed further down the task tree according to the selections, while
the ```objects``` argument is used to select which objects are to be kept for further processing and
creates new columns/fields containing specific selected objects.

Below is an example of a fully written internal Selector with its
{py:class}`~columnflow.selection.SelectionResult` object without ```main``` argument.

```python

# import the Selector class and the selector method
from columnflow.selection import Selector, selector

# also import the SelectionResult class
from columnflow.selection import SelectionResult

# maybe import awkward in case this Selector is actually run
from columnflow.util import maybe_import
ak = maybe_import("awkward")


# now wrap any function with a selector
@selector(
    # define some additional information here, e.g.
    # what columns are needed for this Selector?
    uses={
        "Jet.pt", "Jet.eta"
    },
    # does this Selector produce any columns?
    produces=set(),

    # pass any other variable to the selector class
    some_auxiliary_variable=True,

    # ...
)
def jet_selection_with_result(self: Selector, events: ak.Array, **kwargs) -> tuple[ak.Array, SelectionResult]:
    # require an object of the Jet collection to have at least 20 GeV pt and at most 2.4 eta to be
    # considered a Jet in our analysis
    jet_mask = ((events.Jet.pt > 20.0) & (abs(events.Jet.eta) < 2.4))

    # require an object of the Jet collection to have at least 50 GeV pt and at most 2.4 eta
    jet_50pt_mask = ((events.Jet.pt > 50.0) & (abs(events.Jet.eta) < 2.4))

    # require an event to have at least two jets to be selected
    jet_sel = (ak.sum(jet_mask, axis=1) >= 2)

    # create the list of indices to be kept from the Jet collection using the jet_mask to create the
    # new Jet field containing only the selected Jet objects
    jet_indices = ak.local_index(events.Jet.pt)[jet_mask]

    # create the list of indices to be kept from the Jet collection using the jet_50pt_mask to create the
    # new Jet_50pt field containing only the selected Jet_50pt objects
    jet_50pt_indices = ak.local_index(events.Jet.pt)[jet_50pt_mask]

    return events, SelectionResult(
        steps={
            # boolean mask to create selection of the events with at least two jets, this will be
            # applied in the ReduceEvents task
            "jet": jet_sel,
        },
        objects={
            # in ReduceEvents, the Jet field will be replaced by the new Jet field containing only
            # selected jets, and a new field called Jet_50pt containing the jets with pt higher than
            # 50 GeV will be created
            "Jet": {
                "Jet": jet_indices,
                "Jet_50pt": jet_50pt_indices,
            },
        },
        aux={
            # jet mask that lead to the jet_indices
            "jet_mask": jet_mask,
        },
    )
```



#### Selection using several selection steps

In order for the ```ReduceEvents``` task to apply the final event selection to all events, it is
necessary to input the resulting boolean array in the ```main``` argument of the returned
{py:class}`~columnflow.selection.SelectionResult` by the exposed
{py:class}`~columnflow.selection.Selector`.
When several selection steps do appear in the selection, it is necessary to combine all the masks
from all the steps in order to obtain the final boolean array to be given to the ```main``` argument
of the {py:class}`~columnflow.selection.SelectionResult` and for it to be applied to the events.
This can be achieved in two steps:

- Combining the results from the different selections to a single
{py:class}`~columnflow.selection.SelectionResult` object:
```python
results = SelectionResult()
results += jet_results
results += fatjet_results
```

- Reducing the different steps to a single boolean array and give it to the ```main``` argument of
the {py:class}`~columnflow.selection.SelectionResult` object.
```python
# import the functions to combine the selection masks
from operator import and_
from functools import reduce

# combined event selection after all steps
event_sel = reduce(and_, results.steps.values())
results.main["event"] = event_sel
```

#### Selection stats


In order to use the correct values for the weights to be applied to the Monte Carlo samples while
plotting, it is necessary to save some information which would be lost after the ```ReduceEvents```
task. An example for that would be the sum of all the Monte Carlo weights in a simulation, which is
needed for the normalization weights. In order to propagate this information to tasks further down
the tree, the ```stats.json``` file is created. As it is a json file, it contains a
dictionary with the key corresponding to the name of the information to be saved, while the value
for the key is the information itself. The dictionary is created in the ```SelectEvents``` task and
updated in place in a {py:class}`~columnflow.selection.Selector`. Depending on the weights to be
used, various additional information might need to be saved in the ```stats``` object.

The keys ```"n_events"```, ```"n_events_selected"```, ```"sum_mc_weight"```,
```"sum_mc_weight_selected"``` get printed by the ```SelectEvents``` task along with the
corresponding efficiency. If they are not set, the default value for floats will be printed instead.

Below is an example of such a {py:class}`~columnflow.selection.Selector` updating the ```stats```
dictionary in place. This dictionary will be saved in the ```stats.json``` file. For convenience,
the weights were saved in a weight_map dictionary along with the mask before the sum of the weights
was saved in the ```stats``` dictionary. In this example, the keys to be printed by the
```SelectEvents``` task and the sum of the Monte Carlo weights per process (needed for correct
normalization of the number of Monte Carlo events in the plots) are saved.


```python
from columnflow.selection import Selector, selector

# also import the SelectionResult class
from columnflow.selection import SelectionResult

# maybe import awkward in case this Selector is actually run
from columnflow.util import maybe_import
ak = maybe_import("awkward")
np = maybe_import("numpy")

from collections import defaultdict, OrderedDict


@selector(uses={"process_id", "mc_weight"})
def increment_stats(
    self: Selector,
    events: ak.Array,
    results: SelectionResult,
    stats: dict,
    **kwargs,
) -> ak.Array:
    """
    Unexposed selector that does not actually select objects but instead increments selection
    *stats* in-place based on all input *events* and the final selection *mask*.
    """
    # get event masks
    event_mask = results.main.event

    # increment plain counts
    stats["n_events"] += len(events)
    stats["n_events_selected"] += ak.sum(event_mask, axis=0)

    # get a list of unique process ids present in the chunk
    unique_process_ids = np.unique(events.process_id)

    # create a map of entry names to (weight, mask) pairs that will be written to stats
    weight_map = OrderedDict()
    if self.dataset_inst.is_mc:
        # mc weight for all events
        weight_map["mc_weight"] = (events.mc_weight, Ellipsis)

        # mc weight for selected events
        weight_map["mc_weight_selected"] = (events.mc_weight, event_mask)

    # get and store the sum of weights in the stats dictionary
    for name, (weights, mask) in weight_map.items():
        joinable_mask = True if mask is Ellipsis else mask

        # sum of different weights in weight_map for all processes
        stats[f"sum_{name}"] += ak.sum(weights[mask])

        # sums per process id
        stats.setdefault(f"sum_{name}_per_process", defaultdict(float))
        for p in unique_process_ids:
            stats[f"sum_{name}_per_process"][int(p)] += ak.sum(
                weights[(events.process_id == p) & joinable_mask],
            )

    return events
```


#### Complete example

Overall, creating an exposed {py:class}`~columnflow.selection.Selector` with several selections steps
might look like this:
```python
# coding: utf-8

# import the Selector class and the selector method
from columnflow.selection import Selector, selector

# also import the SelectionResult class
from columnflow.selection import SelectionResult
from columnflow.production.cms.mc_weight import mc_weight
from columnflow.production.processes import process_ids

# import the functions to combine the selection masks
from operator import and_
from functools import reduce

# maybe import awkward in case this Selector is actually run
from columnflow.util import maybe_import
ak = maybe_import("awkward")
np = maybe_import("numpy")

from collections import defaultdict, OrderedDict


# First, define an internal jet Selector to be used by the exposed Selector

@selector(
    # define some additional information here, e.g.
    # what columns are needed for this Selector?
    uses={
        "Jet.pt", "Jet.eta"
    },
    # does this Selector produce any columns?
    produces=set(),

    # pass any other variable to the selector class
    some_auxiliary_variable=True,
)
def jet_selection_with_result(self: Selector, events: ak.Array, **kwargs) -> tuple[ak.Array, SelectionResult]:
    # require an object of the Jet collection to have at least 20 GeV pt and at most 2.4 eta to be
    # considered a Jet in our analysis
    jet_mask = ((events.Jet.pt > 20.0) & (abs(events.Jet.eta) < 2.4))

    # require an object of the Jet collection to have at least 50 GeV pt and at most 2.4 eta
    jet_50pt_mask = ((events.Jet.pt > 50.0) & (abs(events.Jet.eta) < 2.4))

    # require an event to have at least two jets to be selected
    jet_sel = (ak.sum(jet_mask, axis=1) >= 2)

    # create the list of indices to be kept from the Jet collection using the jet_mask to create the
    # new Jet field containing only the selected Jet objects
    jet_indices = ak.local_index(events.Jet.pt)[jet_mask]

    # create the list of indices to be kept from the Jet collection using the jet_50pt_mask to create the
    # new Jet_50pt field containing only the selected Jet_50pt objects
    jet_50pt_indices = ak.local_index(events.Jet.pt)[jet_50pt_mask]

    return events, SelectionResult(
        steps={
            # boolean mask to create selection of the events with at least two jets, this will be
            # applied in the ReduceEvents task
            "jet": jet_sel,
        },
        objects={
            # in ReduceEvents, the Jet field will be replaced by the new Jet field containing only
            # selected jets, and a new field called Jet_50pt containing the jets with pt higher than
            # 50 GeV will be created
            "Jet": {
                "Jet": jet_indices,
                "Jet_50pt": jet_50pt_indices,
            },
        },
        aux={
            # jet mask that lead to the jet_indices
            "jet_mask": jet_mask,
        },
    )


# Next, define an internal fatjet Selector to be used by the exposed Selector

@selector(
    # define some additional information here, e.g.
    # what columns are needed for this Selector?
    uses={
        "FatJet.pt",
    },
    # does this Selector produce any columns?
    produces=set(),

    # ...
)
def fatjet_selection_with_result(self: Selector, events: ak.Array, **kwargs) -> tuple[ak.Array, SelectionResult]:
    # require an object of the FatJet collection to have at least 40 GeV pt to be
    # considered a FatJet in our analysis
    fatjet_mask = (events.FatJet.pt > 40.0)

    # require an event to have at least one AK8-jet (=FatJet) to be selected
    fatjet_sel = (ak.sum(fatjet_mask, axis=1) >= 1)

    # create the list of indices to be kept from the FatJet collection using the fatjet_mask to create the
    # new FatJet field containing only the selected FatJet objects
    fatjet_indices = ak.local_index(events.FatJet.pt)[fatjet_mask]

    return events, SelectionResult(
        steps={
            # boolean mask to create selection of the events with at least two jets, this will be
            # applied in the ReduceEvents task
            "fatjet": fatjet_sel,
        },
        objects={
            # in ReduceEvents, the FatJet field will be replaced by the new FatJet field containing only
            # selected fatjets
            "FatJet": {
                "FatJet": fatjet_indices,
            },
        },
    )


# Implement the task to update the stats object

@selector(uses={"process_id", "mc_weight"})
def increment_stats(
    self: Selector,
    events: ak.Array,
    results: SelectionResult,
    stats: dict,
    **kwargs,
) -> ak.Array:
    """
    Unexposed selector that does not actually select objects but instead increments selection
    *stats* in-place based on all input *events* and the final selection *mask*.
    """
    # get event masks
    event_mask = results.main.event

    # increment plain counts
    stats["n_events"] += len(events)
    stats["n_events_selected"] += ak.sum(event_mask, axis=0)

    # get a list of unique process ids present in the chunk
    unique_process_ids = np.unique(events.process_id)

    # create a map of entry names to (weight, mask) pairs that will be written to stats
    weight_map = OrderedDict()
    if self.dataset_inst.is_mc:
        # mc weight for all events
        weight_map["mc_weight"] = (events.mc_weight, Ellipsis)

        # mc weight for selected events
        weight_map["mc_weight_selected"] = (events.mc_weight, event_mask)

    # get and store the sum of weights in the stats dictionary
    for name, (weights, mask) in weight_map.items():
        joinable_mask = True if mask is Ellipsis else mask

        # sum of different weights in weight_map for all processes
        stats[f"sum_{name}"] += ak.sum(weights[mask])

        # sums per process id
        stats.setdefault(f"sum_{name}_per_process", defaultdict(float))
        for p in unique_process_ids:
            stats[f"sum_{name}_per_process"][int(p)] += ak.sum(
                weights[(events.process_id == p) & joinable_mask],
            )

    return events


# Now create the exposed Selector using the three above defined Selectors

@selector(
    # some information for Selector
    # e.g., if we want to use some internal Selector, make
    # sure that you have all the relevant information
    uses={
        mc_weight, jet_selection_with_result, fatjet_selection_with_result, increment_stats,
        process_ids,
    },
    produces={
        mc_weight, process_ids,
    },

    # this is our top level Selector, so we need to make it reachable
    # for the SelectEvents task
    exposed=True,
)
def Selector_ext(
    self: Selector,
    events: ak.Array,
    stats: defaultdict,
    **kwargs,
) -> tuple[ak.Array, SelectionResult]:

    results = SelectionResult()

    # add corrected mc weights to be used later for plotting and to calculate the sum saved in stats
    if self.dataset_inst.is_mc:
        events = self[mc_weight](events, **kwargs)

    # call the first internal selector, the jet selector, and save its result
    events, jet_results = self[jet_selection_with_result](events, **kwargs)
    results += jet_results

    # call the second internal selector, the fatjet selector, and save its result
    events, fatjet_results = self[fatjet_selection_with_result](events, **kwargs)
    results += fatjet_results

    # combined event selection after all steps
    event_sel = reduce(and_, results.steps.values())
    results.main["event"] = event_sel

    # create process ids, used by increment_stats
    events = self[process_ids](events, **kwargs)

    # use increment stats selector to update dictionary to be saved in json format
    events = self[increment_stats](events, results, stats, **kwargs)

    return events, results
```

Notes:
- If you want to build this exposed {py:class}`~columnflow.selection.Selector` along with the inner
{py:class}`~columnflow.selection.Selector`s in a new file, you will still need to put the name of
the new file along with its path in the ```law.cfg``` file under the ```selection_modules```
argument for law to be able to find the file.

- If you want to use some fields, like the ```Jet``` field, as a Lorentz vector to apply operations
on, you might use the {py:func}`~columnflow.production.util.attach_coffea_behavior` function. This
function can be applied on the ```events``` array using
```python
events = self[attach_coffea_behavior](events, **kwargs)
```
If the name of the field does not correspond to a standard field name, e.g. "BtaggedJets", which
should provide the same behaviour as a normal jet, the behaviour can still be set, using
```python
collections = {x: {"type_name": "Jet"} for x in ["BtaggedJets"]}
events = self[attach_coffea_behavior](events, collections=collections, **kwargs)
```

- The actual creation of the weights to be applied in the histogramms after the selection should be
done in the ```ProduceColumns``` task, using the stats object created in this task if needed.



### Running the SelectEvents task

The ```SelectEvents``` task runs a specific selection script and saves the created masks for event and
objects selections in a parquet file, as well as the statistics of the selection in a json file.

While it is possible to see all the arguments and their explanation for this task using
```law run SelectEvents --help```, the only argument created specifically for this task is the
```--selector``` argument, through which the exposed {py:class}`~columnflow.selection.Selector` to
be used can be chosen.

The masks created by this task are then used by the ```ReduceEvents``` task to reduce the number of
events (see the ```steps``` argument for the {py:class}`~columnflow.selection.SelectionResult`) and
create/update new columns/fields with only the selected objects (see the ```objects``` argument for
the {py:class}`~columnflow.selection.SelectionResult`). The saved statistics are used for plotting. (?????)

It should not be forgotten that any column created in this task should be included in the
```keep_columns``` argument of the config file, as only columns explicitely required to be kept in
this dictionary will be loaded and then saved in the reduced parquet file.

An example of how to run this task for an analysis with several datasets and configs is given below:

```shell
law run SelectEvents --version name_of_your_version \
                     --config name_of_your_config \
                     --selector name_of_the_selector \
                     --dataset name_of_the_dataset_to_be_run
```

It is to be mentioned that this task is run after the ```CalibrateEvents``` task and therefore uses
the default argument for the ```--calibrators``` if not specified otherwise.

Notes:
- Running the exposed {py:class}`~columnflow.selection.Selector` from the ```Complete example```
section would simply require you to give the name ```Selector_ext``` in the ```--selector```
argument.

General questions:
- Where to put "attach_coffea_behavior"?
- Where to put ??? and ```cf_sandbox venv_columnar_dev bash ``` -> use a script in a sandbox
- How to require the output of a task in a general python script which is not a task again? ->
ask marcel where the example was with ".law_run()"
- Where to write about errors in columnflow? first thing to mention: If you get an error and look at
the error stack, you will probably see two errors: 1) the actual error and where it happened in
the code (standard python error) and 2) a sandbox error that you can ignore in most of the cases, as
it does not correspond to your problem, it only says in which sandbox it happened. Should be
somewhere for new users, as the first error to be seen when going up is the sandbox error, which is
not useful.






