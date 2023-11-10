# Selections

## Introduction

In columnflow, selections are defined through the {py:class}`~columnflow.selection.Selector` class.
This class allows for arbitrary selection criteria on event level as well as object level using masks.
The results of the selection (which events or objects are to be conserved) are saved in an instance
of the {py:class}`~columnflow.selection.SelectionResult` class. Similar to
{py:class}`~columnflow.production.Producer`s, it is possible to create new columns in
Selectors. In the original columnflow setup,
Selectors are being run in the
{py:class}`~columnflow.tasks.selection.SelectEvents` task.

## Create an instance of the Selector class

Similar to {py:class}`~columnflow.production.Producer`s, {py:class}`~columnflow.selection.Selector`s
need to declare which columns are to be used (produced) by the
Selector instance in order for them to be taken out of the parquet files
(saved in the new parquet files). An example for this structure is given below (Similar to the
Selector documentation.):

```python

# import the Selector class and the selector method
from columnflow.selection import Selector, selector

# also import the SelectionResult class
from columnflow.selection import SelectionResult

# maybe import awkward in case this Selector is actually run, this needs to be set as columnflow
# would else give an error during setup, as these packages are not in the default sandbox
from columnflow.util import maybe_import
ak = maybe_import("awkward")


# now wrap any function with a selector
@selector(
    # define some additional information here, e.g.
    # what columns are needed for this Selector?
    uses={
        "Jet.pt", "Jet.eta",
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
instance are discussed below in {ref}`SelectionResult`.

### Exposed and internal Selectors

{py:class}`~columnflow.selection.Selector`s can be either available directly from the command line
or only internally, through other selectors. To make a Selector
available from the command line, it should be declared with the ```exposed=True``` argument.
To call a fully functional Selector (in the following referred
as Selector_int) from an other Selector (in the following referred
to as Selector_ext), several steps are required:
- If defined in an other file, Selector_int should be imported in the Selector_ext script,
- The columns needed for Selector_int should be declared in the ```uses``` argument of Selector_ext
(it is possible to simply write the name of the Selector_int in the ```uses``` set, the content of
the ```uses``` set from Selector_int will be added to the ```uses``` set of Selector_ext, see below)
- Selector_int must be run in Selector_ext, e.g. with the
```self[Selector_int](events, **kwargs)``` call.

An example of an exposed Selector_ext with the ```jet_selection```
Selector defined above as Selector_int, assuming the
```jet_selection``` exists in ```analysis/selection/jet.py``` is given below. It should be mentioned
that a few details must be changed for this selector to work within the worklow, the full version
can be found in the {ref}`"Complete Example" <complete_example>` section.

```python

# import the Selector class and the selector method
from columnflow.selection import Selector, selector

# also import the SelectionResult class
from columnflow.selection import SelectionResult

# maybe import awkward in case this Selector is actually run, this needs to be set as columnflow
# would else give an error during setup, as these packages are not in the default sandbox
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

(SelectionResult)=

### SelectionResult

The result of a {py:class}`~columnflow.selection.Selector` is propagated through an instance of the
{py:class}`~columnflow.selection.SelectionResult` class. The
SelectionResult object is instantiated using a dictionary for each
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
- Additional informations to be used by other Selectors, saved
under the ```aux``` argument.
- A combined boolean mask of all steps used, which is saved under the ```event``` argument. An
example with this argument will be shown in the section
{ref}`"Complete Example" <complete_example>`. The final
SelectionResult object to be returned by the exposed selector must
have this field.

While the arguments in the ```aux``` dictionary are discarded after the
{py:class}`~columnflow.tasks.reduction.ReduceEvents` task and are
only used for short-lived saving of internal information that might be needed by other
Selectors, the ```steps``` and ```objects``` arguments are
specifically used by the ReduceEvents task to apply the
given masks to the nanoAOD files (potentially with additional columns). As described above, the
```steps``` argument is used to reduce the number of events to be processed further down the task
tree according to the selections, while the ```objects``` argument is used to select which objects
are to be kept for further processing and creates new columns/fields containing specific selected
objects.

Below is an example of a fully written internal Selector with its
SelectionResult object without ```event``` argument.

```python

# import the Selector class and the selector method
from columnflow.selection import Selector, selector

# also import the SelectionResult class
from columnflow.selection import SelectionResult

# maybe import awkward in case this Selector is actually run, this needs to be set as columnflow
# would else give an error during setup, as these packages are not in the default sandbox
from columnflow.util import maybe_import
ak = maybe_import("awkward")


# now wrap any function with a selector
@selector(
    # define some additional information here, e.g.
    # what columns are needed for this Selector?
    uses={
        "Jet.pt", "Jet.eta",
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



### Selection using several selection steps

In order for the {py:class}`~columnflow.tasks.reduction.ReduceEvents` task to apply the final event
selection to all events, it is necessary to input the resulting boolean array in the ```event```
argument of the returned {py:class}`~columnflow.selection.SelectionResult` by the exposed
{py:class}`~columnflow.selection.Selector`.
When several selection steps do appear in the selection, it is necessary to combine all the masks
from all the steps in order to obtain the final boolean array to be given to the ```event``` argument
of the SelectionResult and for it to be applied to the events.
This can be achieved in two steps:

- Combining the results from the different selections to a single
SelectionResult object:
```python
results = SelectionResult()
results += jet_results
results += fatjet_results
```

- Reducing the different steps to a single boolean array and give it to the ```event``` argument of
the SelectionResult object.
```python
# import the functions to combine the selection masks
from operator import and_
from functools import reduce

# combined event selection after all steps
event_sel = reduce(and_, results.steps.values())
results.event = event_sel
```

### Selection stats


In order to use the correct values for the weights to be applied to the Monte Carlo samples while
plotting, it is necessary to save some information which would be lost after the
{py:class}`~columnflow.tasks.reduction.ReduceEvents`
task. An example for that would be the sum of all the Monte Carlo weights in a simulation, which is
needed for the normalization weights. In order to propagate this information to tasks further down
the tree, the ```stats.json``` file is created. As it is a json file, it contains a
dictionary with the key corresponding to the name of the information to be saved, while the value
for the key is the information itself. The dictionary is created in the
{py:class}`~columnflow.tasks.selection.SelectEvents` task and
updated in place in a {py:class}`~columnflow.selection.Selector`. Depending on the weights to be
used, various additional information might need to be saved in the ```stats``` object.

The keys ```"num_events"```, ```"num_events_selected"```, ```"sum_mc_weight"```,
```"sum_mc_weight_selected"``` get printed by the
SelectEvents task along with the
corresponding efficiency. If they are not set, the default value for floats will be printed instead.

Below is an example of such a Selector updating the ```stats```
dictionary in place. This dictionary will be saved in the ```stats.json``` file. For convenience,
the weights were saved in a weight_map dictionary along with the mask before the sum of the weights
was saved in the ```stats``` dictionary. In this example, the keys to be printed by the
SelectEvents task and the sum of the Monte Carlo weights
per process (needed for correct normalization of the number of Monte Carlo events in the plots)
are saved.


```python
from columnflow.selection import Selector, selector

# also import the SelectionResult class
from columnflow.selection import SelectionResult

# maybe import awkward in case this Selector is actually run, this needs to be set as columnflow
# would else give an error during setup, as these packages are not in the default sandbox
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
    event_mask = results.event

    # increment plain counts
    stats["num_events"] += len(events)
    stats["num_events_selected"] += ak.sum(event_mask, axis=0)

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

Columnflow also provides a helper Selector called
{py:class}`~columnflow.selection.stats.increment_stats`, which calculates and inputs directly the
number of events (the name of the feature should start with "num") for given masks
(`Ellipsis` should be given if no mask is to be used) or sum of
specific columns (usually weights, the name of the feature should start by "sum") for given columns
and masks, using a "weight map". These calculations can also be specified for subgroups of objects
using a "group map". An example of such a call using the number of jets and the processes as
subgroups is given below, with `results.event` the event selection mask after all selections and
having saved during the selection the number of valid jets in each event in the auxiliary field of
the SelectionResult object under the name `n_jets`. This example stems from the analysis template
present in the columnflow Github repository.

```{include} ../../../analysis_templates/cms_minimal/__cf_module_name__/selection/example.py
:start-after: events = self[cutflow_features](events, results.objects, **kwargs)
:end-before: return events, results
```

(complete_example)=
### Complete example

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

# maybe import awkward in case this Selector is actually run, this needs to be set as columnflow
# would else give an error during setup, as these packages are not in the default sandbox
from columnflow.util import maybe_import
ak = maybe_import("awkward")
np = maybe_import("numpy")

from collections import defaultdict, OrderedDict


# First, define an internal jet Selector to be used by the exposed Selector

@selector(
    # define some additional information here, e.g.
    # what columns are needed for this Selector?
    uses={
        "Jet.pt", "Jet.eta",
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
    event_mask = results.event

    # increment plain counts
    stats["num_events"] += len(events)
    stats["num_events_selected"] += ak.sum(event_mask, axis=0)

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
    results.event = event_sel

    # create process ids, used by increment_stats
    events = self[process_ids](events, **kwargs)

    # use increment stats selector to update dictionary to be saved in json format
    events = self[increment_stats](events, results, stats, **kwargs)

    return events, results
```

Notes:
- If you want to use an exposed Selector in a task call, and if
this new Selector is created in a new file, you need to include this file in the ```law.cfg``` file
under the ```selection_modules``` argument. A more detailed explanation of the law config file
can be found in the {ref}`Law config section <law_config_section>`.

- The actual creation of the weights to be applied in the histogramms after the selection might be
done in the {py:class}`~columnflow.tasks.production.ProduceColumns` task, using the stats object
created in this task if needed.

- Other useful functions (e.g. for easier handling of columns) can be found in the
{doc}`best_practices` section of this documentation.



## Running the SelectEvents task

The {py:class}`~columnflow.tasks.selection.SelectEvents` task runs a specific selection script and
saves the created masks for event and objects selections in a parquet file, as well as the
statistics of the selection in a json file.

While it is possible to see all the arguments and their explanation for this task using
```law run cf.SelectEvents --help```, the only argument created specifically for this task is the
```--selector``` argument, through which the exposed {py:class}`~columnflow.selection.Selector` to
be used can be chosen.

The masks created by this task are then used by the
{py:class}`~columnflow.tasks.reduction.ReduceEvents` task to reduce the number of
events (see the ```steps``` argument for the {py:class}`~columnflow.selection.SelectionResult`) and
create/update new columns/fields with only the selected objects (see the ```objects``` argument for
the SelectionResult). The saved statistics are used e.g. for the
weights needed for plotting.

It should not be forgotten that any column created in this task should be included in the
```keep_columns``` argument of the config file, as only columns explicitely required to be kept in
this dictionary will be loaded and then saved in the reduced parquet file.

An example of how to run this task for an analysis with several datasets and configs is given below:

```shell
law run cf.SelectEvents --version name_of_your_version \
                        --config name_of_your_config \
                        --selector name_of_the_selector \
                        --dataset name_of_the_dataset_to_be_run
```

It is to be mentioned that this task is run after the
{py:class}`~columnflow.tasks.calibration.CalibrateEvents` task and therefore uses
the default argument for the ```--calibrators``` if not specified otherwise.

Notes:
- Running the exposed Selector from the
{ref}`"Complete Example" <complete_example>`
section would simply require you to give the name ```Selector_ext``` in the ```--selector```
argument.
