# v0.2 → v0.3 Transition

This document describes changes on columnflow introduced in version 0.3.0 that may affect existing code as well as already created output files.
These changes were made in a refactoring campaign (see [release v0.3](https://github.com/columnflow/columnflow/releases/tag/v0.3.0)) that was necessary to generalize some decisions made in an earlier stage of the project, and to ultimately support more analysis use cases that require a high degree of flexibility in many aspects of the framework.

The changes are grouped into the following categories:

- [Restructured Task Array Functions](#restructured-task-array-functions)
- [Multi-config Tasks](#multi-config-tasks)
- [Reducers](#reducers)
- [Histogram Producers](#histogram-producers)
- [Inference Model Updates](#inference-model-updates)
- [Changed Plotting Task Names](#changed-plotting-task-names)

## Restructured Task Array Functions

The internals of task array functions (TAF) like calibrators, selectors and producers received a major overhaul.
Not all changes affect user code but some might.

Most notably, TAFs **no longer** have the attributes `task`, `global_shift_inst`, `local_shift_inst`.
Instead, some of the configurable functions now receive a `task` argument through which task information and attributes like shifts can be accessed.
In turn, the attributes `analysis_inst`, `config_inst` and `dataset_inst` are guarenteed to **always be available**, and there is no longer the need to dynamically check their existence.

This change reflects the new state separation imposed by the order in which underlying, customizable functions (or *hooks*) are called.
A full overview of these hooks and arguments they received are listed in the [task array functions documentation](./task_array_functions.md).
In short, there are three types of hooks:

1. `pre_init`, `init`, `post_init`: Initialization hooks meant to dynamically update used and produced columns and TAF dependencies. `post_init` is the first hook to receive the `task` argument.
2. `requires`, `setup`, `teardown`: Methods to define custom task requirements, setting up attributes of the task array function before event processing, and to clean up and free resources afterwards.
3. `__call__`: The main callable that is invoked for each event chunk.

`pre_init`, `post_init` and `teardown` have been newly introduced.
See the [task array function interface](./task_array_functions.md#taf-interface) for a full descrption of all hooks and the arguments they receive.

(Note that, as before, while the hooks to register custom functions are named as shown above, the functions stored internally have an additional suffix and are named `<HOOK_NAME>_func`.)

### Example

The example below shows a simple producer that calculates the invariant mass of the two leading jets per event.
The `task` argument is now passed to the function, and the `task.logger` can be used to log messages in the scope of the task.

```python
import law
import awkward as ak
from columnflow.production import Producer, producer
from columnflow.columnar_util import set_ak_column

@producer(
    uses={"Jet.{pt,eta,phi,mass}"},
    produces={"di_jet_mass"},
)
def di_jet_mass(self: Producer, events: ak.Array, task: law.Task) -> ak.Array:
    # issue a warning in case less than 2 jets are present
    if ak.any(ak.num(events.Jet, axis=1) < 2):
        task.logger.warning("encountered event with less than 2 jets")

    di_jet = events.Jet[:, :2].sum(axis=1)
    events = set_ak_column(events, "di_jet_mass", di_jet.mass, value_type="float32")

    return events
```

### Update Instructions

1. Checkout the [TAF interface](./task_array_functions.md#taf-interface) to learn about the arguments that the hooks receive. In particular, the `task` argument is now passed to all hooks after (and including) `post_init`.
2. Make sure to no longer use the TAF attribites `self.task`, `self.global_shift_inst`, and `self.local_shift_inst`. Access them through `task` argument instead.
3. Depending on whether your custom TAF required access to these attributes, for instance in the `init` hook, you need to move your code to a different hook such as `post_init`.
4. If your TAF blocked specific resources, such as a large object, ML model, etc. loaded during `setup`, think about releasing these resources in the `teardown` hook.
5. Also, all TAF instances are chached from now on, given the combination of `self.analysis_inst`, `self.config_inst` and `self.dataset_inst`.

## Multi-config Tasks

Most of the tasks provided by columnflow operate on a single analysis configuration (usually representing self-contained data taking periods or *eras*).
Examples are `cf.CalibrateEvents` and `cf.SelectEvents`, or `cf.ProduceColumns` and `cf.CreateHistograms` which do the heavy lifting in terms of event processing.

However, some tasks require access to data of multiple eras at a time, and therefore, access to multiple analysis configurations.
We refer to these tasks as **multi-config tasks**.

In version 0.3, the following tasks are multi-config tasks:

- Most plotting tasks: tasks like `cf.PlotVariables1D` need to be able to draw events simulated for / recorded in multiple eras into the same plot.
- `cf.MLTraining`: For many ML training applications it is reasonable to train on data from multiple eras, given that detector conditions are not too different. It is now possible to request data from multiple eras to be loaded for a single training.
- `cf.CreateDatacards` (CMS-specific): The inference model interface as well as the datacard export routines now support entries for multiple configurations. See the [changes to the inference model interface](#inference-model-updates) below for details.

### Update Instructions

All instructions only apply to the CLI usage of tasks.

1. Tasks listed above no longer have a `--config` parameter. However, they now have a `--configs` parameter that accepts multiple configuration names as a comma-separate sequece. In order to achieve the single-config behavior, just pass the name of a single configuration here.
2. Specific other parameters of multi-config tasks changed as well. Most notably, the `--datasets` and `--processes` parameters, which previously allowed for defining sequences of dataset and process names on the command line, now accept muliple comma-separated sequences. The number of sequences should be exactly one (applies to all configurations) or match the number of configurations given in `--configs` (one-to-one assignment). Sequences should be separater by colons.
    - Example: `law.run cf.PlotVariables1D --configs 22pre,22post --datasets tt_sl,st_tw:tt_sl,st_s`

## Reducers

Reducers are a new type of task array function that are invoked by the `cf.ReduceEvents` task.
They control how results of the event selection - event and object masks - are applied to the full event data.
See the [types of task array functions](./task_array_functions.md#taf-types) and the detailed [documentation on reducers](./building_blocks/reducers.md) for details.

The reducer's job is

- to apply the event selection mask (booleans) to select only a subset of events,
- to apply object selection masks (booleans or integer indices) to create new collections of objects (e.g. specific jets, or leptons), and
- to drop columns are not needed by any of the downstream tasks.

These three steps were previously part of the default implementation of the `cf.ReduceEvents` tasks but are now fully configurable though custom reducers.
For compatibility with existing analyses, a default reducer called `cf_default` is provided by columnflow that implements exactly the previous behavior.
In doing so, it even relies on the auxiliary entry `keep_columns` in the configuration to determine which columns should be kept after reduction.

### Example

The following example creates a custom reducer that invokes columnflow's default reduction behavior and additionally creates a new column.

```python
from columnflow.reduction import Reducer, reducer
from columnflow.reduction.default import cf_default
from columnflow.util import maybe_import
from columnflow.columnar_util import set_ak_column

ak = maybe_import("awkward")

@reducer(
    uses={cf_default, "Jet.hadronFlavour"},
    produces={cf_default, "Jet.from_b_hadron"},
)
def example(self: Reducer, events: ak.Array, selection: ak.Array, **kwargs) -> ak.Array:
    # run cf's default reduction which handles event selection and collection creation
    events = self[cf_default](events, selection, **kwargs)

    # compute and store additional columns after the default reduction
    # (so only on a subset of the events and objects which might be computationally lighter)
    events = set_ak_column(events, "Jet.from_b_hadron", abs(events.Jet.hadronFlavour) == 5, value_type=bool)

    return events
```

### Update Instructions

1. In general, there is no need to update your code. However, you will notice that output paths of all tasks downstream of (and including) `cf.ReduceEvents` will have an additional fragment like `.../red__cf_default/...` to reflect the choice of the reducer.
2. The reduction behavior that was previously part of the `cf.ReduceEvents` task is now encapsulated by a [default reducer](https://github.com/columnflow/columnflow/blob/refactor/taf_init/columnflow/reduction/default.py) called `cf_default`. To extend or alter its behavior, create your own implementation either from scratch or by inheriting from it and only overwriting some of its hooks.
3. Invoke your reducer by adding `--reducer MY_REDUCER_CLASS` on the command line or by adding an auxiliary entry `default_reducer` to your configuration.
4. If you decide to control the set of columns that should be available after reduction solely through your reducer, and no longer through the `keep_columns` auxiliary entry in your configuration, you can do so by redefining the `produces` set of your reducer.

## Histogram Producers

- [Hist producers](./building_blocks/hist_producers.md)
- TODO.

### Update Instructions

1. TODO.

## Inference Model Updates

As stated [above](#multi-config-tasks), multi-config tasks allow for the inclusion of multiple analysis configurations in a single task to be able to access event data that spans multiple eras.
This is particularly useful for tasks that export statistical models like `cf.CreateDatacards` (CMS-specific), and all other tasks that inherit from the generalized `SerializeInferenceModelBase` task.

To support this new feature, the underlying {py:class}`~columnflow.inference.InferenceModel`, i.e., the container object able to configure statistical models for your analysis, was updated.
Pointers to analysis-specific objects in category and process defintions are now to be stored per configuration (see example below).
This info is picked up by (e.g.) `cf.CreateDatacards` to pull in information and data from multiple data taking eras to potentially fill their event data into the same inference category.

As for all multi-config tasks, pass a sequence of configuration names to the `--configs` parameter on the command line.

### Example

The following example demonstrates how to define an inference model that ...

```python
from columnflow.inference import InferenceModel, inference_model

@inference_model
def example_model(self: InferenceModel) -> None:
    """
    Initialization method for the inference model. Use instance methods to define categories, processes and parameters.
    """
    # add a category
    self.add_category(
        "example_category",
        # add config dependent settings
        config_data={
            config_inst.name: self.category_config_spec(
                category=f"{ch}__{cat}__os__iso",  # name of the analysis category in the config
                variable="jet1_pt",  # name of the variable
                data_datasets=["data_*"],  # names (or patterns) of datasets with real data in the config
            )
            for config_inst in self.config_insts
        },
        # additional category settings
        mc_stats=10.0,
        flow_strategy=FlowStrategy.move,
    )

    # add processes
    self.add_process(
        name="TT",
        # add config dependent settings
        config_data={
            config_inst.name: self.process_config_spec(
                process=["tt_sl", "tt_dl", "tt_fh"],  # names of processes in the config
                mc_datasets=["tt_sl_powheg", "tt_dl_...", ...],  # names of MC datasets in the config
            ),
        },
        # additional process settings
        is_signal=False,
    )
    # more processes here
    ...
```

### Update Instructions

1. In definitions of categories, processes and parameters within your inference model, make sure that all pointers that refer for analysis-specific objects are stored in a dictionary with keys being configuration names.
2. These dictionaries are stored in fields named `config_data`.
3. Use the provided factory functions to create these dictionary structures to invoke some additional value validation:
    - for categories: {py:meth}`~columnflow.inference.InferenceModel.category_config_spec`
    - for processes: {py:meth}`~columnflow.inference.InferenceModel.process_config_spec`
    - for parameters: {py:meth}`~columnflow.inference.InferenceModel.parameter_config_spec`

## Changed Plotting Task Names

The visualization of systematic uncertainties is updated as of v0.3.
A new plot method was introduced to show not only the effect of the statistical uncertainty (due to the limited amount of simulated events) as a grey, hatched area, but also that of systematic uncertainties as a differently colored band.

The task that invokes this plot method by default is `cf.PlotShiftedVariables1D`.
See the full task graph in [our wiki](https://github.com/columnflow/columnflow/wiki#default-task-graph) to see its dependencies to other tasks.

**Note** that this task is not new, but it has been changed to include the systematic uncertainty bands.
In version v0.2 and below, this task was used to plot the effect of a single up or down variation of a single shift.
This behavior is now covered by a task called `cf.PlotShiftedVariablesPerShift1D`.

### Update Instructions

1. If you are interested in creating plots showing the effect of one **or multiple** shifts in the same graph, use the `cf.PlotShiftedVariables1D` task.
2. If you want to plot the effect of a single up or down variation of a single shift, use the `cf.PlotShiftedVariablesPerShift1D` task (formerly known as `cf.PlotShiftedVariables1D`)
