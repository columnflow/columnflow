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

- [Reducers](./building_blocks/reducers.md)
- TODO.

### Update Instructions

1. TODO.

## Histogram Producers

- [Hist producers](./building_blocks/hist_producers.md)
- TODO.

### Update Instructions

1. TODO.

## Inference Model Updates

- TODO.

### Update Instructions

1. TODO.

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
