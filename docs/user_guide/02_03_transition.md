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

(Note that, as before, while the hooks to register custom functions are named as shown above, the functions stored internally have an additional suffix and are named `HOOK_func`.)

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

## Multi-config Tasks

- TODO.

## Reducers

- [Reducers](./building_blocks/reducers.md)
- TODO.

## Histogram Producers

- [Hist producers](./building_blocks/hist_producers.md)
- TODO.

## Inference Model Updates

- TODO.

## Changed Plotting Task Names

- see [our wiki](https://github.com/columnflow/columnflow/wiki#default-task-graph)
- TODO.
