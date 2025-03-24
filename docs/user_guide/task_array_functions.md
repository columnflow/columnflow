# Task Array Functions

Besides [configuration files and objects](./building_blocks/config_objects.md), task array functions constitute the core of columnflow.
They connect array functions - consider them small-ish, reusable code snippets that perform vectorized operations of chunks of events data encoded in awkward arrays - with columnflow's default task structure.
Throughout the documentation, you will sometimes see them abbreviated as `TAFs`.

## Introduction

A streamlined view of this task structure and the dependencies between them can be seen in the figure below (more complex parts of the graph, e.g. those related to cutflows or machine learning, are hidden for the purpose of clarity here).

- Each box denotes a specific task.
- Arrows indicate dependencies between them, usually in the form of persistently stored data.
- Orange boxes at the bottom are placeholders for tasks in the later stage of the graph, usually in the context of creating results.
- **Purple boxes highlight tasks that allow for the inclusion of user-defined code within task array functions**. The command-line parameters to control these functions are added in parentheses below task names.

The five major task array functions are (from top to bottom): calibrators, selectors, reducers, producers, and histogram producers.
They purpose and behavior is explained below, and in more detail in the [columnflow building blocks](./building_blocks/index.rst).

```{mermaid}
graph TD
    classDef PH stroke: #fe8e01, stroke-width: 3px, fill: #ffc78f
    classDef TA stroke: #8833bb, stroke-width: 3px

    GetDatasetLFNs(GetDatasetLFNs)
    CalibrateEvents("CalibrateEvents<br />(--calibrators)")
    SelectEvents("SelectEvents<br />(--selector)")
    ReduceEvents("ReduceEvents<br />(--reducer)")
    MergeReductionStats(MergeReductionStats)
    MergeReducedEvents(MergedReducedEvents)
    ProduceColumns("ProduceColumns<br />(--producers)")
    CreateHistograms("CreateHistograms<br />(--hist-producer)")
    MergeHistograms(MergeHistograms)
    MergeShiftedHistograms(MergeShiftedHistograms)
    Inference(Inference models ...)
    Plots(Variable plots ...)
    ShiftedPlots(Shifted variable plots ...)

    class CalibrateEvents TA
    class SelectEvents TA
    class ReduceEvents TA
    class ProduceColumns TA
    class CreateHistograms TA
    class Plots PH
    class ShiftedPlots PH
    class Inference PH

    %% top part
    GetDatasetLFNs -- lfns --> SelectEvents
    GetDatasetLFNs -- lfns --> CalibrateEvents
    CalibrateEvents -. cols .-> SelectEvents
    SelectEvents -- masks --> ReduceEvents
    SelectEvents -. cols .-> ReduceEvents
    CalibrateEvents -. cols .-> ReduceEvents
    GetDatasetLFNs -- lfns --> ReduceEvents

    %% merging 1 right
    ReduceEvents -- sizes --> MergeReductionStats
    MergeReductionStats -- factors --> MergeReducedEvents
    ReduceEvents -- events --> MergeReducedEvents

    %% additional columns
    MergeReducedEvents -- events --> CreateHistograms
    MergeReducedEvents -- events --> ProduceColumns
    ProduceColumns -. cols .-> CreateHistograms

    %% merging and results
    CreateHistograms -- hists --> MergeHistograms
    MergeHistograms -- hists ---> Plots
    MergeHistograms -- mc hists --> MergeShiftedHistograms
    MergeShiftedHistograms -- mc hists --> ShiftedPlots
    MergeHistograms -- data hists --> ShiftedPlots
    MergeShiftedHistograms -- mc hists --> Inference
    MergeHistograms -- data hists --> Inference

    subgraph "&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; Column merging"
        MergeReductionStats
        MergeReducedEvents
    end

    subgraph "Histogram merging&nbsp;&nbsp;&nbsp;&nbsp;"
        MergeHistograms
        MergeShiftedHistograms
    end
```

## TAF Types

1. Calibrator:
    - Meant to apply calibrations to event data that are stored as additional columns; these columns can then be used in subsequent tasks as if they were part of the original data
    - *Examples*: energy calibration (jets, taus) or object corrections (MET)
    - *Quantity*: zero, one or more
    - *Output length*: same as input
    - *Parameter*: `--calibrators [NAME,[NAME,...]]`
2. Selector:
    - Meant to perform both event and object selection; they must produce event and per-object masks that can be used downstream, as well as event and selection statistics (either in `json` or `hist` format) to be used for normalization later; they can also produce additional columns for use in subsequent tasks
    - *Examples*: the usual event selection
    - *Quantity*: exactly one
    - *Output length*: same as input
    - *Parameter*: `--selector NAME`
3. Reducer:
    - It receives all event data, as well as columns produced during calbration and selection plus the event and object selection masks to perform the reduction step; a default implementation exists that should be sufficient for most use cases; if additional columns should be produced, the default reducer can be extended
    - *Examples*: `cf_default`, i.e., columnflow's default event and object reduction, as well as collection creation
    - *Quantity*: exactly one
    - *Output length*: Any length, but obviously usually shorter than the input
    - *Parameter*: `--reducer NAME(cf_default)`
4. Producer:
    - The go-to mechanism for creating and storing additional variables needed by the analysis, after events were selected and reduced
    - *Examples*: creation of additional variables needed by the analysis
    - *Quantity*: zero, one or more
    - *Output length*: same as input
    - *Parameter*: `--producers [NAME,[NAME,...]]`
5. Histogram producer:
    - More versatile than other TAFs as it allows defining some late event data adjustments and event weight, as well as controls the creation, filling and post-processing of histograms before they are saved to disk
    - *Examples*: calcalation of event weights, plus the usual histogram filling procedure
    - *Quantity*: exactly one
    - *Output length*: does not apply, since histograms are created
    - *Parameter*: `--hist-producer NAME(cf_default)`

## Simple Example

This simple example shows a producer that adds a new column to the events chunk.
Here, it calculates the supercluster eta of electrons, based in the original eta and the delta to the supercluster.
It is obviously a contrived example, but it shows the basic concept of

- declaring which columns need to be read from disk via the `uses` set,
- declaring which columns are produced by the TAF and should be saved to disk via the `produces` set, and
- that the events chunk is never modified in place but potentially copied (**without** copying the underlying data though!).

```python
from columnflow.production import producer
from columnflow.util import maybe_import
from columnflow.columnar_util import set_ak_column

ak = maybe_import("awkward")

@producer(
    uses={"Electron.{pt,phi,eta,deltaEtaSC}"},
    produces={"Electron.superclusterEta"},
)
def electron_sc_eta(self, events: ak.Array, **kwargs) -> ak.Array:
    sc_eta = events.Electron.eta + events.Electron.deltaEtaSC
    events = set_ak_column(events, "Electron.superclusterEta", sc_eta)
    return events
```

TAF can be nested to reuse common functionality, i.e., one TAF can call other TAFs with all information about used and produced columns passed along.
For more information, see [columnflow building blocks](./building_blocks/index.rst).

## TAF Interface

The full interface can be described as a collection of functions that are invoked in specific places by tasks in columnflow, and that can be implemented by the user with very high granularity.

These functions (or *hooks*) are registered using the decorator syntax below.
However, as they are classes under the hood, you can also define them as such.

Upon creation, the `analysis_inst`, `config_inst` and `dataset_inst` objects of a task are passed as members to each TAF instance and form **their state**.
They can be accessed as usual to retrieve information about the context in which they are called (e.g. for a specific config, MC or real data, etc.).

Hooks are called thereafter in various places:

- `pre_init_func(self)`: Called before dependency creation, can be used to control `deps_kwargs` that are passed to dependent TAFs.
- `init_func(self)`: Initialization of the TAF, can control dynamic registration of used and produced columns or dependencies, as well as systemtic shifts.
- `skip_func(self)`: Whether this TAF should be skipped altogether.
- `post_init_func(self, task)`: Can control dynamic registration of used and produced columns, but no additional TAF dependencies.
- `requires_func(self, task, reqs)`: Allows adding extra task requirements to `reqs` that will be resolved before the tasks commences.
- `setup_func(self, task, reqs, inputs, reader_targets)`: Allows setting up objects needed for actual function calls, receiving requirements defined in `requires_func` as well as their produced outputs via `inputs`;
- `call_func(self, events, task, **kwargs)`: Actual events chunk processing, can be called multiple times for different chunks.
- `teardown_func(self, task)`: Called after processing, but potentially before chunk merging, allows reducing memory footprint by eagerly freeing up resources.

## Full example

The following example is not implementing a fake TAF, but extends the example above to show how different hooks can be registered and used.
Note that the decorators miss the `_func` suffix, but they register and bind methods internally that **contain** this suffix.

```python
# same as above
@producer(
    uses={"Electron.{pt,phi,eta,deltaEtaSC}"},
    produces={"Electron.superclusterEta"},
)
def electron_sc_eta(self, events: ak.Array, **kwargs) -> ak.Array:
    ...
    return events

# custom pre-init
@electron_sc_eta.pre_init
def electron_sc_eta_pre_init(self: Producer) -> None:
    ...

# custom init
@electron_sc_eta.init
def electron_sc_eta_init(self: Producer) -> None:
    # e.g. update uses/produces
    self.uses.add(...)

# custom post-init
@electron_sc_eta.post_init
def electron_sc_eta_post_init(self: Producer, task: law.Task) -> None:
    # can access task!
    ...

# custom requires
@electron_sc_eta.requires
def electron_sc_eta_requires(self: Producer, task: law.Task, reqs: dict[str, Any]) -> None:
    # add extra requirements to reqs
    ...
```
