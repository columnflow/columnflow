# v0.2 → v0.3 Transition

This document describes changes on columnflow introduced in version 0.3.0 that may affect existing code as well as already created output files.
These changes were made in a refactoring campaign (see [release v0.3](https://github.com/columnflow/columnflow/releases/tag/v0.3.0) for more info) that was necessary to generalize some decisions made in an earlier stage of the project, and to ultimately support more analysis use cases that require a high degree of flexibility in many aspects of the framework.

The changes are grouped into the following categories:

- [Restructured Task Array Functions](#restructured-task-array-functions)
- [Multi-config Tasks](#multi-config-tasks)
- [Reducers](#reducers)
- [Histogram Producers](#histogram-producers)
- [Inference Model Updates](#inference-model-updates)
- [Changed Plotting Task Names](#changed-plotting-task-names)

## Restructured Task Array Functions

- [Task array functions (TAFs)](./task_array_functions.md)
- removed access to `task`, `global_shift_inst`, `local_shift_inst`.

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
