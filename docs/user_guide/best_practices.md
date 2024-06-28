# Best practices

## Selecting output locations

Tasks usually define their output targets using the generic `self.target()` method, defined by the {py:class}`~columnflow.tasks.framework.base.AnalysisTask` base class.
Depending on specific choices made in the `law.cfg` file (see below), this methods then either calls {py:meth}`~columnflow.tasks.framework.base.AnalysisTask.local_target` or {py:meth}`~columnflow.tasks.framework.base.AnalysisTask.wlcg_target` with appropriate settings, to create either a target acessible on the local file system, or a remote target on a non-local storage system.

The lookup of output locations can be defined in the `law.cfg` file.

```ini
[outputs]

TASK_IDENTIFIER: LOCATION
```

`LOCATION` refers to the output location and can take comma-separated values.

- `local` refers to the default local file system.
- `local, LOCAL_FS_NAME` refers to a specific local file system named `LOCAL_FS_NAME` that should be defined in the `law.cfg` file.
For convenience, if no file system with that name was defined, `LOCAL_FS_NAME` is interpreted as the base path of such a file system.
- `wlcg` refers to the default remote storage system.
- `wlcg, WLCG_FS_NAME` refers to a specific remote storage system named `WLCG_FS_NAME` that should be defined in the `law.cfg` file.

`TASK_IDENTIFIER` identifies the task the location should apply to.
It can be a simple task family such as `cf.CalibrateEvents`, but for larger analyses a more fine grained selection is required.
For this purpose, `TASK_IDENTIFIER` can be a `__`-separated sequence of so-called lookup keys, e.g.

```ini
[outputs]

run3_23__cf.CalibrateEvents__nominal: wlcg, wlcg_fs_run3_23
```

Here, three keys are defined, making use of the config name, the task family, and the name of a systematic shift.
The exact selection of possible keys and their resolution order is defined by the task itself in {py:meth}:`~columnflow.tasks.framework.base.AnalysisTask.get_config_lookup_keys` (and subclasses).
Most tasks, however, define their lookup keys as:

1. analysis name
2. config name
3. task family
4. dataset name
5. shift name

When defining `TASK_IDENTIFIER`'s, not all keys need to be specified, and patterns or regular expressions (`^EXPR$`) can be used.
The definition order is **important** as the first matching definition is used.
This way, output locations are highly customizable.

```ini
[outputs]

# store all run3 outputs on a specific fs, and all other outputs locally
run3_*__cf.CalibrateEvents: wlcg, wlcg_fs_run3
cf.CalibrateEvents: local
```

## Controlling versions of upstream tasks

Just as for the definition of output locations, pinning versions of produced output targets is an important feature to ensure reproducibility and reusability of previous, intermediate analysis results.
In general, versions are defined by passing a `--version VALUE` parameter on the command line to the task to run, which then passes this value upstream to its dependencies.

There are two ways of controlling the version of upstream tasks if the standard way of using the same value everywhere is not desired.
Their order of priority is:

1. Task family specific parameters
2. Pinned versions in the analysis config or `law.cfg` file
3. `--version` command line parameter

### Task family specific parameters

On the command line, versions of upstream tasks can be controlled by `--cf.UPSTREAMTASK-version OTHER_VALUE` if desired.
For example, if a task `B` that depends on task `A` is started from the command line via

```bash
law run B --version v2 --A-version v1
```

the instantiation of task class `A` in the definition of `requires()` in task class `B` will prefer version `v1` over `v2`.
As a result, all tasks upsstream of `A` will, as well, be instantiated with version `v1`.
This is particularly useful if a certain part of an analysis workflow is to be rerun, while keeping upstream parts fixed at a specific version.

### Pinned versions in the analysis config or `law.cfg` file

Versions of tasks can be pinned through an auxiliary entry `config_inst.x.version` in the configuration object or `analysis_inst.x.version` in the analysis object itself, or the `[versions]` section in the `law.cfg` file.
The priority is defined along this order and the lookup is done in the same way as described above for output locations using a `TASK_IDENTIFIER`.

Consider the following two examples for defining versions, one via auxiliary config entries and the other via the `law.cfg` file:

```python
cfg.x.versions = {
    "run3_*": {
        "cf.CalibrateEvents": "v2",
    },
    "cf.CalibrateEvents": "v1",
}
```

```ini
[versions]

run3_*__cf.CalibrateEvents: v2
cf.CalibrateEvents: v1
```

They are **equivalent** since the `__`-separated `TASK_IDENTIFIER`'s in the `law.cfg` are internallly converted to the same nested dictionary structure.

As described above, the exact selection of possible keys and their resolution order is defined in {py:meth}:`~columnflow.tasks.framework.base.AnalysisTask.get_config_lookup_keys` (and subclasses), not all keys need to be specified when defining versions, and they are allowed to be patterns or regular expressions (`^EXPR$`).

## Columnflow convenience tools

- Columnflow defines {py:attr}`~columnflow.columnar_util.EMPTY_FLOAT`, a float variable containing the value `-99999.0`.
This variable is typically used to replace null values in awkward arrays.

- In many cases, one wants to access an object that does not exist for every event (e.g. accessing the transverse momentum of the 3rd jet `events.Jet[:, 2].pt`, even though some events may only contain two jets).
In that case, the {py:class}`~columnflow.columnar_util.Route` class and its {py:meth}`~columnflow.columnar_util.Route.apply` function can be used to access this object by replacing missing values with the given value, e.g. `jet3_pt = Route("Jet.pt[:, 2]").apply(events, null_value=EMPTY_FLOAT)`.

- Columnflow allows the use of some fields out of the `events` array, like the `Jet` field, as a Lorentz vector to apply operations on.
For this purpose, you might use the {py:func}`~columnflow.production.util.attach_coffea_behavior` function.
This function can be applied on the `events` array using

```python
events = self[attach_coffea_behavior](events, **kwargs)
```

If the name of the field does not correspond to a standard coffea field name, e.g. "BtaggedJets", which should provide the same behaviour as a normal jet, the behaviour can still be set, using

```python
collections = {x: {"type_name": "Jet"} for x in ["BtaggedJets"]}
events = self[attach_coffea_behavior](events, collections=collections, **kwargs)
```

- shortcuts / winning strategies / walktrough guides e.g. pilot parameter: TODO

- config utils: TODO

- categorization
  - mutually exclusive leaf categories
  - TODO

## General advices

- When storage space is a limiting factor, it is good practice to produce and store (if possible) columns only after the reduction, using the {py:class}`~columnflow.tasks.production.ProduceColumns` task.

## Using python scripts removed from the standard workflow

- Use a particular cf_sandbox for a python script not implemented in the columnflow workflow: Write `cf_sandbox venv_columnar_dev bash` on the command line.

- Call tasks objects in a python script removed from the standard workflow: An imported task can be run through the `law_run()` command, its output can be accessed through the "output" function of the task.
An example is given in the following code snippet.

```python
from columnflow.tasks.selection import SelectEvents

# run some task
task = SelectEvents(version="v1", dataset="tt_sl_powheg", walltime="1h")
task.law_run()

# do something with the output
output = task.output()["collection"]
```
