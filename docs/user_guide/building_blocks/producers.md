# Production of columns

## Introduction

In columnflow, event/object based information (weights, properties, ...) is stored in columns.
The creation of new columns is managed by instances of the {py:class}`~columnflow.production.Producer` class.
Producers can be called in other classes (e.g. {py:class}`~columnflow.calibration.Calibrator` and {py:class}`~columnflow.selection.Selector`), or directly through the {py:class}`~columnflow.tasks.production.ProduceColumns` task.
It is also possible to create new columns directly within Calibrators and Selectors, without using instances of the Producer class, but the process is the same as for the Producer class.
Therefore, the Producer class, which main purpose is the creation of new columns, will be used to describe the process.
The new columns are saved in a parquet file.
If the column were created before the {py:class}`~columnflow.tasks.reduction.ReduceEvents` task and are still needed afterwards, they should be included in the ```keep_columns``` auxiliary of the config, as they would otherwise not be saved in the output file of the task.
If the columns are created further down the task tree, e.g. in ProduceColumns, they will be stored in another parquet file, namely as the output of the corresponding task, but these parquet files will be loaded similarly to the outputs from ReduceEvents.
It should be mentioned that the parquet files for tasks after ProduceColumns are opened in the following order: first the parquet file from ReduceEvents, then the different parquet files from the different Producers called with the `--producers` argument in the same order as they are given on the command line.
Therefore, in the case of several columns with the exact same name in different parquet files (e.g. a new column `Jet.pt` was created in some producer used in ProduceColumns after the creation of the reduced `Jet.pt` column in ReduceEvents), the tasks after ProduceColumns will open all the parquet file and overwrite the values in this column with the values from the last opened parquet file, according to the previously mentioned ordering.

## Usage

To create new columns, the {py:class}`~columnflow.production.Producer` instance will need to load the columns needed for the production of the new columns from the dataset/parquet files.
This is given by the ```uses``` set of the instance of the Producer class.
Similarly, the newly created columns within the producer need to be declared in the ```produces``` set of the instance of the Producer class to be stored in the output parquet file.
The Producer instance only needs to return the ```events``` array with the additional columns.
New columns can be set using the function {py:func}`~columnflow.columnar_util.set_ak_column`.

An example of a Producer for the ```HT```variable is given below:

```python
# import the Producer class and the producer method
from columnflow.production import Producer, producer

# import two util functions needed below
from columnflow.util import maybe_import
from columnflow.columnar_util import set_ak_column

# maybe import awkward in case this Produser is actually run, this needs to be set as columnflow
# would else give an error during setup, as these packages are not in the default sandbox
np = maybe_import("numpy")
ak = maybe_import("awkward")


@producer(
    # declare which columns are needed for this Producer
    uses={"Jet.pt"},
    # declare which columns are created by this Producer
    produces={"HT"},
)
def features(self: Producer, events: ak.Array, **kwargs) -> ak.Array:
    # reconstruct HT and write in the events
    events = set_ak_column(events, "HT", ak.sum(events.Jet.pt, axis=1))

    return events
```

To call a Producer in an other Producer/Calibrator/Selector, the following expression might be used:

```python
events = self[producer_name](arguments_of_the_producer, **kwargs)
```

Hence, a complete example would be:

```python
# import the Producer class and the producer method
from columnflow.production import Producer, producer

# import two util functions needed below
from columnflow.util import maybe_import
from columnflow.columnar_util import set_ak_column

# maybe import awkward in case this Produser is actually run, this needs to be set as columnflow
# would else give an error during setup, as these packages are not in the default sandbox
np = maybe_import("numpy")
ak = maybe_import("awkward")


@producer(
    # declare which columns are needed for this Producer
    uses={"Jet.pt"},
    # declare which columns are created by this Producer
    produces={"HT"},
)
def HT_feature(self: Producer, events: ak.Array, **kwargs) -> ak.Array:
    # reconstruct HT and write in the events
    events = set_ak_column(events, "HT", ak.sum(events.Jet.pt, axis=1))

    return events


@producer(
    # declare which columns are needed for this Producer, if a Producer is given, takes all columns
    # declared in the corresponding field from the given Producer
    uses={HT_feature},
    # declare which columns are created by this Producer, if a Producer is given, takes all columns
    # declared in the corresponding field from the given Producer
    produces={HT_feature, "Jet.pt_squared"},
)
def all_features(self: Producer, events: ak.Array, **kwargs) -> ak.Array:
    # use other producer to create HT column
    events = self[HT_feature](events, **kwargs)

    # create for all jets a column containing the square of the transverse momentum
    events = set_ak_column(events, "Jet.pt_squared", events.Jet.pt * events.Jet.pt)

    return events
```

The ```all_features``` producer creates therefore two new columns, the ```HT``` column on event level, and the ```pt_squared``` column for each object of the ```Jet``` collection.

Notes:

- If you want to use an exposed Producer in a task call, and if this new Producer is created in a new file, you need to include this file in the ```law.cfg``` file under the ```production_modules``` argument.
A more detailed explanation of the law config file can be found in the {ref}`Law config section <law_config_section>`.

- When storage space is a limiting factor, it is good practice to produce and store (if possible) columns only after the reduction, using the ProduceColumns task.

- Other useful functions (e.g. for easier handling of columns) can be found in the {doc}`../best_practices` section of this documentation.

## ProduceColumns task

The {py:class}`~columnflow.tasks.production.ProduceColumns` task runs a specific instance of the {py:class}`~columnflow.production.Producer` class and stores the additional columns created in a parquet file.

While it is possible to see all the arguments and their explanation for this task using ```law run cf.ProduceColumns --help```, the only argument created specifically for this task is the ```--producer``` argument, through which the Producer to be used
can be chosen.

An example of how to run this task for an analysis with several datasets and configs is given below:

```shell
law run cf.ProduceColumns --version name_of_your_version \
                          --config name_of_your_config \
                          --producer name_of_the_producer \
                          --dataset name_of_the_dataset_to_be_run
```

It is to be mentioned that this task is run after the {py:class}`~columnflow.tasks.selection.SelectEvents` and {py:class}`~columnflow.tasks.calibration.CalibrateEvents` tasks and therefore uses the default arguments for the ```--calibrators``` and the ```--selector``` if not specified otherwise.
