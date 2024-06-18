# Task Overview

This page gives a small overview of the current tasks available by default in columnflow.

In general (at least for people working in the CMS experiment),
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
task to run.

- {py:class}`~columnflow.tasks.calibration.CalibrateEvents`: Task to implement corrections to be
    applied on the datasets, e.g. jet-energy corrections. This task uses objects of the
    {py:class}`~columnflow.calibration.Calibrator` class to apply the calibration. The argument
    ```--calibrator``` followed by the name of the Calibrator
    object to be run is needed for this task to run. A default value for this argument can be set in
    the analysis config. Similarly, the ```--shift``` argument can be given, in order to choose which
    corrections are to be used, e.g. which variation (up, down, nominal) of the jet-energy corrections
    are to be used.

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

- {py:class}`~columnflow.tasks.histograms.CreateHistograms`: Task to create histograms with the
    python package [Hist](https://hist.readthedocs.io/en/latest/) which can be used by the tasks below
    in the task graph. From this task on, the ```--producer``` argument is replaced by
    ```--producers```. The histograms are saved in a pickle file.

- {py:class}`~columnflow.tasks.cms.inference.CreateDatacards`: TODO

- ```Merge``` tasks (e.g. {py:class}`~columnflow.tasks.reduction.MergeReducedEvents`,
{py:class}`~columnflow.tasks.histograms.MergeHistograms`): Tasks to merge the local outputs from
the various occurences of the corresponding tasks.

There are also CMS-specialized tasks, like
{py:class}`~columnflow.tasks.cms.external.CreatePileUpWeights`, which are described in the
{ref}`CMS specializations section <cms_specializations_section>`. As a note, the CreatePileUpWeights task is
interesting from a workflow point of view as it is an example of a task required through an object
of the {py:class}`~columnflow.production.Producer` class. This behaviour can be observed in the
{py:meth}`~columnflow.production.cms.pileup.pu_weight_requires` method.

TODO: maybe interesting to have examples e.g. for the usage of the
parameters for the 2d plots. Maybe in the example section, or after the next subsection, such that
all parameters are explained? If so, at least to be mentioned here.
