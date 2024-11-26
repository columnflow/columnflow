# Getting started

## Tutorial 0: Read the columnflow user guide

A general overview of the structure of columnflow can be found [here](https://github.com/GhentAnalysis/columnflow/blob/master/docs/user_guide/structure.md). The overview starts with a general introduction, followed by a description of the various tasks implemented in columnflow and ending with an introduction on how to configure your analysis on top of columnflow with the analysis and config object from the order package.

After reading the overview, check the documentation on the various building blocks of columnflow.

### [Config Objects:](https://github.com/GhentAnalysis/columnflow/blob/master/docs/user_guide/building_blocks/config_objects.md)

TODO: introduction text

### [Categories](https://github.com/GhentAnalysis/columnflow/blob/master/docs/user_guide/building_blocks/config_objects.md)

In columnflow, there are many tools to create a complex and flexible categorization of all analysed events. Generally, this categorization can be layered. We refer to the smallest building block of these layers as leaf categories, which can subsequently be either run individually or combined into more complex categories. This guide presents how to implement a set of categories in columnflow and shows how to use the resulting categories via the `columnflow.tasks.yields.CreateYieldTable` task.

### [Calibrators:]((https://github.com/GhentAnalysis/columnflow/blob/master/docs/user_guide/building_blocks/calibrators.md))

TODO

### [Selectors:](https://github.com/GhentAnalysis/columnflow/blob/master/docs/user_guide/building_blocks/selectors.md)

In columnflow, selections are defined through the `columnflow.selection.Selector` class. This class allows for arbitrary selection criteria on event level as well as object level using masks. The results of the selection (which events or objects are to be conserved) are saved in an instance of the `columnflow.selection.SelectionResult` class. Similar to `columnflow.production.Producers`, it is possible to create new columns in Selectors. In the original columnflow setup, Selectors are being run in the `columnflow.tasks.selection.SelectEvents` task.

### [Producers:](https://github.com/GhentAnalysis/columnflow/blob/master/docs/user_guide/building_blocks/producers.md)

In columnflow, event/object based information (weights, properties, ...) is stored in columns.
The creation of new columns is managed by instances of the `~columnflow.production.Producer` class.
Producers can be called in other classes (e.g. `~columnflow.calibration.Calibrator` and `~columnflow.selection.Selector`), or directly through the `~columnflow.tasks.production.ProduceColumns` task.
It is also possible to create new columns directly within Calibrators and Selectors, without using instances of the Producer class, but the process is the same as for the Producer class.
Therefore, the Producer class, which main purpose is the creation of new columns, will be used to describe the process.
The new columns are saved in a parquet file.
If the column were created before the `~columnflow.tasks.reduction.ReduceEvents` task and are still needed afterwards, they should be included in the ```keep_columns``` auxiliary of the config, as they would otherwise not be saved in the output file of the task.
If the columns are created further down the task tree, e.g. in ProduceColumns, they will be stored in another parquet file, namely as the output of the corresponding task, but these parquet files will be loaded similarly to the outputs from ReduceEvents.
It should be mentioned that the parquet files for tasks after ProduceColumns are opened in the following order: first the parquet file from ReduceEvents, then the different parquet files from the different Producers called with the `--producers` argument in the same order as they are given on the command line.
Therefore, in the case of several columns with the exact same name in different parquet files (e.g. a new column `Jet.pt` was created in some producer used in ProduceColumns after the creation of the reduced `Jet.pt` column in ReduceEvents), the tasks after ProduceColumns will open all the parquet file and overwrite the values in this column with the values from the last opened parquet file, according to the previously mentioned ordering.

## Tutorial 1: Gen-level plots of a single process

This example will show how to make gen-level plots using the columnflow framework. The example uses a reduced monte carlo sample of $t\overline{t}$ dilepton production for the 2018UL campaign.

The columnflow framework uses the cutflow features to plot gen-level distributions without having to go trough the complete columnflow pipeline. Instead the the pipeline is reduced to the following:

1. Run the `CalibrateEvents` -> `SelectEvents` pipeline for two files of the dataset using the default calibrators and default selector (enter the command below and 'tab-tab' to see all arguments or add `--help` for help). Features that are not straightforward must first be produced in production/cutflow_features.py which is ran during the selection task before being able to plot them. This example will use the gen-level top-quark $p_T$ as a feature which is unique to Monte Carlo and will thus be only produced for Monte Carlo datasets as specified in the initialization. To run the `CalibrateEvents` -> `SelectEvents` pipeline execute the following command:

    ```bash
    law run cf.SelectEvents --version dev1 --config l18
    ```

    You can verify that what you just ran succeeded by adding `--print-status -1` (-1 = fully recursive) to the previous command.

    ```bash
    law run cf.SelectEvents --version dev1 --config l18 --print-status -1
    ```

2. Create the gen-level top-quark $p_T$ distribution plot for the $t\overline{t}$ dilepton dataset The top-quark $p_T$ variable is defined in [config/variables.py](https://gitlab.cern.ch/ghentanalysis/columnflowanalysis/ttz/-/blob/master/ttz/config/variables.py?ref_type=heads#L328).

    ```bash
    law run cf.PlotCutflowVariables1D --version dev1 --config l18 --datasets "tt_dl_powheg" --processes "tt" --variables "genTop_pt" --skip-ratio --categories "incl"
    ```

    To see where the plots are saved, you can add `--print-output 0` to the previous command.
3. The same can be done on the full dataset with all files by using config `c18` instead of `l18`. Running over all files locally will take a long time and it is recommended to use `htcondor` to submit jobs that run over all files in parallel. This can be done by including `--workflow htcondor`, for more options on running with `htcondor`, check the `--help` argument or documentation.

## Tutorial 2: n-jets distribution of multiple processes
