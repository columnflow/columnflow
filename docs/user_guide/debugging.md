# Debugging

(soon to be changed:)

If you get an error while using columnflow and look at the error stack, you will probably see two errors:

1) the actual error and where it happened in the code (standard python error), this is probably the error you are interested in.
2) a sandbox error that you can ignore in most of the cases, as it does not correspond to your problem, it only says in which sandbox it happened.

In this section, debugging tools already implemented in columnflow to inspect the intermediate results of tasks will be presented.

## Debugging outputs

### Debugging outputs of supported extensions (ROOT, Parquet, JSON and Pickle)

columnflow comes equipped with the command `cf_inspect`, which is available in the columnflow environment after sourcing the `setup.sh` file.
The command takes the pathes of one or more files (space seperated) you want to inspect as arguments and enters an IPython shell in a development sandbox, where analysis tools and packages (awkward, coffea etc.) as well as the columnflow API are available.
The input files are loaded into memory and can be accessed via the variables `objetcs`.
Make sure to avoid pathes, which are accesed via file protocol (e.g. `file://`, `davs://`, etc.), as the file loader may not support this.

:::{dropdown} Where to find the path of my outputs?

After running a task, the path of the output can be yielded by appending  `--print-output <index>` to the command, where `<index>` is the task index in the workflow tree.
For the main task of the command the index is 0.
For example, for the output path of the Selection task, the command would be

```shell
law run cf.SelectEvents --version v1 --{other options} --print-output 0
```

:::

### Debugging histograms

Histograms can be inspected with the `cf_inspect` command as well.
However columnflow provides a more specialized tool for this purpose in the form of task.
This can be the better choice if you want to inspect multiple histograms of multiple variables, since this can offer a more structured way to do so.
The task can be accessed by calling `law run cf.InspectHistograms --{options}` for the nominal shift or `law run cf.InspectShiftedHistograms --shift-sources {source} --{options}` for nominal and shifted histograms respectively.
In both cases the `{options}` are the same parameters, which would be passed to create the histogram in the task `law run cf.CreateHistograms --{options}`.
The task also enters an IPython shell in a development sandbox, where the following variables are available:

- `self`: the task instance, from which the config, analysis and datasets instances can be accessed
- `hists`: a dictionary of the histograms, where the keys are the variables names and the values are the histograms
- `dataset`: name of the dataset
- `variable`: name of the last histogrammed variable in the `--variables` option
- `h_in`: histogram of `variable`

An advatage of this debugging methode comapred to the `cf_inspect` command is that the histogram files are not required to exist, since the task (like all other tasks) will set up the workflow to produce missing requirements.

## FAQ

### Troubleshooting:

- "I have changed something in the code and called the corresponding `law run` bash command, but the task isn't starting/the task started is further down the task tree."

A: Do not forget to remove the corresponding intermediate output(s), for example with `--remove-output` (see {doc}`law`), or start a new version with `--version` if you do explicitely want to conserve the previous output before the change in the code.

- "Where do I find the outputs of my tasks?"

A: When you run "source setup.sh {name_of_the_setup}" in columnflow for the first time, you choose the storage locations.
You can find the storage locations again by opening the ".setups/{name_of_the_setup}.sh" file in the analysis repository.
You may also use law functions to ease the search, namely with the `--print-output` command, see in the {doc}`law` section.

- "I get an error telling me that some columns could not be found/produced.
What can I do?"

A: You have declared some columns in the uses or produces set of your Selectors, Producers or Calibrators that are not available.
Maybe you did not produce them before, or they have been removed, for example in the ReduceEvents task.
You should start by verifying that these columns are necessary for you and remove them if not.
If you need them, and this is a "uses" problem:
check if you have created the column in a previous task and declared the column in "keep_columns" in your analysis config if you are in a task after "ReduceEvents".
Do not forget to remove the outputs to start the corresponding tasks again if you have made changes in them after running them.
If this a "produce" error, verify that the column has been produced in your script.
If you still cannot pinpoint the error, use the debugging tools to check your intermediate outputs or an IPython shell in your script.

- "I get an error during the setup telling me that some packages/scripts are not available"

A: When you run "source setup.sh {name_of_the_setup}", columnflow internally checks all the scripts declared in the law.cfg file.
It may lead to two problems:

1) if what is not available is an external package that you try to import, the issue may come from the fact that the package you try to import is only available in a specific cf_sandbox (which is not the default sandbox).
For these, simply use the "maybe_import" function from columnflow.util, in a similar way to {doc}`building_blocks/producers`.
2) If what is not available is a local script in columnflow, you might need to declare it in the law.cfg BEFORE the script causing the error.
For example, if the error is in some calibrator in "calibration/cms/new_calibrator.py" which is using some calibrator in "calibration/cms/jets.py", you might want to declare them in the law.cfg in the following way:
"calibration_modules: columnflow.calibration.cms.{jets,new_calibrator}" (and NOT "calibration_modules: columnflow.calibration.cms.{new_calibrator,jets}").
