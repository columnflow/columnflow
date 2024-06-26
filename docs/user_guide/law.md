# Law introduction

This analysis tool uses [law](https://github.com/riga/law) for the workflow orchestration.
Therefore, a short introduction to the most essential functions of law you should be aware of when using this tool are provided here.
More informations are available for example in the "[Examples](https://github.com/riga/law#examples)" section of this [Github repository](https://github.com/riga/law).
This section can be ignored if you are already familiar with law.

## Tasks and parameters

In [law](https://github.com/riga/law), tasks are objects, which, as the name implies, should be implemented in such a way, that they fulfill a specific task in the workflow.
They are defined and separated by purpose and may have dependencies to each other.
As an example, columnflow defines a task for the creation of histograms and a different task to make a plot of these histograms.
The plotting task requires the histogram task to have already run, in order to have data to plot.
This is internally checked by the presence or absence of the corresponding output file from the required task.
If the required file is not present, the required task will be automatically started with the corresponding parameters before the called task.

The full task tree of general tasks already implemented in columnflow can be seen in [this wikipage](https://github.com/columnflow/columnflow/wiki#default-task-graph).

A task is run with the command ```law run``` followed by the name of the task.
A version, given by the argument ```--version```, followed by the name of the version, is required.

In law, the intermediate results (=the outputs of the different tasks) are saved locally in the corresponding directory (given in the setup, the arguments to run the task are also used for the path).
The name of the version also appears in the path and should therefore be selected to match your purpose, for example ```--version selection_with_gen_matching```.
As the intermediate results are used to decide if a task further down the task tree should run or not, the version argument allows for bookkeeping and storage of several intermediate results, where all other parameters would be equivalent.

Tasks in law are organized as a graph with dependencies.
Therefore a "depth" for the different required tasks exists, depending on which task required which other task.
In order to see the different required tasks for a single task, you might use the argument ```--print-status -1```, which will show all required tasks and the existence or absence of their output for the given input parameters up to depth "-1", hence the deepest one.
The called task with ```law run``` will have depth 0.
You might check the output path of a task with the argument ```--print-output```, followed by the depth of the task.
If you want a finished task to be run anew without changing the version (e.g. do a new histogram with different binning), you might remove the previous outputs with the ```--remove-output``` argument, followed by the depth up to which to remove the outputs.
There are three removal modes:

- ```a``` (all: remove all outputs of the different tasks up to the given depth),
- ```i``` (interactive: prompt a selection of the tasks to remove up to the given depth.
For each task which output you decide to remove, you will be asked how you want to remove the potentially multiple outputs)
- ```d``` (dry: show which files might be deleted with the same selection options, but do not remove the outputs).

The ```--remove-output``` argument does not allow the depth "-1", check the task tree with ```--print-output``` before selecting the depth you want.
The removal mode can be already selected in the command, e.g. with ```--remove-output 1,a``` (remove all outputs up to depth 1).

Once the output has been removed, it is possible to run the task again.
It is also possible to rerun the task in the same command as the removal by adding the ```y``` argument at the end.
Therefore, removing all outputs of a selected task (but not its dependencies) and running it again at once would correspond to the following command:

```shell
law run name_of_the_task --version name_of_the_version --remove-output 0,a,y
```

An example command to see the location of the output file after running a 1D plot of a variable with columnflow using only law functions and the default arguments for the tasks would be:

```shell
law run cf.PlotVariables1D --version test_plot --print-output 0
```

(law_config_section)=

## Law Config

The law config file (`law.cfg`) is a file that is used to define the general parameters needed for law to run the created workflow. In the case of columnflow, the workflow corresponds to the different tasks of the analysis. Examples of the required parameters are the location of the data, the location of the output, the files to be recognized by columnflow as part of the analysis, some default values etc.

The full documentation of the law config file can be found in [the law documentation](https://law.readthedocs.io/en/latest/config.html).

To start your analysis, do not forget to use the already existing analysis template in the `analysis_templates/cms_minimal` Git directory and [its law.cfg file](https://github.com/columnflow/columnflow/blob/master/analysis_templates/cms_minimal/law.cfg).
The analysis template describes the usage of the different parameters in the `law.cfg` file.

As an important reminder for new users, adding new files to the analysis should be accompanied by a new entry in the `law.cfg` file, more specifically the `[analysis]` part of the law config, according to the role of this file (define a Selector, or a Producer, etc.).
For example, adding a new file called `jet.py` in your analysis in the `selection` directory containing a Selector for the Jets would require to change the law.cfg file from the analysis template from

```python
selection_modules: columnflow.selection.{empty}, columnflow.selection.cms.{json_filter,met_filters}, __cf_module_name__.selection.example
```

to

```python
selection_modules: columnflow.selection.{empty}, columnflow.selection.cms.{json_filter,met_filters}, __cf_module_name__.selection.{example,jet}
```

Note: Do NOT add whitespaces after the commas between the curly brackets, as your file will not be recognized by columnflow in this case.

You can also check other examples of law configs with additional parameters in the different analyses using columnflow, like the [hh2bbtautau analysis](https://github.com/uhh-cms/hh2bbtautau).

A more thorough explanation of the selection of the storage locations and of specific versioning examples can be found in the [best practices section](best_practices.md).

An example of a law.cfg file for users without grid certificate can be found in {ref}`the custom law.cfg section <custom_law_config>` of this documentation.

## Running remote

Law defines several possibilities to run the tasks on remote machines, e.g. htcondor or slurm. Several parameters for these remote machines can be defined in the law config file.
The `--workflow` argument of the `law run` command can be used to define the remote machine to run the task on.

An example of such a submission to a remote machine would be:

```shell
law run cf.PlotVariables1D --version test_plot --processes tt --variables n_jet --workflow htcondor
```

This command would submit the task `cf.PlotVariables1D` for the variable `n_jet` and with the version `test_plot` for the datasets corresponding to the process `tt` to the htcondor remote machine.
