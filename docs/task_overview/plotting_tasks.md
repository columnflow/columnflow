# Plotting tasks

```{mermaid}
:zoom:
:align: center
:caption: Class diagram of all plotting tasks
classDiagram
    PlotBase <|-- PlotBase1D
    PlotBase <|-- PlotBase2D
    PlotBase <|-- VariablePlotSettingMixin
    PlotBase <|-- ProcessPlotSettingMixin

    PlotBase1D <|-- PlotVariables1D
    PlotBase1D <|-- PlotShiftedVariables1D

    VariablePlotSettingMixin <|-- PlotVariablesBase
    ProcessPlotSettingMixin <|-- PlotVariablesBase
    PlotVariablesBase <|-- PlotVariablesBaseSingleShift
    PlotVariablesBaseSingleShift <|-- PlotVariables1D
    PlotVariablesBaseSingleShift <|-- PlotVariables2D

    PlotBase2D <|-- PlotVariables2D
    PlotVariables2D <|-- PlotVariablesPerProcess2D
    PlotVariablesBase <|-- PlotVariablesBaseMultiShifts
    PlotVariablesBaseMultiShifts <|-- PlotShiftedVariables1D
    PlotShiftedVariables1D <|-- PlotShiftedVariablesPerProcess1D

    class PlotBase{
        plot_function: luigi.Parameter
        file_types: law.CSVParameter
        plot_suffix: luigi.Parameter
        view_cmd: luigi.Parameter
        general_settings: SettingParameter
        skip_legend: law.OptionalBoolParameter
        cms_label: luigi.Parameter
    }

    class PlotBase1D{
        skip_ratio: law.OptionalBoolParameter
        density: law.OptionalBoolParameter
        yscale: luigi.ChoiceParameter
        shape_norm: law.OptionalBoolParameter
        hide_errors: law.OptionalBoolParameter
    }

    class PlotBase2D{
        zscale: luigi.ChoiceParameter
        density: law.OptionalBoolParameter
        shape_norm: law.OptionalBoolParameter
        colormap: luigi.Parameter
        zlim: law.CSVParameter
        extremes: luigi.ChoiceParameter
        extreme_colors: law.CSVParameter
    }

    class ProcessPlotSettingMixin{
        process_settings: MultiSettingsParameter
    }

    class VariablePlotSettingMixin{
        variable_settings: MultiSettingsParameter
    }

    class PlotVariablesBaseMultiShifts{
        legend: luigi.Parameter
    }
```

The following tasks are dedicated to plotting.
For more information, check out the [plotting user guide](../user_guide/plotting.md)

(PlotVariablesTasks)=

- ```PlotVariables*```, ```PlotShiftedVariables*``` (e.g. {py:class}`~columnflow.tasks.plotting.PlotVariables1D`, {py:class}`~columnflow.tasks.plotting.PlotVariables2D`, {py:class}`~columnflow.tasks.plotting.PlotShiftedVariables1D`):
Tasks to plot the histograms created by {py:class}`~columnflow.tasks.histograms.CreateHistograms` using the python package [matplotlib](https://matplotlib.org/) with [mplhep](https://mplhep.readthedocs.io/en/latest/) style.
Several plot types are possible, including plots of variables for different physical processes or plots of variables for a single physical process but different shifts (e.g. jet-energy correction variations).
The argument ```--variables``` followed by the name of the variables defined in the analysis config, separated by a comma, is needed for these tasks to run.
It is also possible to replace the ```--datasets``` argument for these tasks by the ```--processes``` argument followed by the name of the physical processes to be plotted, as defined in the analysis config.
For the ```PlotShiftedVariables*``` plots, the argument ```shift-sources``` is needed and replaces the argument ```shift```.
The output format for these plots can be given with the ```--file-types``` argument.
It is possible to set a default for the variables in the analysis config.

- ```PlotCutflow*``` (e.g. {py:class}`~columnflow.tasks.cutflow.PlotCutflow`, {py:class}`~columnflow.tasks.cutflow.PlotCutflowVariables1D`):
Tasks to plot the histograms created by {py:class}`~columnflow.tasks.cutflow.CreateCutflowHistograms`.
The {py:class}`~columnflow.tasks.cutflow.PlotCutflowVariables1D` are plotted in a similar way to the [PlotVariables*](PlotVariablesTasks) tasks.
The difference is that these plots show the selection yields of the different selection steps defined in {py:class}`~columnflow.tasks.selection.SelectEvents` instead of only after the {py:class}`~columnflow.tasks.reduction.ReduceEvents` procedure.
The selection steps to be shown can be chosen with the ```--selector-steps``` argument.
Without further argument, the outputs are as much plots as the number of selector steps given.
On the other hand, the PlotCutflow task gives a single histograms containing only the total event yields for each selection step given.
