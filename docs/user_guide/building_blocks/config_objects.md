# Config Objects

Columnflow uses a config file for each analysis, saving the variables specific to the analysis. The
[order](https://github.com/riga/order) library is used for the conservation and use of the
metavariables. In this section, the most important config objects to be defined are presented.

## Datasets

TODO

## Processes

TODO

## Shifts

TODO

## "Variable" creation

In order to plot something out of the processed datasets, columnflow uses
{external+order:py:class}`order.variable.Variable`s. It is therefore not enough to create a new
column in a {py:class}`~columnflow.production.Producer` for the plotting tasks, it must also be
translated in a {external+order:py:class}`order.variable.Variable`, which name can be given
to the argument ```--variables``` for the plotting/histograming task. These
{external+order:py:class}`order.variable.Variable`s need to be added to the config using the
function {external+order:py:meth}`order.config.Config.add_variable`. The standard syntax is as
follows:
```python
config.add_variable(
    name=variable_name,  # this is to be given to the "--variables" argument for the plotting task
    expression=content_of_the_variable,
    null_value=value_to_be_given_if_content_not_available_for_event,
    binning=(bins, lower_edge, upper_edge),
    unit=unit_of_the_variable_if_any,
    x_title=x_title_of_histogram_when_plotted,
)
```

An example with the transverse momentum of the first jet would be:
```python
config.add_variable(
    name="jet1_pt",
    expression="Jet.pt[:,0]",
    null_value=EMPTY_FLOAT,
    binning=(40, 0.0, 400.0),
    unit="GeV",
    x_title=r"Jet 1 $p_{T}$",
)
```

The list of possible keyword arguments can be found in
{external+order:py:class}`order.variable.Variable`. The values in ```expression``` can be either a
one-dimensional or a more dimensional array. In this second case the information is flattened before
plotting. It is to be mentioned that {py:attribute}`~columnflow.columnar_util.EMPTY_FLOAT` is a
columnflow internal null value and corresponds to the value ```-9999```.
