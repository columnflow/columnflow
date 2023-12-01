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

In order to create histograms out of the processed datasets, columnflow uses
{external+order:py:class}`order.variable.Variable`s. These
Variables need to be added to the config using the
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

In histogramming tasks such as
{py:class}`~columnflow.tasks.histograms.CreateHistograms`, one histogram is created per Variable
given via the ```--variables``` argument, accessing information from columns based on
the `expression` of the Variable and storing them in histograms with binning defined
via the `binning` argument of the Variable.

The list of possible keyword arguments can be found in
Variable. The values in the ```expression``` argument can
be either a one-dimensional or a more dimensional array. In this second case the information is
flattened before plotting. It is to be mentioned that
{py:attr}`~columnflow.columnar_util.EMPTY_FLOAT` is a columnflow internal null value and
corresponds to the value ```-9999.0```.
