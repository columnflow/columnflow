# Columnflow Structure


In this section, an overview to the structure of Columnflow is provided, starting with
a general introduction, followed by a description of the various tasks implemented in
columnflow and ending with an introduction on how to configure your analysis on top
of columnflow with the analysis and config object from the
[order](https://github.com/riga/order) package.


## General introduction

Columnflow is a fully orchestrated columnar analysis tool for HEP analyses with Python. The
workflow orchestration is managed by [law](https://github.com/riga/law) and the meta data and
configuration is managed by [order](https://github.com/riga/order). A short introduction to law is
given in the {doc}`law section <law>`. If you have never used law before, this section is highly
recommended as a few very convenient commands are presented there.


The data processing in columnflow is based on columns in [awkward arrays](https://awkward-array.org/doc/main/)
with [coffea](https://coffeateam.github.io/coffea/)-generated behaviour. Fields like "Jet" exist
too, they contain columns with the same first dimension (the parameters of the field, e.g. Jet.pt).
A few additional functions for simplified handling of columns were defined in
{py:mod}`~columnflow.columnar_util`.

As most of the information is conserved in the form of columns, it would be very inefficient
(and might not even fit in the memory) to use all columns and all events from a dataset at once for
each task. Therefore, in order to reduce the impact on the memory:
- a chunking of the datasets is implemented using [dask](https://www.dask.org/): not all events
from a dataset are inputed in a task at once, but only chunked in groups of events.
(100 000 events max per group is default as of 05.2023, default is set in the law.cfg file).
- the user needs to define for each {py:class}`~columnflow.production.Producer`,
{py:class}`~columnflow.calibration.Calibrator` and {py:class}`~columnflow.selection.Selector` which
columns are to be loaded (this happens by defining the ```uses``` set in the header of the
decorator of the class) and which new columns/fields are to be saved in parquet files after the
respective task (this happens by defining the ```produces``` set in the header of the decorator of
the class). The exact implementation for this feature is further detailed in
{doc}`building_blocks/selectors` and {doc}`building_blocks/producers`

## Tasks in columnflow

Tasks are [law](https://github.com/riga/law) objects allowing to control a workflow. All the tasks
presented below are proposed by columnflow and allow for a fairly complete analysis workflow.
However, as analyses are very diverse, it is possible that a specific analysis will need more
stearing options or even completely new tasks. Thankfully, columnflow is not a fixed structure and
you will be able to create new tasks in such a case, following the corresponding example in the
{doc}`examples` section of this documentation.
The full task tree of general columnflow tasks can be seen in
[this wikipage](https://github.com/columnflow/columnflow/wiki#default-task-graph). There are also
experiment-specific tasks which are not present in this graph. However, these are introduced in the
{ref}`CMS specializations section <cms_specializations_section>`.

Further informations about tasks in general and law can be found in the
{doc}`"Law Introduction" <law>` section of this documentation or in the
[example section](https://github.com/riga/law#examples) of the law
Github repository.
For an overview of the tasks that are available with columnflow, please see the
[Task Overview](../task_overview/introduction.md).

## Important note on required parameters

It should also be added that there are additional parameters specific for the tasks in columnflow,
required by the fact that columnflow's purpose is for HEP analysis. These are the ```--analysis```
and ```--config``` parameters, which defaults can be set in the law.cfg. These two parameters
respectively define the config file for the different analyses to be used (where the different
analyses and their parameters should be defined) and the name of the config file for the specific
analysis to be used.

Similarly the ```--version``` parameter, which purpose is explained in the {doc}`law` section of
this documentation, is required to start a task.


## Important modules and configs

The standard syntax to access objects in columnflow is the dot syntax, usable for the
[order](https://github.com/riga/order) metavariables (e.g. campaign.x.year) as well as the
[awkward arrays](https://awkward-array.org/doc/main/) (e.g. events.Jet.pt).

TODO

here mention the analysis template

### Campaigns


### Law config


### Analysis config

The analysis config defines all analysis specific variables and objects that need to be defined for
the analysis to run. Some of them are required for columnflow to run, some are additional and can
be useful, depending on the analysis.

TODO
