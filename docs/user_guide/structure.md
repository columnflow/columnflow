# Columnflow Structure

In this section, an overview to the structure of Columnflow is provided, starting with a general introduction, followed by a description of the various tasks implemented in columnflow and ending with an introduction on how to configure your analysis on top of columnflow with the analysis and config object from the [order](https://github.com/riga/order) package.

## General introduction

Columnflow is a fully orchestrated columnar analysis tool for HEP analyses with Python.
The workflow orchestration is managed by [law](https://github.com/riga/law) and the meta data and configuration is managed by [order](https://github.com/riga/order).
A short introduction to law is given in the {doc}`law section <law>`.
If you have never used law before, this section is highly recommended as a few very convenient commands are presented there.

The data processing in columnflow is based on columns in [awkward arrays](https://awkward-array.org/doc/main/) with [coffea](https://coffeateam.github.io/coffea/)-generated behaviour.
Fields like "Jet" exist too, they contain columns with the same first dimension (the parameters of the field, e.g. Jet.pt). A few additional functions for simplified handling of columns were defined in {py:mod}`~columnflow.columnar_util`.

As most of the information is conserved in the form of columns, it would be very inefficient (and might not even fit in the memory) to use all columns and all events from a dataset at once for each task.
Therefore, in order to reduce the impact on the memory:

- a chunking of the datasets is implemented using [dask](https://www.dask.org/):
not all events from a dataset are inputed in a task at once, but only chunked in groups of events.
(100 000 events max per group is default as of 05.2023, default is set in the law.cfg file).
- the user needs to define for each {py:class}`~columnflow.production.Producer`, {py:class}`~columnflow.calibration.Calibrator` and {py:class}`~columnflow.selection.Selector` which columns are to be loaded (this happens by defining the ```uses``` set in the header of the decorator of the class) and which new columns/fields are to be saved in parquet files after the respective task (this happens by defining the ```produces``` set in the header of the decorator of the class).
The exact implementation for this feature is further detailed in {doc}`building_blocks/selectors` and {doc}`building_blocks/producers`.

## Tasks in columnflow

Tasks are [law](https://github.com/riga/law) objects allowing to control a workflow.
All the tasks presented below are proposed by columnflow and allow for a fairly complete analysis workflow.
However, as analyses are very diverse, it is possible that a specific analysis will need more stearing options or even completely new tasks.
Thankfully, columnflow is not a fixed structure and you will be able to create new tasks in such a case, following the corresponding example in the {doc}`examples` section of this documentation.
The full task tree of general columnflow tasks can be seen in [this wikipage](https://github.com/columnflow/columnflow/wiki#default-task-graph).
There are also experiment-specific tasks which are not present in this graph.
However, these are introduced in the {ref}`CMS specializations section <cms_specializations_section>`.

Further informations about tasks and law can be found in the {doc}`"Law Introduction" <law>` section of this documentation or in the [example section](https://github.com/riga/law#examples) of the law Github repository.
For an overview of the tasks that are available with columnflow, please see the [Task Overview](../task_overview/introduction.md).

## Important note on required parameters

It should also be added that there are additional parameters specific for the tasks in columnflow, required by the fact that columnflow's purpose is for HEP analysis.
These are the ```--analysis``` and ```-config``` parameters, which defaults can be set in the law.cfg.
These two parameters respectively define the config file for the different analyses to be used (where the different analyses and their parameters should be defined) and the name of the config file for the specific analysis to be used.

Similarly the ```--version``` parameter, which purpose is explained in the {doc}`law` section of this documentation, is required to start a task.

## Important modules and configs

The standard syntax to access objects in columnflow is the dot syntax, usable for the [order](https://github.com/riga/order) metavariables (e.g. campaign.x.year) as well as the [awkward arrays](https://awkward-array.org/doc/main/) (e.g. events.Jet.pt).

TODO

here mention the analysis template

### Law config

(analysis_campaign_config)=

### Analysis, Campaign and Config

Columnflow uses the {external+order:py:class}`order.analysis.Analysis` class from the [order](https://github.com/riga/order) package to define a specific analysis.
This object does not contain most of the analysis information by itself.
It is to be linked to objects of the {external+order:py:class}`order.config.Campaign` and {external+order:py:class}`order.config.Config` classes, as described in [the Analysis, Campaign and Config section](https://python-order.readthedocs.io/en/latest/quickstart.html#analysis-campaign-and-config) of the Quickstart section of the order documentation.
An example of an analysis with its full Analysis, Campaign and Config definitions in the same directory is given in the [Analysis Grand Challenge Columnflow repository](https://github.com/columnflow/agc_cms_ttbar/) repository and [its config directory](https://github.com/columnflow/agc_cms_ttbar/tree/master/agc/config).

A Campaign object contains the analysis-independent information related to a specific and well-defined experimental campaign.
We define an experimental campaign as a set of fixed specific conditions for the data-taking or simulations (for example, a period of data-taking for which no significant change to the detector setup or operation was made, like the data-taking period of the year 2017 for the CMS detector at CERN).
This means general information like the center of mass energy of the collisions (argument `ecm`) as well as the datasets created during/for the specific campaign.
An example of a Campaign declaration (from the AGC Columnflow repository linked above) might be:

```python
from order import Campaign

campaign_cms_opendata_2015_agc = cpn = Campaign(
    name="cms_opendata_2015_agc",  # the name of the campaign
    id=1,  # a unique id for this campaign
    ecm=13,  # center of mass energy
    aux={  # additional, arbitrary information
        "tier": "NanoAOD",  # data format, e.g. NanoAOD
        "year": 2015,  # year of data-taking
        "location": "https://xrootd-local.unl.edu:1094//store/user/AGC/nanoAOD",  # url to base path of the nanoAODs
        "wlcg_fs": "wlcg_fs_unl",  # file system to use on the WLCG
    },
)
```

In order to define a {external+order:py:class}`order.dataset.Dataset`, which may be included in the Campaign object, an associated {external+order:py:class}`order.process.Process` object must be defined first.
An example of such a Process, describing the physical process of the top-antitop production associated with jets in a pp-collision is defined in `agc.config.processes` (in the AGC Columnflow repository linked above) as:

```python
from scinum import Number
from order import Process

tt = Process(
    name="tt",  # name of the process
    id=1000,  # unique id for this process
    label=r"$t\bar{t}$ + Jets",  # label to be used in e.g. plot legends
    color=(128, 76, 153),  # color to be used in plots for this process (e.g. in RGB format)
    xsecs={  # cross sections for this process
        # format of these numbers is
        # {
        #    center of mass energy: Number object with nominal prediction + uncertainties
        # }
        13: Number(831.76, {
            "scale": (19.77, 29.20),
            "pdf": 35.06,
            "mtop": (23.18, 22.45),
        }),
    },
)
```

Using the physical Process defined above, one may now create a dataset, which can be added to the Campaign object.
An example of dataset definition with scale variations for this campaign would then be (values taken from [the analysis-grand-challenge GitHub repository](https://github.com/iris-hep/analysis-grand-challenge/blob/be91d2c80225b7a91ce6b153591f8605167bf555/analyses/cms-open-data-ttbar/nanoaod_inputs.json)):

```python
import agc.config.processes as procs
from order import DatasetInfo

# cpn is the Campaign object defined in the previous code block
# the following shows an example for a simulated tt dataset generated with the Powheg package
cpn.add_dataset(
    name="tt_powheg",  # name of the dataset
    id=1,  # unique id for this dataset
    processes=[procs.tt],  # link this dataset to physics processes (which are also order objects in this case)
    info={  # add additional information to this dataset
        # information regarding the 'nominal' generation of the dataset,
        # i.e. with the recommended central values of the MC parameters
        "nominal": DatasetInfo(
            # identifier for this dataset in a meta information database (e.g. DAS at CMS)
            keys=["TT_TuneCUETP8M1_13TeV-powheg-pythia8"],
            n_files=242,  # how many files to process, important for book keeping
            n_events=276079127),  # total number of events
        # you can also add information about additional samples that belong to this dataset
        # for example, add samples where a set of MC parameters (tune) is varied within uncertainties
        "scale_down": DatasetInfo(
            keys=["TT_TuneCUETP8M1_13TeV-powheg-scaledown-pythia8"],
            n_files=32,
            n_events=39329663),
        "scale_up": DatasetInfo(
            keys=["TT_TuneCUETP8M1_13TeV-powheg-scaleup-pythia8"],
            n_files=33,
            n_events=38424467),
    },
    aux={  # additional, general information about this dataset
        "agc_process": "ttbar",
        "agc_shifts": {
            "scale_down": "scaledown",
            "scale_up": "scaleup",
        },
    },
)
```

A Config object is saving the variables specific to the analysis.
It is associated to an Analysis and a Campaign object.
It is to be created using the {external+order:py:meth}`order.analysis.Analysis.add_config` method on the analysis object, with the associated Campaign object as argument.
An example would be:

```python
cfg = analysis.add_config(campaign, name=config_name, id=config_id)
```

In this way, one Analysis can be tied to different Campaigns (e.g. corresponding to different years of data-taking) very naturally.

Several classes of the order library are used for the organization and use of the metavariables.
A more detailed description of the most important objects to be defined in the Config object is presented in the {doc}`building_blocks/config_objects` section of this documentation.
