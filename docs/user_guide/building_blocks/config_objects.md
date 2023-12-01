# Config Objects

## Generalities

The [order](https://github.com/riga/order) package defines several classes to implement the
metavariables of an Analysis. The order documentation and its {external+order:doc}`quickstart` section
provide an introduction to these different classes. In this section we will concentrate on the use
of the order classes to define your analysis.

The three classes main classes needed to define your analysis are
{external+order:py:class}`order.analysis.Analysis`, {external+order:py:class}`order.config.Campaign`
and {external+order:py:class}`order.config.Config`. Their purpose and definition can be found in
[the Analysis, Campaign and Config section](https://python-order.readthedocs.io/en/latest/quickstart.html#analysis-campaign-and-config)
of the Quickstart section of the order documentation.

After defining your Analysis object and your Campaign object(s), you can use the command
```python
cfg = analysis.add_config(campaign, name=your_config_name, id=your_config_id)
```
to create the
new Config object `cfg`, which will be
associated to both the Analysis object and the Campaign object needed for its creation. As the
Config object should contain the analysis-dependent information related to a certain campaign, it
should contain most of the information needed for running your analysis. Therefore, in this section,
the Config parameters required by Columnflow and some convenience parameters will be presented.

To start your analysis, do not forget to use the already existing analysis template in the
`analysis_templates/cms_minimal` Git directory and its
[config](https://github.com/columnflow/columnflow/blob/master/analysis_templates/cms_minimal/__cf_module_name__/config/analysis___cf_short_name_lc__.py).


The Config saves informations under two general formats: the objects
from the order package, which are necessary for your analysis to run in columnflow, and the
additional parameters, which are saved under the auxiliary key, accessible through the "x" key.
Unlike the general order package objects and while several names for parameters in the auxiliary
field do already have a meaning in columnflow and should respect the format used in columnflow,
the auxiliary field can contain any kind of
parameter the user wants to save and reuse for parts of the analysis. These two general formats are
presented below.

It is generally advised to create functions to separate the different parts of the filling of the Config object, especially as some parts will not change very often once the analysis is set up (e.g. luminosity), while others will be changed quite often, e.g. each time new variables are created. An example of such a separation can be found in the existing [hh2bbtautau analysis](https://github.com/uhh-cms/hh2bbtautau).


## Parameters from the order package (required)

### Processes

The physical processes to be investigated in the analysis. These should be saved as objects of the
{external+order:py:class}`order.process.Process` class and added to the Config object using its
{external+order:py:meth}`order.config.Config.add_process()` method. The processes added to the
Config must correspond to the
processes defined for the datasets and added to the Campaign object associated to the Config.
It is possible to get all root processes from a specific campaign using the
{py:func}`~columnflow.config_util.get_root_processes_from_campaign()` function from columnflow.
As an example of information carried by a process, a color for the plotting scripts can be set using
the {external+order:py:attr}`order.mixins.ColorMixin.color1` attribute of the process.
More informations about processes can be found in the
{external+order:py:class}`order.process.Process` and the
{external+order:doc}`quickstart` sections of the order documentation.


### Datasets

The actual datasets to be processed in the analysis. These should be saved as objects of the
{external+order:py:class}`order.dataset.Dataset` class and added to the Config object using its
{external+order:py:meth}`order.config.Config.add_dataset()` method. The datasets added to the
Config object must correspond to the
datasets added to the Campaign object associated to the Config object. They are accessible through
the {external+order:py:meth}`order.config.Campaign.get_dataset()` method of the Campaign class. The
Dataset objects should contain for example informations on the number of files and number of events
present in a Dataset as well as its key and if it is data or a Monte-Carlo dataset. It is also
possible to change informations of a dataset in the config script. An example would be reducing the
number of files to process for test purposes in a specific test config. This could be done with the
following lines of code:
e.g.
```python
n_files_max = 5
for info in dataset.info.values():
    info.n_files = min(info.n_files, n_files_max)
```

Once the processes and datasets have both been added to the config, one can check that the root
process of all datasets is part of any of the registered processes, using the columnflow function
{py:func}`~columnflow.config_util.verify_config_processes()`.


### Variables

In order to create histograms out of the processed datasets, columnflow uses
{external+order:py:class}`order.variable.Variable`s. These
Variables need to be added to the config using the
function {external+order:py:meth}`order.config.Config.add_variable`. The standard syntax is as
follows for `cfg` the Config object:

```python
cfg.add_variable(
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
cfg.add_variable(
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

The list of possible keyword arguments can be found in the order documentation for the class
{external+order:py:class}`order.variable.Variable`. The values in the ```expression``` argument can
be either a one-dimensional or a more dimensional array. In this second case the information is
flattened before plotting. It is to be mentioned that
{py:attr}`~columnflow.columnar_util.EMPTY_FLOAT` is a columnflow internal null value and
corresponds to the value ```-9999.0```.

### Category

Categories built to investigate specific parts of the phase-space, for example for plotting.
These objects are described in [the Channel and Category](https://python-order.readthedocs.io/en/latest/quickstart.html#channel-and-category)
part of the Quickstart section of the order documentation. A specificity of columnflow regarding
this class is the fact that the `selection` argument of this object is expected to take the name of
an object of the {py:class}`~columnflow.categorization.Categorizer` class instead of a boolean
expression in a string format.
Adding a category to the Config object is possible through the
{py:func}`~columnflow.config_util.add_category()` method. An example for an inclusive category with
the Categorizer `cat_incl` defined in the cms_minimal analysis template is given below:

```python
add_category(
    cfg,
    id=1,
    name="incl",
    selection="cat_incl",
    label="inclusive",
)
```

It is recommended to always add an inclusive category with id=1 or name="incl" which is used
in various places, e.g. for the inclusive cutflow plots and the "empty" selector

### Channel

Similarly to categories, Channels are built to investigate specific parts of the phase space and are
described in the [Channel and Category](https://python-order.readthedocs.io/en/latest/quickstart.html#channel-and-category)
part of the Quickstart section of the order documentation. They can be added to the Config object using
{external+order:py:meth}`order.config.Config.add_channel()`.

### Shift

In order to implement systematic variations in the Config object, the
{external+order:py:class}`order.shift.Shift` class can be used. Implementing systematic variations
using shifts can take different forms depending on the kind of systematic variation involved,
therefore a complete section specialized in the description of these implementations is to be found
in (TODO: add link Shift section). Adding a Shift object to the Config object happens through the
{external+order:py:meth}`order.config.Config.add_shift()` function.

Often, shifts are related to auxiliary parameters of the Config, like the name of the scale
factors involved, or in the case of corrections accessible through external files, the paths of
these files.


Notes for the Shift section:

-> present examples auxiliary parameters calibrations: cfg.x.jec, cfg.x.muon_sf_names

-> present shift types -> use whole new NanoAODs with specific tune key defined in dataset, or different
calibrations e.g. cfg.x.jec, or weights through producer e.g. scale factors

-> mention external files

## Auxiliary Parameters (optional)

In principle, the auxiliaries of the Config may contain any kind of variables, but there are
several keys with a special meaning for columnflow, for which you would need to respect the expected
format. These are presented below at first, followed by a few examples of the kind of information
you might want to save in the auxiliary part of the config on top of these.

### Keep_columns

During the Task {py:class}`~columnflow.tasks.reduction.ReduceEvents` new files in parquet format
containing all remaining events and objects after the selections are created. If the auxiliary
argument `keep_columns`, accessible through `cfg.x.keep_columns`, exists in the Config object, only
the columns declared explicitely will be kept after the reduction. Actually, several tasks can make
use of such an argument in the Config object for the reduction of their output. Therefore, the
`keep_columns` argument expects a {py:class}`~columnflow.util.DotDict` containing the name of the
tasks (with the `cf.` prefix) for which such a reduction should be applied as keys and the set of
columns to be kept in the output of this task as values.

An example is given below:
```python
cfg.x.keep_columns = DotDict.wrap({
    "cf.ReduceEvents": {
        # general event info
        "run", "luminosityBlock", "event",
        # object info
        "Jet.pt", "Jet.eta", "Jet.phi", "Jet.mass", "Jet.btagDeepFlavB", "Jet.hadronFlavour",
        "Muon.pt", "Muon.eta", "Muon.phi", "Muon.mass", "Muon.pfRelIso04_all",
        "MET.pt", "MET.phi", "MET.significance", "MET.covXX", "MET.covXY", "MET.covYY",
        "PV.npvs",
        # columns added during selection
        "deterministic_seed", "process_id", "mc_weight", "cutflow.*",
    },
    "cf.MergeSelectionMasks": {
        "cutflow.*",
    },
    "cf.UniteColumns": {
        "*",
    },
})
```

For easier handling of the list of columns, the class {py:class}`~columnflow.columnar_util.ColumnCollection` was created.
It defines several enumerations containing columns to be kept according to a certain category.
For example, it is possible to keep all the columns created during the SelectEvents task with the enum `ALL_FROM_SELECTOR`.
With the ColumnCollection class, the example above could become:
```python
cfg.x.keep_columns = DotDict.wrap({
    "cf.ReduceEvents": {
        # general event info, mandatory for reading files with coffea
        ColumnCollection.MANDATORY_COFFEA
        # object info
        "Jet.pt", "Jet.eta", "Jet.phi", "Jet.mass", "Jet.btagDeepFlavB", "Jet.hadronFlavour",
        "Muon.pt", "Muon.eta", "Muon.phi", "Muon.mass", "Muon.pfRelIso04_all",
        "MET.pt", "MET.phi", "MET.significance", "MET.covXX", "MET.covXY", "MET.covYY",
        "PV.npvs",
        # all columns added during selection using a ColumnCollection flag
        ColumnCollection.ALL_FROM_SELECTOR,
    },
    "cf.MergeSelectionMasks": {
        "cutflow.*",
    },
    "cf.UniteColumns": {
        "*",
    },
})
```



### Custom retrieval of dataset files

The Columnflow task {py:class}`~columnflow.tasks.external.GetDatasetLFNs` obtains by default the
logical file names of the datasets through the
[CMS DAS](https://twiki.cern.ch/twiki/bin/view/CMSPublic/WorkBookLocatingDataSamples).
However, this
default behaviour can be changed using the auxiliary parameter `cfg.x.get_dataset_lfns`,
which must be mapped to either None (triggering the default behaviour and obtaining the LFNs
from DAS) or a custom function with three parameters (`dataset_inst`, the order {external+order:py:class}`order.dataset.Dataset`
object, `shift_inst`, the order {external+order:py:class}`order.shift.Shift` object and
`dataset_key`, the key given in the info dictionary of the dataset when defining it
(TODO: example of a dataset definition). From these parameters, the custom function should implement
a way to create the list of LFNs (= the paths to the root files starting from `/store`) corresponding to this dataset and
return this list. Two other auxiliary parameters can be changed, these are called
`cfg.x.get_dataset_lfns_sandbox` and `cfg.x.get_dataset_lfns_remote_fs`. `get_dataset_lfns_sandbox`
provides the sandbox in which the task GetDatasetLFNS will be run and expects therefore a
{external+law:py:class}`law.sandbox.base.Sandbox` object, which can be for example obtained through
the {py:func}`~columnflow.util.dev_sandbox` function.
`get_dataset_lfns_remote_fs` provides the remote file system on which the LFNs for the specific dataset can be found, it expects a function with the `dataset_inst` as a parameter and returning the name of the file system as defined in the law config file.

An example of such a function and the definition of the corresponding config parameters for a campaign where all datasets have been custom processed and stored on a single remote file system is given below.

```python
# custom lfn retrieval method in case the underlying campaign is custom uhh
if cfg.campaign.x("custom", {}).get("creator") == "uhh":
    def get_dataset_lfns(
        dataset_inst: od.Dataset,
        shift_inst: od.Shift,
        dataset_key: str,
    ) -> list[str]:
        # destructure dataset_key into parts and create the lfn base directory
        dataset_id, full_campaign, tier = dataset_key.split("/")[1:]
        main_campaign, sub_campaign = full_campaign.split("-", 1)
        lfn_base = law.wlcg.WLCGDirectoryTarget(
            f"/store/{dataset_inst.data_source}/{main_campaign}/{dataset_id}/{tier}/{sub_campaign}/0",
            fs=f"wlcg_fs_{cfg.campaign.x.custom['name']}",
        )

        # loop though files and interpret paths as lfns
        return [
            lfn_base.child(basename, type="f").path
            for basename in lfn_base.listdir(pattern="*.root")
        ]

    # define the lfn retrieval function
    cfg.x.get_dataset_lfns = get_dataset_lfns

    # define a custom sandbox
    cfg.x.get_dataset_lfns_sandbox = dev_sandbox("bash::$CF_BASE/sandboxes/cf.sh")

    # define custom remote fs's to look at
    cfg.x.get_dataset_lfns_remote_fs = lambda dataset_inst: f"wlcg_fs_{cfg.campaign.x.custom['name']}"
```

### External_files

If some files from outside columnflow are needed for an analysis, be them local files or online ( and accessible through wget), these can be indicated in the `cfg.x.external_files` auxiliary parameter.
These can then be copied to the columnflow outputs using the {py:class}`~columnflow.tasks.external.BundleExternalFiles` task and used by being required by the object needing them.
The `cfg.x.external_files` parameter expects a {py:class}`~columnflow.util.DotDict` with a string
explaining the role of the file to be downloaded/copied as key and the link/path as value.
It is also possible to give a tuple as value, with the link/path as the first entry of the tuple and a version as a second entry.
As an example, the `cfg.x.external_files` parameter might look like this, for `json_mirror` the local path of the mirror directory to a specific commit of the [jsonPOG-integration Gitlab](https://gitlab.cern.ch/cms-nanoAOD/jsonpog-integration/-/tree/master) (CMS-specific):
```python
cfg.x.external_files = DotDict.wrap({
    # lumi files
    "lumi": {
        "golden": ("/afs/cern.ch/cms/CAF/CMSCOMM/COMM_DQM/certification/Collisions17/13TeV/Legacy_2017/Cert_294927-306462_13TeV_UL2017_Collisions17_GoldenJSON.txt", "v1"),  # noqa
        "normtag": ("/afs/cern.ch/user/l/lumipro/public/Normtags/normtag_PHYSICS.json", "v1"),
    },

    # muon scale factors
    "muon_sf": (f"{json_mirror}/POG/MUO/2017_UL/muon_Z.json.gz", "v1"),

    # some example of a ML training dataset to show an example with URL
    "vehicules_dataset_fastai": ("https://github.com/arunoda/fastai-courses/releases/download/fastai-vehicles-dataset/fastai-vehicles.tgz", "v1"),  # noqa
})
```

An example of usage of the `muon_sf`, including the requirement of the BundleExternalFiles task is given in the {py:mod}`~columnflow.production.cms.muon` file, implementing the {py:class}`~columnflow.production.cms.muon.muon_weights` Producer.

TODO: put the following in drop down menu

Showing how to require the BundleExternalFiles task to have run in the example of the muon weights Producer linked above:
```python
@muon_weights.requires
def muon_weights_requires(self: Producer, reqs: dict) -> None:
    if "external_files" in reqs:
        return

    from columnflow.tasks.external import BundleExternalFiles
    reqs["external_files"] = BundleExternalFiles.req(self.task)
```


### Luminosity

The luminosity, needed for some normalizations and for the labels in the standard columnflow plots, needs to be given in the auxiliary arguments `cfg.x.luminosity` as an object of the {external+scinum:py:class}`scinum.Number` class, such that for example the `nominal` parameter exists.
An example for a CMS luminosity of 2017 with uncertainty sources given as relative errors is given below.

```python
from scinum import Number
cfg.x.luminosity = Number(41480, {
    "lumi_13TeV_2017": 0.02j,
    "lumi_13TeV_1718": 0.006j,
    "lumi_13TeV_correlated": 0.009j,
})
```


### Defaults

Default values can be given for several command line parameters in columnflow, using the `cfg.x.default_{parameter}` entry in the Config object.
The expected format is a single string containing the name of the object to be used as default for parameters accepting only one argument and a tuple for parameters accepting several arguments.
The command-line arguments supporting a default value from the Config object are given in the cms_minimal example of the analysis_templates and shown again below:

```python
cfg.x.default_calibrator = "example"
cfg.x.default_selector = "example"
cfg.x.default_producer = "example"
cfg.x.default_ml_model = None
cfg.x.default_inference_model = "example"
cfg.x.default_categories = ("incl",)
cfg.x.default_variables = ("n_jet", "jet1_pt")
cfg.x.default_weight_producer = "example"
```


### Groups

It is also possible to create groups, which allow to conveniently loop over certain command-line parameters.
This is done with the `cfg.x.{parameter}_group` arguments.
The expected format of the group is a dictionary containing the custom name of the groups as keys and the list of the parameter values as values.
The name of the group can then be given as command-line argument instead of the single values.
An example with a selector_steps group is given below.

```python
# selector step groups for conveniently looping over certain steps
# (used in cutflow tasks)
cfg.x.selector_step_groups = {
    "default": ["muon", "jet"],
}
```

With this group defined in the Config object, running over the "muon" and "jet" selector_steps in this order in a cutflow task can done with the argument `--selector-steps default`.

All other parameters for which groups are possible are given below:
```python
# process groups for conveniently looping over certain processs
# (used in wrapper_factory and during plotting)
cfg.x.process_groups = {}

# dataset groups for conveniently looping over certain datasets
# (used in wrapper_factory and during plotting)
cfg.x.dataset_groups = {}

# category groups for conveniently looping over certain categories
# (used during plotting)
cfg.x.category_groups = {}

# variable groups for conveniently looping over certain variables
# (used during plotting)
cfg.x.variable_groups = {}

# shift groups for conveniently looping over certain shifts
# (used during plotting)
cfg.x.shift_groups = {}
```

### Reduced Files size

The target size of the files in MB after the {py:class}`~columnflow.tasks.reduction.MergeReducedEvents` task can be set in the Config object with the `cfg.x.reduced_file_size` argument. A float number corresponding to the size in MB is expected. If nothing is set, the default value implemented in columnflow will be used (512MB at the time of writing). An example is given below.

```python
# target file size after MergeReducedEvents in MB
cfg.x.reduced_file_size = 512.0
```


### Object-specific variables

Other than the variables mentioned above, several might be needed for specific Producers for example, but these won't be discussed here as they are not general parameters.
Hence, we invite the users to check which Config entries are needed for each {py:class}`~columnflow.calibration.Calibrator`,
{py:class}`~columnflow.selection.Selector` and {py:class}`~columnflow.production.Producer` and in general each CMS-specific object (=objects in the cms-subfolders) they want to use.
Since the {py:class}`~columnflow.production.cms.muon.muon_weights` Producer was already mentioned above, we will remind users here that the `cfg.x.muon_sf_names` Config entry is needed for this Producer to run, as indicated in the docstring of the Producer.

As for the CMS-specific objects, an example could be the task {py:class}`~columnflow.tasks.cms.external.CreatePileupWeights`, which requires additional entries in the Config objects, for example the minimum bias cross sections `cfg.x.minbias_xs` and the `pu` entry to the external files.

Examples of these entries in the Config objects can be found in already existing CMS-analyses working with columnflow, for example the [hh2bbtautau analysis](https://github.com/uhh-cms/hh2bbtautau) or the [hh2bbww analysis](https://github.com/uhh-cms/hh2bbww) from UHH.


### Other examples of auxiliaries in the Config object

As mentioned above, any kind of python variables can be stored in the auxiliary of the Config object.
To give an idea of the kind of variables an analysis might want to include in the Config object additionally to the ones needed by columnflow, a few examples of variables which do not receive any specific treatment in native columnflow are given.

- Triggers

- b-tag working points

- MET filters

For applications of these examples, you can look at already existing columnflow analyses, for example the [hh2bbtautau analysis](https://github.com/uhh-cms/hh2bbtautau) from UHH.




