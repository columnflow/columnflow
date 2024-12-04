# Config Objects

## Generalities

The [order](https://github.com/riga/order) package defines several classes to implement the metavariables of an Analysis.
The order documentation and its {external+order:doc}`quickstart` section provide an introduction to these different classes.
In this section we will concentrate on the use of the order classes to define your analysis.

The three main classes needed to define your analysis are {external+order:py:class}`order.analysis.Analysis`, {external+order:py:class}`order.config.Campaign` and {external+order:py:class}`order.config.Config`.
Their purpose and definition can be found in [the Analysis, Campaign and Config section](https://python-order.readthedocs.io/en/latest/quickstart.html#analysis-campaign-and-config) of the Quickstart section of the order documentation.

After defining your Analysis object and your Campaign object(s), you can use the command

```python
cfg = analysis.add_config(campaign, name=your_config_name, id=your_config_id)
```

to create the new Config object `cfg`, which will be associated to both the Analysis object and the Campaign object needed for its creation.
As the Config object should contain the analysis-dependent information related to a certain campaign, it should contain most of the information needed for running your analysis.
Therefore, in this section, the Config parameters required by Columnflow and some convenience parameters will be presented.

To start your analysis, do not forget to use the already existing analysis template in the `analysis_templates/cms_minimal` Git directory and its [config](https://github.com/columnflow/columnflow/blob/master/analysis_templates/cms_minimal/__cf_module_name__/config/analysis___cf_short_name_lc__.py).

The Config saves information under two general formats: the objects from the order package, which are necessary for your analysis to run in columnflow, and the additional parameters, which are saved under the auxiliary key, accessible through the "x" key.
In principle, the auxiliary field can contain any parameter the user wants to save and reuse for parts of the analysis.
However, several names in the auxiliary field do already have a meaning in columnflow and their values should respect the format used in columnflow.
These two general formats are presented below.

Additionally, please note that some columnflow objects, like some Calibrators and Producers, require specific information that is needed to be accessible with predefined keywords.
As explained in the {ref}`object-specific variables section <object-specific_variables_section>`, please check the documentation of these objects before using them.

It is generally advised to use functions to set up Config objects.
This enables easy and reliable reusage of parts of your analysis that are the same or similar between Campaigns (e.g. parts of the uncertainty model).
Additionally, other parts of the analysis that might be changed quite often, e.g. the definition of variables, can be defined separately, thus improving the overall organization and readability of your code.
An example of such a separation can be found in the existing [hh2bbtautau analysis](https://github.com/uhh-cms/hh2bbtautau).

## Parameters from the order package (required)

### Processes

The physical processes to be included in the analysis.
These should be saved as objects of the {external+order:py:class}`order.process.Process` class and added to the Config object using its {external+order:py:meth}`order.config.Config.add_process()` method.
An example is given in the columnflow analysis template:

```{literalinclude} ../../../analysis_templates/cms_minimal/__cf_module_name__/config/analysis___cf_short_name_lc__.py
:language: python
:start-at: process_names = [
:end-at: proc = cfg.add_process(procs.get(process_name))
```

Additionally, these processes must have corresponding datasets that are to be added to the Config as well (see {ref}`next section <datasets_docu_section>`).
It is possible to get all root processes from a specific campaign using the {py:func}`~columnflow.config_util.get_root_processes_from_campaign()` function from columnflow.
Examples of information carried by a process could be the cross section of the process, registered under the {external+order:py:attr}`order.process.Process.xsecs` attribute and further used for {py:class}`~columnflow.production.normalization.normalization_weights` in columnflow, and a color for the plotting scripts, which can be set using the {external+order:py:attr}`order.mixins.ColorMixin.color1` attribute of the process.
An example of a Process definition is given in the {ref}`Analysis, Campaign and Config <analysis_campaign_config>` section of the columnflow documentation.
More information about processes can be found in the {external+order:py:class}`order.process.Process` and the {external+order:doc}`quickstart` sections of the order documentation.

(datasets_docu_section)=

### Datasets

The actual datasets to be processed in the analysis.
These should be saved as objects of the {external+order:py:class}`order.dataset.Dataset` class and added to the Config object using its {external+order:py:meth}`order.config.Config.add_dataset()` method.
The datasets added to the Config object must correspond to the datasets added to the Campaign object associated to the Config object.
They are accessible through the {external+order:py:meth}`order.config.Campaign.get_dataset()` method of the Campaign class.
An example is given in the columnflow analysis template:

```{literalinclude} ../../../analysis_templates/cms_minimal/__cf_module_name__/config/analysis___cf_short_name_lc__.py
:language: python
:start-at: dataset_names = [
:end-at: dataset = cfg.add_dataset(campaign.get_dataset(dataset_name))
```

The Dataset objects should contain for example information about the number of files and number of events present in a Dataset as well as its keys (= the identifiers or origins of a dataset, used by the `cfg.x.get_dataset_lfns` parameter presented below in {ref}`the section on custom retrieval of datasets <custom_retrieval_of_dataset_files_section>`) and wether it contains observed or simulated data.

It is also possible to change information of a dataset in the config script.
An example would be reducing the number of files to process for test purposes in a specific test config.
This could be done with the following lines of code:
e.g.

```python
n_files_max = 5
for info in dataset.info.values():
    info.n_files = min(info.n_files, n_files_max)
```

Once the processes and datasets have both been added to the config, one can check that the root process of all datasets is part of any of the registered processes, using the columnflow function {py:func}`~columnflow.config_util.verify_config_processes`.

An example of a Dataset definition is given in the {ref}`Analysis, Campaign and Config <analysis_campaign_config>` section of the columnflow documentation.

### Variables

In order to create histograms out of the processed datasets, columnflow uses {external+order:py:class}`order.variable.Variable`s.
These Variables need to be added to the config using the function {external+order:py:meth}`order.config.Config.add_variable`.
An example of the standard syntax for the Config object `cfg` would be as follows for the transverse momentum of the first jet:

```{literalinclude} ../../../analysis_templates/cms_minimal/__cf_module_name__/config/analysis___cf_short_name_lc__.py
:language: python
:start-at: "# pt of the first jet in every event"
:end-before: "# eta of the first jet in every event"
```

It is worth mentioning, that you do not need to select a specific jet per event in the `expression` argument (here with `Jet.pt[:,0]`), you can get a flattened histogram for all jets in all events with `expression="Jet.pt"`.

In histogramming tasks such as {py:class}`~columnflow.tasks.histograms.CreateHistograms`, one histogram is created per Variable given via the ```--variables``` argument, accessing information from columns based on the `expression` of the Variable and storing them in histograms with binning defined via the `binning` argument of the Variable.

The list of possible keyword arguments can be found in the order documentation for the class {external+order:py:class}`order.variable.Variable`.
The values in the ```expression``` argument can be either a one-dimensional or a more dimensional array.
In this second case the information is flattened before plotting.
It is to be mentioned that {py:attr}`~columnflow.columnar_util.EMPTY_FLOAT` is a columnflow internal null value.

### Category

Categories built to investigate specific parts of the phase-space, for example for plotting.
These objects are described in [the Channel and Category](https://python-order.readthedocs.io/en/latest/quickstart.html#channel-and-category) part of the Quickstart section of the order documentation.
You can add such a category with the {py:func}`~columnflow.config_util.add_category()` method.
When adding this object to your Config instance, the `selection` argument is expected to take the name of an object of the {py:class}`~columnflow.categorization.Categorizer` class instead of a boolean expression in a string format.
An example for an inclusive category with the Categorizer `cat_incl` defined in the cms_minimal analysis template is given below:

```{literalinclude} ../../../analysis_templates/cms_minimal/__cf_module_name__/config/analysis___cf_short_name_lc__.py
:language: python
:start-at: add_category(
:end-at: )
```

It is recommended to always add an inclusive category with ``id=1`` or ``name="incl"`` which is used in various places, e.g. for the inclusive cutflow plots and the "empty" selector.

A more detailed description of the usage of categories in columnflow is given in the {ref}`Categories <categories>` section of this documentation.

### Channel

Similarly to categories, Channels are built to investigate specific parts of the phase space and are described in the [Channel and Category](https://python-order.readthedocs.io/en/latest/quickstart.html#channel-and-category) part of the Quickstart section of the order documentation.
They can be added to the Config object using {external+order:py:meth}`order.config.Config.add_channel()`.

(shift)=

### Shift

In order to implement systematic variations in the Config object, the {external+order:py:class}`order.shift.Shift` class can be used.
Implementing systematic variations using shifts can take different forms depending on the kind of systematic variation involved, therefore a complete section specialized in the description of these implementations is to be found in (TODO: add link Shift section).
Adding a Shift object to the Config object happens through the {external+order:py:meth}`order.config.Config.add_shift()` function.
An example is given in the columnflow analysis template:

```{literalinclude} ../../../analysis_templates/cms_minimal/__cf_module_name__/config/analysis___cf_short_name_lc__.py
:language: python
:start-at: cfg.add_shift(name="nominal", id=0)
:end-at: cfg.add_shift(name="nominal", id=0)
```

Often, shifts are related to auxiliary parameters of the Config, like the name of the scale factors involved, or the paths of source files in case the shift requires external information.

<!--

Notes for the Shift section:

-> present examples auxiliary parameters calibrations: cfg.x.jec, cfg.x.muon_sf_names

-> present shift types -> use whole new NanoAODs with specific tune key defined in dataset, or different
calibrations e.g. cfg.x.jec, or weights through producer e.g. scale factors

-> mention external files

-->

## Auxiliary Parameters (optional)

In principle, the auxiliaries of the Config may contain any kind of variables.
However, there are several keys with a special meaning for columnflow, for which you would need to respect the expected format.
These are presented below at first, followed by a few examples of the kind of information you might want to save in the auxiliary part of the config on top of these.
If you would like to use modules that ship with Columnflow, it is generally a good idea to first check their documentation to understand what kind of information you need to specify in the auxiliaries of your Config object for a successful run.

### Keep_columns

During the Task {py:class}`~columnflow.tasks.reduction.ReduceEvents` new files containing all remaining events and objects after the selections are created in parquet format.
If the auxiliary argument `keep_columns`, accessible through `cfg.x.keep_columns`, exists in the Config object, only the columns declared explicitely will be kept after the reduction.
Actually, several tasks can make use of such an argument in the Config object for the reduction of their output.
Therefore, the `keep_columns` argument expects a {py:class}`~columnflow.util.DotDict` containing the name of the tasks (with the `cf.` prefix) for which such a reduction should be applied as keys and the set of columns to be kept in the output of this task as values.

For easier handling of the list of columns, the class {py:class}`~columnflow.columnar_util.ColumnCollection` was created.
It defines several enumerations containing columns to be kept according to a certain category.
For example, it is possible to keep all the columns created during the SelectEvents task with the enum `ALL_FROM_SELECTOR`.
An example is given below:

```{literalinclude} ../../../analysis_templates/cms_minimal/__cf_module_name__/config/analysis___cf_short_name_lc__.py
:language: python
:start-after: "# columns to keep after certain steps"
:end-before: "})"
```

(custom_retrieval_of_dataset_files_section)=

### Custom retrieval of dataset files

The Columnflow task {py:class}`~columnflow.tasks.external.GetDatasetLFNs` obtains by default the logical file names of the datasets based on the `keys` argument of the corresponding order Dataset.
By default, the function {py:meth}`~columnflow.external.GetDatasetLFNs.get_dataset_lfns_dasgoclient` is used, which obtains the information through the [CMS DAS](https://twiki.cern.ch/twiki/bin/view/CMSPublic/WorkBookLocatingDataSamples).
However, this default behaviour can be changed using the auxiliary parameter `cfg.x.get_dataset_lfns`.
You can set this to a custom function with the same keyword arguments as the default.
For more information, please consider the documentation of {py:meth}`~columnflow.external.GetDatasetLFNs.get_dataset_lfns_dasgoclient`.
Based on these parameters, the custom function should implement a way to create the list of paths corresponding to this dataset (the paths should not include the path to the remote file system) and return this list.
Two other auxiliary parameters can be changed:

- `get_dataset_lfns_sandbox` provides the sandbox in which the task GetDatasetLFNS will be run and expects therefore a {external+law:py:class}`law.sandbox.base.Sandbox` object, which can be for example obtained through the {py:func}`~columnflow.util.dev_sandbox` function.

- `get_dataset_lfns_remote_fs` provides the remote file system on which the LFNs for the specific dataset can be found. It expects a function with the `dataset_inst` as a parameter and returning the name of the file system as defined in the law config file.

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

If some files from outside columnflow are needed for an analysis, be them local files or online (and accessible through wget), these can be indicated in the `cfg.x.external_files` auxiliary parameter.
These can then be copied to the columnflow outputs using the {py:class}`~columnflow.tasks.external.BundleExternalFiles` task and used by being required by the object needing them.
The `cfg.x.external_files` parameter expects a (possibly nested) {py:class}`~columnflow.util.DotDict` with a user-defined key to retrieve the target in columnflow and the link/path as value.
It is also possible to give a tuple as value, with the link/path as the first entry of the tuple and a version as a second entry.
As an example, the `cfg.x.external_files` parameter might look like this, where `json_mirror` is a local path to a mirror directory of a specific commit of the [jsonPOG-integration Gitlab](https://gitlab.cern.ch/cms-nanoAOD/jsonpog-integration/-/tree/master) (CMS-specific):

```{literalinclude} ../../../analysis_templates/cms_minimal/__cf_module_name__/config/analysis___cf_short_name_lc__.py
:language: python
:start-at: "cfg.x.external_files = DotDict.wrap({"
:end-at: "})"
```

An example of usage of the `muon_sf`, including the requirement of the BundleExternalFiles task is given in the {py:class}`~columnflow.production.cms.muon.muon_weights` Producer.

:::{dropdown} How to require a task in a Producer?

Showing how to require the BundleExternalFiles task to have run in the example of the muon weights Producer linked above:

```{literalinclude} ../../../columnflow/production/cms/muon.py
:language: python
:start-at: "@muon_weights.requires"
:end-at: reqs["external_files"] = BundleExternalFiles.req(self.task)
```

:::

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
The expected format is either:

- a single string containing the name of the object to be used as default for parameters accepting only one argument or
- a tuple for parameters accepting several arguments.

The command-line arguments supporting a default value from the Config object are given in the cms_minimal example of the analysis_templates and shown again below:

```{literalinclude} ../../../analysis_templates/cms_minimal/__cf_module_name__/config/analysis___cf_short_name_lc__.py
:language: python
:start-at: cfg.x.default_calibrator = "example"
:end-at: cfg.x.default_variables = ("n_jet", "jet1_pt")
```

### Groups

It is also possible to create groups, which allow to conveniently loop over certain command-line parameters.
This is done with the `cfg.x.{parameter}_group` arguments.
The expected format of the group is a dictionary containing the custom name of the groups as keys and the list of the parameter values as values.
The name of the group can then be given as command-line argument instead of the single values.
An example with a selector_steps group is given below.

```{literalinclude} ../../../analysis_templates/cms_minimal/__cf_module_name__/config/analysis___cf_short_name_lc__.py
:language: python
:start-at: "# selector step groups for conveniently looping over certain steps"
:end-at: "}"
```

<!--
```python
# selector step groups for conveniently looping over certain steps
# (used in cutflow tasks)
cfg.x.selector_step_groups = {
    "default": ["muon", "jet"],
}
```
-->

With this group defined in the Config object, running over the "muon" and "jet" selector_steps in this order in a cutflow task can done with the argument `--selector-steps default`.

All parameters for which groups are possible are given below:

```{literalinclude} ../../../analysis_templates/cms_minimal/__cf_module_name__/config/analysis___cf_short_name_lc__.py
:language: python
:start-at: "# process groups for conveniently looping over certain processs"
:end-at: "cfg.x.ml_model_groups = {}"
```

### Reduced File size

The target size of the files in MB after the {py:class}`~columnflow.tasks.reduction.MergeReducedEvents` task can be set in the Config object with the `cfg.x.reduced_file_size` argument.
A float number corresponding to the size in MB is expected.
This value can also be changed with the `merged_size` argument when running the task.
If nothing is set, the default value implemented in columnflow will be used (defined in the {py:meth}`~columnflow.tasks.reduction.MergeReducedEvents.resolve_param_values` method).
An example is given below.

```python
# target file size after MergeReducedEvents in MB
cfg.x.reduced_file_size = 512.0
```

(object-specific_variables_section)=

### Object-specific variables

Other than the variables mentioned above, several might be needed for specific Producers for example.
These won't be discussed here as they are not general parameters.
Hence, we invite the users to check which Config entries are needed for each {py:class}`~columnflow.calibration.Calibrator`,
{py:class}`~columnflow.selection.Selector` and {py:class}`~columnflow.production.Producer` and in general each CMS-specific object (=objects in the cms-subfolders) they want to use.
Since the {py:class}`~columnflow.production.cms.muon.muon_weights` Producer was already mentioned above, we will remind users here that the `cfg.x.muon_sf_names` Config entry is needed for this Producer to run, as indicated in the docstring of the Producer.

As for the CMS-specific objects, an example could be the task {py:class}`~columnflow.tasks.cms.external.CreatePileupWeights`, which requires for example the minimum bias cross sections `cfg.x.minbias_xs` and the `pu` entry in the external files.

Examples of these entries in the Config objects can be found in already existing CMS-analyses working with columnflow, for example the [hh2bbtautau analysis](https://github.com/uhh-cms/hh2bbtautau) or the [hh2bbww analysis](https://github.com/uhh-cms/hh2bbww) from UHH.

### Other examples of auxiliaries in the Config object

As mentioned above, any kind of python variables can be stored in the auxiliary of the Config object.
To give an idea of the kind of variables an analysis might want to include in the Config object additionally to the ones needed by columnflow, a few examples of variables which do not receive any specific treatment in native columnflow are given.

- Triggers

- b-tag working points

- MET filters

For applications of these examples, you can look at already existing columnflow analyses, for example the [hh2bbtautau analysis](https://github.com/uhh-cms/hh2bbtautau) from UHH.
