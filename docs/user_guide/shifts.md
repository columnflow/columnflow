
# Systematic Uncertainties

Many aspects of the work of analyzers revolve around systematic uncertainties on the modeling of the simulation or in the conditions during data taking.
columnflow is designed to support various types of systematic uncertainties, also referred to as *shifts*.
We distinguish two general classes of uncertainties:  

- **rate uncertainties** only modify the overall yield of a distribution.
- **shape uncertainties** can modify both the yield and the shape of distributions.

The following section describe how to configure your modules to correctly register and use these shifts.

## Uncertainties that only modify the yield

The easiest class of systematic uncertainties to implement are shifts that only modify the overall yield in the final template for a given process.
One common example for such parameters within high-energy physics is the uncertainty on the measurement of the luminosity, which directly affects the expected number of events.

Consider the following example from the analysis template for the configuration of the luminosity and its uncertainties:

```{literalinclude} ../../analysis_templates/cms_minimal/__cf_module_name__/config/analysis___cf_short_name_lc__.py
:dedent:
:language: python
:start-at: '# lumi values in inverse pb'
:end-before: '# names of muon correction sets and working points'
```

Note that the luminosity is defined as a {external+scinum:py:class}`scinum.Number` object to define the nominal values as well as multiple sources of uncertainties in one place.

These uncertainties do not impact the workflow itself since they do not modify the selection efficiency or require the processing of additional samples.
Therefore, no additional steps are necessary to register this shift in the workflow, provided you do not want to use it for plotting tasks.
For an example on the latter please see the {ref}`section on weight-based uncertainties <weight_based_uncertainties>`.

Since this uncertainty is defined in the config instance, it's accessible at any point within the workflow.
Consequently, this uncertainty can be integrated into a statistical inference model like this:

```{literalinclude} ../../analysis_templates/cms_minimal/__cf_module_name__/inference/example.py
:dedent:
:language: python
:start-at: '# lumi'
:end-before: '# tune uncertainty'
```

(weight_based_uncertainties)=

## Weight-based uncertainties

This class of shifts consists of uncertainties that enter the analysis with the application of weights to events or individual objects.
This way, they can modify both the yield and the general shape of the distribution.
Popular examples for this type of uncertainty are scale factors to generally improve the compatibility between observed data and simulation before the measurement.
Since these factors are usually measured in dedicated control regions, they are also subject to both statistical and systematic uncertainties that need to be taken into account in the final statistical inference.
The following example is based on scale factors for muons.

columnflow provides the {py:class}`~columnflow.production.cms.muon.muon_weights` Producer to calculate the weights for scale factors.
This module creates three columns: one for weights derived from the nominal scale factors and columns where the scale factors are varied within the 68% confidence intervals of the measurement (up and down variations).
In order to derive templates that correspond to these variations, we need to tell the workflow to use the varied versions of this weight when creating the templates.

As explained in the [config section](building_blocks/config_objects.md#shift), the shift needs to be registered in the config.
As demonstrated in the cms analysis example, this can be done like so:

```{literalinclude} ../../analysis_templates/cms_minimal/__cf_module_name__/config/analysis___cf_short_name_lc__.py
:dedent:
:language: python
:start-at: '# event weights due to muon scale factors'
:end-before: '# add column aliases for shift mu'
```

Next, we define a column alias as auxiliary information for each of these shifts.
This can later be used to switch out the name of a given column (e.g. `muon_weight`) with a varied version of the quantity (e.g. `muon_weight_up`).
For convenience, we provide the {py:func}`~columnflow.config_util.add_shift_aliases` function to easily add the alias for both variations of the shift at once:

```{literalinclude} ../../analysis_templates/cms_minimal/__cf_module_name__/config/analysis___cf_short_name_lc__.py
:dedent:
:language: python
:start-at: '# add column aliases for shift mu'
:end-before: '# external files'
```

Note that this function makes some assumptions on the naming of the varied quantities and the shifts.

Finally, we can now configure the modules within the workflow to use this shift.
As mentioned above, this class of systematic uncertainties does not affect the selection acceptance.
Therefore, it is sufficient to consider the varied quantities in the {py:class}`~columnflow.tasks.histograms.CreateHistograms` task.

The calculation of the final event weight is handled by the {py:class}`~columnflow.weight.WeightProducer` instance you specify at command-line level.
Consequently, this module needs to be aware of the shift.
This can be done with the internal {py:attr}`~columnflow.columnar_util.TaskArrayFunction.shifts` set, see e.g. the analysis example for a weight producer:

```{literalinclude} ../../analysis_templates/cms_minimal/__cf_module_name__/weight/example.py
:dedent:
:language: python
:pyobject: example_init
```

Here, we use the {py:func}`~columnflow.config_util.get_shifts_from_sources` function to extract all shifts for source `mu` from the config inst.

Adding a source of uncertainty to this set denotes that this module is sensitive to this shift.
Whenever a shift that we defined above is requested, the WeightProducer will internally switch to the column defined in the `column_alias` auxiliary information that we defined earlier.
This way, the workflow is now fully aware of the shift.
You can test your implementation e.g. by running the {py:class}`~columnflow.tasks.plotting.PlotShiftedVariables1D` task.
For more information, see the [plotting overview](../task_overview/plotting_tasks.md).

To include this shift in a statistical inference model, we need to add it as a parameter (see CMS analysis example):

```{literalinclude} ../../analysis_templates/cms_minimal/__cf_module_name__/inference/example.py
:dedent:
:language: python
:start-at: '# muon weight uncertainty'
:end-before: '# jet energy correction uncertainty'
```

In particular, note that this type of uncertainty needs to define the keyword argument `config_shift_source` in the {py:func}`~columnflow.inference.InferenceModel.add_parameter` function to properly resolve the necessary shifts to run through the workflow.

## Uncertainties that modify the selection efficiency

A more complex type of shape uncertainty arises from effects that can modify vital quantities for analysis decisions.
One example for this class of uncertainties are calibrations of objects such as jets, which modify the four-momenta content of these objects.
Since these calibrations are generally subject to uncertainties, the corresponding variations can propagate throughout the analysis workflow and modify event and object selection acceptances.
The following will discuss the implementation of such uncertainties based on the energy calibration of jets at CMS.

columnflow provides the {py:class}`~columnflow.calibration.cms.jets.jec` Calibrator module that performs the jet energy calibration.
Apart from the nominal calibration, this module also saves the varied quantities, see e.g. the following code snippet:

Note that in this case, the exact list of source of uncertainty is provided by the config instance itself.
For more information, please consider reading through the {py:class}`~columnflow.calibration.cms.jets.jec` documentation and the config of the analysis example.

As explained in the {ref}`previous section <weight_based_uncertainties>`, the shifts need to be registered in the config instance:

```{literalinclude} ../../analysis_templates/cms_minimal/__cf_module_name__/config/analysis___cf_short_name_lc__.py
:dedent:
:language: python
:start-at: '# fake jet energy correction shift, with aliases flaged as "selection_dependent", i.e. the aliases'
:end-before: "# add column aliases for shift jec"
```

Note that in this case, we have also applied a `tag` to the shifts, which we can also later use to extract information about (groups of) uncertainties.

Next, the aliases for the varied columns are added in accordance with the naming scheme that the Calibrator module uses:

```{literalinclude} ../../analysis_templates/cms_minimal/__cf_module_name__/config/analysis___cf_short_name_lc__.py
:dedent:
:language: python
:start-at: "# add column aliases for shift jec"
:end-before: '# event weights due to muon scale factors'
```

Note that as opposed to the previous example, we use the `name` of the shift to construct the final name of the column containing the varied values.

Finally, we need to make the relevant modules aware of the shift.
As explained above, the jet energy calibration and its variations can have a non-trivial effect on the selection acceptance in the final analysis phase space.
Therefore, the module that defines this phase space needs to consider these variations.
This is generally done within the {py:class}`~columnflow.tasks.selection.SelectEvents` task, where {py:class}`~columnflow.selection.Selector` instances derive boolean masks based on criteria on for example the four-momenta of the objects.
In the scope of this example, the jet selection is the relevant module, and we add the shifts accordingly in the selection of the CMS analysis example:

```{literalinclude} ../../analysis_templates/cms_minimal/__cf_module_name__/selection/example.py
:dedent:
:language: python
:pyobject: jet_selection_init
```

Note that in this example, we chose to retrieve shifts with specific tags, which is useful to map groups of uncertainty to modules.

The workflow is thus fully integrating the jec shifts.
All tasks following the {py:class}`~columnflow.tasks.selection.SelectEvents` will be provided outputs that were obtained with the varied sample.
Including this type of shifts in the statistical inference model is very similar to the previous example:

```{literalinclude} ../../analysis_templates/cms_minimal/__cf_module_name__/inference/example.py
:dedent:
:language: python
:start-at: '# jet energy correction uncertainty'
:end-at: ')'
```

## Uncertainties with dedicated datasets

The final class of systematic variations that is considered in columnflow concerns shifts that require dedicated datasets.
Such measures can be necessary to account for complicated, untracktable effects, such as a modification of one of the parameters of a Monte Carlo generator early-on in the chain to generate simulated samples.
In such cases, it is common practice to generate dedicated samples that need to traverse the complete workflow.

Facilitating this situtation requires a certain amount of (columnflow-external) overhead.
Currently, the metadata database that stores the information about the datasets and processes needs to be defined with a {external+order:py:class}`~order.dataset.DatasetInfo` object.
This object needs to define the nominal as well as the varied datasets in the {external+order:py:attr}`~order.dataset.Dataset.info` attribute.
For more information, please consider the corresponding section in the [general columnflow structure](./structure.md#analysis-campaign-and-config) section.

The names as defined in this structure should correspond to the names of the final shifts in the analysis.
For example, the shift for the `tune` uncertainties can be defined as shown in the CMS analysis example:

```{literalinclude} ../../analysis_templates/cms_minimal/__cf_module_name__/config/analysis___cf_short_name_lc__.py
:dedent:
:language: python
:start-at: '# tune shifts are covered by dedicated, varied datasets, so tag the shift as "disjoint_from_nominal"'
:end-before: '# fake jet energy correction shift, with aliases flaged as "selection_dependent", i.e. the aliases'
```

Since the name of the dataset corresponds to the shift name, this will trigger the entire workflow independently from the nominal dataset and without applying any additional shifts.

Integrating this uncertainty into a statistical inference model works the same as shown previously:

```{literalinclude} ../../analysis_templates/cms_minimal/__cf_module_name__/inference/example.py
:dedent:
:language: python
:start-at: '# tune uncertainty'
:end-at: ')'
```
