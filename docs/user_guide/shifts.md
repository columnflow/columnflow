
# Systematic Uncertainties

Many aspects of the work of analyzers revolves around systematic uncertainties on the modeling of the simulation or in the conditions during data taking.
columnflow is designed to support various types of systematic uncertainties, also referred to as *shifts*.

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
For an example on the latter please see {ref}`section on weight-based uncertainties <weight_based_uncertainties>`.

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
Popular examples for this type of uncertainty are scale factors to generally improve the compatibility between observed data and simulation before the measurement.
Since these factors are usually measured in dedicated control regions, they are also subject to both statistical and systematic uncertainties that need to be taken into account in the final statistical inference.
The following example is based on scale factors for muons.

columnflow provides the {py:class}`~columnflow.production.cms.muon.muon_weights` Producer to calculate the weights for scale factors.
This module creates three columns: one for weights derived from the nominal scale factors and columns where the scale factors are varied within the 68% confidence intervals of the measurement (up and down variations).
In order to derive templates that correspond to these variations, we need to tell the workflow that the varied versions of this weight when creating the templates.

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
As mentioned above, this class of systematic uncertainties does not change any selection efficiencies.
Therefore, it is sufficient to consider the varied quantities in the {py:class}`~columnflow.tasks.histograms.CreateHistograms` task.


