# Scale factors and uncertainties in columnflow

## theory uncertainties

Theory uncertainties discussed below are pdf, factorization and renormalization scale, and parton-shower uncertainties. These uncertainties require the weight variations for each uncertainty to be normalized by the sum of all weights of that variation for a sample. Therefore in `cf.SelectEvents` the producer `event_weights_to_normalize` in [production/weights.py](https://github.com/GhentAnalysis/columnflow/blob/2a24ab1017d55503d9b485ca56cdb28b767a250d/analysis_templates/ghent_template/__cf_module_name__/production/weights.py#L33C5-L33C31) is called and the sum of weights of each variation is stored in [selection stats](https://github.com/GhentAnalysis/columnflow/blob/2a24ab1017d55503d9b485ca56cdb28b767a250d/analysis_templates/ghent_template/__cf_module_name__/selection/stats.py#L57-L71). These selection stats are later combined over sample files in ´cf.MergeSelectionStats´. The event weight for each theory uncertainty variantions are only produced in ´cf.ProduceColumns´ using a helper function [normalized_weight_factory](https://github.com/GhentAnalysis/columnflow/blob/2a24ab1017d55503d9b485ca56cdb28b767a250d/analysis_templates/ghent_template/__cf_module_name__/production/normalized_weights.py#L22). The helper function creates a `producer` for each uncertainty in [weights.py](https://github.com/GhentAnalysis/columnflow/blob/2a24ab1017d55503d9b485ca56cdb28b767a250d/analysis_templates/ghent_template/__cf_module_name__/production/weights.py#L82-L96) that can produce normalized weights for each uncertainty variation. These producers are then called in [event_weights](https://github.com/GhentAnalysis/columnflow/blob/2a24ab1017d55503d9b485ca56cdb28b767a250d/analysis_templates/ghent_template/__cf_module_name__/production/weights.py#L110).

### pdf uncertainties

PDF uncertainties are calculated in [columnflow/production/cms/pdf.py](https://github.com/GhentAnalysis/columnflow/blob/scalefactor-development/columnflow/production/cms/pdf.py) and uses NanoAOD column `LHEPdfWeight`. The column `LHEPdfWeight` should have either 101 or 103 weights of pdf variations for each event. The nominal pdf variation is the first weight in the list, followed by the weights of 100 pdf variations. The last 2 weights (in available) are the two $α_s$ variations. The pdf weight uncertainty in columnflow is by default calculated as half the width of the central 68% CL and procued in the columns `pdf_weight_{up/down}`. Outliers of >50% uncertainty are removed. The option to store all pdf weights individually in addition to the uncertainty is also possible with attribute `store_all_weights=True`. $α_s$ variations are not yet included in columnflow.

[TOP RECOMMENDATION](https://twiki.cern.ch/twiki/bin/viewauth/CMS/TopSystematics#PDF)

### PS uncertainties

PS uncertainties are calculated in [columnflow/production/cmsGhent/parton_shower.py](https://github.com/GhentAnalysis/columnflow/blob/2a24ab1017d55503d9b485ca56cdb28b767a250d/columnflow/production/cmsGhent/parton_shower.py#L26) and uses NanoAOD column `PSWeight`. The column `PSWeight` has 4 weights corresponding to up- and down-variations of the renormalization scale for QCD emissions in initial-state and final-state radiation (`ISR=2 FSR=1`, `ISR=1 FSR=2`, `ISR=0.5 FSR=1` and `ISR=1 FSR=0.5`). The PS uncertainties correspond to the up- and down-variation of the renormalization scale for ISR and FSR individually and are saved in the columns `isr_weight_{up/down}` and `fsr_weight_{up/down}` respectively.

[TOP RECOMMENDATION](https://twiki.cern.ch/twiki/bin/viewauth/CMS/TopSystematics#Parton_shower_uncertainties)

### Factorization and renormalization scale uncertainties

Factorization and renormalization scale uncertainties are calculated in [columnflow/production/cmsGhent/scale.py](https://github.com/GhentAnalysis/columnflow/blob/2a24ab1017d55503d9b485ca56cdb28b767a250d/columnflow/production/cmsGhent/scale.py) and uses NanoAOD column `LHEScaleWeight`. The column `LHEScaleWeight` has 8 weights corresponding to all combinations of the nominal, up and down variation of the factorization and renormalization scale excluding the nominal case. The up- and down- variation of factorization scale, normalization scale, and the combination of the two are the uncertainties produced with the corresponding columns `muf_weight_{up/down}`, `mur_weight_{up/down}`, and `murmuf_weight_{up/down}`.

[TOP RECOMMENDATION](https://twiki.cern.ch/twiki/bin/viewauth/CMS/TopSystematics#Parton_shower_uncertainties)

## experimental scale factors

### lepton scale factors

Lepton scale factors are calculated in the `cf.ProduceColumns` task using the `lepton_weight` producer in [columnflow/production/cmsGhent/lepton.py](https://github.com/GhentAnalysis/columnflow/blob/scalefactor-development/columnflow/production/cmsGhent/lepton.py). The lepton scale factor parameters are configured using a custom class `LeptonWeightConfig` that can be given as attribute to the `lepton_weight` producer. the parameters of `LeptonWeightConfig` to customize the lepton scale factor to produce are:

- `year`: campaign year of the scale factors
- `weight_name`: name of the produced weight columns related to the scale factor
- `correction_set`: name of the correction set(s) in the correctionlib file
- `get_sf_file`: function mapping external files to the scale factor correctionlib file
- `syst_key`: systematic variable key of thecorrection set
- `systematics`: tuples of systematic variable of the correction set and the postfix linked to the systematic (see example)
- `uses`: columns used for the weight calculation in correctionlib
- `input_func`: function that calculates a dictionary with input arrays for the correction set
- `input_pars`: dictionary providing fixed inputs to the correction set
- `mask_func`: function that calculates a mask to apply on the Leptons before calculating the weights

A baseline `LeptonWeightConfig` and examples for electron and muon scale factors are defined in [columnflow/production/cmsGhent/lepton.py](https://github.com/GhentAnalysis/columnflow/blob/scalefactor-development/columnflow/production/cmsGhent/lepton.py#L243-L290). To make a `lepton_weight` producer for a specific `LeptonWeightConfig` (eg the predefined `ElectronRecoBelow20WeightConfig`) to add to the `event_weights` producer of your analysis one can derive the `lepton_weight` producer:

```python
...
from columnflow.production.cmsGhent.lepton import lepton_weight, ElectronRecoBelow20WeightConfig

lepton_weight_electronrecobelow20 = lepton_weight.derive("lepton_weight_electronrecobelow20",cls_dict={lepton_config=ElectronRecoBelow20WeightConfig})

@producer(
    uses={
        normalization_weights,
        lepton_weight_electronrecobelow20
    },
    produces={
        normalization_weights,
        lepton_weight_electronrecobelow20,
    },
    mc_only=True,
)
def event_weights(self: Producer, events: ak.Array, **kwargs) -> ak.Array:

    events = self[normalization_weights](events, **kwargs)
    events = self[lepton_weight_electronrecobelow20](events, **kwargs)

    return events
```

If an analysis has multiple lepton scale factors their `LeptonWeightConfig`'s can be combined in a list and the function mapping to this list in the config can be given to the `bundle_lepton_weights` producer as attribute. The `bundle_lepton_weights` producer derives a `lepton_weight` producer for each given`LeptonWeightConfig` and produces their scale factor. The default mapping for this list of `LeptonWeightConfig`'s is `config.x.lepton_weight_configs`.

The `bundle_lepton_weights` producer produces weight columns for each `LeptonWeightConfig´ seperately. However, one can combine these weight variations in one shift by adding shift aliases. An example here shows how to bundle the variation of electron reconstruction scale factors from different correction sets into one shift.

```python
config.add_shift(name="electron_reco_sf_up", id=42, type="shape")
config.add_shift(name="electron_reco_sf_down", id=43, type="shape")
add_shift_aliases(config, "electron_reco_sf", {
    "electron_weight_recobelow20": "electron_weight_recobelow20_{direction}",
    "electron_weight_reco20to75": "electron_weight_reco20to75_{direction}",
    "electron_weight_recoabove75": "electron_weight_recoabove75_{direction}",
})
```

### b-tagging scale factors

B-tagging scale factors are produces for fixed working points following [BTV recommendations](https://btv-wiki.docs.cern.ch/PerformanceCalibration/fixedWPSFRecommendations/). The configuration of the b-tagging scale factors is done with the class `BTagSFConfig` that has attributes:
- `correction_set`: b-tag algorithm name for which to produce scale factors (deepJet, particleNet, robustParticleTransformer)
- `sources`: uncertainty variations to include when producing the scale factors
- `jec_sources`: JEC uncertainty variations to propagate trough
B-tagging scale factors require the b-tagging efficiency in Monte Carlo to be measured in the analysis phase-space (without b-tagging requirements). For this in addition to the `BTagSFConfig` the production of b-tagging efficiencies requires two additional settings in the config:
- binning variables: the variables in which to bin the measured b-tagging efficiency. These variables are added to the config and their names stored in `config.x.default_btag_variables`.
- dataset groups: the Monte Carlo samples to combine for the b-tagging efficiency measurements. These dataset groups are of the type Dict[name,list(datasets)] and are stored in `config.x.btag_dataset_groups`. An example of dataset groups bundled by the number of top quarks in the process is shown here:

```python
config.x.btag_dataset_groups = {
    "ttx": [dataset.name for dataset in config.datasets if (
        dataset.name.startswith("tt") &
        (dataset.is_mc))],
    "tx": [dataset.name for dataset in config.datasets if (
        (dataset.name.startswith("t") |
        dataset.name.startswith("st")) &
        (not dataset.name.startswith("tt")) &
        (dataset.is_mc))],
    "ewk": [dataset.name for dataset in config.datasets if (
        ~(dataset.name.startswith("t") |
        dataset.name.startswith("st")) &
        (~dataset.name.startswith("tt")) &
        (dataset.is_mc))],
}
```

B-tagging scale factors require the b-tagging efficiency in Monte Carlo to be measured in the analysis phase-space (without b-tagging requirements). In columnflow this is achieved trough the custom `cf.BTagEfficiency` task in [columnflow/tasks/cmsGhent/btagefficiency.py](https://github.com/GhentAnalysis/columnflow/blob/scalefactor-development/columnflow/tasks/cmsGhent/btagefficiency.py). The `cf.BTagEfficiency` task requires histograms to be created in the `cf.SelectEvents` task using the `btag_efficiency_hists` helper function. This helper function can be included in the selection as shown in the [template selection](https://github.com/GhentAnalysis/columnflow/blob/9f4aa6629ef51bc568b02732c19cf55c5624d16b/analysis_templates/ghent_template/__cf_module_name__/selection/default.py#L238-L245). The `cf.BTagEfficiency` task is not required to be run in the command line as it is a requirement of the b-tagging weight producer in [columnflow/production/cmsGnet/btag_weights.py](https://github.com/GhentAnalysis/columnflow/blob/252a1c91a9a1b2238c6fcce219789e3733d1f432/columnflow/production/cmsGhent/btag_weights.py#L129) called `fixed_wp_btag_weights`. Here is an example of a `producer` to produce b-tagging scale factor weights and uncertainties for the `medium` working point:

```python
from columnflow.production.cmsGhent.btag_weights import jet_btag, fixed_wp_btag_weights

@producer(
    uses={fixed_wp_btag_weights, jet_btag},
    produces={fixed_wp_btag_weights, jet_btag},
)
def btag_weights(self: Producer, events: ak.Array, **kwargs) -> ak.Array:
    is_ctjet = abs(events.Jet.eta) < 2.4
    events = self[jet_btag](events, working_points=["L", "M", "T"], jet_mask=is_ctjet)
    events = self[fixed_wp_btag_weights](events, working_points=["M",], jet_mask=is_ctjet)
    return events
```

### trigger scale factors

TODO

## experimental uncertainties

TODO
