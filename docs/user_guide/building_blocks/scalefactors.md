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
- ´get_sf_file´: function mapping external files to the scale factor correctionlib file
- `syst_key`: systematic variable key of thecorrection set
- `systematics`: tuples of systematic variable of the correction set and the postfix linked to the systematic (see example)
- `uses`: columns used for the weight calculation in correctionlib
- `input_func`: function that calculates a dictionary with input arrays for the correction set
- `mask_func`: function that calculates a mask to apply on the Leptons before calculating the weights

A baseline `LeptonWeightConfig` and examples for electron and muon scale factors are defined in [columnflow/production/cmsGhent/lepton.py](https://github.com/GhentAnalysis/columnflow/blob/scalefactor-development/columnflow/production/cmsGhent/lepton.py#L243-L290). To make a `lepton_weight` producer for a specific `LeptonWeightConfig` (eg the predefined `ElectronRecoBelow20WeightConfig`) to add to the `event_weights` producer of your analysis one can derive the `lepton_weight` producer in :
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

### b-tagging scale factors

### trigger scale factors


## experimental uncertainties

