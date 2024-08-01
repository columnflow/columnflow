# CMS specializations

(cms_specializations_section)=

## Tasks

### CreatePileUpWeights

{py:class}`~columnflow.tasks.cms.external.CreatePileUpWeights`:
This task is called because of the {py:class}`~columnflow.production.cms.pileup.pu_weights_from_columnflow` Producer, more specifically due to its requirements, in the {py:meth}`~columnflow.production.cms.pileup.pu_weights_from_columnflow_requires` method, which contains the following lines:

```python
from columnflow.tasks.cms.external import CreatePileupWeights
reqs["pu_weights"] = CreatePileupWeights.req(self.task)
```

This task creates the columns necessary for the pileup weights, which are used to reweight the simulated events to match the pileup distribution of the data, using different files provided by CMS and to be declared in the config.

With the advent of the correctionlib and its entries for pileup, this task is now outdated.
