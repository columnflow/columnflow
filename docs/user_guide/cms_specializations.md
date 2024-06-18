# CMS specializations

(cms_specializations_section)=

## Tasks

### CreatePileUpWeights

{py:class}`~columnflow.tasks.cms.external.CreatePileUpWeights`: This task is called because of the
{py:class}`~columnflow.production.cms.pileup.pu_weight` Producer, more specifically due to its
requirements, in the {py:meth}`~columnflow.production.cms.pileup.pu_weight_requires` method, which
contains the following lines:

```python
from columnflow.tasks.cms.external import CreatePileupWeights
reqs["pu_weights"] = CreatePileupWeights.req(self.task)
```

TODO: purpose of the task? Why CMS-specific?
