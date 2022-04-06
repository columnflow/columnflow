# coding: utf-8

"""
Configuration of the single top analysis.
"""

import order as od

import ap.config.processes as procs
from ap.config.campaign_2018 import campaign_2018


#
# the main analysis object
#

analysis_st = od.Analysis(
    name="analysis_st",
    id=1,
)

# analysis-global versions
analysis_st.set_aux("versions", {
})

# cmssw sandboxes that should be bundled for remote jobs in case they are needed
analysis_st.set_aux("cmssw_sandboxes", [
    "cmssw_default.sh",
])


#
# 2018 standard config
#

# create a config by passing the campaign, so id and name will be identical
config_2018 = analysis_st.add_config(campaign_2018)

# add processes we are interested in
config_2018.add_process(procs.process_st)
config_2018.add_process(procs.process_tt)

# add datasets we need to study
dataset_names = [
    "st_tchannel_t",
    "st_tchannel_tbar",
    "st_twchannel_t",
    "st_twchannel_tbar",
    "tt_sl",
    "tt_dl",
    "tt_fh",
]
for dataset_name in dataset_names:
    config_2018.add_dataset(campaign_2018.get_dataset(dataset_name))

# file merging values
# key -> dataset -> files per branch (-1 or not present = all)
config_2018.set_aux("file_merging", {
})

# versions per task and potentially other keys
config_2018.set_aux("versions", {
})
