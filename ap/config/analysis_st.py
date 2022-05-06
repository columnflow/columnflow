# coding: utf-8

"""
Configuration of the single top analysis.
"""

import re

from order import Analysis, Shift

import ap.config.processes as procs
from ap.config.campaign_2018 import campaign_2018


#
# the main analysis object
#

analysis_st = Analysis(
    name="analysis_st",
    id=1,
)

# analysis-global versions
analysis_st.set_aux("versions", {
})

# cmssw sandboxes that should be bundled for remote jobs in case they are needed
analysis_st.set_aux("cmssw_sandboxes", [
    # "cmssw_default.sh",
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


# helper to add column aliases for both shifts of a source
def add_aliases(shift_source, aliases):
    for direction in ["up", "down"]:
        shift = config_2018.get_shift(Shift.join_name(shift_source, direction))
        # format keys and values
        inject_shift = lambda s: re.sub(r"\{([^_])", r"{_\1", s).format(**shift.__dict__)
        _aliases = {inject_shift(key): inject_shift(value) for key, value in aliases.items()}
        # extend existing or register new column aliases
        shift.set_aux("column_aliases", shift.get_aux("column_aliases", {})).update(_aliases)


# register shifts
config_2018.add_shift(name="nominal", id=0)
config_2018.add_shift(name="tune_up", id=1, type="shape")
config_2018.add_shift(name="tune_down", id=2, type="shape")
config_2018.add_shift(name="hdamp_up", id=3, type="shape")
config_2018.add_shift(name="hdamp_down", id=4, type="shape")
config_2018.add_shift(name="jec_up", id=5, type="shape")
config_2018.add_shift(name="jec_down", id=6, type="shape")
add_aliases("jec", {"Jet_pt": "Jet_pt_{name}", "Jet_mass": "Jet_mass_{name}"})


# file merging values
# key -> dataset -> files per branch (-1 or not present = all)
config_2018.set_aux("file_merging", {
})

# versions per task and potentially other keys
config_2018.set_aux("versions", {
})
