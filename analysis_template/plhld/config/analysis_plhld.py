# coding: utf-8

"""
Configuration of the plc2hldr analysis.
"""

import os

import law
import order as od


thisdir = os.path.dirname(os.path.abspath(__file__))

#
# the main analysis object
#

analysis_plhld = od.Analysis(
    name="analysis_plhld",
    id=1,
)

# analysis-global versions
analysis_plhld.set_aux("versions", {
})

# files of sandboxes that might be required by remote tasks
# (used in cf.HTCondorWorkflow)
analysis_plhld.x.bash_sandboxes = [
    "$CF_BASE/sandboxes/cf_prod.sh",
    "$CF_BASE/sandboxes/venv_columnar.sh",
    # "$PLHLD_BASE/sandboxes/venv_columnar_tf.sh",
]

# cmssw sandboxes that should be bundled for remote jobs in case they are needed
analysis_plhld.set_aux("cmssw_sandboxes", [
    # "$CF_BASE/sandboxes/cmssw_default.sh",
])


# clear the list when cmssw bundling is disabled
if not law.util.flag_to_bool(os.getenv("PLHLD_BUNDLE_CMSSW", "1")):
    del analysis_plhld.x.cmssw_sandboxes[:]

# config groups for conveniently looping over certain configs
# (used in wrapper_factory)
analysis_plhld.set_aux("config_groups", {})

# trailing imports for different configs
import plhld.config.config_2017  # noqa
