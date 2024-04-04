# coding: utf-8

"""
Configuration of the ___cf_short_name_lc__ analysis.
"""

import os

import law
import order as od


thisdir = os.path.dirname(os.path.abspath(__file__))


def create_analysis(
    name,
    id,
    **kwargs,
) -> od.Analysis:

    #
    # the main analysis object
    #

    analysis_inst = od.Analysis(
        name=name,
        id=id,
        **kwargs,
    )

    # analysis-global versions
    analysis_inst.set_aux("versions", {
    })

    # files of sandboxes that might be required by remote tasks
    # (used in cf.HTCondorWorkflow)
    analysis_inst.x.bash_sandboxes = [
        "$CF_BASE/sandboxes/cf.sh",
    ]
    default_sandbox = law.Sandbox.new(law.config.get("analysis", "default_columnar_sandbox"))
    if default_sandbox.sandbox_type == "bash" and default_sandbox.name not in analysis_inst.x.bash_sandboxes:
        analysis_inst.x.bash_sandboxes.append(default_sandbox.name)
    # cmssw sandboxes that should be bundled for remote jobs in case they are needed
    analysis_inst.x.cmssw_sandboxes = [
        "$CF_BASE/sandboxes/cmssw_default.sh",
    ]

    # config groups for conveniently looping over certain configs
    # (used in wrapper_factory)
    analysis_inst.set_aux("config_groups", {})

    #
    # import campaigns and load configs
    #

    from __cf_short_name_lc__.config.config___cf_short_name_lc__ import add_config
    from cmsdb.campaigns.run2_2018_nano_v9 import campaign_run2_2018_nano_v9

    # default config
    c18 = add_config(  # noqa
        analysis_inst,
        campaign_run2_2018_nano_v9.copy(),
        config_name="c18",
        config_id=2,
    )

    # config with limited number of files
    l18 = add_config(  # noqa
        analysis_inst,
        campaign_run2_2018_nano_v9.copy(),
        config_name="l18",
        config_id=12,
        limit_dataset_files=2,
    )

    return analysis_inst
