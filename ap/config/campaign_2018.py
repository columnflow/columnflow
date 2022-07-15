# coding: utf-8

"""
Common, analysis independent definition of the 2018 data-taking campaign.
See https://python-order.readthedocs.io/en/latest/quickstart.html#analysis-campaign-and-config.
"""

from order import Campaign, DatasetInfo, uniqueness_context

import ap.config.processes as procs


#
# campaigns
# (the overall container of the 2018 data-taking period)
#

# campaign
campaign_2018 = Campaign(
    name="run2_pp_2018",
    id=1,
    ecm=13,
    bx=25,
)


#
# datasets
#

# place datasets in a uniqueness context so that names and ids won't collide with other campaigns
with uniqueness_context(campaign_2018.name):
    campaign_2018.add_dataset(
        name="data_mu_a",
        id=200,
        is_data=True,
        processes=[procs.process_data_mu],
        keys=[
            "/SingleMuon/Run2018A-02Apr2020-v1/NANOAOD",
        ],
        n_files=2,  # 225,
        n_events=241608232,
    )

    campaign_2018.add_dataset(
        name="st_tchannel_t",
        id=1,
        processes=[procs.process_st_tchannel_t],
        aux={"has_top": True},
        info=dict(
            nominal=DatasetInfo(
                keys=[
                    "/ST_t-channel_top_4f_InclusiveDecays_TuneCP5_13TeV-powheg-madspin-pythia8/RunIISummer20UL18NanoAODv9-106X_upgrade2018_realistic_v16_L1v1-v1/NANOAODSIM",  # noqa
                ],
                n_files=2,  # 149,
                n_events=178336000,
            ),
            tune_up=DatasetInfo(
                keys=[
                    "/ST_t-channel_top_4f_InclusiveDecays_TuneCP5up_13TeV-powheg-madspin-pythia8/RunIISummer20UL18NanoAODv9-106X_upgrade2018_realistic_v16_L1v1-v1/NANOAODSIM",  # noqa
                ],
                n_files=2,  # 90,
                n_events=75582000,
            ),
            tune_down=DatasetInfo(
                keys=[
                    "/ST_t-channel_top_4f_InclusiveDecays_TuneCP5down_13TeV-powheg-madspin-pythia8/RunIISummer20UL18NanoAODv9-106X_upgrade2018_realistic_v16_L1v1-v1/NANOAODSIM",  # noqa
                ],
                n_files=2,  # 57,
                n_events=74992000,
            ),
            hdamp_up=DatasetInfo(
                keys=[
                    "/ST_t-channel_top_4f_hdampup_InclusiveDecays_TuneCP5_13TeV-powheg-madspin-pythia8/RunIISummer20UL18NanoAODv9-106X_upgrade2018_realistic_v16_L1v1-v1/NANOAODSIM",  # noqa
                ],
                n_files=2,  # 62,
                n_events=59030000,
            ),
            hdamp_down=DatasetInfo(
                keys=[
                    "/ST_t-channel_top_4f_hdampdown_InclusiveDecays_TuneCP5_13TeV-powheg-madspin-pythia8/RunIISummer20UL18NanoAODv9-106X_upgrade2018_realistic_v16_L1v1-v1/NANOAODSIM",  # noqa
                ],
                n_files=2,  # 104,
                n_events=73014000,
            ),
        ),
    )

    campaign_2018.add_dataset(
        name="st_tchannel_tbar",
        id=2,
        processes=[procs.process_st_tchannel_tbar],
        aux={"has_top": True},
        info=dict(
            nominal=DatasetInfo(
                keys=[
                    "/ST_t-channel_antitop_4f_InclusiveDecays_TuneCP5_13TeV-powheg-madspin-pythia8/RunIISummer20UL18NanoAODv9-106X_upgrade2018_realistic_v16_L1v1-v1/NANOAODSIM",  # noqa
                ],
                n_files=2,  # 130,
                n_events=95627000,
            ),
            tune_up=DatasetInfo(
                keys=[
                    "/ST_t-channel_antitop_4f_InclusiveDecays_TuneCP5up_13TeV-powheg-madspin-pythia8/RunIISummer20UL18NanoAODv9-106X_upgrade2018_realistic_v16_L1v1-v1/NANOAODSIM",  # noqa
                ],
                n_files=2,  # 44,
                n_events=36424000,
            ),
            tune_down=DatasetInfo(
                keys=[
                    "/ST_t-channel_antitop_4f_InclusiveDecays_TuneCP5down_13TeV-powheg-madspin-pythia8/RunIISummer20UL18NanoAODv9-106X_upgrade2018_realistic_v16_L1v1-v1/NANOAODSIM",  # noqa
                ],
                n_files=2,  # 55,
                n_events=36144000,
            ),
            hdamp_up=DatasetInfo(
                keys=[
                    "/ST_t-channel_antitop_4f_hdampup_InclusiveDecays_TuneCP5_13TeV-powheg-madspin-pythia8/RunIISummer20UL18NanoAODv9-106X_upgrade2018_realistic_v16_L1v1-v1/NANOAODSIM",  # noqa
                ],
                n_files=2,  # 30,
                n_events=37268000,
            ),
            hdamp_down=DatasetInfo(
                keys=[
                    "/ST_t-channel_antitop_4f_hdampdown_InclusiveDecays_TuneCP5_13TeV-powheg-madspin-pythia8/RunIISummer20UL18NanoAODv9-106X_upgrade2018_realistic_v16_L1v1-v1/NANOAODSIM",  # noqa
                ],
                n_files=2,  # 30,
                n_events=37919000,
            ),
        ),
    )

    campaign_2018.add_dataset(
        name="st_twchannel_t",
        id=3,
        processes=[procs.process_st_twchannel_t],
        aux={"has_top": True},
        info=dict(
            nominal=DatasetInfo(
                keys=[
                    "/ST_tW_top_5f_inclusiveDecays_TuneCP5_13TeV-powheg-pythia8/RunIISummer20UL18NanoAODv9-106X_upgrade2018_realistic_v16_L1v1-v2/NANOAODSIM",  # noqa
                ],
                n_files=2,  # 52,
                n_events=7956000,
            ),
        ),
    )

    campaign_2018.add_dataset(
        name="st_twchannel_tbar",
        id=4,
        processes=[procs.process_st_twchannel_tbar],
        aux={"has_top": True},
        info=dict(
            nominal=DatasetInfo(
                keys=[
                    "/ST_tW_antitop_5f_inclusiveDecays_TuneCP5_13TeV-powheg-pythia8/RunIISummer20UL18NanoAODv9-106X_upgrade2018_realistic_v16_L1v1-v2/NANOAODSIM",  # noqa
                ],
                n_files=2,  # 23,
                n_events=7749000,
            ),
        ),
    )

    campaign_2018.add_dataset(
        name="tt_sl",
        id=5,
        processes=[procs.process_tt_sl],
        aux={"has_top": True, "is_ttbar": True, "event_weights": ["top_pt_weight"]},
        info=dict(
            nominal=DatasetInfo(
                keys=[
                    "/TTToSemiLeptonic_TuneCP5_13TeV-powheg-pythia8/RunIISummer20UL18NanoAODv9-106X_upgrade2018_realistic_v16_L1v1-v1/NANOAODSIM",  # noqa
                ],
                n_files=2,  # 391,
                n_events=476408000,
            ),
            tune_up=DatasetInfo(
                keys=[
                    "/TTToSemiLeptonic_TuneCP5up_13TeV-powheg-pythia8/RunIISummer20UL18NanoAODv9-106X_upgrade2018_realistic_v16_L1v1-v1/NANOAODSIM",  # noqa
                ],
                n_files=2,  # 165,
                n_events=199394000,
            ),
            tune_down=DatasetInfo(
                keys=[
                    "/TTToSemiLeptonic_TuneCP5down_13TeV-powheg-pythia8/RunIISummer20UL18NanoAODv9-106X_upgrade2018_realistic_v16_L1v1-v1/NANOAODSIM",  # noqa
                ],
                n_files=2,  # 156,
                n_events=189888000,
            ),
            hdamp_up=DatasetInfo(
                keys=[
                    "/TTToSemiLeptonic_hdampUP_TuneCP5_13TeV-powheg-pythia8/RunIISummer20UL18NanoAODv9-106X_upgrade2018_realistic_v16_L1v1-v1/NANOAODSIM",  # noqa
                ],
                n_files=2,  # 168,
                n_events=197814000,
            ),
            hdamp_down=DatasetInfo(
                keys=[
                    "/TTToSemiLeptonic_hdampDOWN_TuneCP5_13TeV-powheg-pythia8/RunIISummer20UL18NanoAODv9-106X_upgrade2018_realistic_v16_L1v1-v1/NANOAODSIM",  # noqa
                ],
                n_files=2,  # 171,
                n_events=193212000,
            ),
        ),
    )

    campaign_2018.add_dataset(
        name="tt_dl",
        id=6,
        processes=[procs.process_tt_dl],
        aux={"has_top": True, "is_ttbar": True, "event_weights": ["top_pt_weights"]},
        info=dict(
            nominal=DatasetInfo(
                keys=[
                    "/TTTo2L2Nu_TuneCP5_13TeV-powheg-pythia8/RunIISummer20UL18NanoAODv9-106X_upgrade2018_realistic_v16_L1v1-v1/NANOAODSIM",  # noqa
                ],
                n_files=2,  # 155,
                n_events=145020000,
            ),
            tune_up=DatasetInfo(
                keys=[
                    "/TTTo2L2Nu_TuneCP5up_13TeV-powheg-pythia8/RunIISummer20UL18NanoAODv9-106X_upgrade2018_realistic_v16_L1v1-v1/NANOAODSIM",  # noqa
                ],
                n_files=2,  # 59,
                n_events=57064000,
            ),
            tune_down=DatasetInfo(
                keys=[
                    "/TTTo2L2Nu_TuneCP5down_13TeV-powheg-pythia8/RunIISummer20UL18NanoAODv9-106X_upgrade2018_realistic_v16_L1v1-v1/NANOAODSIM",  # noqa
                ],
                n_files=2,  # 59,
                n_events=59853000,
            ),
            hdamp_up=DatasetInfo(
                keys=[
                    "/TTTo2L2Nu_hdampUP_TuneCP5_13TeV-powheg-pythia8/RunIISummer20UL18NanoAODv9-106X_upgrade2018_realistic_v16_L1v1-v1/NANOAODSIM",  # noqa
                ],
                n_files=2,  # 44,
                n_events=54048000,
            ),
            hdamp_down=DatasetInfo(
                keys=[
                    "/TTTo2L2Nu_hdampDOWN_TuneCP5_13TeV-powheg-pythia8/RunIISummer20UL18NanoAODv9-106X_upgrade2018_realistic_v16_L1v1-v1/NANOAODSIM",  # noqa
                ],
                n_files=2,  # 58,
                n_events=59958000,
            ),
        ),
    )

    campaign_2018.add_dataset(
        name="tt_fh",
        id=7,
        processes=[procs.process_tt_fh],
        aux={"has_top": True, "is_ttbar": True, "event_weights": ["top_pt_weights"]},
        info=dict(
            nominal=DatasetInfo(
                keys=[
                    "/TTToHadronic_TuneCP5_13TeV-powheg-pythia8/RunIISummer20UL18NanoAODv9-106X_upgrade2018_realistic_v16_L1v1-v1/NANOAODSIM",  # noqa
                ],
                n_files=2,  # 339,
                n_events=334206000,
            ),
            tune_up=DatasetInfo(
                keys=[
                    "/TTToHadronic_TuneCP5up_13TeV-powheg-pythia8/RunIISummer20UL18NanoAODv9-106X_upgrade2018_realistic_v16_L1v1-v1/NANOAODSIM",  # noqa
                ],
                n_files=2,  # 114,
                n_events=136785999,
            ),
            tune_down=DatasetInfo(
                keys=[
                    "/TTToHadronic_TuneCP5down_13TeV-powheg-pythia8/RunIISummer20UL18NanoAODv9-106X_upgrade2018_realistic_v16_L1v1-v1/NANOAODSIM",  # noqa
                ],
                n_files=2,  # 131,
                n_events=139688000,
            ),
            hdamp_up=DatasetInfo(
                keys=[
                    "/TTToHadronic_hdampUP_TuneCP5_13TeV-powheg-pythia8/RunIISummer20UL18NanoAODv9-106X_upgrade2018_realistic_v16_L1v1-v1/NANOAODSIM",  # noqa
                ],
                n_files=2,  # 126,
                n_events=138026000,
            ),
            hdamp_down=DatasetInfo(
                keys=[
                    "/TTToHadronic_hdampDOWN_TuneCP5_13TeV-powheg-pythia8/RunIISummer20UL18NanoAODv9-106X_upgrade2018_realistic_v16_L1v1-v1/NANOAODSIM",  # noqa
                ],
                n_files=2,  # 113,
                n_events=139490000,
            ),
        ),
    )
