# coding: utf-8

"""
Example CMS meta data, structured with order.
See https://python-order.readthedocs.io/en/latest/quickstart.html.
"""

from scinum import Number, Correlation
from order import Campaign, Process, DatasetInfo


#
# constants
# (they should usually be kept in a centrally mainted repo or db,
#  such as https://github.com/uhh-cms/cmsdb)
#

# branching ratios
br_w = {}
br_w["had"] = Number(0.6741, {"br_w_had": 0.0027})
br_w["lep"] = 1 - br_w["had"]

br_ww = {
    "fh": br_w["had"] ** 2,
    "dl": br_w["lep"] ** 2,
    "sl": 2 * ((br_w["had"] * Correlation(br_w_had=-1)) * br_w["lep"]),
}


#
# processes and cross sections
# (they should usually be kept in a centrally mainted repo or db,
#  such as https://github.com/uhh-cms/cmsdb)
#

# data
process_data = Process(
    name="data",
    id=1,
    is_data=True,
    label="Data",
)
process_data_mu = process_data.add_process(
    name="data_mu",
    id=10,
    is_data=True,
    label=r"Data $\mu$",
)

# single-top
process_st_tchannel_t = Process(
    name="st_tchannel_t",
    id=3110,
    xsecs={
        13: Number(136.02, dict(
            scale=(4.09, 2.92),
            pdf=3.52,
            mtop=1.11,
        )),
    },
)

# ttbar
process_tt = Process(
    name="tt",
    id=2000,
    label=r"$t\bar{t}$ + Jets",
    color=(205, 0, 9),
    xsecs={
        13: Number(831.76, {
            "scale": (19.77, 29.20),
            "pdf": 35.06,
            "mtop": (23.18, 22.45),
        }),
    },
)
process_tt_sl = process_tt.add_process(
    name="tt_sl",
    id=2100,
    label=f"{process_tt.label}, SL",
    color=(205, 0, 9),
    xsecs={
        13: process_tt.get_xsec(13) * br_ww["sl"],
    },
)


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
    aux={"year": 2018},
)


#
# datasets
#

campaign_2018.add_dataset(
    name="data_mu_a",
    id=14046760,
    is_data=True,
    processes=[process_data_mu],
    keys=[
        "/SingleMuon/Run2018A-02Apr2020-v1/NANOAOD",
    ],
    n_files=2,  # 225,
    n_events=241608232,
)

campaign_2018.add_dataset(
    name="st_tchannel_t",
    id=14293903,
    processes=[process_st_tchannel_t],
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
    ),
)

campaign_2018.add_dataset(
    name="tt_sl",
    id=14235437,
    processes=[process_tt_sl],
    aux={"has_top": True, "is_ttbar": True, "event_weights": ["top_pt_weight"]},
    info=dict(
        nominal=DatasetInfo(
            keys=[
                "/TTToSemiLeptonic_TuneCP5_13TeV-powheg-pythia8/RunIISummer20UL18NanoAODv9-106X_upgrade2018_realistic_v16_L1v1-v1/NANOAODSIM",  # noqa
            ],
            n_files=2,  # 391,
            n_events=476408000,
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
