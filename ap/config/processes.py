# coding: utf-8

"""
Physics processes and cross sections.
"""

from order import Process
from scinum import Number

import ap.config.constants as const


#
# data
#

process_data = Process(
    name="data",
    id=1,
    is_data=True,
    label="data",
)


#
# single-top
#
# https://twiki.cern.ch/twiki/bin/viewauth/CMS/SingleTopSigma?rev=12#Single_Top_Cross_sections_at_13
#

process_st = Process(
    name="st",
    id=3000,
    label=r"Single $t$/$\bar{t}$",
    color=(2, 210, 209),
)

process_st_tchannel = process_st.add_process(
    name="st_tchannel",
    id=3100,
    label=f"{process_st.label}, t-channel",
    xsecs={
        13: Number(216.99, dict(
            scale=(6.62, 4.64),
            pdf=6.16,
            mtop=1.81,
        )),
    },
)

process_st_tchannel_t = process_st_tchannel.add_process(
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

process_st_tchannel_tbar = process_st_tchannel.add_process(
    name="st_tchannel_tbar",
    id=3120,
    xsecs={
        13: Number(80.95, dict(
            scale=(2.53, 1.71),
            pdf=3.18,
            mtop=(0.71, 0.70),
        )),
    },
)

process_st_twchannel = process_st.add_process(
    name="st_twchannel",
    id=3200,
    label=f"{process_st.label}, tW-channel",
    xsecs={
        13: Number(71.7, dict(
            scale=1.8,
            pdf=3.4,
        )),
    },
)

process_st_twchannel_t = process_st_twchannel.add_process(
    name="st_twchannel_t",
    id=3210,
    xsecs={
        13: Number(35.85, dict(
            scale=0.90,
            pdf=1.70,
        )),
    },
)

process_st_twchannel_tbar = process_st_twchannel.add_process(
    name="st_twchannel_tbar",
    id=3220,
    xsecs={
        13: Number(35.85, dict(
            scale=0.90,
            pdf=1.70,
        )),
    },
)

process_st_schannel = process_st.add_process(
    name="st_schannel",
    id=3300,
    label=f"{process_st.label}, s-channel",
    xsecs={
        13: Number(11.36, dict(
            scale=0.18,
            pdf=(0.40, 0.45),
        )),
    },
)

process_st_schannel_t = process_st_schannel.add_process(
    name="st_schannel_t",
    id=3310,
    xsecs={
        13: Number(7.20, dict(
            scale=0.13,
            pdf=(0.29, 0.23),
        )),
    },
)

process_st_schannel_t_lep = process_st_schannel_t.add_process(
    name="st_schannel_t_lep",
    id=3311,
    xsecs={
        13: process_st_schannel_t.get_xsec(13) * const.br_w.lep,
    },
)

process_st_schannel_tbar = process_st_schannel.add_process(
    name="st_schannel_tbar",
    id=3320,
    xsecs={
        13: Number(4.16, dict(
            scale=0.05,
            pdf=(0.12, 0.23),
        )),
    },
)

process_st_schannel_tbar_lep = process_st_schannel_tbar.add_process(
    name="st_schannel_tbar_lep",
    id=3321,
    xsecs={
        13: process_st_schannel_tbar.get_xsec(13) * const.br_w.lep,
    },
)

# define the combined single top cross section as the sum of the three channels
process_st.set_xsec(13, process_st_tchannel.get_xsec(13) + process_st_twchannel.get_xsec(13) +
    process_st_schannel.get_xsec(13))


#
# ttbar
#
# https://twiki.cern.ch/twiki/bin/view/LHCPhysics/TtbarNNLO?rev=16#Top_quark_pair_cross_sections_at
# use mtop = 172.5 GeV, see
# https://twiki.cern.ch/twiki/bin/view/CMS/TopMonteCarloSystematics?rev=7#mtop
#

process_tt = Process(
    name="tt",
    id=2000,
    label=r"$t\bar{t}$ + Jets",
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
        13: process_tt.get_xsec(13) * const.br_ww.sl,
    },
)

process_tt_dl = process_tt.add_process(
    name="tt_dl",
    id=2200,
    label=f"{process_tt.label}, DL",
    color=(235, 230, 10),
    xsecs={
        13: process_tt.get_xsec(13) * const.br_ww.dl,
    },
)

process_tt_fh = process_tt.add_process(
    name="tt_fh",
    id=2300,
    label=f"{process_tt.label}, FH",
    color=(255, 153, 0),
    xsecs={
        13: process_tt.get_xsec(13) * const.br_ww.fh,
    },
)
