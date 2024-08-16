import order as od

from columnflow.util import call_once_on_config

@call_once_on_config()
def add_vetoes(config: od.Config) -> None:
    config.x.veto = {
        'dy_lep_m10to50_amcatnlo': [
            {
                "event": 33098036,
                "luminosityBlock": 20170,
                "run": 1,
                "file": "/store/mc/RunIISummer20UL18NanoAODv9/DYJetsToLL_M-10to50_TuneCP5_13TeV-amcatnloFXFX-pythia8/NANOAODSIM/106X_upgrade2018_realistic_v16_L1v1-v1/50000/296CA60E-0122-2F4F-8B04-17DCF5E3E062.root", # noqa
            }
        ]
    }
