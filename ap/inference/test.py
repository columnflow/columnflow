# coding: utf-8

"""
Dummy inference model.
"""

from ap.inference import InferenceModel, ParameterType


# create a new cached model instance
m = InferenceModel.new("test")


# configure hook
@m.on_config
def configure(self):
    #
    # categories
    #

    self.add_category("cat1", variable="ht", mc_stats=True, source="1e", data=["data_mu_a"])
    self.add_category("cat2", variable="muon1_pt", mc_stats=True, source="1mu", data=["data_mu_a"])

    #
    # processes
    #

    self.add_process("ST", signal=True, source="st_tchannel", mc=["st_tchannel_t", "st_tchannel_tbar"])
    self.add_process("TT", source="tt_sl", mc=["tt_sl"])

    #
    # parameters
    #

    # groups
    self.add_parameter_group("experiment")
    self.add_parameter_group("theory")

    # lumi
    lumi = self.config_inst.x.luminosity
    for unc_name in lumi.uncertainties:
        self.add_parameter(
            unc_name,
            type=ParameterType.rate,
            effect=lumi.get(names=unc_name, direction=("down", "up"), factor=True),
        )
        self.symmetrize_parameter_effect(unc_name)

    # minbias xs
    self.add_parameter("CMS_pileup", type=ParameterType.shape, source="minbias_xs")
    self.add_parameter_to_group("CMS_pileup", "experiment")

    #
    # post-processing
    #

    self.cleanup()
