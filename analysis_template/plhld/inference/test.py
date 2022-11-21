# coding: utf-8

"""
Test inference model.
"""

from columnflow.inference import inference_model, ParameterType, ParameterTransformation


@inference_model
def test(self):

    #
    # categories
    #

    self.add_category(
        "cat1",
        category="1e",
        variable="ht",
        mc_stats=True,
        data_datasets=["data_mu_a"],
    )
    self.add_category(
        "cat2",
        category="1mu",
        variable="muon1_pt",
        mc_stats=True,
        data_datasets=["data_mu_a"],
    )

    #
    # processes
    #

    self.add_process(
        "ST",
        process="st_tchannel",
        signal=True,
        mc_datasets=["st_tchannel_t", "st_tchannel_tbar"],
    )
    self.add_process(
        "TT",
        process="tt_sl",
        mc_datasets=["tt_sl"],

    )

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
            type=ParameterType.rate_gauss,
            effect=lumi.get(names=unc_name, direction=("down", "up"), factor=True),
            transformations=[ParameterTransformation.symmetrize],
        )

    # minbias xs
    self.add_parameter(
        "CMS_pileup",
        type=ParameterType.shape,
        shift_source="minbias_xs",
    )
    self.add_parameter_to_group("CMS_pileup", "experiment")

    # and again minbias xs, but encoded as symmetrized rate
    self.add_parameter(
        "CMS_pileup2",
        type=ParameterType.rate_uniform,
        transformations=[ParameterTransformation.effect_from_shape, ParameterTransformation.symmetrize],
        shift_source="minbias_xs",
    )
    self.add_parameter_to_group("CMS_pileup2", "experiment")

    # a custom asymmetric uncertainty that is converted from rate to shape
    self.add_parameter(
        "QCDscale_ST",
        process="ST",
        type=ParameterType.shape,
        transformations=[ParameterTransformation.effect_from_rate],
        effect=(0.5, 1.1),
    )

    # test
    self.add_parameter(
        "QCDscale_ttbar",
        category="cat1",
        process="TT",
        type=ParameterType.rate_uniform,
    )
    self.add_parameter(
        "QCDscale_ttbar_norm",
        category="cat1",
        process="TT",
        type=ParameterType.rate_unconstrained,
    )

    #
    # post-processing
    #

    self.cleanup()
