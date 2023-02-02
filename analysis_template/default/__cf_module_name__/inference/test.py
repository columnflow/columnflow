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
        config_category="incl",
        config_variable="ht",
        config_data_datasets=["data_mu_b"],
        mc_stats=True,
    )
    self.add_category(
        "cat2",
        config_category="2j",
        config_variable="jet1_pt",
        # fake data from TT
        data_from_processes=["TT"],
        mc_stats=True,
    )

    #
    # processes
    #

    self.add_process(
        "HH",
        is_signal=True,
        config_process="hh_ggf_bbtautau",
        config_mc_datasets=["hh_ggf_bbtautau_madgraph"],
    )
    self.add_process(
        "TT",
        config_process="tt",
        config_mc_datasets=["tt_sl_powheg"],
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
        config_shift_source="minbias_xs",
    )
    self.add_parameter_to_group("CMS_pileup", "experiment")

    # and again minbias xs, but encoded as a symmetrized rate
    self.add_parameter(
        "CMS_pileup2",
        type=ParameterType.rate_uniform,
        transformations=[ParameterTransformation.effect_from_shape, ParameterTransformation.symmetrize],
        config_shift_source="minbias_xs",
    )
    self.add_parameter_to_group("CMS_pileup2", "experiment")

    # a custom asymmetric uncertainty that is converted from rate to shape
    self.add_parameter(
        "QCDscale_ttbar",
        process="TT",
        type=ParameterType.shape,
        transformations=[ParameterTransformation.effect_from_rate],
        effect=(0.5, 1.1),
    )

    # test
    self.add_parameter(
        "QCDscale_ttbar_uniform",
        category="cat1",
        process="TT",
        type=ParameterType.rate_uniform,
    )
    self.add_parameter(
        "QCDscale_ttbar_unconstrained",
        category="cat2",
        process="TT",
        type=ParameterType.rate_unconstrained,
    )

    #
    # post-processing
    #

    self.cleanup()


@inference_model
def test_no_shifts(self):
    # same initialization as "test" above
    test.init_func.__get__(self, self.__class__)()

    #
    # remove all parameters that require a shift source
    #

    for category_name, process_name, parameter in self.iter_parameters():
        if parameter.config_shift_source:
            self.remove_parameter(parameter.name, process=process_name, category=category_name)

    #
    # post-processing
    #

    self.cleanup()
