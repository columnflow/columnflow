# coding: utf-8

"""
Example inference model.
"""

from columnflow.inference import inference_model, ParameterType, ParameterTransformation


@inference_model
def example(self):

    #
    # categories
    #

    self.add_category(
        "cat1",
        config_category="incl",
        config_variable="jet1_pt",
        config_data_datasets=["data_mu_b"],
        mc_stats=True,
    )
    self.add_category(
        "cat2",
        config_category="2j",
        config_variable="jet1_eta",
        # fake data from TT
        data_from_processes=["TT"],
        mc_stats=True,
    )

    #
    # processes
    #

    self.add_process(
        "ST",
        is_signal=True,
        config_process="st",
        config_mc_datasets=["st_tchannel_t_powheg"],
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

    # tune uncertainty
    self.add_parameter(
        "tune",
        process="TT",
        type=ParameterType.shape,
        config_shift_source="tune",
    )

    # muon weight uncertainty
    self.add_parameter(
        "mu",
        process=["ST", "TT"],
        type=ParameterType.shape,
        config_shift_source="mu",
    )

    # jet energy correction uncertainty
    self.add_parameter(
        "jec",
        process=["ST", "TT"],
        type=ParameterType.shape,
        config_shift_source="jec",
    )

    # a custom asymmetric uncertainty that is converted from rate to shape
    self.add_parameter(
        "QCDscale_ttbar",
        process="TT",
        type=ParameterType.shape,
        transformations=[ParameterTransformation.effect_from_rate],
        effect=(0.5, 1.1),
    )


@inference_model
def example_no_shapes(self):
    # same initialization as "example" above
    example.init_func.__get__(self, self.__class__)()

    #
    # remove all shape parameters
    #

    for category_name, process_name, parameter in self.iter_parameters():
        if parameter.type.is_shape or any(trafo.from_shape for trafo in parameter.transformations):
            self.remove_parameter(parameter.name, process=process_name, category=category_name)
