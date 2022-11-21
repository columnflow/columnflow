# coding: utf-8

"""
plhld inference model.
"""

from columnflow.inference import inference_model, ParameterType, ParameterTransformation


@inference_model
def default(self):

    year = self.config_inst.campaign.x.year
    ecm = self.config_inst.campaign.ecm

    #
    # categories
    #

    self.add_category(
        "cat1",
        category="1e",
        variable="m_bb",
        mc_stats=True,
        data_datasets=["data_e_b"],
    )
    self.add_category(
        "cat2",
        category="1mu",
        variable="m_bb",
        mc_stats=True,
        data_datasets=["data_mu_b"],
    )

    #
    # processes
    #

    signals_ggHH = [
        "ggHH_kl_0_kt_1_sl_hbbhww", "ggHH_kl_1_kt_1_sl_hbbhww",
        "ggHH_kl_2p45_kt_1_sl_hbbhww", "ggHH_kl_5_kt_1_sl_hbbhww",
    ]
    signals_qqHH = [
        "qqHH_CV_1_C2V_1_kl_1_sl_hbbhww", "qqHH_CV_1_C2V_1_kl_0_sl_hbbhww", "qqHH_CV_1_C2V_1_kl_2_sl_hbbhww",
        "qqHH_CV_1_C2V_0_kl_1_sl_hbbhww", "qqHH_CV_1_C2V_2_kl_1_sl_hbbhww",
        "qqHH_CV_0p5_C2V_1_kl_1_sl_hbbhww", "qqHH_CV_1p5_C2V_1_kl_1_sl_hbbhww",
    ]

    processes = [
        # "ggHH_kl_0_kt_1_sl_hbbhww",
        "ggHH_kl_1_kt_1_sl_hbbhww",
        # "ggHH_kl_2p45_kt_1_sl_hbbhww",
        # "ggHH_kl_5_kt_1_sl_hbbhww",
        "tt",
        # "ttv", "ttvv",
        "st_schannel", "st_tchannel", "st_twchannel",
        # "dy_lep",
        # "w_lnu",
        # "vv",
        # "vvv",
        # "qcd",
        # "ggZH", "tHq", "tHW", "ggH", "qqH", "ZH", "WH", "VH", "ttH", "bbH",
    ]

    # if process names need to be changed to fit some convention
    inference_procnames = {
        "foo": "bar",
        # "st": "ST",
        # "tt": "TT",
    }

    # add processes with corresponding datasets to inference model
    for proc in processes:
        if not self.config_inst.has_process(proc):
            raise Exception(f"Process {proc} not included in the config {self.config_inst.name}")
        sub_process_insts = [p for p, _, _ in self.config_inst.get_process(proc).walk_processes(include_self=True)]
        datasets = [
            dataset_inst.name for dataset_inst in self.config_inst.datasets
            if any(map(dataset_inst.has_process, sub_process_insts))
        ]

        self.add_process(
            inference_procnames.get(proc, proc),
            process=proc,
            signal=("HH_" in proc),
            mc_datasets=datasets,
        )

    #
    # parameters
    #

    # groups
    self.add_parameter_group("experiment")
    self.add_parameter_group("theory")

    # add QCD scale uncertainties to inference model
    proc_QCDscale = {
        "ttbar": ["tt", "st_tchannel", "st_schannel", "st_twchannel", "ttW", "ttZ"],
        "V": ["dy_lep", "w_lnu"],
        "VV": ["WW", "ZZ", "WZ", "qqZZ"],
        "VVV": ["vvv"],
        "ggH": ["ggH"],
        "qqH": ["qqH"],
        "VH": ["ZH", "WH", "VH"],
        "ttH": ["ttH", "tHq", "tHW"],
        "bbH": ["bbH"],  # contains also pdf and alpha_s
        # "ggHH": signals_ggHH,  # included in inference model (THU_HH)
        "qqHH": signals_qqHH,
        "VHH": [],
        "ttHH": [],
    }

    # TODO: combine scale and mtop uncertainties for specific processes?
    # TODO: some scale/pdf uncertainties should be rounded to 3 digits, others to 4 digits
    # NOTE: it might be easier to just take the recommended uncertainty values from HH conventions at
    #       https://gitlab.cern.ch/plhld/naming-conventions instead of taking the values from CMSDB
    for k, procs in proc_QCDscale.items():
        for proc in procs:
            if proc not in processes:
                continue
            process_inst = self.config_inst.get_process(proc)
            if "scale" not in process_inst.xsecs[ecm]:
                continue
            self.add_parameter(
                f"QCDscale_{k}",
                process=inference_procnames.get(proc, proc),
                type=ParameterType.rate_gauss,
                effect=tuple(map(
                    lambda f: round(f, 3),
                    process_inst.xsecs[ecm].get(names=("scale"), direction=("down", "up"), factor=True),
                )),
            )
        self.add_parameter_to_group(f"QCDscale_{k}", "theory")

    # add PDF rate uncertainties to inference model
    proc_pdf = {
        "gg": ["tt", "ttZ", "ggZZ"],
        "qqbar": ["st_schannel", "st_tchannel", "dy_lep", "w_lnu", "vvv", "qqZZ", "ttW"],
        "qg": ["st_twchannel"],
        "Higgs_gg": ["ggH"],
        "Higgs_qqbar": ["qqH", "ZH", "WH", "VH"],
        # "Higgs_qg": [],  # none so far
        "Higgs_ttH": ["ttH", "tHq", "tHW"],
        # "Higgs_bbh": ["bbH"],  # removed
        "Higgs_ggHH": signals_ggHH,
        "Higgs_qqHH": signals_qqHH,
        "Higgs_VHH": ["HHZ", "HHW+", "HHW-"],
        "Higgs_ttHH": ["ttHH"],
    }

    for k, procs in proc_pdf.items():
        for proc in procs:
            if proc not in processes:
                continue
            process_inst = self.config_inst.get_process(proc)
            if "pdf" not in process_inst.xsecs[ecm]:
                continue
            # from IPython import embed; embed()
            self.add_parameter(
                f"pdf_{k}",
                process=inference_procnames.get(proc, proc),
                type=ParameterType.rate_gauss,
                effect=tuple(map(
                    lambda f: round(f, 3),
                    process_inst.xsecs[ecm].get(names=("pdf"), direction=("down", "up"), factor=True),
                )),
            )
        self.add_parameter_to_group(f"pdf_{k}", "theory")

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
        f"CMS_pileup_{year}",
        type=ParameterType.shape,
        shift_source="minbias_xs",
    )
    self.add_parameter_to_group(f"CMS_pileup_{year}", "experiment")

    # scale + pdf (shape)
    for proc in processes:
        for unc in ("scale", "pdf"):
            self.add_parameter(
                f"{unc}_{proc}",
                process=inference_procnames.get(proc, proc),
                type=ParameterType.shape,
                shift_source=f"{unc}",
            )
            self.add_parameter_to_group(f"{unc}_{proc}", "theory")

    # Leftovers from test inference model, kept as an example for further implementations of uncertainties
    """
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
    """
    #
    # post-processing
    #

    self.cleanup()
