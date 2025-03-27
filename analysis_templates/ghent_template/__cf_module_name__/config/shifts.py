# coding: utf-8

"""
Definition of shifts.
"""

import law

from columnflow.util import DotDict, call_once_on_config
from columnflow.config_util import add_shift_aliases, get_shifts_from_sources

import order as od

logger = law.logger.get_logger(__name__)


@call_once_on_config()
def add_shifts(config: od.Config) -> None:
    """
    Adds categories to a *config*, that are typically produced in `SelectEvents`.
    """

    config.add_shift(name="nominal", id=0)
    config.add_shift(name="minbias_xs_up", id=7, type="shape")
    config.add_shift(name="minbias_xs_down", id=8, type="shape")
    add_shift_aliases(
        config,
        "minbias_xs",
        {
            "pu_weight": "pu_weight_{name}",
            "normalized_pu_weight": "normalized_pu_weight_{name}",
        },
    )

    config.add_shift(name="top_pt_up", id=9, type="shape", tags={"selection_dependent"})
    config.add_shift(name="top_pt_down", id=10, type="shape", tags={"selection_dependent"})
    add_shift_aliases(config, "top_pt", {"top_pt_weight": "top_pt_weight_{direction}"})

    # lepton uncertainties
    config.add_shift(name="electron_wp90iso_up", id=40, type="shape")
    config.add_shift(name="electron_wp90iso_down", id=41, type="shape")
    add_shift_aliases(config, "electron_wp90iso", {
                      "electron_weight_wp90iso": "electron_weight_wp90iso_{direction}"})

    config.add_shift(name="electron_reco_up", id=42, type="shape")
    config.add_shift(name="electron_reco_down", id=43, type="shape")
    add_shift_aliases(
        config,
        "electron_reco",
        {
            "electron_weight_recobelow20": "electron_weight_recobelow20_{direction}",
            "electron_weight_recoabove20": "electron_weight_recoabove20_{direction}",
        },
    )

    config.add_shift(name="muon_mediumid_up", id=50, type="shape")
    config.add_shift(name="muon_mediumid_down", id=51, type="shape")
    add_shift_aliases(config, "muon_mediumid", {
                      "muon_weight_mediumid": "muon_weight_mediumid_{direction}"})

    config.add_shift(name="muon_loosereliso_up", id=52, type="shape")
    config.add_shift(name="muon_loosereliso_down", id=53, type="shape")
    add_shift_aliases(config, "muon_loosereliso", {
                      "muon_weight_loosereliso": "muon_weight_loosereliso_{direction}"})

    # dataset shift tag is added to prevent shifts to be added to config.x.event_weights
    config.add_shift(name="mur_up", id=201, type="shape", tags={"dataset_shift"})
    config.add_shift(name="mur_down", id=202, type="shape", tags={"dataset_shift"})
    config.add_shift(name="muf_up", id=203, type="shape", tags={"dataset_shift"})
    config.add_shift(name="muf_down", id=204, type="shape", tags={"dataset_shift"})
    config.add_shift(name="pdf_up", id=207, type="shape", tags={"dataset_shift"})
    config.add_shift(name="pdf_down", id=208, type="shape", tags={"dataset_shift"})
    config.add_shift(name="fsr_up", id=209, type="shape", tags={"dataset_shift"})
    config.add_shift(name="fsr_down", id=210, type="shape", tags={"dataset_shift"})
    config.add_shift(name="isr_up", id=211, type="shape", tags={"dataset_shift"})
    config.add_shift(name="isr_down", id=212, type="shape", tags={"dataset_shift"})

    for unc in ["mur", "muf", "pdf", "isr", "fsr"]:
        add_shift_aliases(
            config,
            unc,
            {f"normalized_{unc}_weight": f"normalized_{unc}_weight_" + "{direction}"},
        )

    all_jec_sources = [
        "AbsoluteStat",
        "AbsoluteScale",
        "AbsoluteSample",
        "AbsoluteFlavMap",
        "AbsoluteMPFBias",
        "Fragmentation",
        "SinglePionECAL",
        "SinglePionHCAL",
        "FlavorQCD",
        "TimePtEta",
        "RelativeJEREC1",
        "RelativeJEREC2",
        "RelativeJERHF",
        "RelativePtBB",
        "RelativePtEC1",
        "RelativePtEC2",
        "RelativePtHF",
        "RelativeBal",
        "RelativeSample",
        "RelativeFSR",
        "RelativeStatFSR",
        "RelativeStatEC",
        "RelativeStatHF",
        "PileUpDataMC",
        "PileUpPtRef",
        "PileUpPtBB",
        "PileUpPtEC1",
        "PileUpPtEC2",
        "PileUpPtHF",
        "PileUpMuZero",
        "PileUpEnvelope",
        "SubTotalPileUp",
        "SubTotalRelative",
        "SubTotalPt",
        "SubTotalScale",
        "SubTotalAbsolute",
        "SubTotalMC",
        "Total",
        "TotalNoFlavor",
        "TotalNoTime",
        "TotalNoFlavorNoTime",
        "FlavorZJet",
        "FlavorPhotonJet",
        "FlavorPureGluon",
        "FlavorPureQuark",
        "FlavorPureCharm",
        "FlavorPureBottom",
        "TimeRunA",
        "TimeRunB",
        "TimeRunC",
        "TimeRunD",
        "CorrelationGroupMPFInSitu",
        "CorrelationGroupIntercalibration",
        "CorrelationGroupbJES",
        "CorrelationGroupFlavor",
        "CorrelationGroupUncorrelated",
    ]

    for jec_source in config.x.jec["Jet"]["uncertainty_sources"]:
        idx = all_jec_sources.index(jec_source)
        config.add_shift(name=f"jec_{jec_source}_up", id=5000 + 2 * idx, type="shape", tags={"selection_dependent"})
        config.add_shift(name=f"jec_{jec_source}_down", id=5001 + 2 * idx, type="shape", tags={"selection_dependent"})
        add_shift_aliases(
            config,
            f"jec_{jec_source}",
            {"Jet.pt": "Jet.pt_{name}", "Jet.mass": "Jet.mass_{name}"},
        )

    config.add_shift(name="jer_up", id=6000, type="shape", tags={"selection_dependent"})
    config.add_shift(name="jer_down", id=6001, type="shape", tags={"selection_dependent"})
    add_shift_aliases(config, "jer", {"Jet.pt": "Jet.pt_{name}", "Jet.mass": "Jet.mass_{name}"})

    for shift_inst in config.shifts:
        if shift_inst.tags:
            continue

        # to prevent duplicates
        if shift_inst.name.endswith("up"):
            continue

        for weight in shift_inst.x("column_aliases", {}):
            if weight in config.x.event_weights:
                config.x.event_weights[weight] += get_shifts_from_sources(config, shift_inst.source)
            else:
                config.x.event_weights[weight] = get_shifts_from_sources(config, shift_inst.source)
    # default weight producer for histograms
    config.x.default_weight_producer = "all_weights"

    for dataset in config.datasets:
        dataset.x.event_weights = DotDict()
        if not dataset.has_tag("skip_scale"):
            # pdf/scale weights for all non-qcd datasets
            dataset.x.event_weights["normalized_mur_weight"] = get_shifts_from_sources(config, "mur")
            dataset.x.event_weights["normalized_muf_weight"] = get_shifts_from_sources(config, "muf")

        if not dataset.has_tag("skip_pdf"):
            dataset.x.event_weights["normalized_pdf_weight"] = get_shifts_from_sources(config, "pdf")

        if not dataset.has_tag("skip_ps"):
            dataset.x.event_weights["normalized_isr_weight"] = get_shifts_from_sources(config, "isr")
            dataset.x.event_weights["normalized_fsr_weight"] = get_shifts_from_sources(config, "fsr")
