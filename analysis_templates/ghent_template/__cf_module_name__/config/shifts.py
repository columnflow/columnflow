# coding: utf-8

"""
Definition of shifts.
"""

import law
import re

from columnflow.util import DotDict, call_once_on_config

import order as od

logger = law.logger.get_logger(__name__)


@call_once_on_config()
def add_shifts(config: od.Config) -> None:
    """
    Adds categories to a *config*, that are typically produced in `SelectEvents`.
    """

    def add_shift_aliases(shift_source: str, aliases: dict[str], selection_dependent: bool):

        for direction in ["up", "down"]:
            shift = config.get_shift(od.Shift.join_name(shift_source, direction))
            # format keys and values
            inject_shift = lambda s: re.sub(r"\{([^_])", r"{_\1", s).format(**shift.__dict__)
            _aliases = {inject_shift(key): inject_shift(value) for key, value in aliases.items()}
            alias_type = "column_aliases_selection_dependent" if selection_dependent else "column_aliases"
            # extend existing or register new column aliases
            shift.set_aux(alias_type, shift.get_aux(alias_type, {})).update(_aliases)

    config.add_shift(name="nominal", id=0)
    config.add_shift(name="minbias_xs_up", id=7, type="shape")
    config.add_shift(name="minbias_xs_down", id=8, type="shape")
    add_shift_aliases(
        "minbias_xs",
        {
            "pu_weight": "pu_weight_{name}",
            "normalized_pu_weight": "normalized_pu_weight_{name}",
        },
        selection_dependent=False)

    config.add_shift(name="top_pt_up", id=9, type="shape")
    config.add_shift(name="top_pt_down", id=10, type="shape")
    add_shift_aliases("top_pt", {"top_pt_weight": "top_pt_weight_{direction}"}, selection_dependent=False)

    # lepton uncertainties
    config.add_shift(name="e_sf_up", id=40, type="shape")
    config.add_shift(name="e_sf_down", id=41, type="shape")
    config.add_shift(name="e_trig_sf_up", id=42, type="shape")
    config.add_shift(name="e_trig_sf_down", id=43, type="shape")
    add_shift_aliases("e_sf", {"electron_weight": "electron_weight_{direction}"}, selection_dependent=False)

    config.add_shift(name="mu_sf_up", id=50, type="shape")
    config.add_shift(name="mu_sf_down", id=51, type="shape")
    config.add_shift(name="mu_trig_sf_up", id=52, type="shape")
    config.add_shift(name="mu_trig_sf_down", id=53, type="shape")
    add_shift_aliases("mu_sf", {"muon_weight": "muon_weight_{direction}"}, selection_dependent=False)

    config.add_shift(name="mur_up", id=201, type="shape")
    config.add_shift(name="mur_down", id=202, type="shape")
    config.add_shift(name="muf_up", id=203, type="shape")
    config.add_shift(name="muf_down", id=204, type="shape")
    config.add_shift(name="murf_envelope_up", id=205, type="shape")
    config.add_shift(name="murf_envelope_down", id=206, type="shape")
    config.add_shift(name="pdf_up", id=207, type="shape")
    config.add_shift(name="pdf_down", id=208, type="shape")

    for unc in ["mur", "muf", "murf_envelope", "pdf"]:
        # add_shift_aliases(unc, {f"{unc}_weight": f"{unc}_weight_" + "{direction}"}, selection_dependent=False)
        add_shift_aliases(
            unc,
            {f"normalized_{unc}_weight": f"normalized_{unc}_weight_" + "{direction}"},
            selection_dependent=False,
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
        config.add_shift(name=f"jec_{jec_source}_up", id=5000 + 2 * idx, type="shape")
        config.add_shift(name=f"jec_{jec_source}_down", id=5001 + 2 * idx, type="shape")
        add_shift_aliases(
            f"jec_{jec_source}",
            {"Jet.pt": "Jet.pt_{name}", "Jet.mass": "Jet.mass_{name}"},
            selection_dependent=True,
        )

    config.add_shift(name="jer_up", id=6000, type="shape", tags={"selection_dependent"})
    config.add_shift(name="jer_down", id=6001, type="shape", tags={"selection_dependent"})
    add_shift_aliases("jer", {"Jet.pt": "Jet.pt_{name}", "Jet.mass": "Jet.mass_{name}"}, selection_dependent=True)

    get_shifts = lambda *keys: sum(([config.get_shift(f"{k}_up"), config.get_shift(f"{k}_down")] for k in keys), [])

    config.x.event_weights["normalized_pu_weight"] = get_shifts("minbias_xs")
    config.x.event_weights["electron_weight"] = get_shifts("e_sf")
    config.x.event_weights["muon_weight"] = get_shifts("mu_sf")

    for dataset in config.datasets:
        dataset.x.event_weights = DotDict()
        if not dataset.has_tag("skip_scale"):
            # pdf/scale weights for all non-qcd datasets
            dataset.x.event_weights["normalized_murf_envelope_weight"] = get_shifts("murf_envelope")
            dataset.x.event_weights["normalized_mur_weight"] = get_shifts("mur")
            dataset.x.event_weights["normalized_muf_weight"] = get_shifts("muf")

        if not dataset.has_tag("skip_pdf"):
            dataset.x.event_weights["normalized_pdf_weight"] = get_shifts("pdf")
