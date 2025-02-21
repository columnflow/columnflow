# coding: utf-8
"""
Producer that produces a histogram during cf.SelectEvents
"""

from __future__ import annotations

import law

from columnflow.production import producer, Producer
from columnflow.util import maybe_import, DotDict
from columnflow.columnar_util import has_ak_column, Route, fill_hist
import columnflow.production.cmsGhent.trigger.util as util
from columnflow.selection import SelectionResult
import order as od

np = maybe_import("numpy")
ak = maybe_import("awkward")
hist = maybe_import("hist")

logger = law.logger.get_logger(__name__)


@producer(
    # only run on mc
    trigger_config=lambda self: self.config_inst.x.trigger_sf,
)
def trigger_efficiency_hists(
    self: Producer,
    events: ak.Array,
    results: SelectionResult,
    hists: DotDict | dict = None,
    object_mask: dict = None,
    **kwargs,
) -> ak.Array:
    if hists is None:
        logger.warning_once(self.cls_name + " did not get any histograms")
        return events
    if object_mask is None:
        object_mask = {}

    no_trigger_selection = self.get_no_trigger_selection(results)
    assert no_trigger_selection is not None, "results does not contain mask without trigger selection"

    if not ak.any(no_trigger_selection):
        return events

    selected_events = ak.Array({
        obj: events[obj][object_mask.get(obj, results.objects[obj][obj])]
        for obj in self.objects
    } | {
        "mc_weight": events.mc_weight if self.dataset_inst.is_mc else np.ones(len(events)),
        "HLT": events.HLT,
    })[no_trigger_selection]

    fill_data = {
        # broadcast event weight and process-id to jet weight
        "weight": selected_events.mc_weight,
    }

    # loop over variables in which the efficiency is binned
    for var_inst in self.variables:
        expr = var_inst.expression
        if isinstance(expr, str):
            route = Route(expr)

            def expr(evs, *args, **kwargs):
                if len(evs) == 0 and not has_ak_column(evs, route):
                    return ak.Array(np.array([], dtype=np.float32))
                return route.apply(evs, null_value=var_inst.null_value)

        # apply the variable (flatten to fill histogram)
        fill_data[var_inst.name] = expr(selected_events)
        if fill_data[var_inst.name].ndim == 1:
            fill_data[var_inst.name] = fill_data[var_inst.name][:, None]

    if f"{self.config_name}_efficiencies" not in hists:
        histogram = hist.Hist.new
        # add variables for binning the efficiency
        for var_inst in self.variable_insts:
            histogram = histogram.Var(
                var_inst.bin_edges,
                name=var_inst.name,
                label=var_inst.get_full_x_title(),
            )
        hists[f"{self.config_name}_efficiencies"] = histogram.Weight()
    fill_hist(
        hists[f"{self.config_name}_efficiencies"],
        fill_data,
        last_edge_inclusive=False,
    )

    return events


@trigger_efficiency_hists.init
def trigger_efficiency_hists_init(self: Producer):
    if hasattr(self, "dataset_inst") and self.dataset_inst.is_mc:
        self.uses.add("mc_weight")

    util.init_trigger_config(self)

    eff_bin_vars_inputs = {
        inp
        for variable_inst in self.variables
        for inp in (
            [variable_inst.expression]
            if isinstance(variable_inst.expression, str)
            else variable_inst.x("inputs", [])
        )
    }
    if getattr(self, "objects", None) is None:
        self.objects = {inp.split(".")[0] for inp in eff_bin_vars_inputs if "." in inp}

    # add variable to bin measured trigger PASS / FAIL
    self.variables = self.variables + (
        od.Variable(
            self.tag,
            expression=lambda events: np.any([events.HLT[trigger] for trigger in self.triggers], axis=0),
            binning=(2, -0.5, 1.5),
            x_labels=["FAIL", "PASS"],
            aux={"inputs": [f"HLT.{trigger}" for trigger in self.triggers]},
        ),
        od.Variable(
            self.ref_tag,
            expression=lambda events: np.any([events.HLT[trigger] for trigger in self.ref_triggers], axis=0),
            binning=(2, -0.5, 1.5),
            x_labels=["FAIL", "PASS"],
            aux={"inputs": {f"HLT.{trigger}" for trigger in self.ref_triggers}},
        ),
    )

    util.init_uses_variables(self)

@producer(
    # only run on mc
    mc_only=True,
    # lepton config bundle, function to determine the location of a list of LeptonWeightConfig's
    trigger_configs=lambda self: self.config_inst.x.trigger_sfs,
    config_naming=lambda self, cfg: "hist_" + cfg.sf_name
)
def bundle_trigger_histograms(
    self: Producer,
    events: ak.Array,
    results: SelectionResult,
    hists: DotDict | dict = None,
    object_mask: dict = None,
    **kwargs,
) -> ak.Array:

    for trigger_hist_producer in self.uses:
        events = self[trigger_hist_producer](events, results, hists, object_mask, **kwargs)

    return events


@bundle_trigger_histograms.init
def bundle_trigger_histograms_init(self: Producer) -> None:

    trigger_configs = self.trigger_configs()
    for config in trigger_configs:
        self.uses.add(trigger_efficiency_hists.derive(
            self.config_naming(config),
            cls_dict=dict(trigger_config=config),
        ))

    self.produces |= self.uses