# coding: utf-8
"""
Producer that produces a trigger scalefactors
"""

from __future__ import annotations

import law

from columnflow.production import producer, Producer
from columnflow.util import maybe_import
from columnflow.columnar_util import set_ak_column, has_ak_column, Route
from columnflow.production.cmsGhent.trigger.util import init_trigger


np = maybe_import("numpy")
ak = maybe_import("awkward")
hist = maybe_import("hist")

logger = law.logger.get_logger(__name__)


@producer(
    mc_only=True,
    trigger_config=lambda self: self.config_inst.x.trigger_sf,
)
def trigger_scale_factors(
    self: Producer,
    events: ak.Array,
    event_mask: ak.Array = None,
    **kwargs,
):
    inputs = []
    sf = {vr: np.ones(len(events)) for vr in ["central", "down", "up"]}

    selected_events = events
    if event_mask or self.event_mask_func:
        if self.event_mask_func:
            event_mask = (event_mask or True) & self.event_mask_func(events)
        selected_events = selected_events[event_mask]

    if len(selected_events) > 0:
        for variable_inst in self.variable_insts:
            if variable_inst.x("auxiliary", False) is not False:
                continue
            # prepare the expression
            expr = variable_inst.expression
            if isinstance(expr, str):
                route = Route(expr)

                def expr(evs, *args, **kwargs):
                    if not has_ak_column(evs, route):
                        return ak.Array(np.array([], dtype=np.float32))
                    return route.apply(evs, null_value=variable_inst.null_value)

            # apply it
            inputs.append(expr(selected_events))
        for vr in sf:
            sf[vr][event_mask] = self.sf_corrector.evaluate(vr, *inputs)
    for vr in sf:
        name = self.sf_name + ("" if vr == "central" else ("_" + vr))
        events = set_ak_column(events, name, sf[vr])
    return events


@trigger_scale_factors.init
def trigger_scale_factors_init(self: Producer):
    init_trigger(self, add_eff_vars=True, add_hists=False)
    self.uses |= self.event_mask_uses
    self.produces = {self.sf_name + suff for suff in ["", "_down", "_up"]}


@trigger_scale_factors.requires
def trigger_scale_factors_requires(self: Producer, reqs: dict) -> None:

    if self.get_sf_file:
        if "external_files" in reqs:
            return

        from columnflow.tasks.external import BundleExternalFiles
        reqs["external_files"] = BundleExternalFiles.req(self.task)
    else:
        from columnflow.tasks.cmsGhent.trigger_scale_factors import TriggerScaleFactors

        reqs["trigger_scalefactor"] = TriggerScaleFactors.req(
            self.task,
            datasets=self.datasets,
            variables=self.variables,
            trigger=self.tag,
            ref_trigger=self.ref_tag,
        )


@trigger_scale_factors.setup
def trigger_scale_factors_setup(
    self: Producer,
    reqs: dict,
    inputs: dict,
    reader_targets,
) -> None:

    # create the corrector
    import correctionlib

    if self.get_sf_file:
        bundle = reqs["external_files"]
        correctionlib.highlevel.Correction.__call__ = correctionlib.highlevel.Correction.evaluate
        correction_set = correctionlib.CorrectionSet.from_file(
            self.get_sf_file(bundle.files).path,
        )
    else:
        correctionlib.highlevel.Correction.__call__ = correctionlib.highlevel.Correction.evaluate
        correction_set = correctionlib.CorrectionSet.from_file(
            reqs["trigger_scalefactor"].output()["json"].path,
        )

    self.sf_corrector = correction_set["scale_factors"]


@producer(
    # only run on mc
    mc_only=True,
    # lepton config bundle, function to determine the location of a list of LeptonWeightConfig's
    trigger_configs=lambda self: self.config_inst.x.trigger_sfs,
    config_naming=lambda self, cfg: cfg.sf_name
)
def bundle_trigger_weights(
    self: Producer,
    events: ak.Array,
    **kwargs,
) -> ak.Array:

    for trigger_weight_producer in self.uses:
        events = self[trigger_weight_producer](events, **kwargs)

    return events


@bundle_trigger_weights.init
def bundle_trigger_weights_init(self: Producer) -> None:

    trigger_configs = self.trigger_configs()
    for config in trigger_configs:
        self.uses.add(trigger_scale_factors.derive(
            self.config_naming(config),
            cls_dict=dict(trigger_config=config),
        ))

    self.produces |= self.uses
