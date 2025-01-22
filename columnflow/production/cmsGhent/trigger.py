# coding: utf-8
"""
Producer that produces a trigger scalefactors
"""

from __future__ import annotations

import law
import order as od

from dataclasses import dataclass, field

from columnflow.selection import SelectionResult
from columnflow.production import producer, Producer
from columnflow.weight import WeightProducer, weight_producer
from columnflow.util import maybe_import, DotDict
from columnflow.columnar_util import set_ak_column, has_ak_column, Route, fill_hist

from columnflow.types import Any, Iterable

np = maybe_import("numpy")
ak = maybe_import("awkward")
hist = maybe_import("hist")

logger = law.logger.get_logger(__name__)


@dataclass
class TriggerSFConfig:
    triggers: str | Iterable[str]
    ref_triggers: str | Iterable[str]
    variables: Iterable[str]
    corrector_kwargs: dict[str, Any] = field(default_factory=dict)
    tag: str = None,
    ref_tag: str = None,

    def __post_init__(self):

        # reformat self.trigger to tuple
        if isinstance(self.triggers, str):
            self.triggers = {self.triggers, }
        elif not isinstance(self.triggers, set):
            self.triggers = set(self.triggers)

        # reformat self.ref_trigger to tuple
        if isinstance(self.ref_triggers, str):
            self.ref_triggers = {self.ref_triggers, }
        elif not isinstance(self.ref_triggers, set):
            self.ref_triggers = set(self.ref_triggers)

        if not self.tag:
            self.tag = self.triggers[0]
        if not self.ref_tag:
            self.ref_tag = self.ref_triggers[0]

    @classmethod
    def new(
        cls,
        obj: TriggerSFConfig | tuple[str, list[str]] | tuple[str, list[str], str],
    ) -> TriggerSFConfig:
        # purely for backwards compatibility with the old tuple format
        return obj if isinstance(obj, cls) else cls(*obj)


def init_trigger(self: Producer | WeightProducer, add_eff_vars=True, add_hists=True):

    if not hasattr(self, "get_trigger_config"):
        self.trigger_config = self.config_inst.x(
            "trigger_sf",
            TriggerSFConfig(
                triggers=None,  # TODO fix the default values
                ref_triggers=None,
                variables=None,
            ),
        )
        self.trigger_config = TriggerSFConfig.new(self.trigger_config)
    else:
        self.trigger_config = self.get_trigger_config()

    self.triggers = self.trigger_config.triggers
    self.tag = self.trigger_config.tag

    self.ref_triggers = self.trigger_config.ref_triggers
    self.ref_tag = self.trigger_config.ref_tag

    self.tag_name = f"hlt_{self.tag.lower()}_ref_{self.ref_tag.lower()}"

    if add_eff_vars:

        # add variables to bin trigger efficiency
        self.variables = self.trigger_config.variables
        self.variable_insts = list(map(self.config_inst.get_variable, self.variables))

        eff_bin_vars_inputs = {
            inp
            for variable_inst in self.variable_insts
            for inp in (
                [variable_inst.expression]
                if isinstance(variable_inst.expression, str)
                else variable_inst.x("inputs", [])
            )
        }

        if add_hists:
            if self.objects is None:
                self.objects = {inp.split(".")[0] for inp in eff_bin_vars_inputs if "." in inp}

            # add variable to bin measured trigger PASS / FAIL
            self.variable_insts += [
                od.Variable(
                    self.tag,
                    expression=lambda events: np.any([events.HLT[trigger] for trigger in self.triggers], axis=0),
                    binning=(2, -0.5, 1.5),
                    x_labels=["FAIL", "PASS"],
                    aux={"inputs": [f"HLT.{trigger}" for trigger in self.triggers]}
                )
                od.Variable(
                    self.ref_tag,
                    expression=lambda events: np.any([events.HLT[trigger] for trigger in self.ref_triggers], axis=0),
                    binning=(2, -0.5, 1.5),
                    x_labels=["FAIL", "PASS"],
                    aux={"inputs": {f"HLT.{trigger}" for trigger in self.ref_triggers}}
                )
            ]

        self.uses.update({
            inp
            for variable_inst in self.variable_insts
            for inp in (
                [variable_inst.expression] if isinstance(variable_inst.expression, str) else variable_inst.x("inputs",
                                                                                                            [])
            )
        })


@producer(
    trigger="SingleElectron",
    get_sf_file=lambda self, external_files: external_files.trigger_sf[self.trigger],
    sf_name=lambda self: f"trig_sf_{self.trigger}",
)
def trigger_scale_factors(
    self: Producer,
    events: ak.Array,
    event_mask: ak.Array,
    **kwargs,
):
    inputs = []
    sf = {vr: np.ones(len(events)) for vr in ["central", "down", "up"]}
    selected_events = events[event_mask]
    if len(selected_events) > 0:
        for variable_inst in self.variable_insts:
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
        name = self.sf_name() + ("" if vr == "central" else ("_" + vr))
        events = set_ak_column(events, name, sf[vr])
    return events


@trigger_scale_factors.init
def trigger_scale_factors_init(self: Producer):
    eff_bin_vars = list(map(
        self.config_inst.get_variable,
        self.config_inst.x.analysis_triggers[self.trigger][1],
    ))

    self.variable_insts = [vr for vr in eff_bin_vars if not vr.x("auxiliary", False)]

    self.uses = {
        inp
        for variable_inst in self.variable_insts
        for inp in (
            [variable_inst.expression]
            if isinstance(variable_inst.expression, str)
            else variable_inst.x("inputs", [])
        )
    }
    self.produces = {self.sf_name() + suff for suff in ["", "_down", "_up"]}


@trigger_scale_factors.requires
def trigger_scale_factors_requires(self: Producer, reqs: dict) -> None:
    if "external_files" in reqs:
        return

    from columnflow.tasks.external import BundleExternalFiles
    reqs["external_files"] = BundleExternalFiles.req(self.task)


@trigger_scale_factors.setup
def trigger_scale_factors_setup(
    self: Producer,
    reqs: dict,
    inputs: dict,
    reader_targets,
) -> None:
    bundle = reqs["external_files"]

    # create the corrector
    import correctionlib
    correctionlib.highlevel.Correction.__call__ = correctionlib.highlevel.Correction.evaluate
    correction_set = correctionlib.CorrectionSet.from_file(
        self.get_sf_file(bundle.files).path,
    )
    self.sf_corrector = correction_set["scale_factors"]


#
# trigger histograms called in cf.SelectEvents
#

@producer(
    # only run on mc
    get_no_trigger_selection=lambda self, results: results.x("event_no_trigger", None),
    get_trigger_config=(lambda self: TriggerSFConfig.new(self.config_inst.x.trigger_sf)),
    objects=None,
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
    for var_inst in self.variable_insts:
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

    if f"{self.tag_name}_efficiencies" not in hists:
        histogram = hist.Hist.new
        # add variables for binning the efficiency
        for var_inst in self.variable_insts:
            histogram = histogram.Var(
                var_inst.bin_edges,
                name=var_inst.name,
                label=var_inst.get_full_x_title(),
            )
        hists[f"{self.tag_name}_efficiencies"] = histogram.Weight()
    fill_hist(
        hists[f"{self.tag_name}_efficiencies"],
        fill_data,
        last_edge_inclusive=False,
    )


ref_tag
breakpoint()

return events


@trigger_efficiency_hists.init
def trigger_efficiency_hists_init(self: Producer):
    if hasattr(self, "dataset_inst") and self.dataset_inst.is_mc:
        self.uses.add("mc_weight")

    init_trigger(self)
