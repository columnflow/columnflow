# coding: utf-8
"""
Producer that produces a trigger scalefactors
"""

from __future__ import annotations

import law
import order as od

from dataclasses import dataclass, field, replace, asdict

from columnflow.selection import SelectionResult
from columnflow.production import producer, Producer
from columnflow.weight import WeightProducer
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
    datasets: Iterable[str]
    corrector_kwargs: dict[str, Any] = field(default_factory=dict)
    
    tag: str = "trig"
    ref_tag: str = "ref"
    sf_name: str = f"trig_sf"
    aux: dict = None
    objects = None  # list of objects used in the calculation: derived from the variables if None

    # functions
    get_sf_file = None
    get_no_trigger_selection = lambda results: results.x("event_no_trigger", None)
    event_mask_func = None
    event_mask_uses = None

    def __post_init__(self):

        # reformat self.trigger to tuple
        if isinstance(self.triggers, str):
            self.triggers = {self.triggers}
        elif not isinstance(self.triggers, set):
            self.triggers = set(self.triggers)

        # reformat self.ref_trigger to tuple
        if isinstance(self.ref_triggers, str):
            self.ref_triggers = {self.ref_triggers}
        elif not isinstance(self.ref_triggers, set):
            self.ref_triggers = set(self.ref_triggers)

        if not isinstance(self.datasets, set):
            self.datasets = set(self.datasets)

        self.aux = self.aux or {}
        self.x = DotDict(self.aux)
        self.event_mask_uses = self.event_mask_uses or set()

    def copy(self, **changes):
        return replace(self, **changes)


    def event_mask(self, func: Callable[[ak.Array], ak.Array] = None, uses: set = None) -> None:
        """
        Decorator to wrap a function *func* that should be registered as :py:meth:`mask_func`
        which is used to calculate the mask that should be applied to the lepton

        The function should accept one positional argument:

            - *events*, an awkward array from which the inouts are calculate


        The decorator does not return the wrapped function.
        """

        def decorator(func: Callable[[ak.Array], dict[ak.Array]]):
            self.event_mask_func = func
            self.event_mask_uses = self.event_mask_uses | event_mask_uses

        return decorator(func) if func else decorator


def init_trigger(self: Producer | WeightProducer, add_eff_vars=True, add_hists=True):
    if callable(self.trigger_config):
        self.trigger_config = self.trigger_config() 

    for key, value in asdict(self.trigger_config).items():
        if not hasattr(self, key):
            setattr(self, key, value)
            
    self.tag_name = f"hlt_{self.tag.lower()}_ref_{self.ref_tag.lower()}"

    if add_eff_vars:
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
            if not hasattr(self, "objects"):
                self.objects = {inp.split(".")[0] for inp in eff_bin_vars_inputs if "." in inp}

            # add variable to bin measured trigger PASS / FAIL
            self.variable_insts += [
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


#
# trigger histograms called in cf.SelectEvents
#

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

    return events


@trigger_efficiency_hists.init
def trigger_efficiency_hists_init(self: Producer):
    if hasattr(self, "dataset_inst") and self.dataset_inst.is_mc:
        self.uses.add("mc_weight")

    init_trigger(self)
    

@producer(
    # only run on mc
    mc_only=True,
    # lepton config bundle, function to determine the location of a list of LeptonWeightConfig's
    trigger_configs=lambda self: self.config_inst.x.trigger_sfs,
    config_naming=lambda self, cfg: config.sf_name
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
    
    

