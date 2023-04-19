# coding: utf-8

"""
Task to produce and merge histograms.
"""

import luigi
import law

from columnflow.tasks.framework.base import Requirements, AnalysisTask, DatasetTask, wrapper_factory
from columnflow.tasks.framework.mixins import (
    CalibratorsMixin, SelectorStepsMixin, ProducersMixin, MLModelsMixin, VariablesMixin,
    ShiftSourcesMixin, EventWeightMixin, ChunkedIOMixin,
)
from columnflow.tasks.framework.remote import RemoteWorkflow
from columnflow.tasks.reduction import MergeReducedEventsUser, MergeReducedEvents
from columnflow.tasks.production import ProduceColumns
from columnflow.tasks.ml import MLEvaluation
from columnflow.util import dev_sandbox


class CreateHistograms(
    VariablesMixin,
    MLModelsMixin,
    ProducersMixin,
    SelectorStepsMixin,
    CalibratorsMixin,
    EventWeightMixin,
    ChunkedIOMixin,
    MergeReducedEventsUser,
    law.LocalWorkflow,
    RemoteWorkflow,
):
    sandbox = dev_sandbox("bash::$CF_BASE/sandboxes/venv_columnar.sh")

    # upstream requirements
    reqs = Requirements(
        MergeReducedEventsUser.reqs,
        RemoteWorkflow.reqs,
        MergeReducedEvents=MergeReducedEvents,
        ProduceColumns=ProduceColumns,
        MLEvaluation=MLEvaluation,
    )

    def workflow_requires(self):
        reqs = super().workflow_requires()

        # require the full merge forest
        reqs["events"] = self.reqs.MergeReducedEvents.req(self, tree_index=-1)

        if not self.pilot:
            if self.producers:
                reqs["producers"] = [
                    self.reqs.ProduceColumns.req(self, producer=p)
                    for p in self.producers
                ]
            if self.ml_models:
                reqs["ml"] = [
                    self.reqs.MLEvaluation.req(self, ml_model=m)
                    for m in self.ml_models
                ]

        return reqs

    def requires(self):
        reqs = {
            "events": self.reqs.MergeReducedEvents.req(self, tree_index=self.branch, _exclude={"branch"}),
        }

        if self.producers:
            reqs["producers"] = [
                self.reqs.ProduceColumns.req(self, producer=p)
                for p in self.producers
            ]
        if self.ml_models:
            reqs["ml"] = [
                self.reqs.MLEvaluation.req(self, ml_model=m)
                for m in self.ml_models
            ]

        return reqs

    @MergeReducedEventsUser.maybe_dummy
    def output(self):
        return {
            "hists": self.target(f"histograms__vars_{self.variables_repr}__{self.branch}.pickle"),
        }

    @law.decorator.log
    @law.decorator.localize(input=True, output=False)
    @law.decorator.safe_output
    def run(self):
        import hist
        import numpy as np
        import awkward as ak
        from columnflow.columnar_util import Route, update_ak_array, add_ak_aliases, has_ak_column

        # prepare inputs and outputs
        inputs = self.input()

        # declare output: dict of histograms
        histograms = {}

        # create a temp dir for saving intermediate files
        tmp_dir = law.LocalDirectoryTarget(is_tmp=True)
        tmp_dir.touch()

        # get shift dependent aliases
        aliases = self.local_shift_inst.x("column_aliases", {})

        # define columns that need to be read
        read_columns = {"category_ids", "process_id"} | set(aliases.values())
        read_columns |= {
            Route(variable_inst.expression)
            if isinstance(variable_inst.expression, str) else
            None  # TODO: handle variable_inst with custom expressions, can they declare columns?
            for variable_inst in (
                self.config_inst.get_variable(var_name)
                for var_name in law.util.flatten(self.variable_tuples.values())
            )
        }
        if self.dataset_inst.is_mc:
            read_columns |= {Route(column) for column in self.config_inst.x.event_weights}
            read_columns |= {Route(column) for column in self.dataset_inst.x("event_weights", [])}
        read_columns = {Route(c) for c in read_columns}

        # empty float array to use when input files have no entries
        empty_f32 = ak.Array(np.array([], dtype=np.float32))

        # iterate over chunks of events and diffs
        files = [inputs["events"]["collection"][0]["events"].path]
        if self.producers:
            files.extend([inp["columns"].path for inp in inputs["producers"]])
        if self.ml_models:
            files.extend([inp["mlcolumns"].path for inp in inputs["ml"]])
        for (events, *columns), pos in self.iter_chunked_io(
            files,
            source_type=len(files) * ["awkward_parquet"],
            read_columns=len(files) * [read_columns],
        ):
            # add additional columns
            events = update_ak_array(events, *columns)

            # add aliases
            events = add_ak_aliases(events, aliases, remove_src=True)

            # build the full event weight
            weight = ak.Array(np.ones(len(events)))
            if self.dataset_inst.is_mc and len(events):
                for column in self.config_inst.x.event_weights:
                    weight = weight * Route(column).apply(events)
                for column in self.dataset_inst.x("event_weights", []):
                    if has_ak_column(events, column):
                        weight = weight * Route(column).apply(events)
                    else:
                        self.logger.warning_once(
                            f"missing_dataset_weight_{column}",
                            f"weight '{column}' for dataset {self.dataset_inst.name} not found",
                        )

            # define and fill histograms, taking into account multiple axes
            for var_key, var_names in self.variable_tuples.items():
                # get variable instances
                variable_insts = [self.config_inst.get_variable(var_name) for var_name in var_names]

                # create the histogram if not present yet
                if var_key not in histograms:
                    h = (
                        hist.Hist.new
                        .IntCat([], name="category", growth=True)
                        .IntCat([], name="process", growth=True)
                        .IntCat([], name="shift", growth=True)
                    )
                    # add variable axes
                    for variable_inst in variable_insts:
                        h = h.Var(
                            variable_inst.bin_edges,
                            name=variable_inst.name,
                            label=variable_inst.get_full_x_title(),
                        )
                    # enable weights and store it
                    histograms[var_key] = h.Weight()

                # broadcast arrays so that each event can be filled for all its categories
                fill_kwargs = {
                    "category": events.category_ids,
                    "process": events.process_id,
                    "shift": np.ones(len(events), dtype=np.int32) * self.global_shift_inst.id,
                    "weight": weight,
                }
                for variable_inst in variable_insts:
                    # prepare the expression
                    expr = variable_inst.expression
                    if isinstance(expr, str):
                        route = Route(expr)
                        def expr(events, *args, **kwargs):
                            if len(events) == 0 and not has_ak_column(events, route):
                                return empty_f32
                            return route.apply(events, null_value=variable_inst.null_value)
                    # apply it
                    fill_kwargs[variable_inst.name] = expr(events)

                # broadcast and fill
                arrays = ak.flatten(ak.cartesian(fill_kwargs))
                histograms[var_key].fill(**{field: arrays[field] for field in arrays.fields})

        # merge output files
        self.output()["hists"].dump(histograms, formatter="pickle")


CreateHistogramsWrapper = wrapper_factory(
    base_cls=AnalysisTask,
    require_cls=CreateHistograms,
    enable=["configs", "skip_configs", "datasets", "skip_datasets", "shifts", "skip_shifts"],
)


class MergeHistograms(
    VariablesMixin,
    MLModelsMixin,
    ProducersMixin,
    SelectorStepsMixin,
    CalibratorsMixin,
    DatasetTask,
    law.LocalWorkflow,
    RemoteWorkflow,
):
    only_missing = luigi.BoolParameter(
        default=False,
        description="when True, identify missing variables first and only require histograms of "
        "missing ones; default: False",
    )
    remove_previous = luigi.BoolParameter(
        default=False,
        significant=False,
        description="when True, remove particlar input histograms after merging; default: False",
    )

    sandbox = dev_sandbox("bash::$CF_BASE/sandboxes/venv_columnar.sh")

    # upstream requirements
    reqs = Requirements(
        RemoteWorkflow.reqs,
        CreateHistograms=CreateHistograms,
    )

    def create_branch_map(self):
        # create a dummy branch map so that this task could as a job
        return {0: None}

    def workflow_requires(self):
        reqs = super().workflow_requires()

        reqs["hists"] = self.as_branch().requires()

        return reqs

    def requires(self):
        # optional dynamic behavior: determine not yet created variables and require only those
        prefer_cli = {"variables"}
        variables = self.variables
        if self.only_missing:
            prefer_cli.clear()
            missing = self.output().count(existing=False, keys=True)[1]
            variables = tuple(sorted(missing, key=variables.index))
            if not variables:
                return []

        return self.reqs.CreateHistograms.req(
            self,
            branch=-1,
            variables=tuple(variables),
            _exclude={"branches"},
            _prefer_cli=prefer_cli,
        )

    def output(self):
        return {"hists": law.SiblingFileCollection({
            variable_name: self.target(f"hist__{variable_name}.pickle")
            for variable_name in self.variables
        })}

    @law.decorator.log
    def run(self):
        # preare inputs and outputs
        inputs = self.input()["collection"]
        outputs = self.output()

        # load input histograms
        hists = [
            inp["hists"].load(formatter="pickle")
            for inp in self.iter_progress(inputs.targets.values(), len(inputs), reach=(0, 50))
        ]

        # create a separate file per output variable
        variable_names = list(hists[0].keys())
        for variable_name in self.iter_progress(variable_names, len(variable_names), reach=(50, 100)):
            self.publish_message(f"merging histograms for '{variable_name}'")

            variable_hists = [h[variable_name] for h in hists]
            merged = sum(variable_hists[1:], variable_hists[0].copy())
            outputs["hists"][variable_name].dump(merged, formatter="pickle")

        # optionally remove inputs
        if self.remove_previous:
            inputs.remove()


MergeHistogramsWrapper = wrapper_factory(
    base_cls=AnalysisTask,
    require_cls=MergeHistograms,
    enable=["configs", "skip_configs", "datasets", "skip_datasets", "shifts", "skip_shifts"],
)


class MergeShiftedHistograms(
    VariablesMixin,
    ShiftSourcesMixin,
    MLModelsMixin,
    ProducersMixin,
    SelectorStepsMixin,
    CalibratorsMixin,
    DatasetTask,
    law.LocalWorkflow,
    RemoteWorkflow,
):
    sandbox = dev_sandbox("bash::$CF_BASE/sandboxes/venv_columnar.sh")

    # disable the shift parameter
    shift = None
    effective_shift = None
    allow_empty_shift = True

    # allow only running on nominal
    allow_empty_shift_sources = True

    # upstream requirements
    reqs = Requirements(
        RemoteWorkflow.reqs,
        MergeHistograms=MergeHistograms,
    )

    def create_branch_map(self):
        # create a dummy branch map so that this task could as a job
        return {0: None}

    def workflow_requires(self):
        reqs = super().workflow_requires()

        # add nominal and both directions per shift source
        for shift in ["nominal"] + self.shifts:
            reqs[shift] = self.reqs.MergeHistograms.req(self, shift=shift, _prefer_cli={"variables"})

        return reqs

    def requires(self):
        return {
            shift: self.reqs.MergeHistograms.req(self, shift=shift, _prefer_cli={"variables"})
            for shift in ["nominal"] + self.shifts
        }

    def store_parts(self):
        parts = super().store_parts()
        parts.insert_after("dataset", "shift_sources", f"shifts_{self.shift_sources_repr}")
        return parts

    def output(self):
        return {"hists": law.SiblingFileCollection({
            variable_name: self.target(f"shifted_hist__{variable_name}.pickle")
            for variable_name in self.variables
        })}

    @law.decorator.log
    def run(self):
        # preare inputs and outputs
        inputs = self.input()
        outputs = self.output()["hists"].targets

        for variable_name, outp in self.iter_progress(outputs.items(), len(outputs)):
            self.publish_message(f"merging histograms for '{variable_name}'")

            # load hists
            variable_hists = [
                coll["hists"].targets[variable_name].load(formatter="pickle")
                for coll in inputs.values()
            ]

            # merge and write the output
            merged = sum(variable_hists[1:], variable_hists[0].copy())
            outp.dump(merged, formatter="pickle")


MergeShiftedHistogramsWrapper = wrapper_factory(
    base_cls=AnalysisTask,
    require_cls=MergeShiftedHistograms,
    enable=["configs", "skip_configs", "datasets", "skip_datasets"],
)
