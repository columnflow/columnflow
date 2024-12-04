# coding: utf-8

"""
Task to produce and merge histograms.
"""

from __future__ import annotations

import luigi
import law

from columnflow.tasks.framework.base import Requirements, AnalysisTask, DatasetTask, wrapper_factory
from columnflow.tasks.framework.mixins import (
    CalibratorsMixin, SelectorStepsMixin, ProducersMixin, MLModelsMixin, VariablesMixin,
    ShiftSourcesMixin, WeightProducerMixin, ChunkedIOMixin,
)
from columnflow.tasks.framework.remote import RemoteWorkflow
from columnflow.tasks.framework.parameters import last_edge_inclusive_inst
from columnflow.tasks.reduction import ReducedEventsUser
from columnflow.tasks.production import ProduceColumns
from columnflow.tasks.ml import MLEvaluation
from columnflow.util import dev_sandbox
from columnflow.hist_util import create_hist_from_variables


class CreateHistograms(
    VariablesMixin,
    WeightProducerMixin,
    MLModelsMixin,
    ProducersMixin,
    ReducedEventsUser,
    ChunkedIOMixin,
    law.LocalWorkflow,
    RemoteWorkflow,
):
    last_edge_inclusive = last_edge_inclusive_inst

    sandbox = dev_sandbox(law.config.get("analysis", "default_columnar_sandbox"))

    # upstream requirements
    reqs = Requirements(
        ReducedEventsUser.reqs,
        RemoteWorkflow.reqs,
        ProduceColumns=ProduceColumns,
        MLEvaluation=MLEvaluation,
    )

    # strategy for handling missing source columns when adding aliases on event chunks
    missing_column_alias_strategy = "original"

    # names of columns that contain category ids
    # (might become a parameter at some point)
    category_id_columns = {"category_ids"}

    # register sandbox and shifts found in the chosen weight producer to this task
    register_weight_producer_sandbox = True
    register_weight_producer_shifts = True

    @law.util.classproperty
    def mandatory_columns(cls) -> set[str]:
        return set(cls.category_id_columns) | {"process_id"}

    def workflow_requires(self):
        reqs = super().workflow_requires()

        # require the full merge forest
        reqs["events"] = self.reqs.ProvideReducedEvents.req(self)

        if not self.pilot:
            if self.producer_insts:
                reqs["producers"] = [
                    self.reqs.ProduceColumns.req(self, producer=producer_inst.cls_name)
                    for producer_inst in self.producer_insts
                    if producer_inst.produced_columns
                ]
            if self.ml_model_insts:
                reqs["ml"] = [
                    self.reqs.MLEvaluation.req(self, ml_model=ml_model_inst.cls_name)
                    for ml_model_inst in self.ml_model_insts
                ]

            # add weight_producer dependent requirements
            reqs["weight_producer"] = law.util.make_unique(law.util.flatten(self.weight_producer_inst.run_requires()))

        return reqs

    def requires(self):
        reqs = {"events": self.reqs.ProvideReducedEvents.req(self)}

        if self.producer_insts:
            reqs["producers"] = [
                self.reqs.ProduceColumns.req(self, producer=producer_inst.cls_name)
                for producer_inst in self.producer_insts
                if producer_inst.produced_columns
            ]
        if self.ml_model_insts:
            reqs["ml"] = [
                self.reqs.MLEvaluation.req(self, ml_model=ml_model_inst.cls_name)
                for ml_model_inst in self.ml_model_insts
            ]

        # add weight_producer dependent requirements
        reqs["weight_producer"] = law.util.make_unique(law.util.flatten(self.weight_producer_inst.run_requires()))

        return reqs

    workflow_condition = ReducedEventsUser.workflow_condition.copy()

    @workflow_condition.output
    def output(self):
        return {"hists": self.target(f"histograms__vars_{self.variables_repr}__{self.branch}.pickle")}

    @law.decorator.log
    @law.decorator.localize(input=True, output=False)
    @law.decorator.safe_output
    def run(self):
        import numpy as np
        import awkward as ak
        from columnflow.columnar_util import (
            Route, update_ak_array, add_ak_aliases, has_ak_column, fill_hist,
        )

        # prepare inputs
        inputs = self.input()

        # declare output: dict of histograms
        histograms = {}

        # run the weight_producer setup
        producer_reqs = self.weight_producer_inst.run_requires()
        reader_targets = self.weight_producer_inst.run_setup(producer_reqs, luigi.task.getpaths(producer_reqs))

        # create a temp dir for saving intermediate files
        tmp_dir = law.LocalDirectoryTarget(is_tmp=True)
        tmp_dir.touch()

        # get shift dependent aliases
        aliases = self.local_shift_inst.x("column_aliases", {})

        # define columns that need to be read
        read_columns = {Route("process_id")}
        read_columns |= set(map(Route, self.category_id_columns))
        read_columns |= set(self.weight_producer_inst.used_columns)
        read_columns |= set(map(Route, aliases.values()))
        read_columns |= {
            Route(inp)
            for variable_inst in (
                self.config_inst.get_variable(var_name)
                for var_name in law.util.flatten(self.variable_tuples.values())
            )
            for inp in ((
                {variable_inst.expression}
                if isinstance(variable_inst.expression, str)
                # for variable_inst with custom expressions, read columns declared via aux key
                else set(variable_inst.x("inputs", []))
            ) | (
                # for variable_inst with selection, read columns declared via aux key
                set(variable_inst.x("inputs", []))
                if variable_inst.selection != "1"
                else set()
            ))
        }

        # empty float array to use when input files have no entries
        empty_f32 = ak.Array(np.array([], dtype=np.float32))

        # iterate over chunks of events and diffs
        file_targets = [inputs["events"]["events"]]
        if self.producer_insts:
            file_targets.extend([inp["columns"] for inp in inputs["producers"]])
        if self.ml_model_insts:
            file_targets.extend([inp["mlcolumns"] for inp in inputs["ml"]])

        # prepare inputs for localization
        with law.localize_file_targets(
            [*file_targets, *reader_targets.values()],
            mode="r",
        ) as inps:
            for (events, *columns), pos in self.iter_chunked_io(
                [inp.abspath for inp in inps],
                source_type=len(file_targets) * ["awkward_parquet"] + [None] * len(reader_targets),
                read_columns=(len(file_targets) + len(reader_targets)) * [read_columns],
                chunk_size=self.weight_producer_inst.get_min_chunk_size(),
            ):
                # optional check for overlapping inputs
                if self.check_overlapping_inputs:
                    self.raise_if_overlapping([events] + list(columns))

                # add additional columns
                events = update_ak_array(events, *columns)

                # add aliases
                events = add_ak_aliases(
                    events,
                    aliases,
                    remove_src=True,
                    missing_strategy=self.missing_column_alias_strategy,
                )

                # build the full event weight
                if hasattr(self.weight_producer_inst, "skip_func") and not self.weight_producer_inst.skip_func():
                    events, weight = self.weight_producer_inst(events)
                else:
                    weight = ak.Array(np.ones(len(events), dtype=np.float32))

                # define and fill histograms, taking into account multiple axes
                for var_key, var_names in self.variable_tuples.items():
                    # get variable instances
                    variable_insts = [self.config_inst.get_variable(var_name) for var_name in var_names]

                    if var_key not in histograms:
                        # create the histogram in the first chunk
                        histograms[var_key] = create_hist_from_variables(*variable_insts, add_default_axes=True)

                    # mask events and weights when selection expressions are found
                    masked_events = events
                    masked_weights = weight
                    for variable_inst in variable_insts:
                        sel = variable_inst.selection
                        if sel == "1":
                            continue
                        if not callable(sel):
                            raise ValueError(f"invalid selection '{sel}', for now only callables are supported")
                        mask = sel(masked_events)
                        masked_events = masked_events[mask]
                        masked_weights = masked_weights[mask]

                    # merge category ids
                    category_ids = ak.concatenate(
                        [Route(c).apply(masked_events) for c in self.category_id_columns],
                        axis=-1,
                    )

                    # broadcast arrays so that each event can be filled for all its categories
                    fill_data = {
                        "category": category_ids,
                        "process": masked_events.process_id,
                        "shift": np.ones(len(masked_events), dtype=np.int32) * self.global_shift_inst.id,
                        "weight": masked_weights,
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
                        fill_data[variable_inst.name] = expr(masked_events)

                    # fill it
                    fill_hist(
                        histograms[var_key],
                        fill_data,
                        last_edge_inclusive=self.last_edge_inclusive,
                    )

        # merge output files
        self.output()["hists"].dump(histograms, formatter="pickle")


# overwrite class defaults
check_overlap_tasks = law.config.get_expanded("analysis", "check_overlapping_inputs", [], split_csv=True)
CreateHistograms.check_overlapping_inputs = ChunkedIOMixin.check_overlapping_inputs.copy(
    default=CreateHistograms.task_family in check_overlap_tasks,
    add_default_to_description=True,
)


CreateHistogramsWrapper = wrapper_factory(
    base_cls=AnalysisTask,
    require_cls=CreateHistograms,
    enable=["configs", "skip_configs", "datasets", "skip_datasets", "shifts", "skip_shifts"],
)


class MergeHistograms(
    VariablesMixin,
    WeightProducerMixin,
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

    sandbox = dev_sandbox(law.config.get("analysis", "default_columnar_sandbox"))

    # upstream requirements
    reqs = Requirements(
        RemoteWorkflow.reqs,
        CreateHistograms=CreateHistograms,
    )

    @classmethod
    def req_params(cls, inst: AnalysisTask, **kwargs) -> dict:
        _prefer_cli = law.util.make_set(kwargs.get("_prefer_cli", [])) | {"variables"}
        kwargs["_prefer_cli"] = _prefer_cli
        return super().req_params(inst, **kwargs)

    def create_branch_map(self):
        # create a dummy branch map so that this task could be submitted as a job
        return {0: None}

    def _get_variables(self):
        if self.is_workflow():
            return self.as_branch()._get_variables()

        variables = self.variables

        # optional dynamic behavior: determine not yet created variables and require only those
        if self.only_missing:
            missing = self.output().count(existing=False, keys=True)[1]
            variables = sorted(missing, key=variables.index)

        return variables

    def workflow_requires(self):
        reqs = super().workflow_requires()

        if not self.pilot:
            variables = self._get_variables()
            if variables:
                reqs["hists"] = self.reqs.CreateHistograms.req_different_branching(
                    self,
                    branch=-1,
                    variables=tuple(variables),
                )

        return reqs

    def requires(self):
        variables = self._get_variables()
        if not variables:
            return []

        return self.reqs.CreateHistograms.req_different_branching(
            self,
            branch=-1,
            variables=tuple(variables),
            workflow="local",
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
    WeightProducerMixin,
    MLModelsMixin,
    ProducersMixin,
    SelectorStepsMixin,
    CalibratorsMixin,
    DatasetTask,
    law.LocalWorkflow,
    RemoteWorkflow,
):
    sandbox = dev_sandbox(law.config.get("analysis", "default_columnar_sandbox"))

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
