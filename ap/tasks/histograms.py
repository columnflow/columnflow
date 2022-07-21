# coding: utf-8

"""
Task to produce and merge histograms.
"""

import functools

import law

from ap.tasks.framework.base import AnalysisTask, DatasetTask, wrapper_factory
from ap.tasks.framework.mixins import (
    CalibratorsMixin, SelectorStepsMixin, ProducersMixin, MLModelsMixin, VariablesMixin,
    ShiftSourcesMixin,
)
from ap.tasks.framework.remote import HTCondorWorkflow
from ap.tasks.reduction import MergeReducedEventsUser, MergeReducedEvents
from ap.tasks.production import ProduceColumns
from ap.tasks.ml import MLEvaluation
from ap.util import dev_sandbox


class CreateHistograms(
    MergeReducedEventsUser,
    MLModelsMixin,
    ProducersMixin,
    SelectorStepsMixin,
    CalibratorsMixin,
    VariablesMixin,
    law.LocalWorkflow,
    HTCondorWorkflow,
):

    sandbox = dev_sandbox("bash::$AP_BASE/sandboxes/venv_columnar.sh")

    shifts = MergeReducedEvents.shifts | ProduceColumns.shifts

    def workflow_requires(self, only_super: bool = False):
        reqs = super(CreateHistograms, self).workflow_requires()
        if only_super:
            return reqs

        reqs["events"] = MergeReducedEvents.req(self, _exclude={"branches"})
        if not self.pilot:
            if self.producers:
                reqs["producers"] = [ProduceColumns.req(self, producer=p) for p in self.producers]
            if self.ml_models:
                reqs["ml"] = [MLEvaluation.req(self, ml_model=m) for m in self.ml_models]

        return reqs

    def requires(self):
        reqs = {"events": MergeReducedEvents.req(self, tree_index=self.branch, _exclude={"branch"})}
        if self.producers:
            reqs["producers"] = [ProduceColumns.req(self, producer=p) for p in self.producers]
        if self.ml_models:
            reqs["ml"] = [MLEvaluation.req(self, ml_model=m) for m in self.ml_models]
        return reqs

    @MergeReducedEventsUser.maybe_dummy
    def output(self):
        return self.local_target(f"histograms_vars_{self.variables_repr}_{self.branch}.pickle")

    @law.decorator.safe_output
    @law.decorator.localize
    def run(self):
        import hist
        import numpy as np
        import awkward as ak
        from ap.columnar_util import (
            Route, ChunkedReader, update_ak_array, add_ak_aliases, has_ak_column,
        )

        # prepare inputs and outputs
        inputs = self.input()

        # declare output: dict of histograms
        histograms = {}

        # create a temp dir for saving intermediate files
        tmp_dir = law.LocalDirectoryTarget(is_tmp=True)
        tmp_dir.touch()

        # get shift dependent aliases
        aliases = self.shift_inst.x("column_aliases", {})

        # iterate over chunks of events and diffs
        files = [inputs["events"]["collection"][0].path]
        if self.producers:
            files.extend([inp.path for inp in inputs["producers"]])
        if self.ml_models:
            files.extend([inp.path for inp in inputs["ml"]])
        with ChunkedReader(
            files,
            source_type=len(files) * ["awkward_parquet"],
            # TODO: not working yet since parquet columns are nested
            # open_options=[{"columns": load_columns}] + (len(files) - 1) * [None],
        ) as reader:
            msg = f"iterate through {reader.n_entries} events ..."
            for (events, *columns), pos in self.iter_progress(reader, reader.n_chunks, msg=msg):
                # add additional columns
                events = update_ak_array(events, *columns)

                # add aliases
                events = add_ak_aliases(events, aliases, remove_src=True)

                # build the full event weight
                weight = ak.Array(np.ones(len(events)))
                if self.dataset_inst.is_mc:
                    for column in self.config_inst.x.event_weights:
                        weight = weight * events[Route(column).fields]
                    for column in self.dataset_inst.x("event_weights", []):
                        if has_ak_column(events, column):
                            weight = weight * events[Route(column).fields]
                        else:
                            self.logger.warning_once(
                                "missing_dataset_weight",
                                f"weight '{column}' for dataset {self.dataset_inst.name} not found",
                            )

                # define and fill histograms
                for var_name in self.variables:
                    variable_inst = self.config_inst.get_variable(var_name)

                    # get the expression and when it's a string, parse it to extract index lookups
                    expr = variable_inst.expression
                    if isinstance(expr, str):
                        route = Route.check(expr)
                        expr = functools.partial(route.apply, null_value=variable_inst.null_value)

                    if var_name not in histograms:
                        histograms[var_name] = (
                            hist.Hist.new
                            .IntCat([], name="category", growth=True)
                            .IntCat([], name="process", growth=True)
                            .IntCat([], name="shift", growth=True)
                            .Var(
                                variable_inst.bin_edges,
                                name=var_name,
                                label=variable_inst.get_full_x_title(),
                            )
                            .Weight()
                        )
                    # broadcast arrays so that each event can be filled for all its categories
                    fill_kwargs = {
                        var_name: expr(events),
                        "category": events.category_ids,
                        "process": events.process_id,
                        "shift": self.shift_inst.id,
                        "weight": weight,
                    }
                    arrays = (ak.flatten(a) for a in ak.broadcast_arrays(*fill_kwargs.values()))
                    histograms[var_name].fill(**dict(zip(fill_kwargs, arrays)))

        # merge output files
        self.output().dump(histograms, formatter="pickle")


CreateHistogramsWrapper = wrapper_factory(
    base_cls=AnalysisTask,
    require_cls=CreateHistograms,
    enable=["configs", "skip_configs", "datasets", "skip_datasets", "shifts", "skip_shifts"],
)


class MergeHistograms(
    DatasetTask,
    MLModelsMixin,
    ProducersMixin,
    SelectorStepsMixin,
    CalibratorsMixin,
    VariablesMixin,
    law.tasks.ForestMerge,
    HTCondorWorkflow,
):

    sandbox = dev_sandbox("bash::$AP_BASE/sandboxes/venv_columnar.sh")

    shifts = set(CreateHistograms.shifts)

    # in each step, merge 10 into 1
    merge_factor = 10

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # tell ForestMerge to not cache the internal merging structure by default,
        # (this is enabled in merge_workflow_requires)
        self._cache_forest = False

    def create_branch_map(self):
        # DatasetTask implements a custom branch map, but we want to use the one in ForestMerge
        return law.tasks.ForestMerge.create_branch_map(self)

    def merge_workflow_requires(self):
        req = CreateHistograms.req(self, _exclude={"branches"})

        # if the merging stats exist, allow the forest to be cached
        self._cache_forest = req.merging_stats_exist

        return req

    def merge_requires(self, start_leaf, end_leaf):
        return [CreateHistograms.req(self, branch=i) for i in range(start_leaf, end_leaf)]

    def merge_output(self):
        return self.local_target(f"histograms_vars_{self.variables_repr}.pickle")

    def merge(self, inputs, output):
        inputs_list = [inp.load(formatter="pickle") for inp in inputs]
        inputs_dict = {
            var_name: [hists[var_name] for hists in inputs_list]
            for var_name in inputs_list[0]
        }

        # do the merging
        merged = {
            var_name: sum(inputs_dict[var_name][1:], inputs_dict[var_name][0])
            for var_name in inputs_dict
        }

        output.dump(merged, formatter="pickle")


MergeHistogramsWrapper = wrapper_factory(
    base_cls=AnalysisTask,
    require_cls=MergeHistograms,
    enable=["configs", "skip_configs", "datasets", "skip_datasets", "shifts", "skip_shifts"],
)


class MergeShiftedHistograms(
    DatasetTask,
    MLModelsMixin,
    ProducersMixin,
    SelectorStepsMixin,
    CalibratorsMixin,
    VariablesMixin,
    ShiftSourcesMixin,
    law.LocalWorkflow,
    HTCondorWorkflow,
):

    sandbox = dev_sandbox("bash::$AP_BASE/sandboxes/venv_columnar.sh")

    # disable the shift parameter
    shift = None
    effective_shift = None
    allow_empty_shift = True

    def workflow_requires(self, only_super: bool = False):
        reqs = super(MergeShiftedHistograms, self).workflow_requires()
        if only_super:
            return reqs

        # add nominal and both directions per shift source
        for shift in ["nominal"] + self.shifts:
            reqs[shift] = MergeHistograms.req(
                self,
                shift=shift,
                tree_index=0,
                _exclude={"branches"},
                _prefer_cli={"variables"},
            )

        return reqs

    def requires(self):
        return {
            shift: MergeHistograms.req(
                self,
                shift=shift,
                branch=-1,
                tree_index=0,
                _exclude={"branches"},
                _prefer_cli={"variables"},
            )
            for shift in ["nominal"] + self.shifts
        }

    def create_branch_map(self):
        # create a dummy branch map so that this task could as a job
        return {0: None}

    def store_parts(self):
        parts = super(MergeShiftedHistograms, self).store_parts()
        parts.insert_after("dataset", "shift_sources", f"shifts_{self.shift_sources_repr}")
        return parts

    def output(self):
        return self.local_target(f"shifted_histograms_vars_{self.variables_repr}.pickle")

    def run(self):
        with self.publish_step(f"merging shift sources {', '.join(self.shift_sources)} ..."):
            inputs_list = [
                inp["collection"][0].load(formatter="pickle")
                for inp in self.input().values()
            ]
            inputs_dict = {
                var_name: [hists[var_name] for hists in inputs_list]
                for var_name in inputs_list[0]
            }

            # do the merging
            merged = {
                var_name: sum(inputs_dict[var_name][1:], inputs_dict[var_name][0])
                for var_name in inputs_dict
            }

            self.output().dump(merged, formatter="pickle")


MergeShiftedHistogramsWrapper = wrapper_factory(
    base_cls=AnalysisTask,
    require_cls=MergeShiftedHistograms,
    enable=["configs", "skip_configs", "datasets", "skip_datasets"],
)
