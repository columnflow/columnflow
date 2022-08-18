# coding: utf-8

"""
Task to produce and merge histograms.
"""

import functools

import luigi
import law

from columnflow.tasks.framework.base import AnalysisTask, DatasetTask, wrapper_factory
from columnflow.tasks.framework.mixins import (
    CalibratorsMixin, SelectorStepsMixin, ProducersMixin, MLModelsMixin, VariablesMixin,
    ShiftSourcesMixin,
)
from columnflow.tasks.framework.remote import HTCondorWorkflow
from columnflow.tasks.reduction import MergeReducedEventsUser, MergeReducedEvents
from columnflow.tasks.production import ProduceColumns
from columnflow.tasks.ml import MLEvaluation
from columnflow.util import dev_sandbox


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

    sandbox = dev_sandbox("bash::$CF_BASE/sandboxes/venv_columnar.sh")

    shifts = MergeReducedEvents.shifts | ProduceColumns.shifts

    # default upstream dependency task classes
    dep_MergeReducedEvents = MergeReducedEvents
    dep_ProduceColumns = ProduceColumns
    dep_MLEvaluation = MLEvaluation

    def workflow_requires(self, only_super: bool = False):
        reqs = super(CreateHistograms, self).workflow_requires()
        if only_super:
            return reqs

        reqs["events"] = self.dep_MergeReducedEvents.req(self, _exclude={"branches"})
        if not self.pilot:
            if self.producers:
                reqs["producers"] = [
                    self.dep_ProduceColumns.req(self, producer=p)
                    for p in self.producers
                ]
            if self.ml_models:
                reqs["ml"] = [
                    self.dep_MLEvaluation.req(self, ml_model=m)
                    for m in self.ml_models
                ]

        return reqs

    def requires(self):
        reqs = {"events": self.dep_MergeReducedEvents.req(self, tree_index=self.branch, _exclude={"branch"})}
        if self.producers:
            reqs["producers"] = [
                self.dep_ProduceColumns.req(self, producer=p)
                for p in self.producers
            ]
        if self.ml_models:
            reqs["ml"] = [
                self.dep_MLEvaluation.req(self, ml_model=m)
                for m in self.ml_models
            ]
        return reqs

    @MergeReducedEventsUser.maybe_dummy
    def output(self):
        return self.target(f"histograms__vars_{self.variables_repr}__{self.branch}.pickle")

    @law.decorator.log
    @law.decorator.localize(input=True, output=False)
    @law.decorator.safe_output
    def run(self):
        import hist
        import numpy as np
        import awkward as ak
        from columnflow.columnar_util import (
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
                        route = Route(expr)
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
    law.LocalWorkflow,
    HTCondorWorkflow,
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

    shifts = set(CreateHistograms.shifts)

    # default upstream dependency task classes
    dep_CreateHistograms = CreateHistograms

    def create_branch_map(self):
        # create a dummy branch map so that this task could as a job
        return {0: None}

    def workflow_requires(self, only_super: bool = False):
        reqs = super().workflow_requires()
        if only_super:
            return reqs

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

        return self.dep_CreateHistograms.req(
            self,
            branch=-1,
            variables=tuple(variables),
            _exclude={"branches"},
            _prefer_cli=prefer_cli,
        )

    def output(self):
        return law.SiblingFileCollection({
            variable_name: self.target(f"hist__{variable_name}.pickle")
            for variable_name in self.variables
        })

    @law.decorator.log
    def run(self):
        # preare inputs and outputs
        inputs = self.input()["collection"]
        outputs = self.output()

        # load input histograms
        hists = [
            inp.load(formatter="pickle")
            for inp in self.iter_progress(inputs.targets.values(), len(inputs), reach=(0, 50))
        ]

        # create a separate file per output variable
        variable_names = list(hists[0].keys())
        for variable_name in self.iter_progress(variable_names, len(variable_names), reach=(50, 100)):
            self.publish_message(f"merging histograms for '{variable_name}'")

            variable_hists = [h[variable_name] for h in hists]
            merged = sum(variable_hists[1:], variable_hists[0].copy())
            outputs[variable_name].dump(merged, formatter="pickle")

        # optionally remove inputs
        if self.remove_previous:
            inputs.remove()


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

    sandbox = dev_sandbox("bash::$CF_BASE/sandboxes/venv_columnar.sh")

    # disable the shift parameter
    shift = None
    effective_shift = None
    allow_empty_shift = True

    # default upstream dependency task classes
    dep_MergeHistograms = MergeHistograms

    def create_branch_map(self):
        # create a dummy branch map so that this task could as a job
        return {0: None}

    def workflow_requires(self, only_super: bool = False):
        reqs = super(MergeShiftedHistograms, self).workflow_requires()
        if only_super:
            return reqs

        # add nominal and both directions per shift source
        for shift in ["nominal"] + self.shifts:
            reqs[shift] = self.dep_MergeHistograms.req(self, shift=shift, _prefer_cli={"variables"})

        return reqs

    def requires(self):
        return {
            shift: self.dep_MergeHistograms.req(self, shift=shift, _prefer_cli={"variables"})
            for shift in ["nominal"] + self.shifts
        }

    def store_parts(self):
        parts = super(MergeShiftedHistograms, self).store_parts()
        parts.insert_after("dataset", "shift_sources", f"shifts_{self.shift_sources_repr}")
        return parts

    def output(self):
        return law.SiblingFileCollection({
            variable_name: self.target(f"shifted_hist__{variable_name}.pickle")
            for variable_name in self.variables
        })

    @law.decorator.log
    def run(self):
        # preare inputs and outputs
        inputs = self.input()
        outputs = self.output().targets

        for variable_name, outp in self.iter_progress(outputs.items(), len(outputs)):
            self.publish_message(f"merging histograms for '{variable_name}'")

            # load hists
            variable_hists = [
                coll.targets[variable_name].load(formatter="pickle")
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
