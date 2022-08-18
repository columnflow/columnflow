# coding: utf-8

"""
Tasks related to ML workflows.
"""

import law
import luigi

from columnflow.tasks.framework.base import AnalysisTask, DatasetTask, wrapper_factory
from columnflow.tasks.framework.mixins import (
    CalibratorsMixin, SelectorMixin, ProducersMixin, MLModelMixin,
)
from columnflow.tasks.framework.remote import HTCondorWorkflow
from columnflow.tasks.reduction import MergeReducedEventsUser, MergeReducedEvents
from columnflow.tasks.production import ProduceColumns
from columnflow.util import dev_sandbox, safe_div


class PrepareMLEvents(
    MergeReducedEventsUser,
    MLModelMixin,
    ProducersMixin,
    SelectorMixin,
    CalibratorsMixin,
    law.LocalWorkflow,
    HTCondorWorkflow,
):

    sandbox = dev_sandbox("bash::$CF_BASE/sandboxes/venv_columnar.sh")

    shifts = MergeReducedEvents.shifts | ProduceColumns.shifts

    # default upstream dependency task classes
    dep_MergeReducedEvents = MergeReducedEvents
    dep_ProduceColumns = ProduceColumns

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # complain when this task is run for events that are not needed for training
        if not self.events_used_in_training(self.dataset_inst, self.shift_inst):
            raise Exception(
                f"for ML model '{self.ml_model_inst.name}', the dataset '{self.dataset_inst.name}' "
                f"with shift '{self.shift_inst.name}' is not intended to be run by "
                f"{self.__class__.__name__}",
            )

    def workflow_requires(self, only_super: bool = False):
        reqs = super().workflow_requires()
        if only_super:
            return reqs

        reqs["events"] = self.dep_MergeReducedEvents.req(self, _exclude={"branches"})
        if not self.pilot and self.producers:
            reqs["producers"] = [
                self.dep_ProduceColumns.req(self, producer=p)
                for p in self.producers
            ]

        return reqs

    def requires(self):
        reqs = {"events": self.dep_MergeReducedEvents.req(self, tree_index=self.branch, _exclude={"branch"})}
        if self.producers:
            reqs["producers"] = [
                self.dep_ProduceColumns.req(self, producer=p)
                for p in self.producers
            ]
        return reqs

    @MergeReducedEventsUser.maybe_dummy
    def output(self):
        k = self.ml_model_inst.folds
        return law.SiblingFileCollection([
            self.target(f"mlevents_fold{f}of{k}_{self.branch}.parquet")
            for f in range(k)
        ])

    @law.decorator.log
    @law.decorator.localize
    @law.decorator.safe_output
    def run(self):
        from columnflow.columnar_util import (
            RouteFilter, ChunkedReader, sorted_ak_to_parquet, update_ak_array, add_ak_aliases,
        )

        # prepare inputs and outputs
        inputs = self.input()
        outputs = self.output()
        output_chunks = [{} for _ in range(self.ml_model_inst.folds)]

        # create a temp dir for saving intermediate files
        tmp_dir = law.LocalDirectoryTarget(is_tmp=True)
        tmp_dir.touch()

        # get shift dependent aliases
        aliases = self.shift_inst.x("column_aliases", {})

        # define nano columns that should be loaded and those that should be kept
        keep_columns = self.ml_model_inst.used_columns
        load_columns = keep_columns | {"deterministic_seed"}  # noqa
        route_filter = RouteFilter(keep_columns)

        # stats for logging
        n_events = 0
        n_fold_events = self.ml_model_inst.folds * [0]

        # iterate over chunks of events and columns
        files = [inputs["events"]["collection"][0].path]
        if self.producers:
            files.extend([inp.path for inp in inputs["producers"]])
        with ChunkedReader(
            files,
            source_type=len(files) * ["awkward_parquet"],
            # TODO: not working yet since parquet columns are nested
            # open_options=[{"columns": load_columns}] + (len(files) - 1) * [None],
        ) as reader:
            msg = f"iterate through {reader.n_entries} events ..."
            for (events, *columns), pos in self.iter_progress(reader, reader.n_chunks, msg=msg):
                n_events += len(events)

                # add additional columns
                events = update_ak_array(events, *columns)

                # add aliases
                events = add_ak_aliases(events, aliases, remove_src=True)

                # generate fold indices
                fold_indices = events.deterministic_seed % self.ml_model_inst.folds

                # remove columns
                events = route_filter(events)

                # loop over folds, use indices to generate masks and project into files
                for f in range(self.ml_model_inst.folds):
                    fold_events = events[fold_indices == f]
                    n_fold_events[f] += len(fold_events)

                    # save as parquet via a thread in the same pool
                    chunk = tmp_dir.child(f"file_{f}_{pos.index}.parquet", type="f")
                    output_chunks[f][pos.index] = chunk
                    reader.add_task(sorted_ak_to_parquet, (fold_events, chunk.path))

        # merge output files of all folds
        for _output_chunks, output in zip(output_chunks, outputs.targets):
            sorted_chunks = [_output_chunks[key] for key in sorted(_output_chunks)]
            law.pyarrow.merge_parquet_task(self, sorted_chunks, output, local=True)

        # some logs
        self.publish_message(f"total events: {n_events}")
        for f, n in enumerate(n_fold_events):
            r = 100 * safe_div(n, n_events)
            self.publish_message(f"partition {' ' if f < 10 else ''}{f}: {n} ({r:.2f}%)")


PrepareMLEventsWrapper = wrapper_factory(
    base_cls=AnalysisTask,
    require_cls=PrepareMLEvents,
    enable=["configs", "skip_configs", "datasets", "skip_datasets"],
)


class MergeMLEvents(
    DatasetTask,
    MLModelMixin,
    ProducersMixin,
    SelectorMixin,
    CalibratorsMixin,
    law.tasks.ForestMerge,
    HTCondorWorkflow,
):

    fold = luigi.IntParameter(
        default=0,
        description="the fold index of the prepared ML events to use; must be compatible with the "
        "number of folds defined in the ML model; default: 0",
    )

    sandbox = dev_sandbox("bash::$CF_BASE/sandboxes/venv_columnar.sh")

    # disable the shift parameter
    shift = None
    effective_shift = None
    allow_empty_shift = True

    # in each step, merge 10 into 1
    merge_factor = 10

    # default upstream dependency task classes
    dep_PrepareMLEvents = PrepareMLEvents

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        if not (0 <= self.fold < self.ml_model_inst.folds):
            raise ValueError(
                "the fold index is incompatible with the number of folds "
                f"({self.ml_model_inst.fold}) for the ML model '{self.ml_model}'",
            )

        # tell ForestMerge to not cache the internal merging structure by default,
        # (this is enabled in merge_workflow_requires)
        self._cache_forest = False

    def create_branch_map(self):
        # DatasetTask implements a custom branch map, but we want to use the one in ForestMerge
        return law.tasks.ForestMerge.create_branch_map(self)

    def merge_workflow_requires(self):
        req = self.dep_PrepareMLEvents.req(self, _exclude={"branches"})

        # if the merging stats exist, allow the forest to be cached
        self._cache_forest = req.merging_stats_exist

        return req

    def merge_requires(self, start_leaf, end_leaf):
        return [
            self.dep_PrepareMLEvents.req(self, branch=i)
            for i in range(start_leaf, end_leaf)
        ]

    def trace_merge_inputs(self, inputs):
        return super().trace_merge_inputs([inp[self.fold] for inp in inputs])

    def merge_output(self):
        k = self.ml_model_inst.folds
        return self.target(f"mlevents_f{self.fold}of{k}.parquet")

    @law.decorator.log
    def run(self):
        return super().run()

    def merge(self, inputs, output):
        law.pyarrow.merge_parquet_task(self, inputs, output)


MergeMLEventsWrapper = wrapper_factory(
    base_cls=AnalysisTask,
    require_cls=MergeMLEvents,
    enable=["configs", "skip_configs", "datasets", "skip_datasets"],
)


class MLTraining(MLModelMixin, ProducersMixin, SelectorMixin, CalibratorsMixin):

    fold = luigi.IntParameter(
        default=0,
        description="the fold index of the prepared ML events to _skip_; must be compatible with "
        "the number of folds defined in the ML model; default: 0",
    )

    # default upstream dependency task classes
    dep_MergeMLEvents = MergeMLEvents

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # verify the fold
        if not (0 <= self.fold < self.ml_model_inst.folds):
            raise ValueError(
                "the fold index is incompatible with the number of folds "
                f"({self.ml_model_inst.fold}) for the ML model '{self.ml_model}'",
            )

    @property
    def sandbox(self):
        # determine the sandbox dynamically based on the response of the model
        return self.ml_model_inst.sandbox(self)

    def requires(self):
        return {
            dataset_inst.name: [
                self.dep_MergeMLEvents.req(self, dataset=dataset_inst.name, fold=f)
                for f in range(self.ml_model_inst.folds)
                if f != self.fold
            ]
            for dataset_inst in self.ml_model_inst.used_datasets
        }

    def output(self):
        return self.ml_model_inst.output(self)

    @law.decorator.log
    @law.decorator.safe_output
    def run(self):
        # prepare inputs and outputs
        inputs = self.input()
        outputs = self.output()

        # call the training function
        self.ml_model_inst.train(self, inputs, outputs)


class MLEvaluation(
    MergeReducedEventsUser,
    MLModelMixin,
    ProducersMixin,
    SelectorMixin,
    CalibratorsMixin,
    law.LocalWorkflow,
    HTCondorWorkflow,
):

    sandbox = None

    shifts = MergeReducedEvents.shifts | ProduceColumns.shifts

    # default upstream dependency task classes
    dep_MLTraining = MLTraining
    dep_MergeReducedEvents = MergeReducedEvents
    dep_ProduceColumns = ProduceColumns

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # set the sandbox
        self.sandbox = self.ml_model_inst.sandbox(self)

    def workflow_requires(self, only_super: bool = False):
        reqs = super(MLEvaluation, self).workflow_requires()
        if only_super:
            return reqs

        reqs["models"] = [
            self.dep_MLTraining.req(self, fold=f)
            for f in range(self.ml_model_inst.folds)
        ]

        reqs["events"] = self.dep_MergeReducedEvents.req(self, _exclude={"branches"})
        if not self.pilot and self.producers:
            reqs["producers"] = [
                self.dep_ProduceColumns.req(self, producer=p)
                for p in self.producers
            ]

        return reqs

    def requires(self):
        reqs = {"models": [
            self.dep_MLTraining.req(self, fold=f)
            for f in range(self.ml_model_inst.folds)
        ]}

        reqs["events"] = self.dep_MergeReducedEvents.req(self, tree_index=self.branch, _exclude={"branch"})
        if self.producers:
            reqs["producers"] = [
                self.dep_ProduceColumns.req(self, producer=p)
                for p in self.producers
            ]

        return reqs

    @MergeReducedEventsUser.maybe_dummy
    def output(self):
        return self.target(f"mlcols_{self.branch}.pickle")

    @law.decorator.log
    @law.decorator.localize
    @law.decorator.safe_output
    def run(self):
        from columnflow.columnar_util import (
            RouteFilter, ChunkedReader, sorted_ak_to_parquet, update_ak_array, add_ak_aliases,
        )

        # prepare inputs and outputs
        inputs = self.input()
        output = self.output()
        output_chunks = {}

        # create a temp dir for saving intermediate files
        tmp_dir = law.LocalDirectoryTarget(is_tmp=True)
        tmp_dir.touch()

        # open all model files
        models = [self.ml_model_inst.open_model(inp) for inp in inputs["models"]]

        # get shift dependent aliases
        aliases = self.shift_inst.x("column_aliases", {})

        # check once if the events were used during trainig
        events_used_in_training = self.events_used_in_training(self.dataset_inst, self.shift_inst)

        # define nano columns that should be loaded and those that should be kept
        load_columns = self.ml_model_inst.used_columns | {"deterministic_seed"}  # noqa
        keep_columns = self.ml_model_inst.produced_columns
        route_filter = RouteFilter(keep_columns)

        # iterate over chunks of events and diffs
        files = [inputs["events"]["collection"][0].path]
        if self.producers:
            files.extend([inp.path for inp in inputs["producers"]])
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

                # asdasd
                fold_indices = events.deterministic_seed % self.ml_model_inst.folds

                # evaluate the model
                self.ml_model_inst.evaluate(
                    self,
                    events,
                    models,
                    fold_indices,
                    events_used_in_training=events_used_in_training,
                )

                # remove columns
                events = route_filter(events)

                # save as parquet via a thread in the same pool
                chunk = tmp_dir.child(f"file_{pos.index}.parquet", type="f")
                output_chunks[pos.index] = chunk
                reader.add_task(sorted_ak_to_parquet, (events, chunk.path))

        # merge output files
        sorted_chunks = [output_chunks[key] for key in sorted(output_chunks)]
        law.pyarrow.merge_parquet_task(self, sorted_chunks, output, local=True)


MLEvaluationWrapper = wrapper_factory(
    base_cls=AnalysisTask,
    require_cls=MLEvaluation,
    enable=["configs", "skip_configs", "shifts", "skip_shifts", "datasets", "skip_datasets"],
)
