# coding: utf-8

"""
Tasks related to ML workflows.
"""
from __future__ import annotations

from collections import OrderedDict, defaultdict

import law
import luigi

from columnflow.tasks.framework.base import Requirements, AnalysisTask, DatasetTask, wrapper_factory
from columnflow.tasks.framework.mixins import (
    CalibratorsMixin,
    SelectorMixin,
    ProducerMixin,
    ProducersMixin,
    MLModelDataMixin,
    MLModelTrainingMixin,
    MLModelMixin,
    ChunkedIOMixin,
    CategoriesMixin,
    SelectorStepsMixin,
)
from columnflow.tasks.framework.plotting import ProcessPlotSettingMixin, PlotBase
from columnflow.tasks.framework.remote import RemoteWorkflow
from columnflow.tasks.framework.decorators import view_output_plots
from columnflow.tasks.reduction import ReducedEventsUser
from columnflow.tasks.production import ProduceColumns
from columnflow.util import dev_sandbox, safe_div, DotDict, maybe_import
from columnflow.columnar_util import set_ak_column


ak = maybe_import("awkward")


class PrepareMLEvents(
    MLModelDataMixin,
    ProducersMixin,
    ChunkedIOMixin,
    ReducedEventsUser,
    law.LocalWorkflow,
    RemoteWorkflow,
):
    sandbox = dev_sandbox(law.config.get("analysis", "default_columnar_sandbox"))

    allow_empty_ml_model = False

    # upstream requirements
    reqs = Requirements(
        ReducedEventsUser.reqs,
        RemoteWorkflow.reqs,
        ProduceColumns=ProduceColumns,
    )

    # strategy for handling missing source columns when adding aliases on event chunks
    missing_column_alias_strategy = "original"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # cache for producer inst
        self._preparation_producer_inst = law.no_value

        # complain when this task is run for events that are not needed for training
        if not self.events_used_in_training(
            self.config_inst,
            self.dataset_inst,
            self.global_shift_inst,
        ):
            raise Exception(
                f"for ML model '{self.ml_model_inst.cls_name}', the dataset "
                f"'{self.dataset_inst.name}' of config '{self.config_inst.name}' with shift "
                f"'{self.global_shift_inst.name}' is not intended to be run by "
                f"{self.__class__.__name__}",
            )

    @property
    def preparation_producer_inst(self):
        if self._preparation_producer_inst is not law.no_value:
            # producer has already been cached
            return self._preparation_producer_inst

        producer = self.ml_model_inst.preparation_producer(self.config_inst)

        if not producer:
            # set producer inst to None when no producer is requested
            self._preparation_producer_inst = None
            return self._preparation_producer_inst

        self._preparation_producer_inst = ProducerMixin.get_producer_inst(producer, {"task": self})

        # overwrite the sandbox when set
        sandbox = self._preparation_producer_inst.get_sandbox()
        if sandbox:
            self.sandbox = sandbox
            # rebuild the sandbox inst when already initialized
            if self._sandbox_initialized:
                self._initialize_sandbox(force=True)

        return self._preparation_producer_inst

    def workflow_requires(self):
        reqs = super().workflow_requires()

        # require the full merge forest
        reqs["events"] = self.reqs.ProvideReducedEvents.req(self)

        # add producer dependent requirements
        if self.preparation_producer_inst:
            reqs["preparation_producer"] = self.preparation_producer_inst.run_requires()

        # add producers to requirements
        if not self.pilot and self.producer_insts:
            reqs["producers"] = [
                self.reqs.ProduceColumns.req(self, producer=producer_inst.cls_name)
                for producer_inst in self.producer_insts
                if producer_inst.produced_columns
            ]

        return reqs

    def requires(self):
        reqs = {"events": self.reqs.ProvideReducedEvents.req(self)}

        if self.preparation_producer_inst:
            reqs["preparation_producer"] = self.preparation_producer_inst.run_requires()

        if self.producer_insts:
            reqs["producers"] = [
                self.reqs.ProduceColumns.req(self, producer=producer_inst.cls_name)
                for producer_inst in self.producer_insts
                if producer_inst.produced_columns
            ]

        return reqs

    workflow_condition = ReducedEventsUser.workflow_condition.copy()

    @workflow_condition.output
    def output(self):
        k = self.ml_model_inst.folds
        outputs = {
            "mlevents": law.SiblingFileCollection([
                self.target(f"mlevents_fold{f}of{k}_{self.branch}.parquet")
                for f in range(k)
            ]),
            "stats": self.target(f"stats_{self.branch}.parquet"),
        }
        return outputs

    @law.decorator.log
    @law.decorator.localize
    @law.decorator.safe_output
    def run(self):
        from columnflow.columnar_util import (
            Route, RouteFilter, sorted_ak_to_parquet, update_ak_array, add_ak_aliases,
        )

        # prepare inputs and outputs
        reqs = self.requires()
        inputs = self.input()
        outputs = self.output()
        output_chunks = [{} for _ in range(self.ml_model_inst.folds)]
        stats = defaultdict(float)

        # run the setup of the optional producer
        reader_targets = {}
        if self.preparation_producer_inst:
            reader_targets = self.preparation_producer_inst.run_setup(
                reqs["preparation_producer"],
                inputs["preparation_producer"],
            )

        # create a temp dir for saving intermediate files
        tmp_dir = law.LocalDirectoryTarget(is_tmp=True)
        tmp_dir.touch()

        # get shift dependent aliases
        aliases = self.local_shift_inst.x("column_aliases", {})

        # define columns that will to be written
        write_columns = set.union(*self.ml_model_inst.used_columns.values())
        route_filter = RouteFilter(write_columns)

        # define columns that need to be read
        read_columns = {Route("deterministic_seed")}
        read_columns |= set(map(Route, aliases.values()))
        read_columns |= write_columns
        if self.preparation_producer_inst:
            read_columns |= self.preparation_producer_inst.used_columns

        # stats for logging
        n_events = 0
        num_fold_events = {f: 0 for f in range(self.ml_model_inst.folds)}

        # iterate over chunks of events and columns
        files = [inputs["events"]["events"]]
        if self.producer_insts:
            files.extend([inp["columns"] for inp in inputs["producers"]])

        # prepare inputs for localization
        with law.localize_file_targets(
            [*files, *reader_targets.values()],
            mode="r",
        ) as inps:
            for (events, *columns), pos in self.iter_chunked_io(
                [inp.path for inp in inps],
                source_type=len(files) * ["awkward_parquet"] + [None] * len(reader_targets),
                read_columns=(len(files) + len(reader_targets)) * [read_columns],
            ):
                n_events += len(events)

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

                # generate fold indices
                events = set_ak_column(events, "fold_indices", events.deterministic_seed % self.ml_model_inst.folds)
                # invoke the optional producer
                if len(events) and self.preparation_producer_inst:
                    events = self.preparation_producer_inst(
                        events,
                        stats=stats,
                        fold_indices=events.fold_indices,
                        ml_model_inst=self.ml_model_inst,
                    )

                # read fold_indices from events array to allow masking training events
                fold_indices = events.fold_indices

                # remove columns
                events = route_filter(events)

                # optional check for finite values
                if self.check_finite_output:
                    self.raise_if_not_finite(events)

                # loop over folds, use indices to generate masks and project into files
                for f in range(self.ml_model_inst.folds):
                    fold_events = events[fold_indices == f]
                    num_fold_events[f] += len(fold_events)

                    # save as parquet via a thread in the same pool
                    chunk = tmp_dir.child(f"file_{f}_{pos.index}.parquet", type="f")
                    output_chunks[f][pos.index] = chunk
                    self.chunked_io.queue(sorted_ak_to_parquet, (fold_events, chunk.path))

            # merge output files of all folds
            for _output_chunks, output in zip(output_chunks, outputs["mlevents"].targets):
                sorted_chunks = [_output_chunks[key] for key in sorted(_output_chunks)]
                law.pyarrow.merge_parquet_task(
                    self, sorted_chunks, output, local=True, writer_opts=self.get_parquet_writer_opts(),
                )

            # save stats
            if not getattr(stats, "num_fold_events", None):
                stats["num_fold_events"] = num_fold_events
            outputs["stats"].dump(stats, indent=4, formatter="json")

            # some logs
            self.publish_message(f"total events: {n_events}")
            for f, n in num_fold_events.items():
                r = 100 * safe_div(n, n_events)
                self.publish_message(f"fold {' ' if f < 10 else ''}{f}: {n} ({r:.2f}%)")


# overwrite class defaults
check_finite_tasks = law.config.get_expanded("analysis", "check_finite_output", [], split_csv=True)
PrepareMLEvents.check_finite_output = ChunkedIOMixin.check_finite_output.copy(
    default=PrepareMLEvents.task_family in check_finite_tasks,
    add_default_to_description=True,
)

check_overlap_tasks = law.config.get_expanded("analysis", "check_overlapping_inputs", [], split_csv=True)
PrepareMLEvents.check_overlapping_inputs = ChunkedIOMixin.check_overlapping_inputs.copy(
    default=PrepareMLEvents.task_family in check_overlap_tasks,
    add_default_to_description=True,
)

PrepareMLEventsWrapper = wrapper_factory(
    base_cls=AnalysisTask,
    require_cls=PrepareMLEvents,
    enable=["configs", "skip_configs", "datasets", "skip_datasets"],
)


class MergeMLStats(
    MLModelDataMixin,
    ProducersMixin,
    SelectorMixin,
    CalibratorsMixin,
    DatasetTask,
    law.tasks.ForestMerge,
):
    # recursively merge 20 files into one
    merge_factor = 20

    # skip receiving some parameters via req
    exclude_params_req_get = {"workflow"}

    # upstream requirements
    reqs = Requirements(
        PrepareMLEvents=PrepareMLEvents,
    )

    def create_branch_map(self):
        # DatasetTask implements a custom branch map, but we want to use the one in ForestMerge
        return law.tasks.ForestMerge.create_branch_map(self)

    def merge_workflow_requires(self):
        return self.reqs.PrepareMLEvents.req(self, _exclude={"branches"})

    def merge_requires(self, start_branch, end_branch):
        return self.reqs.PrepareMLEvents.req(
            self,
            branches=((start_branch, end_branch),),
            workflow="local",
            _exclude={"branch"},
        )

    def merge_output(self):
        return {"stats": self.target("stats.json")}

    def trace_merge_inputs(self, inputs):
        return super().trace_merge_inputs(inputs["collection"].targets.values())

    @law.decorator.log
    def run(self):
        return super().run()

    def merge(self, inputs, output):
        # merge input stats
        merged_stats = defaultdict(float)
        for inp in inputs:
            stats = inp["stats"].load(formatter="json", cache=False)
            self.merge_counts(merged_stats, stats)

        # write the output
        output["stats"].dump(merged_stats, indent=4, formatter="json", cache=False)

    @classmethod
    def merge_counts(cls, dst: dict, src: dict) -> dict:
        """
        Adds counts (integers or floats) in a *src* dictionary recursively into a *dst* dictionary.
        *dst* is updated in-place and also returned.
        """
        for key, obj in src.items():
            if isinstance(obj, dict):
                cls.merge_counts(dst.setdefault(key, {}), obj)
            else:
                if key not in dst:
                    dst[key] = 0.0
                dst[key] += obj
        return dst


MergeMLStatsWrapper = wrapper_factory(
    base_cls=AnalysisTask,
    require_cls=MergeMLStats,
    enable=["configs", "skip_configs", "datasets", "skip_datasets", "shifts", "skip_shifts"],
)


class MergeMLEvents(
    MLModelDataMixin,
    ProducersMixin,
    SelectorMixin,
    CalibratorsMixin,
    DatasetTask,
    law.tasks.ForestMerge,
    RemoteWorkflow,
):
    sandbox = dev_sandbox(law.config.get("analysis", "default_columnar_sandbox"))

    fold = luigi.IntParameter(
        default=0,
        description="the fold index of the prepared ML events to use; must be compatible with the "
        "number of folds defined in the ML model; default: 0",
    )

    # disable the shift parameter
    shift = None
    effective_shift = None
    allow_empty_shift = True

    # in each step, merge 10 into 1
    merge_factor = 10

    allow_empty_ml_model = False

    # upstream requirements
    reqs = Requirements(
        RemoteWorkflow.reqs,
        PrepareMLEvents=PrepareMLEvents,
    )

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
        req = self.reqs.PrepareMLEvents.req(self, _exclude={"branches"})

        # if the workflow shape is known, allow the forest to be cached
        self._cache_forest = req.workflow_condition()

        return req

    def merge_requires(self, start_leaf, end_leaf):
        return [
            self.reqs.PrepareMLEvents.req(self, branch=i)
            for i in range(start_leaf, end_leaf)
        ]

    def trace_merge_inputs(self, inputs):
        return super().trace_merge_inputs([inp["mlevents"][self.fold] for inp in inputs])

    def merge_output(self):
        k = self.ml_model_inst.folds
        return {"mlevents": self.target(f"mlevents_f{self.fold}of{k}.parquet")}

    @law.decorator.log
    def run(self):
        return super().run()

    def merge(self, inputs, output):
        if not self.is_leaf():
            inputs = [inp["mlevents"] for inp in inputs]

        law.pyarrow.merge_parquet_task(
            self, inputs, output["mlevents"], writer_opts=self.get_parquet_writer_opts(),
        )


MergeMLEventsWrapper = wrapper_factory(
    base_cls=AnalysisTask,
    require_cls=MergeMLEvents,
    enable=["configs", "skip_configs", "datasets", "skip_datasets"],
)


class MLTraining(
    MLModelTrainingMixin,
    law.LocalWorkflow,
    RemoteWorkflow,
):

    allow_empty_ml_model = False

    # upstream requirements
    reqs = Requirements(
        RemoteWorkflow.reqs,
        MergeMLEvents=MergeMLEvents,
        MergeMLStats=MergeMLStats,
    )

    @property
    def sandbox(self):
        # determine the sandbox dynamically based on the response of the model
        return self.ml_model_inst.sandbox(self)

    @property
    def accepts_messages(self):
        return self.ml_model_inst.accepts_scheduler_messages

    @property
    def fold(self):
        return self.branch if self.is_branch() else None

    def create_branch_map(self):
        # each fold to train corresponds to one branch
        return list(range(self.ml_model_inst.folds))

    def workflow_requires(self):
        reqs = super().workflow_requires()

        reqs["events"] = {
            config_inst.name: {
                dataset_inst.name: [
                    self.reqs.MergeMLEvents.req(
                        self,
                        config=config_inst.name,
                        dataset=dataset_inst.name,
                        calibrators=_calibrators,
                        selector=_selector,
                        producers=_producers,
                        fold=fold,
                        tree_index=-1)
                    for fold in range(self.ml_model_inst.folds)
                ]
                for dataset_inst in dataset_insts
            }
            for (config_inst, dataset_insts), _calibrators, _selector, _producers in zip(
                self.ml_model_inst.used_datasets.items(),
                self.calibrators,
                self.selectors,
                self.producers,
            )
        }
        reqs["stats"] = {
            config_inst.name: {
                dataset_inst.name: self.reqs.MergeMLStats.req(
                    self,
                    config=config_inst.name,
                    dataset=dataset_inst.name,
                    calibrators=_calibrators,
                    selector=_selector,
                    producers=_producers,
                    tree_index=-1)
                for dataset_inst in dataset_insts
            }
            for (config_inst, dataset_insts), _calibrators, _selector, _producers in zip(
                self.ml_model_inst.used_datasets.items(),
                self.calibrators,
                self.selectors,
                self.producers,
            )
        }

        # ml model requirements
        reqs["model"] = self.ml_model_inst.requires(self)

        return reqs

    def requires(self):
        reqs = {}

        # require prepared events
        reqs["events"] = {
            config_inst.name: {
                dataset_inst.name: [
                    self.reqs.MergeMLEvents.req(
                        self,
                        config=config_inst.name,
                        dataset=dataset_inst.name,
                        calibrators=_calibrators,
                        selector=_selector,
                        producers=_producers,
                        fold=f,
                    )
                    for f in range(self.ml_model_inst.folds)
                    if f != self.branch
                ]
                for dataset_inst in dataset_insts
            }
            for (config_inst, dataset_insts), _calibrators, _selector, _producers in zip(
                self.ml_model_inst.used_datasets.items(),
                self.calibrators,
                self.selectors,
                self.producers,
            )
        }

        # ml model requirements
        reqs["model"] = self.ml_model_inst.requires(self)

        return reqs

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
    MLModelMixin,
    ProducersMixin,
    ChunkedIOMixin,
    ReducedEventsUser,
    law.LocalWorkflow,
    RemoteWorkflow,
):
    sandbox = None

    allow_empty_ml_model = False

    # strategy for handling missing source columns when adding aliases on event chunks
    missing_column_alias_strategy = "original"

    # upstream requirements
    reqs = Requirements(
        ReducedEventsUser.reqs,
        RemoteWorkflow.reqs,
        MLTraining=MLTraining,
        ProduceColumns=ProduceColumns,
    )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # cache for producer inst
        self._preparation_producer_inst = law.no_value

        # set the sandbox
        self.sandbox = self.ml_model_inst.sandbox(self)

    @property
    def preparation_producer_inst(self):
        if self._preparation_producer_inst is not law.no_value:
            # producer has already been cached
            return self._preparation_producer_inst

        producer = None
        if self.ml_model_inst.preparation_producer_in_ml_evaluation:
            # only consider preparation_producer in MLEvaluation if requested by model
            producer = self.ml_model_inst.preparation_producer(self.config_inst)

        if not producer:
            # set producer inst to None when no producer is requested
            self._preparation_producer_inst = None
            return self._preparation_producer_inst

        self._preparation_producer_inst = ProducerMixin.get_producer_inst(producer, {"task": self})

        # check that preparation_producer does not clash with ml_model_inst sandbox
        if (
            self._preparation_producer_inst.sandbox and
            self.sandbox != self._preparation_producer_inst.sandbox
        ):
            raise Exception(
                f"Task {self.__class__.__name__} got different sandboxes from the MLModel ({self.sandbox}) "
                f"than from the preparation_producer ({self._preparation_producer_inst.sandbox})",
            )

        return self._preparation_producer_inst

    def workflow_requires(self):
        reqs = super().workflow_requires()

        reqs["models"] = self.reqs.MLTraining.req_different_branching(
            self,
            configs=(self.config_inst.name,),
            calibrators=(self.calibrators,),
            selectors=(self.selector,),
            producers=(self.producers,),
        )

        reqs["events"] = self.reqs.ProvideReducedEvents.req(self)

        # add producer dependent requirements
        if self.preparation_producer_inst:
            reqs["preparation_producer"] = self.preparation_producer_inst.run_requires()

        if not self.pilot and self.producer_insts:
            reqs["producers"] = [
                self.reqs.ProduceColumns.req(self, producer=producer_inst.cls_name)
                for producer_inst in self.producer_insts
                if producer_inst.produced_columns
            ]

        return reqs

    def requires(self):
        reqs = {
            "models": self.reqs.MLTraining.req_different_branching(
                self,
                configs=(self.config_inst.name,),
                calibrators=(self.calibrators,),
                selectors=(self.selector,),
                producers=(self.producers,),
                branch=-1,
            ),
            "events": self.reqs.ProvideReducedEvents.req(self, _exclude=self.exclude_params_branch),
        }
        if self.preparation_producer_inst:
            reqs["preparation_producer"] = self.preparation_producer_inst.run_requires()

        if self.producer_insts:
            reqs["producers"] = [
                self.reqs.ProduceColumns.req(self, producer=producer_inst.cls_name)
                for producer_inst in self.producer_insts
                if producer_inst.produced_columns
            ]

        return reqs

    workflow_condition = ReducedEventsUser.workflow_condition.copy()

    @workflow_condition.output
    def output(self):
        return {"mlcolumns": self.target(f"mlcolumns_{self.branch}.parquet")}

    @law.decorator.log
    @law.decorator.localize
    @law.decorator.safe_output
    def run(self):
        from columnflow.columnar_util import (
            Route, RouteFilter, sorted_ak_to_parquet, update_ak_array, add_ak_aliases,
        )

        # prepare inputs and outputs
        reqs = self.requires()
        inputs = self.input()
        output = self.output()
        output_chunks = {}
        stats = defaultdict(float)

        # create a temp dir for saving intermediate files
        tmp_dir = law.LocalDirectoryTarget(is_tmp=True)
        tmp_dir.touch()

        # run the setup of the optional producer
        reader_targets = {}
        if self.preparation_producer_inst:
            reader_targets = self.preparation_producer_inst.run_setup(
                reqs["preparation_producer"],
                inputs["preparation_producer"],
            )

        # open all model files
        models = [
            self.ml_model_inst.open_model(inp)
            for inp in inputs["models"]["collection"].targets.values()
        ]

        # get shift dependent aliases
        aliases = self.local_shift_inst.x("column_aliases", {})

        # check once if the events were used during trainig
        events_used_in_training = self.events_used_in_training(
            self.config_inst,
            self.dataset_inst,
            self.global_shift_inst,
        )

        # define columns that need to be read
        read_columns = {Route("deterministic_seed")}
        read_columns |= set(map(Route, aliases.values()))
        read_columns |= set.union(*self.ml_model_inst.used_columns.values())
        if self.preparation_producer_inst:
            read_columns |= self.preparation_producer_inst.used_columns

        # define columns that will be written
        write_columns = set.union(*self.ml_model_inst.produced_columns.values())
        route_filter = RouteFilter(write_columns)

        # iterate over chunks of events and columns
        file_targets = [inputs["events"]["events"]]
        if self.producer_insts:
            file_targets.extend([inp["columns"] for inp in inputs["producers"]])
        if reader_targets:
            file_targets.extend(reader_targets.values())

        # prepare inputs for localization
        with law.localize_file_targets(
            [*file_targets, *reader_targets.values()],
            mode="r",
        ) as inps:
            for (events, *columns), pos in self.iter_chunked_io(
                [inp.path for inp in inps],
                source_type=len(file_targets) * ["awkward_parquet"] + [None] * len(reader_targets),
                read_columns=(len(file_targets) + len(reader_targets)) * [read_columns],
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

                # generate fold indices
                events = set_ak_column(events, "fold_indices", events.deterministic_seed % self.ml_model_inst.folds)

                # invoke the optional producer
                if len(events) and self.preparation_producer_inst:
                    events = self.preparation_producer_inst(
                        events,
                        stats=stats,
                        fold_indices=events.fold_indices,
                        ml_model_inst=self.ml_model_inst,
                    )

                # evaluate the model
                events = self.ml_model_inst.evaluate(
                    self,
                    events,
                    models,
                    events.fold_indices,
                    events_used_in_training=events_used_in_training,
                )

                # remove columns
                events = route_filter(events)

                # optional check for finite values
                if self.check_finite_output:
                    self.raise_if_not_finite(events)

                # save as parquet via a thread in the same pool
                chunk = tmp_dir.child(f"file_{pos.index}.parquet", type="f")
                output_chunks[pos.index] = chunk
                self.chunked_io.queue(sorted_ak_to_parquet, (events, chunk.path))

        # merge output files
        sorted_chunks = [output_chunks[key] for key in sorted(output_chunks)]
        law.pyarrow.merge_parquet_task(
            self, sorted_chunks, output["mlcolumns"], local=True, writer_opts=self.get_parquet_writer_opts(),
        )


# overwrite class defaults
MLEvaluation.check_finite_output = ChunkedIOMixin.check_finite_output.copy(
    default=MLEvaluation.task_family in check_finite_tasks,
    add_default_to_description=True,
)

check_overlap_tasks = law.config.get_expanded("analysis", "check_overlapping_inputs", [], split_csv=True)
MLEvaluation.check_overlapping_inputs = ChunkedIOMixin.check_overlapping_inputs.copy(
    default=MLEvaluation.task_family in check_overlap_tasks,
    add_default_to_description=True,
)


MLEvaluationWrapper = wrapper_factory(
    base_cls=AnalysisTask,
    require_cls=MLEvaluation,
    enable=["configs", "skip_configs", "shifts", "skip_shifts", "datasets", "skip_datasets"],
)


class MergeMLEvaluation(
    MLModelMixin,
    ProducersMixin,
    SelectorMixin,
    CalibratorsMixin,
    DatasetTask,
    law.tasks.ForestMerge,
    RemoteWorkflow,
):
    """
    Task to merge events for a dataset, where the `MLEvaluation` produces multiple parquet files.
    The task serves as a helper task for plotting the ML evaluation results in the `PlotMLResults` task.
    """
    sandbox = dev_sandbox("bash::$CF_BASE/sandboxes/venv_columnar.sh")

    # recursively merge 20 files into one
    merge_factor = 20

    # upstream requirements
    reqs = Requirements(
        RemoteWorkflow.reqs,
        MLEvaluation=MLEvaluation,
    )

    def create_branch_map(self):
        # DatasetTask implements a custom branch map, but we want to use the one in ForestMerge
        return law.tasks.ForestMerge.create_branch_map(self)

    def merge_workflow_requires(self):
        return self.reqs.MLEvaluation.req(self, _exclude={"branches"})

    def merge_requires(self, start_branch, end_branch):
        return [
            self.reqs.MLEvaluation.req(self, branch=b)
            for b in range(start_branch, end_branch)
        ]

    def merge_output(self):
        return {"mlcolumns": self.target("mlcolumns.parquet")}

    def merge(self, inputs, output):
        inputs = [inp["mlcolumns"] for inp in inputs]
        law.pyarrow.merge_parquet_task(
            self, inputs, output["mlcolumns"], writer_opts=self.get_parquet_writer_opts(),
        )


MergeMLEvaluationWrapper = wrapper_factory(
    base_cls=AnalysisTask,
    require_cls=MergeMLEvaluation,
    enable=["configs", "skip_configs", "datasets", "skip_datasets", "shifts", "skip_shifts"],
    docs="""
    Wrapper task to merge events for multiple datasets.

    :enables: ["configs", "skip_configs", "datasets", "skip_datasets", "shifts", "skip_shifts"]
    """,
)


class PlotMLResultsBase(
    ProcessPlotSettingMixin,
    CategoriesMixin,
    MLModelMixin,
    ProducersMixin,
    SelectorStepsMixin,
    CalibratorsMixin,
    law.LocalWorkflow,
    RemoteWorkflow,
):
    """
    A base class, used for the implementation of the ML plotting tasks. This class implements
    a ``plot_function`` parameter for choosing a desired plotting function and a ``prepare_inputs`` method,
    that returns a dict with the chosen events.
    """
    sandbox = dev_sandbox("bash::$CF_BASE/sandboxes/venv_columnar.sh")

    plot_function = PlotBase.plot_function.copy(
        default="columnflow.plotting.plot_ml_evaluation.plot_ml_evaluation",
        add_default_to_description=True,
        description="the full path given using the dot notation of the desired plot function.",
    )

    skip_processes = law.CSVParameter(
        default=(),
        description="comma seperated list of process names to skip; these processes will not be included in the plots. "
        "default: ()",
        brace_expand=True,
    )

    plot_sub_processes = luigi.BoolParameter(
        default=False,
        significant=False,
        description="when True, each process is divided into the different subprocesses; "
        "this option requires a ``process_ids`` column to be stored in the events; "
        "the ``process_ids`` column assignes a subprocess id number (predefined in the config) to each event; "
        "default: False",
    )

    skip_uncertainties = luigi.BoolParameter(
        default=False,
        significant=False,
        description="when True, count uncertainties (if available) are not included in the plot; default: False",
    )

    # upstream requirements
    reqs = Requirements(
        RemoteWorkflow.reqs,
        MergeMLEvaluation=MergeMLEvaluation,
    )

    def store_parts(self: PlotMLResultsBase):
        parts = super().store_parts()
        parts.insert_before("version", "plot", f"datasets_{self.datasets_repr}")
        return parts

    def create_branch_map(self: PlotMLResultsBase):
        return [
            DotDict({"category": cat_name})
            for cat_name in sorted(self.categories)
        ]

    def requires(self: PlotMLResultsBase):
        return {
            d: self.reqs.MergeMLEvaluation.req(
                self,
                dataset=d,
                branch=-1,
                _exclude={"branches"},
            )
            for d in self.datasets
        }

    def workflow_requires(self: PlotMLResultsBase, only_super: bool = False):
        reqs = super().workflow_requires()
        if only_super:
            return reqs

        reqs["merged_ml_evaluation"] = self.requires_from_branch()

        return reqs

    def output(self: PlotMLResultsBase) -> dict[str, list]:
        b = self.branch_data
        return {"plots": [
            self.target(name)
            for name in self.get_plot_names(f"plot__proc_{self.processes_repr}__cat_{b.category}")
        ]}

    def prepare_inputs(self: PlotMLResultsBase) -> dict[str, ak.Array]:
        """
        prepare the inputs for the plot function, based on the given configuration and category.

        :raises NotImplementedError: This error is raised if a given dataset contains more than one process.
        :raises ValueError: This error is raised if ``plot_sub_processes`` is used without providing the
            ``process_ids`` column in the data

        :return: dict[str, ak.Array]: A dictionary with the dataset names as keys and
            the corresponding predictions as values.
        """
        category_inst = self.config_inst.get_category(self.branch_data.category)
        leaf_category_insts = category_inst.get_leaf_categories() or [category_inst]
        process_insts = list(map(self.config_inst.get_process, self.processes))
        sub_process_insts = {
            proc: [sub for sub, _, _ in proc.walk_processes(include_self=True)]
            for proc in process_insts
        }

        all_events = OrderedDict()
        for dataset, inp in self.input().items():
            dataset_inst = self.config_inst.get_dataset(dataset)
            if len(dataset_inst.processes) != 1:
                raise NotImplementedError(
                    f"dataset {dataset_inst.name} has {len(dataset_inst.processes)} assigned, "
                    "which is not implemented yet.",
                )

            events = ak.from_parquet(inp["mlcolumns"].path)

            # masking with leaf categories
            category_mask = False
            for leaf in leaf_category_insts:
                category_mask = ak.where(ak.any(events.category_ids == leaf.id, axis=1), True, category_mask)

            events = events[category_mask]
            # loop per process
            for process_inst in process_insts:
                # skip when the dataset is already known to not contain any sub process
                if not any(map(dataset_inst.has_process, sub_process_insts[process_inst])):
                    continue

                if not self.plot_sub_processes:
                    if process_inst.name in all_events.keys():
                        all_events[process_inst.name] = ak.concatenate([
                            all_events[process_inst.name], getattr(events, self.ml_model),
                        ])
                    else:
                        all_events[process_inst.name] = getattr(events, self.ml_model)
                else:
                    if "process_ids" not in events.fields:
                        raise ValueError(
                            "No `process_ids` column stored in the events! "
                            f"Process selection for {dataset} cannot not be applied!",
                        )
                    for sub_process in sub_process_insts[process_inst]:
                        if sub_process.name in self.skip_processes:
                            continue

                        process_mask = ak.where(events.process_ids == sub_process.id, True, False)
                        if sub_process.name in all_events.keys():
                            all_events[sub_process.name] = ak.concatenate([
                                all_events[sub_process.name],
                                getattr(events[process_mask], self.ml_model),
                            ])
                        else:
                            all_events[sub_process.name] = getattr(events[process_mask], self.ml_model)
        return all_events


class PlotMLResults(PlotMLResultsBase):
    """
    A task that generates plots for machine learning results.

    This task generates plots for machine learning results based on the given
    configuration and category. The plots can be either a confusion matrix (CM) or a
    receiver operating characteristic (ROC) curve. This task uses the output of the
    MergeMLEvaluation task as input and saves the plots with the corresponding array
    used to create the plot.

    For the function to run correctly, the following input structure is required:
    * The ``category_ids`` column must be kept in the Evaluation and passed with the network outputs.
    (must be accessible via ``events.category_id`` and can be set by adding ``category_ids`` to the
    :py:meth:`~columnflow.ml.MLModel.uses` and :py:meth:`~columnflow.ml.MLModel.produces`
    methode of the ML-Model)
    * The outputs of the ML model must be stored under a column with the name of the model itself
    (This can be set in the :py:meth:`~columnflow.ml.MLModel.evaluate` methode of the model via:

    ``events = set_ak_column(events, f"{self.cls_name}.{output_i}", output_i)``

    ).
    """

    # override the plot_function parameter to be able to only choose between CM and ROC
    plot_function = luigi.ChoiceParameter(
        default="plot_cm",
        choices=["cm", "roc"],
        description="The name of the plot function to use. Can be either 'cm' or 'roc'.",
    )

    def prepare_plot_parameters(self: PlotMLResults):
        """
        Helper function to prepare the plot parameters for the plot function.
        Implemented to parse the axes labels from the general settings.
        """
        params = self.get_plot_parameters()

        # parse x_label and y_label from general settings
        for label in ["x_labels", "y_labels"]:
            if label in params.general_settings.keys():
                params.general_settings[label] = params.general_settings[label].split(";")

    def output(self: PlotMLResults):
        """
        override the output method to return the plots and the array used for plotting.
        """
        b = self.branch_data
        return {
            "plots": [
                self.target(name)
                for name in self.get_plot_names(
                    f"plot__{self.plot_function}__proc_{self.processes_repr}__cat_{b.category}/plot__0",
                )
            ],
            "array": self.target(
                f"plot__{self.plot_function}__proc_{self.processes_repr}__cat_{b.category}/data.pickle",
            ),
        }

    @law.decorator.log
    @view_output_plots
    def run(self: PlotMLResults):
        func_path = {
            "cm": "columnflow.plotting.plot_ml_evaluation.plot_cm",
            "roc": "columnflow.plotting.plot_ml_evaluation.plot_roc",
        }
        category_inst = self.config_inst.get_category(self.branch_data.category)
        self.prepare_plot_parameters()

        # call the plot function
        with self.publish_step(f"plotting in {category_inst.name}"):
            all_events = self.prepare_inputs()
            figs, array = self.call_plot_func(
                func_path.get(self.plot_function, self.plot_function),
                events=all_events,
                config_inst=self.config_inst,
                category_inst=category_inst,
                skip_uncertainties=self.skip_uncertainties,
                cms_llabel=self.cms_label,
                **self.get_plot_parameters(),
            )

            # save the outputs
            self.output()["array"].dump(array, formatter="pickle")
            for file_path in self.output()["plots"]:
                if file_path.ext() == "pdf":
                    from matplotlib.backends.backend_pdf import PdfPages
                    with PdfPages(file_path.abspath) as pdf:
                        for f in figs:
                            f.savefig(pdf, format="pdf")
                    continue

                for index, f in enumerate(figs):
                    f.savefig(
                        file_path.abs_dirname + "/" + file_path.basename.replace("0", str(index)),
                        format=file_path.ext(),
                    )
