# coding: utf-8

"""
Tasks related to selecting events and performing post-selection bookkeeping.
"""

import copy
from collections import defaultdict

import luigi
import law

from columnflow.types import Any

from columnflow.tasks.framework.base import Requirements, AnalysisTask, wrapper_factory
from columnflow.tasks.framework.mixins import CalibratorsMixin, SelectorMixin, ChunkedIOMixin, ProducerMixin
from columnflow.tasks.framework.remote import RemoteWorkflow
from columnflow.tasks.framework.decorators import on_failure
from columnflow.tasks.external import GetDatasetLFNs
from columnflow.tasks.calibration import CalibrateEvents
from columnflow.util import maybe_import, ensure_proxy, dev_sandbox, safe_div, DotDict
from columnflow.tasks.framework.parameters import DerivableInstParameter


np = maybe_import("numpy")
ak = maybe_import("awkward")


logger = law.logger.get_logger(__name__)

default_create_selection_hists = law.config.get_expanded_bool(
    "analysis",
    "default_create_selection_hists",
    True,
)


class _SelectEvents(
    CalibratorsMixin,
    SelectorMixin,
    ChunkedIOMixin,
    law.LocalWorkflow,
    RemoteWorkflow,
):
    """
    Base classes for :py:class:`SelectEvents`.
    """


class SelectEvents(_SelectEvents):

    # disable selector steps
    selector_steps = None

    # default sandbox, might be overwritten by selector function
    sandbox = dev_sandbox(law.config.get("analysis", "default_columnar_sandbox"))

    # upstream requirements
    reqs = Requirements(
        RemoteWorkflow.reqs,
        GetDatasetLFNs=GetDatasetLFNs,
        CalibrateEvents=CalibrateEvents,
    )

    invokes_selector = True
    missing_column_alias_strategy = "original"
    create_selection_hists = default_create_selection_hists

    def workflow_requires(self):
        reqs = super().workflow_requires()

        reqs["lfns"] = self.reqs.GetDatasetLFNs.req(self)

        if not self.pilot:
            reqs["calibrations"] = [
                self.reqs.CalibrateEvents.req(
                    self,
                    calibrator=calibrator_inst.cls_name,
                    calibrator_inst=calibrator_inst,
                )
                for calibrator_inst in self.calibrator_insts
                if calibrator_inst.produced_columns
            ]
        elif self.calibrator_insts:
            # pass-through pilot workflow requirements of upstream task
            t = self.reqs.CalibrateEvents.req(self)
            reqs = law.util.merge_dicts(reqs, t.workflow_requires(), inplace=True)

        # add selector dependent requirements
        reqs["selector"] = law.util.make_unique(law.util.flatten(
            self.selector_inst.run_requires(task=self),
        ))

        return reqs

    def requires(self):
        reqs = {
            "lfns": self.reqs.GetDatasetLFNs.req(self),
            "calibrations": [
                self.reqs.CalibrateEvents.req(
                    self,
                    calibrator=calibrator_inst.cls_name,
                    calibrator_inst=calibrator_inst,
                )
                for calibrator_inst in self.calibrator_insts
                if calibrator_inst.produced_columns
            ],
        }

        # add selector dependent requirements
        reqs["selector"] = law.util.make_unique(law.util.flatten(
            self.selector_inst.run_requires(task=self),
        ))

        return reqs

    def output(self):
        outputs = {
            "results": self.target(f"results_{self.branch}.parquet"),
            "stats": self.target(f"stats_{self.branch}.json"),
        }

        # add histograms if requested
        if self.create_selection_hists:
            outputs["hists"] = self.target(f"hists_{self.branch}.pickle")

        # add additional columns in case the selector produces some
        if self.selector_inst.produced_columns:
            outputs["columns"] = self.target(f"columns_{self.branch}.parquet")

        return outputs

    @law.decorator.notify
    @law.decorator.log
    @ensure_proxy
    @law.decorator.localize(input=False)
    @law.decorator.safe_output
    @on_failure(callback=lambda task: task.teardown_selector_inst())
    def run(self):
        from columnflow.tasks.histograms import CreateHistograms
        from columnflow.columnar_util import (
            Route, RouteFilter, mandatory_coffea_columns, update_ak_array, add_ak_aliases,
            sorted_ak_to_parquet,
        )

        # prepare inputs and outputs
        lfn_task = self.requires()["lfns"]
        inputs = self.input()
        outputs = self.output()
        result_chunks = {}
        column_chunks = {}
        stats = defaultdict(float)
        hists = DotDict()

        # run the selector setup
        self._array_function_post_init()
        selector_reqs = self.selector_inst.run_requires(task=self)
        reader_targets = self.selector_inst.run_setup(
            task=self,
            reqs=selector_reqs,
            inputs=luigi.task.getpaths(selector_reqs),
        )
        n_ext = len(reader_targets)

        # show an early warning in case the selector does not produce some mandatory columns
        produced_columns = self.selector_inst.produced_columns
        for c in self.reqs.get("CreateHistograms", CreateHistograms).mandatory_columns:
            if Route(c) not in produced_columns:
                self.logger.warning(
                    f"selector {self.selector_inst.cls_name} does not produce column {c} "
                    "which might be required later on for creating histograms downstream",
                )

        # create a temp dir for saving intermediate files
        tmp_dir = law.LocalDirectoryTarget(is_tmp=True)
        tmp_dir.touch()

        # get shift dependent aliases
        aliases = self.local_shift_inst.x("column_aliases", {})

        # define columns that need to be read
        read_columns = set(map(Route, mandatory_coffea_columns))
        read_columns |= self.selector_inst.used_columns
        read_columns |= set(map(Route, aliases.values()))

        # define columns that will be written
        write_columns = set(map(Route, mandatory_coffea_columns))
        write_columns |= self.selector_inst.produced_columns
        route_filter = RouteFilter(keep=write_columns)

        # let the lfn_task prepare the nano file (basically determine a good pfn)
        [(lfn_index, input_file)] = lfn_task.iter_nano_files(self)

        # prepare inputs for localization
        with law.localize_file_targets(
            [
                input_file,
                *(inp["columns"] for inp in inputs["calibrations"]),
                *reader_targets.values(),
            ],
            mode="r",
        ) as inps:
            # iterate over chunks of events and diffs
            n_calib = len(inputs["calibrations"])
            for (events, *cols), pos in self.iter_chunked_io(
                [inp.abspath for inp in inps],
                source_type=["coffea_root"] + ["awkward_parquet"] * n_calib + [None] * n_ext,
                read_columns=[read_columns] * (1 + n_calib + n_ext),
                chunk_size=self.selector_inst.get_min_chunk_size(),
            ):
                # optional check for overlapping inputs within additional columns
                if self.check_overlapping_inputs:
                    self.raise_if_overlapping(list(cols))

                # insert additional columns
                events = update_ak_array(events, *cols)

                # add aliases
                events = add_ak_aliases(
                    events,
                    aliases,
                    remove_src=True,
                    missing_strategy=self.missing_column_alias_strategy,
                )

                # invoke the selection function
                events, results = self.selector_inst(events, task=self, stats=stats, hists=hists)

                # complain when there is no event mask
                if results.event is None:
                    raise Exception(
                        f"selector {self.selector_inst.cls_name} returned {results!r} object that "
                        "does not contain 'event' mask",
                    )

                # convert to array
                results_array = results.to_ak()

                # optional check for finite values
                if self.check_finite_output:
                    self.raise_if_not_finite(results_array)

                # save results as parquet via a thread in the same pool
                chunk = tmp_dir.child(f"res_{lfn_index}_{pos.index}.parquet", type="f")
                result_chunks[(lfn_index, pos.index)] = chunk
                self.chunked_io.queue(sorted_ak_to_parquet, (results_array, chunk.abspath))

                # remove columns
                if write_columns:
                    events = route_filter(events)

                    # optional check for finite values
                    if self.check_finite_output:
                        self.raise_if_not_finite(events)

                    # save additional columns as parquet via a thread in the same pool
                    chunk = tmp_dir.child(f"cols_{lfn_index}_{pos.index}.parquet", type="f")
                    column_chunks[(lfn_index, pos.index)] = chunk
                    self.chunked_io.queue(sorted_ak_to_parquet, (events, chunk.abspath))

        # teardown the selector
        self.teardown_selector_inst()

        # merge the result files
        sorted_chunks = [result_chunks[key] for key in sorted(result_chunks)]
        writer_opts_masks = self.get_parquet_writer_opts(repeating_values=True)
        law.pyarrow.merge_parquet_task(
            task=self,
            inputs=sorted_chunks,
            output=outputs["results"],
            local=True,
            writer_opts=writer_opts_masks,
            target_row_group_size=self.merging_row_group_size,
        )

        # merge the column files
        if write_columns:
            sorted_chunks = [column_chunks[key] for key in sorted(column_chunks)]
            law.pyarrow.merge_parquet_task(
                task=self,
                inputs=sorted_chunks,
                output=outputs["columns"],
                local=True,
                writer_opts=self.get_parquet_writer_opts(),
                target_row_group_size=self.merging_row_group_size,
            )

        # save stats
        outputs["stats"].dump(stats, formatter="json")
        if self.create_selection_hists:
            outputs["hists"].dump(hists, formatter="pickle")

        # print some stats
        eff = safe_div(stats["num_events_selected"], stats["num_events"])
        eff_weighted = safe_div(stats["sum_mc_weight_selected"], stats["sum_mc_weight"])
        self.publish_message(f"all events         : {int(stats['num_events'])}")
        self.publish_message(f"sel. events        : {int(stats['num_events_selected'])}")
        self.publish_message(f"efficiency         : {eff:.4f}")
        self.publish_message(f"sum mc weights     : {stats['sum_mc_weight']}")
        self.publish_message(f"sum sel. mc weights: {stats['sum_mc_weight_selected']}")
        self.publish_message(f"efficiency         : {eff_weighted:.4f}")
        if not eff:
            self.publish_message(law.util.colored("no events selected", "red"))


# overwrite class defaults
check_finite_tasks = law.config.get_expanded("analysis", "check_finite_output", [], split_csv=True)
SelectEvents.check_finite_output = ChunkedIOMixin.check_finite_output.copy(
    default=SelectEvents.task_family in check_finite_tasks,
    add_default_to_description=True,
)

check_overlap_tasks = law.config.get_expanded("analysis", "check_overlapping_inputs", [], split_csv=True)
SelectEvents.check_overlapping_inputs = ChunkedIOMixin.check_overlapping_inputs.copy(
    default=SelectEvents.task_family in check_overlap_tasks,
    add_default_to_description=True,
)


SelectEventsWrapper = wrapper_factory(
    base_cls=AnalysisTask,
    require_cls=SelectEvents,
    enable=["configs", "skip_configs", "datasets", "skip_datasets", "shifts", "skip_shifts"],
)


class _MergeSelectionStats(
    CalibratorsMixin,
    SelectorMixin,
    law.LocalWorkflow,
    RemoteWorkflow,
):
    """
    Base classes for :py:class:`MergeSelectionStats`.
    """


class MergeSelectionStats(_MergeSelectionStats):

    # default sandbox, might be overwritten by selector function (needed to load hist objects)
    sandbox = dev_sandbox(law.config.get("analysis", "default_columnar_sandbox"))

    # whether histogram outputs should be created
    create_selection_hists = default_create_selection_hists

    # upstream requirements
    reqs = Requirements(
        RemoteWorkflow.reqs,
        SelectEvents=SelectEvents,
    )

    def create_branch_map(self):
        # single branch without payload
        return {0: None}

    def workflow_requires(self):
        reqs = super().workflow_requires()
        reqs["stats"] = self.reqs.SelectEvents.req_different_branching(self)
        return reqs

    def requires(self):
        return self.reqs.SelectEvents.req_different_branching(self, workflow="local", branch=-1)

    def output(self):
        outputs = {"stats": self.target("stats.json")}
        if self.create_selection_hists:
            outputs["hists"] = self.target("hists.pickle")
        return outputs

    @law.decorator.notify
    @law.decorator.log
    def run(self):
        # merge input stats
        merged_stats = defaultdict(float)
        merged_hists = {}
        for inp in self.input().collection.targets.values():
            stats = inp["stats"].load(formatter="json", cache=False)
            self.merge_counts(merged_stats, stats)
            if self.create_selection_hists:
                hists = inp["hists"].load(formatter="pickle", cache=False)
                self.merge_counts(merged_hists, hists)

        # write outputs
        outputs = self.output()
        outputs["stats"].dump(merged_stats, formatter="json", cache=False)
        if self.create_selection_hists:
            outputs["hists"].dump(merged_hists, formatter="pickle", cache=False)

    @classmethod
    def merge_counts(cls, dst: dict, src: dict) -> dict:
        """
        Adds counts (integers or floats) in a *src* dictionary recursively into a *dst* dictionary.
        *dst* is updated in-place and also returned.
        """
        for key, obj in src.items():
            if key not in dst:
                dst[key] = copy.deepcopy(obj)
            elif isinstance(obj, dict):
                cls.merge_counts(dst[key], obj)
            else:
                dst[key] += obj
        return dst


MergeSelectionStatsWrapper = wrapper_factory(
    base_cls=AnalysisTask,
    require_cls=MergeSelectionStats,
    enable=["configs", "skip_configs", "datasets", "skip_datasets", "shifts", "skip_shifts"],
)


class _MergeSelectionMasks(
    CalibratorsMixin,
    SelectorMixin,
    law.tasks.ForestMerge,
    RemoteWorkflow,
):
    """
    Base classes for :py:class:`MergeSelectionMasks`.
    """


class MergeSelectionMasks(_MergeSelectionMasks):

    sandbox = dev_sandbox(law.config.get("analysis", "default_columnar_sandbox"))

    # recursively merge 8 files into one
    merge_factor = 8

    # upstream requirements
    reqs = Requirements(
        RemoteWorkflow.reqs,
        SelectEvents=SelectEvents,
    )

    norm_weights_producer = "normalization_weights"
    norm_weight_producer_inst = DerivableInstParameter(
        default=None,
        visibility=luigi.parameter.ParameterVisibility.PRIVATE,
    )

    exclude_params_index = {"norm_weight_producer_inst"}
    exclude_params_repr = {"norm_weight_producer_inst"}
    exclude_params_sandbox = {"norm_weight_producer_inst"}
    exclude_params_remote_workflow = {"norm_weight_producer_inst"}

    @classmethod
    def get_producer_dict(cls, params: dict[str, Any]) -> dict[str, Any]:
        return cls.get_array_function_dict(params)

    build_producer_inst = ProducerMixin.build_producer_inst

    @classmethod
    def resolve_instances(cls, params: dict[str, Any], shifts) -> dict[str, Any]:
        if not params.get("norm_weight_producer_inst"):
            params["norm_weight_producer_inst"] = cls.build_producer_inst(cls.norm_weights_producer, params)

        params = super().resolve_instances(params, shifts)

        return params

    def create_branch_map(self):
        # DatasetTask implements a custom branch map, but we want to use the one in ForestMerge
        return law.tasks.ForestMerge.create_branch_map(self)

    def merge_workflow_requires(self):
        reqs = {"selection": self.reqs.SelectEvents.req_different_branching(self)}

        if self.dataset_inst.is_mc:
            reqs["normalization"] = self.norm_weight_producer_inst.run_requires(task=self)

        return reqs

    def merge_requires(self, start_branch, end_branch):
        reqs = {
            "selection": [
                self.reqs.SelectEvents.req_different_branching(self, branch=b)
                for b in range(start_branch, end_branch)
            ],
        }

        if self.dataset_inst.is_mc:
            reqs["normalization"] = self.norm_weight_producer_inst.run_requires(task=self)

        return reqs

    def trace_merge_workflow_inputs(self, inputs):
        return super().trace_merge_workflow_inputs(inputs["selection"])

    def trace_merge_inputs(self, inputs):
        return super().trace_merge_inputs(inputs["selection"])

    def merge_output(self):
        return {"masks": self.target("masks.parquet")}

    def merge(self, inputs, output):
        # in the lowest (leaf) stage, zip selection results with additional columns first
        if self.is_leaf():
            # create a temp dir for saving intermediate files
            tmp_dir = law.LocalDirectoryTarget(is_tmp=True)
            tmp_dir.touch()
            inputs = self.zip_results_and_columns(inputs, tmp_dir)
        else:
            inputs = [inp["masks"] for inp in inputs]

        law.pyarrow.merge_parquet_task(
            self, inputs, output["masks"], writer_opts=self.get_parquet_writer_opts(),
        )

    def zip_results_and_columns(self, inputs, tmp_dir):
        from columnflow.columnar_util import (
            Route, RouteFilter, sorted_ak_to_parquet, mandatory_coffea_columns,
        )

        # setup the normalization weights producer
        if self.dataset_inst.is_mc:
            self._array_function_post_init()
            self.norm_weight_producer_inst.run_post_init(task=self)
            self.norm_weight_producer_inst.run_setup(
                task=self,
                reqs=self.requires()["forest_merge"]["normalization"],
                inputs=self.input()["forest_merge"]["normalization"],
            )

        # define columns that will be written
        write_columns: set[Route] = set()
        skip_columns: set[str] = set()
        for c in self.config_inst.x.keep_columns.get(self.task_family, []):
            for r in self._expand_keep_column(c):
                if r.has_tag("skip"):
                    skip_columns.add(r.column)
                else:
                    write_columns.add(r)
        write_columns = {
            r for r in write_columns
            if not law.util.multi_match(r.column, skip_columns, mode=any)
        }
        # add some mandatory columns
        write_columns |= set(map(Route, mandatory_coffea_columns))
        write_columns |= set(map(Route, {"category_ids", "process_id", "normalization_weight"}))
        route_filter = RouteFilter(keep=write_columns)

        chunks = []
        for inp in inputs:
            events = inp["columns"].load(formatter="awkward", cache=False)
            steps = inp["results"].load(formatter="awkward", cache=False).steps

            # add normalization weight
            if self.dataset_inst.is_mc:
                events = self.norm_weight_producer_inst(events, task=self)

            # remove columns
            events = route_filter(events)

            # zip them
            out = ak.zip({"steps": steps, "events": events})

            chunk = tmp_dir.child(f"tmp_{inp['results'].basename}", type="f")
            chunks.append(chunk)
            sorted_ak_to_parquet(out, chunk.abspath)

        # teardown the normalization weights producer
        if self.dataset_inst.is_mc:
            self.norm_weight_producer_inst.run_teardown(task=self)

        return chunks


MergeSelectionMasksWrapper = wrapper_factory(
    base_cls=AnalysisTask,
    require_cls=MergeSelectionMasks,
    enable=["configs", "skip_configs", "datasets", "skip_datasets", "shifts", "skip_shifts"],
)
