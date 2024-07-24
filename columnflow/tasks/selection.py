# coding: utf-8

"""
Tasks related to selecting events and performing post-selection bookkeeping.
"""

import copy
from collections import defaultdict

import luigi
import law

from columnflow.tasks.framework.base import Requirements, AnalysisTask, DatasetTask, wrapper_factory
from columnflow.tasks.framework.mixins import CalibratorsMixin, SelectorMixin, ChunkedIOMixin
from columnflow.tasks.framework.remote import RemoteWorkflow
from columnflow.tasks.external import GetDatasetLFNs
from columnflow.tasks.calibration import CalibrateEvents
from columnflow.production import Producer
from columnflow.util import maybe_import, ensure_proxy, dev_sandbox, safe_div, DotDict

np = maybe_import("numpy")
ak = maybe_import("awkward")


logger = law.logger.get_logger(__name__)

default_selection_hists_optional = law.config.get_expanded_bool(
    "analysis",
    "default_selection_hists_optional",
    True,
)


class SelectEvents(
    SelectorMixin,
    CalibratorsMixin,
    ChunkedIOMixin,
    DatasetTask,
    law.LocalWorkflow,
    RemoteWorkflow,
):
    # flag that sets the *hists* output to optional if True
    selection_hists_optional = default_selection_hists_optional

    # default sandbox, might be overwritten by selector function
    sandbox = dev_sandbox(law.config.get("analysis", "default_columnar_sandbox"))

    # upstream requirements
    reqs = Requirements(
        RemoteWorkflow.reqs,
        GetDatasetLFNs=GetDatasetLFNs,
        CalibrateEvents=CalibrateEvents,
    )

    # register sandbox and shifts found in the chosen selector to this task
    register_selector_sandbox = True
    register_selector_shifts = True

    # strategy for handling missing source columns when adding aliases on event chunks
    missing_column_alias_strategy = "original"

    def workflow_requires(self):
        reqs = super().workflow_requires()

        reqs["lfns"] = self.reqs.GetDatasetLFNs.req(self)

        if not self.pilot:
            reqs["calibrations"] = [
                self.reqs.CalibrateEvents.req(self, calibrator=calibrator_inst.cls_name)
                for calibrator_inst in self.calibrator_insts
                if calibrator_inst.produced_columns
            ]
        elif self.calibrator_insts:
            # pass-through pilot workflow requirements of upstream task
            t = self.reqs.CalibrateEvents.req(self)
            reqs = law.util.merge_dicts(reqs, t.workflow_requires(), inplace=True)

        # add selector dependent requirements
        reqs["selector"] = law.util.make_unique(law.util.flatten(self.selector_inst.run_requires()))

        return reqs

    def requires(self):
        reqs = {
            "lfns": self.reqs.GetDatasetLFNs.req(self),
            "calibrations": [
                self.reqs.CalibrateEvents.req(self, calibrator=calibrator_inst.cls_name)
                for calibrator_inst in self.calibrator_insts
                if calibrator_inst.produced_columns
            ],
        }

        # add selector dependent requirements
        reqs["selector"] = law.util.make_unique(law.util.flatten(self.selector_inst.run_requires()))

        return reqs

    def output(self):
        outputs = {
            "results": self.target(f"results_{self.branch}.parquet"),
            "stats": self.target(f"stats_{self.branch}.json"),
            "hists": self.target(f"hists_{self.branch}.pickle", optional=self.selection_hists_optional),
        }

        # add additional columns in case the selector produces some
        if self.selector_inst.produced_columns:
            outputs["columns"] = self.target(f"columns_{self.branch}.parquet")

        return outputs

    @law.decorator.log
    @ensure_proxy
    @law.decorator.localize(input=False)
    @law.decorator.safe_output
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
        selector_reqs = self.selector_inst.run_requires()
        reader_targets = self.selector_inst.run_setup(selector_reqs, luigi.task.getpaths(selector_reqs))
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
        route_filter = RouteFilter(write_columns)

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
                events, results = self.selector_inst(events, stats, hists=hists)

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

        # merge the result files
        sorted_chunks = [result_chunks[key] for key in sorted(result_chunks)]
        writer_opts_masks = self.get_parquet_writer_opts(repeating_values=True)
        law.pyarrow.merge_parquet_task(
            self, sorted_chunks, outputs["results"], local=True, writer_opts=writer_opts_masks,
        )

        # merge the column files
        if write_columns:
            sorted_chunks = [column_chunks[key] for key in sorted(column_chunks)]
            law.pyarrow.merge_parquet_task(
                self, sorted_chunks, outputs["columns"], local=True, writer_opts=self.get_parquet_writer_opts(),
            )

        # save stats
        outputs["stats"].dump(stats, indent=4, formatter="json")
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


class MergeSelectionStats(
    SelectorMixin,
    CalibratorsMixin,
    DatasetTask,
    law.tasks.ForestMerge,
):
    # flag that sets the *hists* output to optional if True
    selection_hists_optional = default_selection_hists_optional

    # default sandbox, might be overwritten by selector function (needed to load hist objects)
    sandbox = dev_sandbox(law.config.get("analysis", "default_columnar_sandbox"))

    # merge 25 stats files into 1 at every step of the merging cascade
    merge_factor = 25

    # skip receiving some parameters via req
    exclude_params_req_get = {"workflow"}

    # upstream requirements
    reqs = Requirements(
        SelectEvents=SelectEvents,
    )

    def create_branch_map(self):
        # DatasetTask implements a custom branch map, but we want to use the one in ForestMerge
        return law.tasks.ForestMerge.create_branch_map(self)

    def merge_workflow_requires(self):
        return self.reqs.SelectEvents.req(self, _exclude={"branches"})

    def merge_requires(self, start_branch, end_branch):
        return self.reqs.SelectEvents.req(
            self,
            branches=((start_branch, end_branch),),
            workflow="local",
            _exclude={"branch"},
        )

    def merge_output(self):
        return {
            "stats": self.target("stats.json"),
            "hists": self.target("hists.pickle", optional=self.selection_hists_optional),
        }

    def trace_merge_inputs(self, inputs):
        return super().trace_merge_inputs(inputs["collection"].targets.values())

    @law.decorator.log
    def run(self):
        return super().run()

    def merge(self, inputs, output):
        # merge input stats
        merged_stats = defaultdict(float)
        merged_hists = {}

        # check that hists are present for all inputs
        hist_inputs_exist = [inp["hists"].exists() for inp in inputs]
        if any(hist_inputs_exist) and not all(hist_inputs_exist):
            logger.warning(
                f"For dataset {self.dataset_inst.name}, cf.SelectEvents has produced hists for "
                "some but not all files. Histograms will not be merged and an empty pickle file will be stored.",
            )

        for inp in inputs:
            stats = inp["stats"].load(formatter="json", cache=False)
            self.merge_counts(merged_stats, stats)

        # merge hists only if all hists are present
        if all(hist_inputs_exist):
            for inp in inputs:
                hists = inp["hists"].load(formatter="pickle", cache=False)
                self.merge_counts(merged_hists, hists)

        # write the output
        output["stats"].dump(merged_stats, indent=4, formatter="json", cache=False)
        output["hists"].dump(merged_hists, formatter="pickle", cache=False)

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


class MergeSelectionMasks(
    SelectorMixin,
    CalibratorsMixin,
    DatasetTask,
    law.tasks.ForestMerge,
    RemoteWorkflow,
):
    sandbox = dev_sandbox(law.config.get("analysis", "default_columnar_sandbox"))

    # recursively merge 8 files into one
    merge_factor = 8

    # upstream requirements
    reqs = Requirements(
        RemoteWorkflow.reqs,
        SelectEvents=SelectEvents,
    )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # store the normalization weight producer for MC
        self.norm_weight_producer = None
        if self.dataset_inst.is_mc:
            self.norm_weight_producer = Producer.get_cls("normalization_weights")(
                inst_dict=self.get_producer_kwargs(self),
            )

    def create_branch_map(self):
        # DatasetTask implements a custom branch map, but we want to use the one in ForestMerge
        return law.tasks.ForestMerge.create_branch_map(self)

    def merge_workflow_requires(self):
        reqs = {
            "selection": self.reqs.SelectEvents.req(self, _exclude={"branches"}),
        }

        if self.dataset_inst.is_mc:
            reqs["normalization"] = self.norm_weight_producer.run_requires()

        return reqs

    def merge_requires(self, start_branch, end_branch):
        reqs = {
            "selection": [
                self.reqs.SelectEvents.req(self, branch=b)
                for b in range(start_branch, end_branch)
            ],
        }

        if self.dataset_inst.is_mc:
            reqs["normalization"] = self.norm_weight_producer.run_requires()

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

        chunks = []

        # setup the normalization weights producer
        if self.dataset_inst.is_mc:
            self.norm_weight_producer.run_setup(
                self.requires()["forest_merge"]["normalization"],
                self.input()["forest_merge"]["normalization"],
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
        route_filter = RouteFilter(write_columns)

        for inp in inputs:
            events = inp["columns"].load(formatter="awkward")
            steps = inp["results"].load(formatter="awkward").steps

            # add normalization weight
            if self.dataset_inst.is_mc:
                events = self.norm_weight_producer(events)

            # remove columns
            events = route_filter(events)

            # zip them
            out = ak.zip({"steps": steps, "events": events})

            chunk = tmp_dir.child(f"tmp_{inp['results'].basename}", type="f")
            chunks.append(chunk)
            sorted_ak_to_parquet(out, chunk.abspath)

        return chunks


MergeSelectionMasksWrapper = wrapper_factory(
    base_cls=AnalysisTask,
    require_cls=MergeSelectionMasks,
    enable=["configs", "skip_configs", "datasets", "skip_datasets", "shifts", "skip_shifts"],
)
