# coding: utf-8

"""
Tasks related to selecting events and performing post-selection bookkeeping.
"""

from collections import defaultdict

import law

from columnflow.tasks.framework.base import Requirements, AnalysisTask, DatasetTask, wrapper_factory
from columnflow.tasks.framework.mixins import CalibratorsMixin, SelectorMixin, ChunkedIOMixin
from columnflow.tasks.framework.remote import RemoteWorkflow
from columnflow.tasks.external import GetDatasetLFNs
from columnflow.tasks.calibration import CalibrateEvents
from columnflow.production import Producer
from columnflow.util import maybe_import, ensure_proxy, dev_sandbox, safe_div


ak = maybe_import("awkward")


class SelectEvents(
    SelectorMixin,
    CalibratorsMixin,
    ChunkedIOMixin,
    DatasetTask,
    law.LocalWorkflow,
    RemoteWorkflow,
):
    # default sandbox, might be overwritten by selector function
    sandbox = dev_sandbox("bash::$CF_BASE/sandboxes/venv_columnar.sh")

    # upstream requirements
    reqs = Requirements(
        RemoteWorkflow.reqs,
        GetDatasetLFNs=GetDatasetLFNs,
        CalibrateEvents=CalibrateEvents,
    )

    register_selector_shifts = True

    def workflow_requires(self, only_super: bool = False):
        reqs = super().workflow_requires()
        if only_super:
            return reqs

        reqs["lfns"] = self.reqs.GetDatasetLFNs.req(self)

        if not self.pilot:
            reqs["calib"] = [
                self.reqs.CalibrateEvents.req(self, calibrator=c)
                for c in self.calibrators
            ]
        else:
            # pass-through pilot workflow requirements of upstream task
            t = self.reqs.CalibrateEvents.req(self)
            reqs = law.util.merge_dicts(reqs, t.workflow_requires(), inplace=True)

        # add selector dependent requirements
        reqs["selector"] = self.selector_inst.run_requires()

        return reqs

    def requires(self):
        reqs = {
            "lfns": self.reqs.GetDatasetLFNs.req(self),
            "calibrations": [
                self.reqs.CalibrateEvents.req(self, calibrator=c)
                for c in self.calibrators
            ],
        }

        # add selector dependent requirements
        reqs["selector"] = self.selector_inst.run_requires()

        return reqs

    def output(self):
        outputs = {
            "results": self.target(f"results_{self.branch}.parquet"),
            "stats": self.target(f"stats_{self.branch}.json"),
        }

        # add additional columns in case the selector produces some
        if self.selector_inst.produced_columns:
            outputs["columns"] = self.target(f"columns_{self.branch}.parquet")

        return outputs

    @law.decorator.log
    @ensure_proxy
    @law.decorator.localize
    @law.decorator.safe_output
    def run(self):
        from columnflow.columnar_util import (
            Route, RouteFilter, mandatory_coffea_columns, update_ak_array, add_ak_aliases,
            sorted_ak_to_parquet,
        )

        # prepare inputs and outputs
        reqs = self.requires()
        lfn_task = reqs["lfns"]
        inputs = self.input()
        outputs = self.output()
        result_chunks = {}
        column_chunks = {}
        stats = defaultdict(float)

        # run the selector setup
        self.selector_inst.run_setup(reqs["selector"], inputs["selector"])

        # create a temp dir for saving intermediate files
        tmp_dir = law.LocalDirectoryTarget(is_tmp=True)
        tmp_dir.touch()

        # get shift dependent aliases
        aliases = self.local_shift_inst.x("column_aliases", {})

        # define columns that need to be read
        read_columns = mandatory_coffea_columns | self.selector_inst.used_columns | set(aliases.values())
        read_columns = {Route(c) for c in read_columns}

        # define columns that will be written
        write_columns = self.selector_inst.produced_columns
        route_filter = RouteFilter(write_columns)

        # let the lfn_task prepare the nano file (basically determine a good pfn)
        [(lfn_index, input_file)] = lfn_task.iter_nano_files(self)

        # open the input file with uproot
        with self.publish_step("load and open ..."):
            nano_file = input_file.load(formatter="uproot")

        # iterate over chunks of events and diffs
        n_calib = len(inputs["calibrations"])
        for (events, *diffs), pos in self.iter_chunked_io(
            [nano_file] + [inp.path for inp in inputs["calibrations"]],
            source_type=["coffea_root"] + n_calib * ["awkward_parquet"],
            read_columns=(1 + n_calib) * [read_columns],
        ):
            # apply the calibrated diffs
            events = update_ak_array(events, *diffs)

            # add aliases
            events = add_ak_aliases(events, aliases, remove_src=True)

            # invoke the selection function
            events, results = self.selector_inst(events, stats)
            results_array = results.to_ak()

            # optional check for finite values
            if self.check_finite:
                self.raise_if_not_finite(results_array)

            # save results as parquet via a thread in the same pool
            chunk = tmp_dir.child(f"res_{lfn_index}_{pos.index}.parquet", type="f")
            result_chunks[(lfn_index, pos.index)] = chunk
            self.chunked_io.queue(sorted_ak_to_parquet, (results_array, chunk.path))

            # remove columns
            if write_columns:
                events = route_filter(events)

                # optional check for finite values
                if self.check_finite:
                    self.raise_if_not_finite(events)

                # save additional columns as parquet via a thread in the same pool
                chunk = tmp_dir.child(f"cols_{lfn_index}_{pos.index}.parquet", type="f")
                column_chunks[(lfn_index, pos.index)] = chunk
                self.chunked_io.queue(sorted_ak_to_parquet, (events, chunk.path))

        # merge the result files
        sorted_chunks = [result_chunks[key] for key in sorted(result_chunks)]
        law.pyarrow.merge_parquet_task(self, sorted_chunks, outputs["results"], local=True)

        # merge the column files
        if write_columns:
            sorted_chunks = [column_chunks[key] for key in sorted(column_chunks)]
            law.pyarrow.merge_parquet_task(self, sorted_chunks, outputs["columns"], local=True)

        # save stats
        outputs["stats"].dump(stats, indent=4, formatter="json")

        # print some stats
        eff = safe_div(stats["n_events_selected"], stats["n_events"])
        eff_weighted = safe_div(stats["sum_mc_weight_selected"], stats["sum_mc_weight"])
        self.publish_message(f"all events         : {int(stats['n_events'])}")
        self.publish_message(f"sel. events        : {int(stats['n_events_selected'])}")
        self.publish_message(f"efficiency         : {eff:.4f}")
        self.publish_message(f"sum mc weights     : {stats['sum_mc_weight']}")
        self.publish_message(f"sum sel. mc weights: {stats['sum_mc_weight_selected']}")
        self.publish_message(f"efficiency         : {eff_weighted:.4f}")
        if not stats["n_events_selected"]:
            self.publish_message(law.util.colored("no events selected", "red"))


# overwrite class defaults
check_finite_tasks = law.config.get_expanded("analysis", "check_finite_output", [], split_csv=True)
SelectEvents.check_finite = ChunkedIOMixin.check_finite.copy(
    default=SelectEvents.task_family in check_finite_tasks,
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
    # recursively merge 20 files into one
    merge_factor = 20

    # default upstream dependency task classes
    dep_SelectEvents = SelectEvents

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
        return self.target("stats.json")

    def trace_merge_inputs(self, inputs):
        return super().trace_merge_inputs(
            inp["stats"] for inp in inputs["collection"].targets.values()
        )

    @law.decorator.log
    def run(self):
        return super().run()

    def merge(self, inputs, output):
        # merge input stats
        merged_stats = defaultdict(float)
        for inp in inputs:
            stats = inp.load(formatter="json", cache=False)
            self.merge_counts(merged_stats, stats)

        # write the output
        output.dump(merged_stats, indent=4, formatter="json", cache=False)

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
    sandbox = dev_sandbox("bash::$CF_BASE/sandboxes/venv_columnar.sh")

    # recursively merge 8 files into one
    merge_factor = 8

    # upstream requirements
    reqs = Requirements(
        RemoteWorkflow.reqs,
        SelectEvents=SelectEvents,
    )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # store the normalization weight producer
        self.norm_weight_producer = Producer.get_cls("normalization_weights")(
            inst_dict=self.get_producer_kwargs(self),
        )

    def create_branch_map(self):
        # DatasetTask implements a custom branch map, but we want to use the one in ForestMerge
        return law.tasks.ForestMerge.create_branch_map(self)

    def merge_workflow_requires(self):
        return {
            "selection": self.reqs.SelectEvents.req(self, _exclude={"branches"}),
            "normalization": self.norm_weight_producer.run_requires(),
        }

    def merge_requires(self, start_branch, end_branch):
        return {
            "selection": [
                self.reqs.SelectEvents.req(self, branch=b)
                for b in range(start_branch, end_branch)
            ],
            "normalization": self.norm_weight_producer.run_requires(),
        }

    def trace_merge_workflow_inputs(self, inputs):
        return super().trace_merge_workflow_inputs(inputs["selection"])

    def trace_merge_inputs(self, inputs):
        return super().trace_merge_inputs(inputs["selection"])

    def merge_output(self):
        return self.target("masks.parquet")

    def merge(self, inputs, output):
        # in the lowest (leaf) stage, zip selection results with additional columns first
        if self.is_leaf():
            # create a temp dir for saving intermediate files
            tmp_dir = law.LocalDirectoryTarget(is_tmp=True)
            tmp_dir.touch()
            inputs = self.zip_results_and_columns(inputs, tmp_dir)

        law.pyarrow.merge_parquet_task(self, inputs, output)

    def zip_results_and_columns(self, inputs, tmp_dir):
        import awkward as ak
        from columnflow.columnar_util import RouteFilter, sorted_ak_to_parquet

        chunks = []

        # setup the normalization weights producer
        self.norm_weight_producer.run_setup(
            self.requires()["forest_merge"]["normalization"],
            self.input()["forest_merge"]["normalization"],
        )

        # define columns that will be written
        write_columns = set(self.config_inst.x.keep_columns[self.task_family])
        route_filter = RouteFilter(write_columns)

        for inp in inputs:
            events = inp["columns"].load(formatter="awkward")
            steps = inp["results"].load(formatter="awkward").steps

            # add normalization weight
            events = self.norm_weight_producer(events)

            # remove columns
            events = route_filter(events)

            # zip them
            out = ak.zip({"steps": steps, "events": events})

            chunk = tmp_dir.child(f"tmp_{inp['results'].basename}", type="f")
            chunks.append(chunk)
            sorted_ak_to_parquet(out, chunk.path)

        return chunks


MergeSelectionMasksWrapper = wrapper_factory(
    base_cls=AnalysisTask,
    require_cls=MergeSelectionMasks,
    enable=["configs", "skip_configs", "datasets", "skip_datasets", "shifts", "skip_shifts"],
)
