# coding: utf-8

"""
Tasks related to obtaining, preprocessing and selecting events.
"""

from collections import defaultdict

import law

from ap.tasks.framework import DatasetTask, HTCondorWorkflow
from ap.tasks.external import GetDatasetLFNs
from ap.util import ensure_proxy
from ap.calibration import calibration_functions


class CalibrateObjects(DatasetTask, law.LocalWorkflow, HTCondorWorkflow):

    sandbox = "bash::$AP_BASE/sandboxes/venv_selection.sh"

    def workflow_requires(self):
        reqs = super(CalibrateObjects, self).workflow_requires()
        reqs["lfns"] = GetDatasetLFNs.req(self)
        return reqs

    def requires(self):
        return {
            "lfns": GetDatasetLFNs.req(self),
        }

    def output(self):
        return self.local_target(f"diff_{self.branch}.parquet")

    @law.decorator.safe_output
    @ensure_proxy
    def run(self):
        import awkward as ak
        from ap.columnar_util import ChunkedReader, mandatory_coffea_columns

        # prepare inputs and outputs
        lfn_task = self.requires()["lfns"]
        output = self.output()
        output_chunks = {}

        # create a temp dir for saving intermediate files
        tmp_dir = law.LocalDirectoryTarget(is_tmp=True)
        tmp_dir.touch()

        # get the calibration function
        # TODO: get this from the config or a parameter?
        calib_fn = calibration_functions["calib_test"]

        # define nano columns that need to be loaded
        load_columns = mandatory_coffea_columns | calib_fn.load_columns

        # loop over all input file indices requested by this branch (most likely just one)
        for (file_index, input_file) in lfn_task.iter_nano_files(self):
            # open with uproot
            with self.publish_step("load and open ..."):
                ufile = input_file.load(formatter="uproot")

            # iterate over chunks
            with ChunkedReader(
                ufile,
                source_type="coffea_root",
                read_options={"iteritems_options": {"filter_name": load_columns}},
            ) as reader:
                msg = f"iterate through {reader.n_entries} events ..."
                for events, pos in self.iter_progress(reader, reader.n_chunks, msg=msg):
                    # just invoke the calibration function
                    events = calib_fn(events)

                    # save as parquet via a thread in the same pool
                    chunk = tmp_dir.child(f"file_{file_index}_{pos.index}.parquet", type="f")
                    output_chunks[(file_index, pos.index)] = chunk
                    reader.add_task(ak.to_parquet, (events, chunk.path))

        # merge output files
        with output.localize("w") as outp:
            sorted_chunks = [output_chunks[key] for key in sorted(output_chunks)]
            law.pyarrow.merge_parquet_task(self, sorted_chunks, outp, local=True)


class SelectEvents(DatasetTask, law.LocalWorkflow, HTCondorWorkflow):

    sandbox = "bash::$AP_BASE/sandboxes/venv_selection.sh"

    shifts = {"jec_up", "jec_down"}

    def workflow_requires(self):
        # workflow super classes might already define requirements, so extend them
        reqs = super(SelectEvents, self).workflow_requires()
        reqs["lfns"] = GetDatasetLFNs.req(self)
        if not self.pilot:
            reqs["diff"] = CalibrateObjects.req(self)
        return reqs

    def requires(self):
        # workflow branches are normal tasks, so define requirements the normal way
        return {
            "lfns": GetDatasetLFNs.req(self),
            "diff": CalibrateObjects.req(self),
        }

    def output(self):
        return {
            "mask": self.local_target(f"mask_{self.branch}.parquet"),
            "stat": self.local_target(f"stat_{self.branch}.json"),
        }

    @law.decorator.safe_output
    @law.decorator.localize
    @ensure_proxy
    def run(self):
        import awkward as ak
        from ap.columnar_util import (
            ChunkedReader, mandatory_coffea_columns, update_ak_array, add_nano_aliases,
        )

        # prepare inputs and outputs
        inputs = self.input()
        lfn_task = self.requires()["lfns"]
        outputs = self.output()
        output_chunks = {}
        stats = defaultdict(float)

        # create a temp dir for saving intermediate files
        tmp_dir = law.LocalDirectoryTarget(is_tmp=True)
        tmp_dir.touch()

        # get shift dependent aliases
        aliases = self.shift_inst.x("column_aliases", {})

        # define nano columns that need to be loaded
        load_columns = mandatory_coffea_columns | {
            "nJet", "Jet_pt", "nMuon", "Muon_pt", "LHEWeight_originalXWGTUP",
        }

        # loop over all input file indices requested by this branch (most likely just one)
        for (file_index, input_file) in lfn_task.iter_nano_files(self):
            # open the input file with uproot
            with self.publish_step("load and open ..."):
                ufile = input_file.load(formatter="uproot")

            # iterate over chunks of events and diffs
            with ChunkedReader(
                [ufile, inputs["diff"].path],
                source_type=["coffea_root", "awkward_parquet"],
                read_options=[{"iteritems_options": {"filter_name": load_columns}}, None],
            ) as reader:
                msg = f"iterate through {reader.n_entries} events ..."
                for (events, diff), pos in self.iter_progress(reader, reader.n_chunks, msg=msg):
                    # here, we would start evaluating object and event selection criteria, store
                    # new columns related to the selection (e.g. categories or regions), and
                    # book-keeping of stats

                    # apply the calibrated diff
                    events = update_ak_array(events, diff)

                    # add aliases
                    events = add_nano_aliases(events, aliases)

                    # example cuts:
                    # - require at least 4 jets with pt>30
                    # - require exactly one muon with pt>25
                    # example columns:
                    # - high jet multiplicity region (>=6 selected jets)
                    # example stats:
                    # - number of events before and after selection
                    # - sum of mc weights before and after selection

                    # jet selection
                    jet_mask = events.Jet.pt > 30
                    jet_indices = ak.argsort(events.Jet.pt, axis=-1, ascending=False)[jet_mask]
                    jet_sel = ak.sum(jet_mask, axis=1) >= 4
                    jet_high_multiplicity = ak.sum(jet_mask, axis=1) >= 6

                    # muon selection
                    muon_mask = events.Muon.pt > 25
                    muon_indices = ak.argsort(events.Muon.pt, axis=-1, ascending=False)[muon_mask]
                    muon_sel = ak.sum(muon_mask, axis=1) == 1

                    # combined event selection
                    event_sel = jet_sel & muon_sel

                    # view of selected events
                    events_sel = events[event_sel]

                    # store decisions
                    decisions = ak.zip({
                        "event": event_sel,
                        "step": ak.zip({
                            "jet": jet_sel,
                            "muon": muon_sel,
                        }),
                        "object": ak.zip({
                            "jet": jet_indices,
                            "muon": muon_indices,
                        }, depth_limit=1),
                        "columns": ak.zip({
                            "jet_high_multiplicity": jet_high_multiplicity,
                        }),
                    })

                    # store stats
                    stats["n_events"] += len(events)
                    stats["n_events_selected"] += ak.sum(event_sel, axis=0)
                    stats["sum_mc_weight"] += ak.sum(events.LHEWeight.originalXWGTUP)
                    stats["sum_mc_weight_selected"] += ak.sum(events_sel.LHEWeight.originalXWGTUP)

                    # save as parquet via a thread in the same pool
                    chunk = tmp_dir.child(f"file_{file_index}_{pos.index}.parquet", type="f")
                    output_chunks[(file_index, pos.index)] = chunk
                    reader.add_task(ak.to_parquet, (decisions, chunk.path))

        # merge the mask files
        sorted_chunks = [output_chunks[key] for key in sorted(output_chunks)]
        law.pyarrow.merge_parquet_task(self, sorted_chunks, outputs["mask"], local=True)

        # save stats
        outputs["stat"].dump(stats, formatter="json")

        # print some stats
        eff = stats["n_events_selected"] / stats["n_events"]
        eff_weighted = stats["sum_mc_weight_selected"] / stats["sum_mc_weight"]
        self.publish_message(f"all events         : {int(stats['n_events'])}")
        self.publish_message(f"sel. events        : {int(stats['n_events_selected'])}")
        self.publish_message(f"efficiency         : {eff:.4f}")
        self.publish_message(f"sum mc weights     : {stats['sum_mc_weight']}")
        self.publish_message(f"sum sel. mc weights: {stats['sum_mc_weight_selected']}")
        self.publish_message(f"efficiency         : {eff_weighted:.4f}")


class ReduceEvents(DatasetTask, law.LocalWorkflow, HTCondorWorkflow):

    sandbox = "bash::$AP_BASE/sandboxes/venv_selection.sh"

    shifts = CalibrateObjects.shifts | SelectEvents.shifts

    def workflow_requires(self):
        reqs = super(ReduceEvents, self).workflow_requires()
        reqs["lfns"] = GetDatasetLFNs.req(self)
        if not self.pilot:
            reqs["diff"] = CalibrateObjects.req(self)
            reqs["mask"] = SelectEvents.req(self)
        return reqs

    def requires(self):
        return {
            "lfns": GetDatasetLFNs.req(self),
            "diff": CalibrateObjects.req(self),
            "mask": SelectEvents.req(self),
        }

    def output(self):
        return self.local_target(f"events_{self.branch}.parquet")

    @law.decorator.safe_output
    @law.decorator.localize
    @ensure_proxy
    def run(self):
        import awkward as ak
        from ap.columnar_util import (
            ChunkedReader, mandatory_coffea_columns, get_ak_routes, update_ak_array,
            add_nano_aliases, remove_nano_column,
        )

        # prepare inputs and outputs
        inputs = self.input()
        lfn_task = self.requires()["lfns"]
        output = self.output()
        output_chunks = {}

        # create a temp dir for saving intermediate files
        tmp_dir = law.LocalDirectoryTarget(is_tmp=True)
        tmp_dir.touch()

        # get shift dependent aliases
        aliases = self.shift_inst.x("column_aliases", {})

        # define nano columns that should be kept, and that need to be loaded
        keep_columns = set(self.config_inst.x.keep_columns[self.__class__.__name__])
        load_columns = keep_columns | set(mandatory_coffea_columns)
        remove_routes = None

        # loop over all input file indices requested by this branch (most likely just one)
        for (file_index, input_file) in lfn_task.iter_nano_files(self):
            # open the input file with uproot
            with self.publish_step("load and open ..."):
                ufile = input_file.load(formatter="uproot")

            # iterate over chunks of events and diffs
            with ChunkedReader(
                [ufile, inputs["diff"].path, inputs["mask"]["mask"].path],
                source_type=["coffea_root", "awkward_parquet", "awkward_parquet"],
                read_options=[{"iteritems_options": {"filter_name": load_columns}}, None, None],
            ) as reader:
                msg = f"iterate through {reader.n_entries} events ..."
                for (events, diff, mask), pos in self.iter_progress(reader, reader.n_chunks, msg=msg):
                    # here, we would simply apply the mask to filter events and objects

                    # add the calibrated diff and new columns from masks
                    events = update_ak_array(events, diff, mask.columns)

                    # add aliases
                    events = add_nano_aliases(events, aliases, remove_src=True)

                    # apply the event mask
                    events = events[mask.event]

                    # apply jet and muon masks
                    events.Jet = events.Jet[mask.object.jet[mask.event]]
                    events.Muon = events.Muon[mask.object.muon[mask.event]]

                    # manually remove colums that should not be kept
                    # define routes to remove once
                    if not remove_routes:
                        remove_routes = {
                            route
                            for route in get_ak_routes(events)
                            if not law.util.multi_match("_".join(route), keep_columns)
                        }
                    for route in remove_routes:
                        events = remove_nano_column(events, route)

                    # save as parquet via a thread in the same pool
                    chunk = tmp_dir.child(f"file_{pos.index}.parquet", type="f")
                    output_chunks[pos.index] = chunk
                    reader.add_task(ak.to_parquet, (events, chunk.path))

        # merge output files
        sorted_chunks = [output_chunks[key] for key in sorted(output_chunks)]
        law.pyarrow.merge_parquet_task(self, sorted_chunks, output, local=True)
