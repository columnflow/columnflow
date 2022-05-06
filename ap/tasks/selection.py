# coding: utf-8

"""
Tasks related to obtaining, preprocessing and selecting events.
"""

from collections import defaultdict

import law

from ap.tasks.framework import DatasetTask, HTCondorWorkflow
from ap.tasks.external import GetDatasetLFNs
from ap.util import ensure_proxy


def nano_inputs(
    branch_task: DatasetTask,
    lfn_collection: law.FileCollection,
    remote_fs: str = "wlcg_fs_desy_store",
) -> None:
    """
    Generator function that reduces the boiler plate code for looping over files referred to by the
    branch data of a law *branch_task*, given the collection of LFN files *lfn_collection* (the
    output of :py:class:`GetDatasetLFNs`). Iterating yields a 3-tuple (file index, lfn, input file)
    where the latter is a :py:class:`law.wlcg.WLCGFileTarget` with its fs set to *remote_fs*.
    """
    assert branch_task.is_branch()

    # get all lfns
    lfns = lfn_collection.random_target().load(formatter="json")

    # loop
    for file_index in branch_task.branch_data:
        branch_task.publish_message(f"handling file {file_index}")

        # get the lfn of the file referenced by this file index
        lfn = str(lfns[file_index])

        # get the input file
        input_file = law.wlcg.WLCGFileTarget(lfn, fs=remote_fs)
        input_size = law.util.human_bytes(input_file.stat().st_size, fmt=True)
        branch_task.publish_message(f"lfn {lfn}, size is {input_size}")

        yield (file_index, lfn, input_file)


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
        import numpy as np
        import awkward as ak
        from ap.columnar_util import ChunkedReader, mandatory_coffea_columns

        # prepare inputs and outputs
        inputs = self.input()
        output = self.output()
        output_chunks = {}

        # create a temp dir for saving intermediate files
        tmp_dir = law.LocalDirectoryTarget(is_tmp=True)
        tmp_dir.touch()

        # define nano columns that need to be loaded
        load_columns = mandatory_coffea_columns + ["nJet", "Jet_pt", "Jet_mass"]

        # loop over all input file indices requested by this branch (most likely just one)
        for (file_index, lfn, input_file) in nano_inputs(self, inputs["lfns"]):
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
                    # here, we would start correcting objects, adding new columns, etc
                    # examples in the following:
                    #   a) "correct" Jet.pt by scaling four momenta by 1.1 (pt<30) or 0.9 (pt<=30)
                    #   b) add 4 new columns representing the effect of JEC variations

                    # a)
                    a_mask = ak.flatten(events.Jet.pt < 30)
                    n_jet_pt = np.asarray(ak.flatten(events.Jet.pt))
                    n_jet_mass = np.asarray(ak.flatten(events.Jet.mass))
                    n_jet_pt[a_mask] *= 1.1
                    n_jet_pt[~a_mask] *= 0.9
                    n_jet_mass[a_mask] *= 1.1
                    n_jet_mass[~a_mask] *= 0.9

                    # b)
                    events["Jet", "pt_jec_up"] = events.Jet.pt * 1.05
                    events["Jet", "mass_jec_up"] = events.Jet.mass * 1.05
                    events["Jet", "pt_jec_down"] = events.Jet.pt * 0.95
                    events["Jet", "mass_jec_down"] = events.Jet.mass * 0.95

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
        outputs = self.output()
        output_chunks = {}
        stats = defaultdict(float)

        # create a temp dir for saving intermediate files
        tmp_dir = law.LocalDirectoryTarget(is_tmp=True)
        tmp_dir.touch()

        # get shift dependent aliases
        aliases = self.shift_inst.x("column_aliases", {})

        # define nano columns that need to be loaded
        load_columns = mandatory_coffea_columns + [
            "nJet", "Jet_pt", "nMuon", "Muon_pt", "LHEWeight_originalXWGTUP",
        ]

        # loop over all input file indices requested by this branch (most likely just one)
        for (file_index, lfn, input_file) in nano_inputs(self, inputs["lfns"]):
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
                    # here, we would start evaluating object and event selection criteria and
                    # the book keeping of stats

                    # apply the calibrated diff
                    events = update_ak_array(events, diff)

                    # add aliases
                    events = add_nano_aliases(events, aliases)

                    # example cuts:
                    # - require at least 4 jets with pt>30
                    # - require exactly one muon with pt>25
                    # example stats:
                    # - number of events before and after selection
                    # - sum of mc weights before and after selection

                    jet_mask = events.Jet.pt > 30
                    jet_indices = ak.argsort(events.Jet.pt, axis=-1, ascending=False)[jet_mask]
                    jet_sel = ak.sum(jet_mask, axis=1) >= 4

                    # muon selection
                    muon_mask = events.Muon.pt > 25
                    muon_indices = ak.argsort(events.Muon.pt, axis=-1, ascending=False)[muon_mask]
                    muon_sel = ak.sum(muon_mask, axis=1) == 1

                    # combined event selection
                    event_sel = jet_sel & muon_sel

                    # view of selected events
                    events_sel = events[event_sel]

                    # store decisions
                    # TODO: can one define just the nested dict and automate where and how the
                    #       ak.zip's are used? this might depend on whether the depth_limit can be
                    #       derived based on the types
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
            ChunkedReader, mandatory_coffea_columns, update_ak_array, add_nano_aliases,
        )

        # prepare inputs and outputs
        inputs = self.input()
        output = self.output()
        output_chunks = {}

        # create a temp dir for saving intermediate files
        tmp_dir = law.LocalDirectoryTarget(is_tmp=True)
        tmp_dir.touch()

        # get shift dependent aliases
        aliases = self.shift_inst.x("column_aliases", {})

        # define nano columns that need to be loaded
        load_columns = mandatory_coffea_columns + [
            "nJet", "Jet_pt", "nMuon", "Muon_pt", "LHEWeight_originalXWGTUP",
        ]

        # loop over all input file indices requested by this branch (most likely just one)
        for (file_index, lfn, input_file) in nano_inputs(self, inputs["lfns"]):
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

                    # apply the calibrated diff
                    events = update_ak_array(events, diff)

                    # add aliases
                    # TODO: we actually want to remove source columns but this is not supported yet,
                    #       see add_nano_aliases for more info
                    events = add_nano_aliases(events, aliases)

                    # apply the event mask
                    events = events[mask.event]

                    # apply jet and muon masks
                    events.Jet = events.Jet[mask.object.jet[mask.event]]
                    events.Muon = events.Muon[mask.object.muon[mask.event]]

                    # save as parquet via a thread in the same pool
                    chunk = tmp_dir.child(f"file_{pos.index}.parquet", type="f")
                    output_chunks[pos.index] = chunk
                    reader.add_task(ak.to_parquet, (events, chunk.path))

        # merge output files
        sorted_chunks = [output_chunks[key] for key in sorted(output_chunks)]
        law.pyarrow.merge_parquet_task(self, sorted_chunks, output, local=True)
