# coding: utf-8

"""
Tasks related to obtaining, preprocessing and selecting events.
"""

import math

import law

from ap.tasks.framework import DatasetTask, HTCondorWorkflow
from ap.util import ensure_proxy, process_nano_events


class CalibrateObjects(DatasetTask, law.LocalWorkflow, HTCondorWorkflow):

    sandbox = "bash::$AP_BASE/sandboxes/venv_selection.sh"

    def workflow_requires(self):
        # workflow super classes might already define requirements, so extend them
        reqs = super(CalibrateObjects, self).workflow_requires()
        reqs["lfns"] = GetDatasetLFNs.req(self)
        return reqs

    def requires(self):
        # workflow branches are normal tasks, so define requirements the normal way
        return {"lfns": GetDatasetLFNs.req(self)}

    def output(self):
        return self.local_target(f"data_{self.branch}.parquet")

    @law.decorator.safe_output
    @ensure_proxy
    def run(self):
        import numpy as np
        import awkward as ak

        # get all lfns
        lfns = self.input()["lfns"].random_target().load(formatter="json")

        # create a temp dir for saving intermediate files
        tmp_dir = law.LocalDirectoryTarget(is_tmp=True)
        tmp_dir.touch()
        output_chunks = {}

        # loop over all input file indices requested by this branch (most likely just one)
        for file_index in self.branch_data:
            self.publish_message(f"handling file {file_index}")

            # get the lfn of the file referenced by this file index
            lfn = str(lfns[file_index])

            # always use the INFN redirector for now
            # input_file = law.wlcg.WLCGFileTarget(lfn, fs="wlcg_fs_infn")
            input_file = law.law.LocalFileTarget("/nfs/dust/cms/user/riegerma/analysis_st_cache/WLCGFileSystem_f760560584/c226ad324e_2325C825-4095-D74F-A98A-5B42318F8DC4.root")
            input_size = law.util.human_bytes(input_file.stat().st_size, fmt=True)
            self.publish_message(f"lfn {lfn}, size is {input_size}")

            # open with uproot
            with self.publish_step("load and open ..."):
                ufile = input_file.load(formatter="uproot")
                utree = ufile["Events"]
                self.publish_message(f"found {utree.num_entries} events")

            # prepare processing
            chunk_size = 40000
            n_chunks = int(math.ceil(utree.num_entries / chunk_size))

            # list the names (or name patterns) of colums to load
            columns = ["run", "luminosityBlock", "event", "nJet", "Jet_pt", "Jet_phi", "Jet_mass"]

            # iterate over chunks
            gen = process_nano_events(ufile, chunk_size=chunk_size, pool_insert=True,
                filter_name=columns)
            for events, pos, pool_insert in self.iter_progress(gen, n_chunks, msg="iterate ..."):
                # here, we would start correcting objects, adding new columns, etc
                # examples in the following:
                #   a) "correct" Jet.pt by scaling four momenta by 1.1 (pt<30) or 0.9 (pt<=30)
                #   b) add a new column Jet.px based on pt and phi
                print(f"handling chunk {pos.index}")

                # a)
                a_mask = ak.flatten(events.Jet.pt < 30)
                n_jet_pt = np.asarray(ak.flatten(events.Jet.pt))
                n_jet_mass = np.asarray(ak.flatten(events.Jet.mass))
                n_jet_pt[a_mask] *= 1.1
                n_jet_pt[~a_mask] *= 0.9
                n_jet_mass[a_mask] *= 1.1
                n_jet_mass[~a_mask] *= 0.9

                # b)
                events["Jet", "px"] = events.Jet.pt * np.cos(events.Jet.phi)

                # save as parquet via a thread in the same pool
                chunk = tmp_dir.child(f"file_{file_index}_{pos.index}.parquet", type="f")
                pool_insert(chunk.dump, (events,), {"formatter": "awkward"})
                output_chunks[(file_index, pos.index)] = chunk

                # Q 1: How to add new columns? SOLVED
                # Q 2: How to change existing columns, either fully or partially? SOLVED
                # Q 3: Instead of saving the array with coffea events behavior, can we recreate an
                #      array with nano-like structure instead? One can always wrap it into coffea on
                #      demand later on, but it would be good not to force this decision here.

        # merge the files
        with self.output().localize("w") as output:
            sorted_chunks = [output_chunks[key] for key in sorted(output_chunks)]
            law.pyarrow.merge_parquet_task(self, sorted_chunks, output, local=True)


class SelectEvents(DatasetTask, law.LocalWorkflow, HTCondorWorkflow):

    sandbox = "bash::$AP_BASE/sandboxes/venv_selection.sh"

    shifts = {"jec_up", "jec_down"}

    def workflow_requires(self):
        # workflow super classes might already define requirements, so extend them
        reqs = super(SelectEvents, self).workflow_requires()
        reqs["lfns"] = GetDatasetLFNs.req(self)
        return reqs

    def requires(self):
        # workflow branches are normal tasks, so define requirements the normal way
        return {"lfns": GetDatasetLFNs.req(self)}

    def output(self):
        return self.local_target(f"data_{self.branch}.npz")

    @law.decorator.safe_output
    @ensure_proxy
    def run(self):
        import numpy as np

        # get all lfns
        lfns = self.input()["lfns"].random_target().load(formatter="json")

        # prepare output arrays to be concated
        output_arrays = []

        # loop over all input file indices requested by this branch
        for file_index in self.branch_data:
            # get the lfn of the file referenced by this file index
            lfn = str(lfns[file_index])
            self.publish_message(f"file {file_index}: fround {lfn}")

            # always use the INFN redirector for now
            input_file = law.wlcg.WLCGFileTarget(lfn, fs="wlcg_fs_infn")

            # open with uproot
            with self.publish_step("loading content with uproot ..."):
                data = input_file.load(formatter="uproot")
                events = data["Events"]
                self.publish_message(f"file {file_index}: found {events.num_entries} events")

            # dummy task: get all jet 1 pt values
            step_size = 1000
            steps = int(math.ceil(events.num_entries / step_size))
            events = events.iterate(["nJet", "Jet_pt"], step_size=step_size)
            for batch in self.iter_progress(events, steps, msg=f"file {file_index}: select ..."):
                print("batch")
                mask = batch["nJet"] >= 1
                jet_pt = batch["Jet_pt"][mask][:, 0]

                # emulate jec
                jec_factor = {"jec_up": 1.05, "jec_down": 0.95}.get(self.effective_shift, 1.0)
                jet_pt = jet_pt * jec_factor

                # store the jet pt
                output_arrays.append(jet_pt.to_numpy())

        data = np.concatenate(output_arrays, axis=0)
        self.output().dump(data=data, formatter="numpy")


# trailing imports
from ap.tasks.external import GetDatasetLFNs
