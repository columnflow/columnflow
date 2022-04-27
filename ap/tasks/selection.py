# coding: utf-8

"""
Tasks related to obtaining, preprocessing and selecting events.
"""

import math

import law

from ap.tasks.framework import DatasetTask, HTCondorWorkflow
from ap.util import ensure_proxy


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
        import uproot
        import awkward as ak

        # get all lfns
        lfns = self.input()["lfns"].random_target().load(formatter="json")

        # create a temp dir for saving intermediate files
        tmp_dir = law.LocalDirectoryTarget(is_tmp=True)
        tmp_dir.touch()
        output_chunks = []

        # loop over all input file indices requested by this branch
        for file_index in self.branch_data:
            # get the lfn of the file referenced by this file index
            lfn = str(lfns[file_index])

            # always use the INFN redirector for now
            input_file = law.wlcg.WLCGFileTarget(lfn, fs="wlcg_fs_infn")
            input_size = law.util.human_bytes(input_file.stat().st_size, fmt=True)
            self.publish_message(f"file {file_index}: fround {lfn}, size is {input_size}")

            # open with uproot
            with self.publish_step(f"file {file_index}: load and open with uproot ..."):
                data = input_file.load(formatter="uproot")
                events = data["Events"]
                self.publish_message(f"file {file_index}: found {events.num_entries} events")

            with self.publish_step(f"iterating over file {file_index} ..."):
                # optimization targets: threads and step size
                pool = uproot.ThreadPoolExecutor(4)
                step_size = "50MB"
                for i, batch in enumerate(events.iterate(step_size=step_size,
                        interpretation_executor=pool, decompression_executor=pool)):
                    # change objects per batch here ...
                    pass

                    # dummy implementation:
                    # scale 4-vec of jets with pt > 30 by 1.1 (phi and eta are invariant)
                    # note for future: ak made some unintuitive design decisions about mutability
                    # (see https://github.com/scikit-hep/awkward-1.0/discussions/609), which are
                    # contradicting numpy / tf standards, with many consequences, one of them being
                    # that in-place assignment is not supported, and we have to go via plain numpy
                    # to do manipulations on flattened objects, so inconvenient syntactic mess ahead
                    jes_mask = batch.Jet_pt > 30
                    np.asarray(ak.flatten(batch.Jet_pt))[np.asarray(ak.flatten(jes_mask))] *= 1.1
                    np.asarray(ak.flatten(batch.Jet_mass))[np.asarray(ak.flatten(jes_mask))] *= 1.1

                    # save the batch as parquet
                    chunk = tmp_dir.child(f"file_{file_index}_{i}.parquet", type="f")
                    ak.to_parquet(batch, chunk.path)
                    output_chunks.append(chunk)

        # merge the files
        with self.output().localize("w") as output:
            law.pyarrow.merge_parquet_task(self, output_chunks, output, local=True)



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
