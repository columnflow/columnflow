# coding: utf-8

"""
Task to produce the first histograms
"""

import math

import luigi
import law

from ap.tasks.framework import DatasetTask, HTCondorWorkflow
from ap.util import ensure_proxy

from ap.tasks.external import GetDatasetLFNs
from ap.tasks.selection import CalibrateObjects, SelectEvents



class CreateHistograms(DatasetTask, law.LocalWorkflow, HTCondorWorkflow):

    sandbox = "bash::$AP_BASE/sandboxes/venv_columnar.sh"


    shifts = CalibrateObjects.shifts | SelectEvents.shifts

    def workflow_requires(self):
        reqs = super(CreateHistograms, self).workflow_requires()
        reqs["lfns"] = GetDatasetLFNs.req(self)
        if not self.pilot:
            reqs["diff"] = CalibrateObjects.req(self)
            reqs["sel"] = SelectEvents.req(self)
        return reqs

    def requires(self):
        return {
            "lfns": GetDatasetLFNs.req(self),
            "diff": CalibrateObjects.req(self),
            "sel": SelectEvents.req(self),
        }

    def output(self):
        return self.local_target(f"histograms_{self.branch}.pickle")

    @law.decorator.safe_output
    @law.decorator.localize
    @ensure_proxy
    def run(self):
        import awkward as ak
        import numpy as np
        import hist
        from ap.columnar_util import (
            ChunkedReader, mandatory_coffea_columns, get_ak_routes, update_ak_array,
            add_nano_aliases, remove_nano_column,
        )
        from ap.selection import Selector

        # prepare inputs and outputs
        inputs = self.input()
        lfn_task = self.requires()["lfns"]
        # declare output: dict of histograms
        histograms = {}
        first = True
        #output = []

        # create a temp dir for saving intermediate files
        tmp_dir = law.LocalDirectoryTarget(is_tmp=True)
        tmp_dir.touch()

        # get shift dependent aliases
        aliases = self.shift_inst.x("column_aliases", {})

        # define nano columns that should be kept, and that need to be loaded
        keep_columns = set(self.config_inst.x.keep_columns[self.__class__.__name__])
        categories = Selector.get("categories")
        variables = Selector.get("variables")
        load_columns = keep_columns | set(mandatory_coffea_columns) | categories.used_columns #| variables.used_columns
        print("load_columns:")
        print(load_columns)
        remove_routes = None

        # loop over all input file indices requested by this branch (most likely just one)
        for (file_index, input_file) in lfn_task.iter_nano_files(self):
            # open the input file with uproot
            with self.publish_step("load and open ..."):
                ufile = input_file.load(formatter="uproot")

            # iterate over chunks of events and diffs
            with ChunkedReader(
                [ufile, inputs["diff"].path, inputs["sel"]["res"].path],
                source_type=["coffea_root", "awkward_parquet", "awkward_parquet"],
                read_options=[{"iteritems_options": {"filter_name": load_columns}}, None, None],
            ) as reader:
                msg = f"iterate through {reader.n_entries} events ..."
                for (events, diff, sel), pos in self.iter_progress(reader, reader.n_chunks, msg=msg):

                    # here, we would simply apply the mask from the selection results
                    # to filter events and objects
                    
                    # add the calibrated diff and new columns from selection results
                    events = update_ak_array(events, diff, sel.columns)

                    # add aliases
                    events = add_nano_aliases(events, aliases, remove_src=True)
                    # apply the event mask
                    events = events[sel.event]                    
                    print(len(events))

                    
                    # determine which event belongs in which leaf category
                    #cat_titles = categories(events, self.config_inst).columns.cat_titles
                    #cat_array = categories(events, self.config_inst).columns.cat_array
                    
                    # apply object masks (NOTE: when applying object masks before categorisation, the categorisation is not working....?)
                    #events.Deepjet = events.Jet[sel.objects.deepjet[sel.event]]
                    events.Jet = events.Jet[sel.objects.jet[sel.event]]
                    events.Muon = events.Muon[sel.objects.muon[sel.event]]
                    events.Electron = events.Electron[sel.objects.electron[sel.event]]

                    # weights
                    sampleweight = self.config_inst.x.luminosity / self.config_inst.get_dataset(self.dataset).n_events
                    weight = sampleweight * events.LHEWeight.originalXWGTUP
                    #print("weight = ", weight)
                    
                    results = variables(events)
                    
                    # define & fill histograms
                    var_names = self.config_inst.variables.names()
                    with self.publish_step("looping over all variables in config ...."):
                        for var_name in var_names:
                            with self.publish_step("var: %s" % var_name):
                                var = self.config_inst.variables.get(var_name)
                                h_var = (
                                    hist.Hist.new
                                    #.StrCategory(events.columns.cat_titles, name="category")
                                    .StrCategory([], name="category", growth=True)
                                    .StrCategory([], name="shift", growth=True)
                                    .Reg(*var.binning, name=var_name, label=var.get_full_x_title())
                                    .Weight()
                                )
                                fill_kwargs = {
                                    #"category": cat_array,
                                    "category": events.cat_array,
                                    "shift": self.shift,
                                    var_name: results.columns[var_name],
                                    "weight": weight,
                                }
                                h_var.fill(**fill_kwargs)
                                if first:
                                    histograms[var_name] = h_var
                                else:
                                    histograms[var_name] += h_var
                    first = False


        # merge output files
        self.output().dump(histograms, formatter="pickle")
