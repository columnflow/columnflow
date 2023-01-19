# coding: utf-8

"""
Tasks to produce yield tables
"""

from collections import OrderedDict

import law

from columnflow.tasks.framework.mixins import DatasetProcessesMixin
from columnflow.tasks.framework.remote import RemoteWorkflow

from columnflow.tasks.histograms import MergeHistograms
from colummnflow.util import dev_sandbox


class CreateYieldTable(
    DatasetProcessesMixin,
    law.LocalWorkflow,
    RemoteWorkflow,
):
    sandbox = dev_sandbox("bash::$CF_BASE/sandboxes/venv_columnar.sh")

    dep_MergeHistograms = MergeHistograms

    def requires(self):
        return {
            d: self.dep_MergeHistograms.req(
                self,
                dataset=d,
                variables=("mc_weight",),
                _prefer_cli={"variables"},
            )
            for d in self.datasets
        }

    def output(self):
        return self.target("yields.txt")

    @law.decorator.log
    def run(self):
        import hist

        process_insts = list(map(self.config_inst.get_process, self.processes))
        sub_process_insts = {
            proc: [sub for sub, _, _ in proc.walk_processes(include_self=True)]
            for proc in process_insts
        }

        # histogram data per process
        hists = {}

        with self.publish_step("dummy text"):
            for dataset, inp in self.input().items():
                dataset_inst = self.config_inst.get_dataset(dataset)
                # TODO: get correct input
                h_in = inp["collection"][0].load(formatter="pickle")

                # loop and extract one histogram per process
                for process_inst in process_insts:
                    # skip when the dataset is already known to not contain any sub process
                    if not any(map(dataset_inst.has_process, sub_process_insts[process_inst])):
                        continue

                    # work on a copy
                    h = h_in.copy()

                    # axis selections
                    h = h[{
                        "process": [
                            hist.loc(p.id)
                            for p in sub_process_insts[process_inst]
                            if p.id in h.axes["process"]
                        ],
                    }]

                    # axis reductions
                    h = h[{"process": sum, "category": sum, "shift": sum}]

                    # add the histogram
                    if process_inst in hists:
                        hists[process_inst] += h
                    else:
                        hists[process_inst] = h

            # there should be hists to plot
            if not hists:
                raise Exception("no histograms found to plot")

            # sort hists by process order
            hists = OrderedDict(
                (process_inst, hists[process_inst])
                for process_inst in sorted(hists, key=process_insts.index)
            )

            # TODO: create some output

            # save the output (TODO)
            # outp.dump(fig, formatter="txt")
