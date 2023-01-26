# coding: utf-8

"""
Tasks to produce yield tables
"""

from collections import OrderedDict

import law
import luigi

from columnflow.tasks.framework.mixins import DatasetsProcessesMixin
from columnflow.tasks.framework.remote import RemoteWorkflow

from columnflow.tasks.histograms import MergeHistograms
from columnflow.util import dev_sandbox


class CreateYieldTable(
    DatasetsProcessesMixin,
    law.LocalWorkflow,
    RemoteWorkflow,
):
    sandbox = dev_sandbox("bash::$CF_BASE/sandboxes/venv_columnar.sh")

    dep_MergeHistograms = MergeHistograms

    table_format = luigi.Parameter(
        default="latex_raw",
        significant=False,
        description="format of the yield table, takes all fromats taken by the tabulate package; default: latex_raw",
    )

    # dummy branch map
    def create_branch_map(self):
        return [0]

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
        from tabulate import tabulate

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
                h_in = inp["mc_weight"].load(formatter="pickle")

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
                    h = h[{"process": sum, "shift": sum, "mc_weight": sum}]

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

            yields = []
            yield_header = ["Process"]

            for process_inst, h in hists.items():
                row = []
                row.append(process_inst.label)

                for i in range(h.axes["category"].size):
                    if len(yield_header) <= h.axes["category"].size:
                        yield_header.append(self.config_inst.get_category(h.axes["category"].bin(i)).label)
                    row.append(f"{round(h[i].value)} $\pm$ {round(h[i].variance)}")

                yields.append(row)

            yield_table = tabulate(yields, headers=yield_header, tablefmt=self.table_format)

            # TODO: create some output using something like
            #
            # self.output().dump(yield_table, formatter="txt")

            with open(self.output().fn, "w") as f:
                f.write(yield_table)
