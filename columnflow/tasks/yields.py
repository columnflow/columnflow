# coding: utf-8

"""
Tasks to produce yield tables
"""

from collections import OrderedDict

import law
import luigi

from columnflow.tasks.framework.mixins import (
    CalibratorsMixin, SelectorStepsMixin, ProducersMixin,
    DatasetsProcessesMixin, CategoriesMixin,
)
from columnflow.tasks.framework.remote import RemoteWorkflow

from columnflow.tasks.histograms import MergeHistograms
from columnflow.util import dev_sandbox


class CreateYieldTable(
    DatasetsProcessesMixin,
    CategoriesMixin,
    law.LocalWorkflow,
    ProducersMixin,
    SelectorStepsMixin,
    CalibratorsMixin,
    RemoteWorkflow,
):
    sandbox = dev_sandbox("bash::$CF_BASE/sandboxes/venv_columnar.sh")

    dep_MergeHistograms = MergeHistograms

    table_format = luigi.Parameter(
        default="latex_raw",
        significant=False,
        description="format of the yield table, takes all fromats taken by the tabulate package; default: latex_raw",
    )

    skip_variance = luigi.BoolParameter(
        default=False,
        significant=False,
        description="wether to skip variance or not; default: False",
    )

    # dummy branch map
    def create_branch_map(self):
        return [0]

    def requires(self):
        return {
            d: self.dep_MergeHistograms.req(
                self,
                dataset=d,
                variables=("event",),
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

        category_insts = list(map(self.config_inst.get_category, self.categories))
        process_insts = list(map(self.config_inst.get_process, self.processes))
        sub_process_insts = {
            proc: [sub for sub, _, _ in proc.walk_processes(include_self=True)]
            for proc in process_insts
        }

        # histogram data per process
        hists = {}

        with self.publish_step(f"Creating yields for processes {self.processes}, categories {self.categories}"):
            for dataset, inp in self.input().items():
                dataset_inst = self.config_inst.get_dataset(dataset)
                h_in = inp["event"].load(formatter="pickle")

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
                    h = h[{"process": sum, "shift": sum, "event": sum}]

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
            yield_header = ["Process"] + [category_inst.label for category_inst in category_insts]



            for process_inst, h in hists.items():
                row = []
                row.append(process_inst.label)

                for category_inst in category_insts:
                    leaf_category_insts = category_inst.get_leaf_categories() or [category_inst]

                    h_cat = h[{"category": [
                        hist.loc(c.id)
                        for c in leaf_category_insts
                        if c.id in h.axes["category"]
                    ]}]
                    h_cat = h_cat[{"category": sum}]

                    value = h_cat.value
                    variance = h_cat.variance
                    # TODO: rounding should be less arbitrary, e.g. always round to 4 significant
                    row.append(
                        f"{value:.1f}" if self.skip_variance else
                        rf"${value:.1f} \pm {variance:.1f}$"
                    )

                yields.append(row)

            yield_table = tabulate(yields, headers=yield_header, tablefmt=self.table_format)
            print(yield_table)

            self.output().dump(yield_table)
