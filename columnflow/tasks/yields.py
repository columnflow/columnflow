# coding: utf-8

"""
Tasks to produce yield tables
"""

from collections import OrderedDict

import math

import law
import luigi
from scinum import Number

from columnflow.tasks.framework.base import Requirements
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
    ProducersMixin,
    SelectorStepsMixin,
    CalibratorsMixin,
    law.LocalWorkflow,
    RemoteWorkflow,
):
    sandbox = dev_sandbox("bash::$CF_BASE/sandboxes/venv_columnar.sh")

    dep_MergeHistograms = MergeHistograms

    # upstream requirements
    reqs = Requirements(
        RemoteWorkflow.reqs,
        MergeHistograms=MergeHistograms,
    )

    table_format = luigi.Parameter(
        default="latex_raw",
        significant=False,
        description="format of the yield table, takes all formats taken by the tabulate package; default: latex_raw",
    )

    number_format = luigi.Parameter(
        default="pdg",
        significant=False,
        description=(
            "format of each number in the yield table, takes all formats taken by "
            "Number.str, e.g. 'pdg', 'publication' or '%.1f'; default: pdg"
        ),
    )

    skip_uncert = luigi.BoolParameter(
        default=False,
        significant=False,
        description="when True, uncertainties are not displayed in the table; default: False",
    )

    # dummy branch map
    def create_branch_map(self):
        return [0]

    def requires(self):
        return {
            d: self.reqs.MergeHistograms.req(
                self,
                dataset=d,
                variables=("event",),
                _prefer_cli={"variables"},
            )
            for d in self.datasets
        }

    def workflow_requires(self):
        reqs = super().workflow_requires()

        reqs["merged_hists"] = [
            self.reqs.MergeHistograms.req(
                self,
                dataset=d,
                variables=("event",),
                _exclude={"branches"},
            )
            for d in self.datasets
        ]

        return reqs

    def output(self):
        return self.target(f"yields__proc_{self.processes_repr}__cat_{self.categories_repr}.txt")

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

                    value = Number(h_cat.value, math.sqrt(h_cat.variance))

                    # TODO: allow normalizing per process or per category (or both?)
                    if self.skip_uncert:
                        value_str = value.str(format=self.number_format).split(" +- ")[0]
                    else:
                        value_str = value.str(
                            format=self.number_format,
                            style="latex" if "latex" in self.table_format else "plain",
                        )

                    row.append(value_str)

                yields.append(row)

            yield_table = tabulate(yields, headers=yield_header, tablefmt=self.table_format)
            print(yield_table)

            self.output().dump(yield_table)
