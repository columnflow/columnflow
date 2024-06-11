# coding: utf-8

"""
Tasks to produce yield tables
"""

import math
from collections import defaultdict, OrderedDict

import law
import luigi
from scinum import Number

from columnflow.tasks.framework.base import Requirements
from columnflow.tasks.framework.mixins import (
    CalibratorsMixin, SelectorStepsMixin, ProducersMixin,
    DatasetsProcessesMixin, CategoriesMixin, WeightProducerMixin,
)
from columnflow.tasks.framework.remote import RemoteWorkflow
from columnflow.tasks.histograms import MergeHistograms
from columnflow.util import dev_sandbox, try_int


class CreateYieldTable(
    DatasetsProcessesMixin,
    CategoriesMixin,
    WeightProducerMixin,
    ProducersMixin,
    SelectorStepsMixin,
    CalibratorsMixin,
    law.LocalWorkflow,
    RemoteWorkflow,
):
    sandbox = dev_sandbox(law.config.get("analysis", "default_columnar_sandbox"))

    table_format = luigi.Parameter(
        default="fancy_grid",
        significant=False,
        description="format of the yield table; accepts all formats of the tabulate package; "
        "default: fancy_grid",
    )
    number_format = luigi.Parameter(
        default="pdg",
        significant=False,
        description="rounding format of each number in the yield table; accepts all formats "
        "understood by scinum.Number.str(), e.g. 'pdg', 'publication', '%.1f' or an integer "
        "(number of signficant digits); default: pdg",
    )
    skip_uncertainties = luigi.BoolParameter(
        default=False,
        significant=False,
        description="when True, uncertainties are not displayed in the table; default: False",
    )
    normalize_yields = luigi.ChoiceParameter(
        choices=(law.NO_STR, "per_process", "per_category", "all"),
        default=law.NO_STR,
        significant=False,
        description="string parameter to define the normalization of the yields; "
        "choices: '', per_process, per_category, all; empty default",
    )
    output_suffix = luigi.Parameter(
        default=law.NO_STR,
        description="Adds a suffix to the output name of the yields table; empty default",
    )

    # upstream requirements
    reqs = Requirements(
        RemoteWorkflow.reqs,
        MergeHistograms=MergeHistograms,
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

    @classmethod
    def resolve_param_values(cls, params):
        params = super().resolve_param_values(params)

        if "number_format" in params and try_int(params["number_format"]):
            # convert 'number_format' in integer if possible
            params["number_format"] = int(params["number_format"])

        return params

    def output(self):
        suffix = ""
        if self.output_suffix and self.output_suffix != law.NO_STR:
            suffix = f"__{self.output_suffix}"

        return {
            "table": self.target(f"table__proc_{self.processes_repr}__cat_{self.categories_repr}{suffix}.txt"),
            "yields": self.target(f"yields__proc_{self.processes_repr}__cat_{self.categories_repr}{suffix}.json"),
        }

    @law.decorator.log
    def run(self):
        import hist
        from tabulate import tabulate

        inputs = self.input()
        outputs = self.output()

        category_insts = list(map(self.config_inst.get_category, self.categories))
        process_insts = list(map(self.config_inst.get_process, self.processes))
        sub_process_insts = {
            proc: [sub for sub, _, _ in proc.walk_processes(include_self=True)]
            for proc in process_insts
        }

        # histogram data per process
        hists = {}

        with self.publish_step(f"Creating yields for processes {self.processes}, categories {self.categories}"):
            for dataset, inp in inputs.items():
                dataset_inst = self.config_inst.get_dataset(dataset)

                # load the histogram of the variable named "event"
                h_in = inp["hists"]["event"].load(formatter="pickle")

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

            yields, processes = defaultdict(list), []

            # read out yields per category and per process
            for process_inst, h in hists.items():
                processes.append(process_inst)

                for category_inst in category_insts:
                    leaf_category_insts = category_inst.get_leaf_categories() or [category_inst]

                    h_cat = h[{"category": [
                        hist.loc(c.id)
                        for c in leaf_category_insts
                        if c.id in h.axes["category"]
                    ]}]
                    h_cat = h_cat[{"category": sum}]

                    value = Number(h_cat.value)
                    if not self.skip_uncertainties:
                        # set a unique uncertainty name for correct propagation below
                        value.set_uncertainty(
                            f"mcstat_{process_inst.name}_{category_inst.name}",
                            math.sqrt(h_cat.variance),
                        )
                    yields[category_inst].append(value)

            # obtain normalizaton factors
            norm_factors = 1
            if self.normalize_yields == "all":
                norm_factors = sum(
                    sum(category_yields)
                    for category_yields in yields.values()
                )
            elif self.normalize_yields == "per_process":
                norm_factors = [
                    sum(yields[category][i] for category in yields.keys())
                    for i in range(len(yields[category_insts[0]]))
                ]
            elif self.normalize_yields == "per_category":
                norm_factors = {
                    category: sum(category_yields)
                    for category, category_yields in yields.items()
                }

            # initialize dicts
            yields_str = defaultdict(list, {"Process": [proc.label for proc in processes]})
            raw_yields = defaultdict(dict, {})

            # apply normalization and format
            for category, category_yields in yields.items():
                for i, value in enumerate(category_yields):
                    # get correct norm factor per category and process
                    if self.normalize_yields == "per_process":
                        norm_factor = norm_factors[i]
                    elif self.normalize_yields == "per_category":
                        norm_factor = norm_factors[category]
                    else:
                        norm_factor = norm_factors

                    raw_yield = (value / norm_factor).nominal
                    raw_yields[category.name][processes[i].name] = raw_yield

                    # format yields into strings
                    yield_str = (value / norm_factor).str(
                        combine_uncs="all",
                        format=self.number_format,
                        style="latex" if "latex" in self.table_format else "plain",
                    )
                    if "latex" in self.table_format:
                        yield_str = f"${yield_str}$"
                    yields_str[category.label].append(yield_str)

            # create, print and save the yield table
            yield_table = tabulate(yields_str, headers="keys", tablefmt=self.table_format)
            self.publish_message(yield_table)

            outputs["table"].dump(yield_table, formatter="text")
            outputs["yields"].dump(raw_yields, formatter="json")
