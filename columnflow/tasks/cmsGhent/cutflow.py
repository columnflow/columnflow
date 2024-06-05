
from collections import defaultdict, OrderedDict
from scinum import Number


from columnflow.tasks.cutflow import (
    RemoteWorkflow, Requirements, MergeCutflowHistograms,
)
# from columnflow.tasks.framework.decorators import view_output_plots
from columnflow.tasks.framework.mixins import (
    CalibratorsMixin, SelectorStepsMixin, CategoriesMixin, DatasetsProcessesMixin,
)

import luigi
import law
from columnflow.util import maybe_import, dev_sandbox, try_int
# from columnflow.util import DotDict

np = maybe_import("numpy")


class CreateCutflowTable(
    DatasetsProcessesMixin,
    CategoriesMixin,
    SelectorStepsMixin,
    CalibratorsMixin,
    law.LocalWorkflow,
    RemoteWorkflow,
):
    sandbox = dev_sandbox(law.config.get("analysis", "default_columnar_sandbox"))

    table_format = luigi.Parameter(
        default="fancy_grid",
        significant=False,
        description="format of the yield table; accepts all formats of the tabulate package; default: fancy_grid. "
                    "See https://github.com/astanin/python-tabulate/blob/master/README.md?plain=1#L147",
    )
    number_format = luigi.Parameter(
        default="pdg",
        significant=False,
        description="rounding format of each number in the yield table; accepts all formats "
        "understood by scinum.Number.str(), e.g. 'pdg', 'publication', '%%.1f' or an integer "
        "(number of signficant digits); default: pdg",
    )
    skip_uncertainties = luigi.BoolParameter(
        default=False,
        significant=False,
        description="when True, uncertainties are not displayed in the table; default: False",
    )
    normalize_yields = luigi.ChoiceParameter(
        choices=(law.NO_STR, "per_process", "per_step", "per_process_100", "per_step_100", "all_100"),
        default=law.NO_STR,
        significant=False,
        description="string parameter to define the normalization of the yields; "
        "choices: '', per_process, per_category, all; Append 100 to express as percentage; empty default",
    )
    output_suffix = luigi.Parameter(
        default=law.NO_STR,
        description="Adds a suffix to the output name of the yields table; empty default",
    )

    selector_steps_order_sensitive = True

    # upstream requirements
    reqs = Requirements(
        RemoteWorkflow.reqs,
        MergeCutflowHistograms=MergeCutflowHistograms,
    )

    def create_branch_map(self):
        # one category per branch
        if not self.categories:
            raise Exception(
                f"{self.__class__.__name__} task cannot build branch map when no category is "
                "set",
            )

        return list(self.categories)

    def workflow_requires(self):
        reqs = super().workflow_requires()

        reqs["hists"] = [
            self.reqs.MergeCutflowHistograms.req(
                self,
                dataset=d,
                variables=("event",),
                _exclude={"branches"},
            )
            for d in self.datasets
        ]
        return reqs

    def requires(self):
        return {
            d: self.reqs.MergeCutflowHistograms.req(
                self,
                branch=0,
                dataset=d,
                variables=("event",),
            )
            for d in self.datasets
        }

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
            "table": self.target(f"table__proc_{self.processes_repr}__steps_{self.branch_data}{suffix}.txt"),
            "yields": self.target(f"yields__proc_{self.processes_repr}__steps_{self.branch_data}{suffix}.json"),
        }

    @law.decorator.log
    def run(self):
        import hist
        from tabulate import tabulate

        inputs = self.input()
        outputs = self.output()

        category_inst = self.config_inst.get_category(self.branch_data)
        leaf_category_insts = category_inst.get_leaf_categories() or [category_inst]
        process_insts = list(map(self.config_inst.get_process, self.processes))
        sub_process_insts = {
            proc: [sub for sub, _, _ in proc.walk_processes(include_self=True)]
            for proc in process_insts
        }

        # histogram data per process
        hists = {}

        with self.publish_step(f"Creating cutflow table in {category_inst.name}"):
            for dataset, inp in inputs.items():
                dataset_inst = self.config_inst.get_dataset(dataset)

                # load the histogram of the variable named "event"
                h_in = inp["hists"]["event"].load(formatter="pickle")

                # sanity checks
                n_shifts = len(h_in.axes["shift"])
                if n_shifts != 1:
                    raise Exception(f"shift axis is supposed to only contain 1 bin, found {n_shifts}")

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
                        "category": [
                            hist.loc(c.id)
                            for c in leaf_category_insts
                            if c.id in h.axes["category"]
                        ],
                    }]

                    # axis reductions
                    h = h[{"shift": sum, "process": sum, "category": sum, "event": sum}]

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

            # read out yields per step and per process
            for process_inst, h in hists.items():
                processes.append(process_inst)

                for step in self.selector_steps:
                    h_step = h[{"step": [step]}]
                    h_step = h_step[{"step": sum}]
                    value = Number(h_step.value)
                    if not self.skip_uncertainties:
                        # set a unique uncertainty name for correct propagation below
                        value.set_uncertainty(
                            f"mcstat_{process_inst.name}_{step}",
                            np.sqrt(h_step.variance),
                        )
                    yields[step].append(value)

            # obtain normalizaton factors
            norm_factors = 0.01 if "100" in self.normalize_yields else 1
            if self.normalize_yields == "all":
                norm_factors *= sum(
                    sum(step_yields)
                    for step_yields in yields.values()
                )
            elif self.normalize_yields.startswith("per_process"):
                norm_factors = [
                    norm_factors * sum(yields[step][i] for step in yields.keys())
                    for i in range(len(yields[self.selector_steps[0]]))
                ]
            elif self.normalize_yields.startswith("per_step"):
                norm_factors = {
                    step: norm_factors * sum(step_yields)
                    for step, step_yields in yields.items()
                }

            # initialize dicts
            yields_str = defaultdict(list, {"Process": [proc.label for proc in processes]})
            raw_yields = defaultdict(dict, {})

            # apply normalization and format
            for step, step_yields in yields.items():
                for i, value in enumerate(step_yields):
                    # get correct norm factor per category and process
                    if self.normalize_yields.startswith("per_process"):
                        norm_factor = norm_factors[i]
                    elif self.normalize_yields.startswith("per_step"):
                        norm_factor = norm_factors[step]
                    else:
                        norm_factor = norm_factors

                    raw_yield = (value / norm_factor).nominal
                    raw_yields[step][processes[i].name] = raw_yield

                    # format yields into strings
                    yield_str = (value / norm_factor).str(
                        combine_uncs="all",
                        format=self.number_format,
                        style="latex" if "latex" in self.table_format else "plain",
                    )
                    if "latex" in self.table_format:
                        yield_str = f"${yield_str}$"
                    yields_str[step].append(yield_str)

            # create, print and save the yield table
            yield_table = tabulate(yields_str, headers="keys", tablefmt=self.table_format)
            self.publish_message(yield_table)

            outputs["table"].dump(yield_table, formatter="text")
            outputs["yields"].dump(raw_yields, formatter="json")
