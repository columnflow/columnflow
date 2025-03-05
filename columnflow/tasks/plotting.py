# coding: utf-8

"""
Tasks to plot different types of histograms.
"""

from collections import OrderedDict, defaultdict
from abc import abstractmethod

from columnflow.types import Any

import law
import luigi
import order as od

from columnflow.tasks.framework.base import Requirements, ShiftTask
from columnflow.tasks.framework.mixins import (
    CalibratorClassesMixin, SelectorClassMixin, ProducerClassesMixin, WeightProducerClassMixin,
    CategoriesMixin, DatasetsProcessesMixin, DatasetsProcessesShiftSourcesMixin, HistHookMixin,
)
from columnflow.tasks.framework.plotting import (
    PlotBase, PlotBase1D, PlotBase2D, ProcessPlotSettingMixin, VariablePlotSettingMixin,
)
from columnflow.tasks.framework.decorators import view_output_plots
from columnflow.tasks.framework.remote import RemoteWorkflow
from columnflow.tasks.histograms import MergeHistograms, MergeShiftedHistograms
from columnflow.util import DotDict, dev_sandbox, dict_add_strict
from columnflow.hist_util import add_missing_shifts


class PlotVariablesBase(
    CalibratorClassesMixin,
    SelectorClassMixin,
    ProducerClassesMixin,
    # MLModelsMixin,
    WeightProducerClassMixin,
    CategoriesMixin,
    ProcessPlotSettingMixin,
    DatasetsProcessesMixin,
    VariablePlotSettingMixin,
    HistHookMixin,
    law.LocalWorkflow,
    RemoteWorkflow,
):
    # single_config = True set via MultiConfigDatasetsProcessesMixin

    sandbox = dev_sandbox(law.config.get("analysis", "default_columnar_sandbox"))

    # upstream requirements
    reqs = Requirements(
        RemoteWorkflow.reqs,
        MergeHistograms=MergeHistograms,
    )

    exclude_index = True

    def store_parts(self) -> law.util.InsertableDict:
        parts = super().store_parts()
        parts.insert_before("version", "datasets", f"datasets_{self.datasets_repr}")
        return parts

    def create_branch_map(self):
        return [
            DotDict({"category": cat_name, "variable": var_name})
            for cat_name in sorted(self.categories)
            for var_name in sorted(self.variables)
        ]

    def workflow_requires(self):
        reqs = super().workflow_requires()
        reqs["merged_hists"] = self.requires_from_branch()
        return reqs

    @abstractmethod
    def get_plot_shifts(self):
        return

    @property
    def config_inst(self):
        return self.config_insts[0]

    def get_config_process_map(self) -> dict[od.Config, dict[od.Process, dict[str, Any]]]:
        """
        Function that maps the config and process instances to the datasets and shifts they
        are supposed to be plotted with. The mapping from processes to datasets is done by
        checking the dataset instances for the presence of the process instances. The mapping
        from processes to shifts is done by checking the upstream requirements for the presence
        of a shift in the requires method of the task.

        :return: A dictionary mapping config instances to dictionaries mapping process instances
            to dictionaries containing the dataset-process mapping and the shifts to be considered.
        """
        reqs = self.requires()

        config_process_map = {config_inst: {} for config_inst in self.config_insts}
        process_shift_map = defaultdict(set)

        for i, config_inst in enumerate(self.config_insts):
            process_insts = [config_inst.get_process(p) for p in self.processes[i]]
            dataset_insts = [config_inst.get_dataset(d) for d in self.datasets[i]]

            requested_shifts_per_dataset: dict[od.Dataset, list[od.Shift]] = {}
            for dataset_inst in dataset_insts:
                _req = reqs[config_inst.name][dataset_inst.name]
                if hasattr(_req, "shift") and _req.shift:
                    # when a shift is found, use it
                    requested_shifts = [_req.shift]
                else:
                    # when no shift is found, check upstream requirements
                    requested_shifts = [sub_req.shift for sub_req in _req.requires().values()]

                requested_shifts_per_dataset[dataset_inst] = requested_shifts

            for process_inst in process_insts:
                sub_process_insts = [sub for sub, _, _ in process_inst.walk_processes(include_self=True)]
                dataset_procid_map = {}
                for dataset_inst in dataset_insts:
                    matched_proc_ids = [p.id for p in sub_process_insts if dataset_inst.has_process(p.name)]
                    if matched_proc_ids:
                        dataset_procid_map[dataset_inst] = matched_proc_ids

                if not dataset_procid_map:
                    # no datasets found for this process
                    continue

                process_info = {
                    "dataset_procid_map": dataset_procid_map,
                    # "sub_process_ids": {sub.id for sub in sub_process_insts},
                    "config_shifts": {
                        shift
                        for dataset_inst in dataset_procid_map.keys()
                        for shift in requested_shifts_per_dataset[dataset_inst]
                    },
                }
                process_shift_map[process_inst.name].update(process_info["config_shifts"])
                config_process_map[config_inst][process_inst] = process_info

        # assign the combination of all shifts to each config-process pair
        for config_inst, process_info_dict in config_process_map.items():
            for process_inst, process_info in process_info_dict.items():
                if process_inst.name in process_shift_map:
                    config_process_map[config_inst][process_inst]["shifts"] = process_shift_map[process_inst.name]

        return config_process_map, process_shift_map

    @law.decorator.log
    @view_output_plots
    def run(self):
        import hist

        # prepare other config objects
        variable_tuple = self.variable_tuples[self.branch_data.variable]
        variable_insts = [
            self.config_inst.get_variable(var_name)
            for var_name in variable_tuple
        ]
        plot_shifts = self.get_plot_shifts()

        # get assignment of processes to datasets and shifts
        config_process_map, process_shift_map = self.get_config_process_map()

        # histogram data per process copy
        hists: dict[od.Config, dict[od.Process, hist.Hist]] = {}
        with self.publish_step(f"plotting {self.branch_data.variable} in {self.branch_data.category}"):
            for i, (config, dataset_dict) in enumerate(self.input().items()):
                config_inst = self.config_insts[i]
                category_inst = config_inst.get_category(self.branch_data.category)
                leaf_category_insts = category_inst.get_leaf_categories() or [category_inst]

                hists_config = {}

                for dataset, inp in dataset_dict.items():
                    dataset_inst = config_inst.get_dataset(dataset)
                    h_in = inp["collection"][0]["hists"].targets[self.branch_data.variable].load(formatter="pickle")

                    # loop and extract one histogram per process
                    for process_inst, process_info in config_process_map[config_inst].items():
                        if dataset_inst not in process_info["dataset_procid_map"].keys():
                            continue

                        # select processes and reduce axis
                        h = h_in.copy()
                        h = h[{
                            "process": [
                                hist.loc(proc_id)
                                for proc_id in process_info["dataset_procid_map"][dataset_inst]
                                if proc_id in h.axes["process"]
                            ],
                        }]
                        h = h[{"process": sum}]

                        # create expected shift bins and fill them with the nominal histogram
                        expected_shifts = set(plot_shifts) & process_shift_map[process_inst.name]
                        add_missing_shifts(h, expected_shifts, str_axis="shift", nominal_bin="nominal")

                        # add the histogram
                        if process_inst in hists_config:
                            hists_config[process_inst] += h
                        else:
                            hists_config[process_inst] = h

                # after merging all processes, sort the histograms by process order and store them
                hists[config] = {
                    proc_inst: hists_config[proc_inst]
                    for proc_inst in sorted(
                        hists_config.keys(), key=list(config_process_map[config_inst].keys()).index,
                    )
                }

                # there should be hists to plot
                if not hists:
                    raise Exception(
                        "no histograms found to plot; possible reasons:\n"
                        "  - requested variable requires columns that were missing during histogramming\n"
                        "  - selected --processes did not match any value on the process axis of the input histogram",
                    )

            # update histograms using custom hooks
            hists = self.invoke_hist_hooks(hists)

            # merge configs
            if len(self.config_insts) != 1:
                process_memory = {}
                merged_hists = {}
                for config, _hists in hists.items():
                    for process_inst, h in _hists.items():

                        if process_inst.id in merged_hists:
                            merged_hists[process_inst.id] += h
                        else:
                            merged_hists[process_inst.id] = h
                            process_memory[process_inst.id] = process_inst

                process_insts = list(process_memory.values())
                hists = {process_memory[process_id]: h for process_id, h in merged_hists.items()}
            else:
                hists = hists[self.config_inst.name]
                process_insts = list(hists.keys())

            # axis selections and reductions
            _hists = OrderedDict()
            for process_inst in hists.keys():
                h = hists[process_inst]
                # selections
                h = h[{
                    "category": [
                        hist.loc(c.name)
                        for c in leaf_category_insts
                        if c.name in h.axes["category"]
                    ],
                    "shift": [
                        hist.loc(s_name)
                        for s_name in process_shift_map[process_inst.name]
                        if s_name in h.axes["shift"]
                    ],
                }]
                # reductions
                h = h[{"category": sum}]
                # store
                _hists[process_inst] = h
            hists = _hists

            # copy process instances once so that their auxiliary data fields can be used as a storage
            # for process-specific plot parameters later on in plot scripts without affecting the
            # original instances
            fake_root = od.Process(
                name=f"{hex(id(object()))[2:]}",
                id="+",
                processes=list(hists.keys()),
            ).copy()
            process_insts = list(fake_root.processes)
            fake_root.processes.clear()
            hists = dict(zip(process_insts, hists.values()))

            # correct luminosity label in case of multiple configs
            config_inst = self.config_inst.copy(id=-1, name=f"{self.config_inst.name}_merged")

            if len(self.config_insts) != 1:
                config_inst.x.luminosity = sum([_config_inst.x.luminosity for _config_inst in self.config_insts])

            # call the plot function
            fig, _ = self.call_plot_func(
                self.plot_function,
                hists=hists,
                config_inst=config_inst,
                category_inst=category_inst.copy_shallow(),
                variable_insts=[var_inst.copy_shallow() for var_inst in variable_insts],
                **self.get_plot_parameters(),
            )

            # save the plot
            for outp in self.output()["plots"]:
                outp.dump(fig, formatter="mpl")


class PlotVariablesBaseSingleShift(
    ShiftTask,
    PlotVariablesBase,
):

    exclude_index = True

    # upstream requirements
    reqs = Requirements(
        PlotVariablesBase.reqs,
        MergeHistograms=MergeHistograms,
    )

    def create_branch_map(self):
        return [
            DotDict({"category": cat_name, "variable": var_name})
            for var_name in sorted(self.variables)
            for cat_name in sorted(self.categories)
        ]

    def workflow_requires(self):
        reqs = super().workflow_requires()
        return reqs

    def requires(self):
        req = {}

        for i, config_inst in enumerate(self.config_insts):
            sub_datasets = self.datasets[i]
            req[config_inst.name] = {}
            for d in sub_datasets:
                if d in config_inst.datasets.names():
                    req[config_inst.name][d] = self.reqs.MergeHistograms.req(
                        self,
                        config=config_inst.name,
                        dataset=d,
                        branch=-1,
                        _exclude={"branches"},
                        _prefer_cli={"variables"},
                    )
        return req

    def plot_parts(self) -> law.util.InsertableDict:
        parts = super().plot_parts()

        parts["processes"] = f"proc_{self.processes_repr}"
        parts["category"] = f"cat_{self.branch_data.category}"
        parts["variable"] = f"var_{self.branch_data.variable}"

        hooks_repr = self.hist_hooks_repr
        if hooks_repr:
            parts["hook"] = f"hooks_{hooks_repr}"

        return parts

    def output(self):
        return {
            "plots": [self.target(name) for name in self.get_plot_names("plot")],
        }

    def store_parts(self) -> law.util.InsertableDict:
        parts = super().store_parts()
        if "shift" in parts:
            parts.insert_before("datasets", "shift", parts.pop("shift"))
        return parts

    def get_plot_shifts(self):
        return [self.shift]


class PlotVariables1D(
    PlotVariablesBaseSingleShift,
    PlotBase1D,
):
    plot_function = PlotBase.plot_function.copy(
        default="columnflow.plotting.plot_functions_1d.plot_variable_per_process",
        add_default_to_description=True,
    )


class PlotVariablesPerConfig1D(
    law.WrapperTask,
    PlotVariables1D,
):
    # force this one to be a local workflow
    workflow = "local"
    output_collection_cls = law.NestedSiblingFileCollection

    def requires(self):
        return {
            config: PlotVariables1D.req(
                self,
                datasets=(self.datasets[i],),
                processes=(self.processes[i],),
                configs=(config,),
            )
            for i, config in enumerate(self.configs)
        }


class PlotVariables2D(
    PlotVariablesBaseSingleShift,
    PlotBase2D,
):
    plot_function = PlotBase.plot_function.copy(
        default="columnflow.plotting.plot_functions_2d.plot_2d",
        add_default_to_description=True,
    )


class PlotVariablesPerConfig2D(
    law.WrapperTask,
    PlotVariables1D,
):
    # force this one to be a local workflow
    workflow = "local"
    output_collection_cls = law.NestedSiblingFileCollection

    def requires(self):
        return {
            config: PlotVariablesPerConfig2D.req(
                self,
                datasets=(self.datasets[i],),
                processes=(self.processes[i],),
                configs=(config,),
            )
            for i, config in enumerate(self.configs)
        }


class PlotVariablesPerProcess2D(
    PlotVariables2D,
    law.WrapperTask,
):
    # force this one to be a local workflow
    workflow = "local"

    def requires(self):
        return {
            process: PlotVariables2D.req(self, processes=(process,))
            for process in self.processes
        }


class PlotVariablesBaseMultiShifts(
    DatasetsProcessesShiftSourcesMixin,
    PlotVariablesBase,
):
    legend_title = luigi.Parameter(
        default=law.NO_STR,
        significant=False,
        description="sets the title of the legend; when empty and only one process is present in "
        "the plot, the process_inst label is used; empty default",
    )

    # use the MergeHistograms task to validate shift sources against the requested dataset
    shift_validation_task_cls = MergeHistograms

    exclude_index = True

    # upstream requirements
    reqs = Requirements(
        PlotVariablesBase.reqs,
        MergeShiftedHistograms=MergeShiftedHistograms,
    )

    def create_branch_map(self):
        return [
            DotDict({"category": cat_name, "variable": var_name, "shift_source": source})
            for var_name in sorted(self.variables)
            for cat_name in sorted(self.categories)
            for source in sorted(self.shift_sources)
        ]

    def requires(self):
        req = {}

        for i, config_inst in enumerate(self.config_insts):
            sub_datasets = self.datasets[i]
            req[config_inst.name] = {}
            for d in sub_datasets:
                if d in config_inst.datasets.names():
                    # TODO: for data, request MergeHistograms
                    req[config_inst.name][d] = self.reqs.MergeShiftedHistograms.req(
                        self,
                        config=config_inst.name,
                        dataset=d,
                        branch=-1,
                        _exclude={"branches"},
                        _prefer_cli={"variables"},
                    )
        return req

    def plot_parts(self) -> law.util.InsertableDict:
        parts = super().plot_parts()

        parts["processes"] = f"proc_{self.processes_repr}"
        parts["shift_source"] = f"unc_{self.branch_data.shift_source}"
        parts["category"] = f"cat_{self.branch_data.category}"
        parts["variable"] = f"var_{self.branch_data.variable}"

        hooks_repr = self.hist_hooks_repr
        if hooks_repr:
            parts["hook"] = f"hooks_{hooks_repr}"

        return parts

    def output(self):
        return {
            "plots": [self.target(name) for name in self.get_plot_names("plot")],
        }

    def store_parts(self) -> law.util.InsertableDict:
        parts = super().store_parts()
        parts.insert_before("datasets", "shifts", f"shifts_{self.shift_sources_repr}")
        return parts

    def get_plot_shifts(self):
        return [
            "nominal",
            f"{self.branch_data.shift_source}_up",
            f"{self.branch_data.shift_source}_down",
        ]

    def get_plot_parameters(self):
        # convert parameters to usable values during plotting
        params = super().get_plot_parameters()
        dict_add_strict(params, "legend_title", None if self.legend_title == law.NO_STR else self.legend_title)
        return params


class PlotShiftedVariables1D(
    PlotBase1D,
    PlotVariablesBaseMultiShifts,
):
    plot_function = PlotBase.plot_function.copy(
        default="columnflow.plotting.plot_functions_1d.plot_shifted_variable",
        add_default_to_description=True,
    )


class PlotShiftedVariablesPerConfig1D(
    law.WrapperTask,
    PlotShiftedVariables1D,
):
    # force this one to be a local workflow
    workflow = "local"
    output_collection_cls = law.NestedSiblingFileCollection

    def requires(self):
        return {
            config: PlotShiftedVariables1D.req(
                self,
                datasets=(self.datasets[i],),
                processes=(self.processes[i],),
                configs=(config,),
            )
            for i, config in enumerate(self.configs)
        }


class PlotShiftedVariablesPerProcess1D(law.WrapperTask):

    # upstream requirements
    reqs = Requirements(
        PlotShiftedVariables1D.reqs,
        PlotShiftedVariables1D=PlotShiftedVariables1D,
    )

    def requires(self):
        return {
            process: self.reqs.PlotShiftedVariables1D.req(self, processes=(process,))
            for process in self.processes
        }
