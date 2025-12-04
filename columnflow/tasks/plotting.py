# coding: utf-8

"""
Tasks to plot different types of histograms.
"""

import itertools
import functools
from collections import OrderedDict, defaultdict
from abc import abstractmethod

import law
import luigi
import order as od

from columnflow.tasks.framework.base import Requirements, ShiftTask
from columnflow.tasks.framework.mixins import (
    CalibratorClassesMixin, SelectorClassMixin, ReducerClassMixin, ProducerClassesMixin, HistProducerClassMixin,
    CategoriesMixin, ShiftSourcesMixin, HistHookMixin, MLModelsMixin,
)
from columnflow.tasks.framework.plotting import (
    PlotBase, PlotBase1D, PlotBase2D, ProcessPlotSettingMixin, VariablePlotSettingMixin,
)
from columnflow.tasks.framework.decorators import view_output_plots
from columnflow.tasks.framework.remote import RemoteWorkflow
from columnflow.tasks.histograms import MergeHistograms, MergeShiftedHistograms
from columnflow.util import DotDict, dev_sandbox, dict_add_strict
from columnflow.hist_util import add_missing_shifts, sum_hists
from columnflow.config_util import get_shift_from_configs, expand_shift_sources
from columnflow.types import Any


class _PlotVariablesBase(
    CalibratorClassesMixin,
    SelectorClassMixin,
    ReducerClassMixin,
    ProducerClassesMixin,
    MLModelsMixin,
    HistProducerClassMixin,
    CategoriesMixin,
    ProcessPlotSettingMixin,
    VariablePlotSettingMixin,
    HistHookMixin,
    law.LocalWorkflow,
    RemoteWorkflow,
):
    """
    Base classes for :py:class:`PlotVariablesBase`.
    """


class PlotVariablesBase(_PlotVariablesBase):

    multi_variable = luigi.BoolParameter(
        default=False,
        description="whether a single plot for all variables should be created; this requires that the used plot "
        "function accepts a nested dictionary with all variable and process histograms as an input; default: False",
    )
    bypass_branch_requirements = luigi.BoolParameter(
        default=False,
        description="whether to skip branch requirements and only use that of the workflow; default: False",
    )

    single_config = False

    sandbox = dev_sandbox(law.config.get("analysis", "default_columnar_sandbox"))

    exclude_params_repr = {"bypass_branch_requirements"}
    exclude_params_index = {"bypass_branch_requirements"}
    exclude_params_repr = {"bypass_branch_requirements"}

    exclude_index = True

    def store_parts(self) -> law.util.InsertableDict:
        parts = super().store_parts()
        parts.insert_before("version", "datasets", f"datasets_{self.datasets_repr}")
        return parts

    def create_branch_map(self):
        keys = ["category"]
        seqs = [self.categories]
        if not self.multi_variable:
            keys.append("variable")
            seqs.append(self.variables)
        return [DotDict(zip(keys, vals)) for vals in itertools.product(*seqs)]

    def workflow_requires(self):
        reqs = super().workflow_requires()
        reqs["merged_hists"] = self.requires_from_branch()
        return reqs

    def local_workflow_pre_run(self):
        # when branches are cached, reinitiate the branch tasks with dropped branch level requirements since this
        # method is called from a context where the identical workflow level requirements are already resolved
        if self.cache_branch_map:
            self._branch_tasks = None
            self.get_branch_tasks(bypass_branch_requirements=True)

    @abstractmethod
    def get_plot_shifts(self):
        return

    @property
    def config_inst(self):
        return self.config_insts[0]

    def get_config_process_map(self) -> tuple[dict[od.Config, dict[od.Process, dict[str, Any]]], dict[str, set[str]]]:
        """
        Function that maps the config and process instances to the datasets and shifts they are supposed to be plotted
        with. The mapping from processes to datasets is done by checking the dataset instances for the presence of the
        process instances. The mapping from processes to shifts is done by checking the upstream requirements for the
        presence of a shift in the requires method of the task.

        :return: A 2-tuple with a dictionary mapping config instances to dictionaries mapping process instances to
            dictionaries containing the dataset-process mapping and the shifts to be considered, and a dictionary
            mapping process names to the shifts to be considered.
        """
        reqs = self.requires() or self.as_workflow().requires().merged_hists

        config_process_map = {config_inst: {} for config_inst in self.config_insts}
        process_shift_map = defaultdict(set)

        for i, config_inst in enumerate(self.config_insts):
            process_insts = [config_inst.get_process(p) for p in self.processes[i]]
            dataset_insts = [config_inst.get_dataset(d) for d in self.datasets[i]]

            requested_shifts_per_dataset: dict[od.Dataset, list[str]] = {}
            for dataset_inst in dataset_insts:
                _req = reqs[config_inst.name][dataset_inst.name]
                if isinstance(_req, ShiftTask) and _req.shift:
                    # when a shift is found, use it
                    requested_shifts = [_req.shift]
                elif isinstance(_req, ShiftSourcesMixin):
                    # when no shift is found, check for shift sources and expand to up/down variations
                    requested_shifts = expand_shift_sources(_req.shift_sources)
                else:
                    raise Exception(
                        f"no shift or shift source found in requirements for dataset {dataset_inst.name} "
                        f"of config {config_inst.name}",
                    )

                requested_shifts_per_dataset[dataset_inst] = requested_shifts

            for process_inst in process_insts:
                sub_process_insts = [sub for sub, _, _ in process_inst.walk_processes(include_self=True)]
                dataset_proc_name_map = {}
                for dataset_inst in dataset_insts:
                    matched_proc_names = [p.name for p in sub_process_insts if dataset_inst.has_process(p.name)]
                    if matched_proc_names:
                        dataset_proc_name_map[dataset_inst] = matched_proc_names

                if not dataset_proc_name_map:
                    # no datasets found for this process
                    continue

                process_info = {
                    "dataset_proc_name_map": dataset_proc_name_map,
                    "config_shifts": {
                        shift
                        for dataset_inst in dataset_proc_name_map.keys()
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
        variables = list(self.variables) if self.multi_variable else [self.branch_data.variable]
        plot_shifts = self.get_plot_shifts()
        plot_shift_names = set(shift_inst.name for shift_inst in plot_shifts)

        # get assignment of processes to datasets and shifts
        config_process_map, process_shift_map = self.get_config_process_map()

        # read histograms per variable name, config and process
        hists: dict[str, dict[od.Config, dict[od.Process, hist.Hist]]] = {var_name: {} for var_name in variables}
        with self.publish_step(f"plotting {','.join(variables)} in {self.branch_data.category}"):
            inputs = self.input() or self.workflow_input().merged_hists
            for var_name in variables:
                for i, (config, dataset_dict) in enumerate(inputs.items()):
                    config_inst = self.config_insts[i]
                    category_inst = config_inst.get_category(self.branch_data.category)
                    leaf_category_insts = category_inst.get_leaf_categories() or [category_inst]

                    hists_config = {}

                    for dataset, inps in dataset_dict.items():
                        dataset_inst = config_inst.get_dataset(dataset)

                        # load input histograms, summing over outputs of histogram tasks
                        h_in = sum_hists([
                            inp["hists"].targets[var_name].load(formatter="pickle")
                            for inp in inps["collection"].targets.values()
                        ])

                        # loop and extract one histogram per process
                        for process_inst, process_info in config_process_map[config_inst].items():
                            if dataset_inst not in process_info["dataset_proc_name_map"].keys():
                                continue

                            # select processes and reduce axis
                            h = h_in[{
                                "process": [
                                    hist.loc(proc_name)
                                    for proc_name in process_info["dataset_proc_name_map"][dataset_inst]
                                    if proc_name in h_in.axes["process"]
                                ],
                            }]
                            h = h[{"process": sum}]

                            # create expected shift bins and fill them with the nominal histogram
                            expected_shifts = plot_shift_names & process_shift_map[process_inst.name]
                            add_missing_shifts(h, expected_shifts, str_axis="shift", nominal_bin="nominal")

                            # add the histogram
                            if process_inst in hists_config:
                                hists_config[process_inst] += h
                            else:
                                hists_config[process_inst] = h

                        # free memory
                        del h_in

                    # after merging all processes, sort the histograms by process order and store them
                    hists[var_name][config_inst] = {
                        proc_inst: hists_config[proc_inst]
                        for proc_inst in sorted(
                            hists_config.keys(),
                            key=list(config_process_map[config_inst].keys()).index,
                        )
                    }

                    # there should be hists to plot
                    if not hists:
                        raise Exception(
                            "no histograms found to plot; possible reasons:\n"
                            "  - requested variable requires columns that were missing during histogramming\n"
                            "  - selected --processes did not match any value on the input histogram process axis",
                        )

                # update histograms using custom hooks
                hists[var_name] = self.invoke_hist_hooks(
                    hists[var_name],
                    hook_kwargs={
                        "category_name": self.branch_data.category,
                        "variable_name": var_name,
                    },
                )

                # merge configs
                if len(self.config_insts) != 1:
                    process_memory = {}
                    merged_hists = {}
                    for _hists in hists[var_name].values():
                        for process_inst, h in _hists.items():
                            if process_inst.id in merged_hists:
                                merged_hists[process_inst.id] += h
                            else:
                                merged_hists[process_inst.id] = h
                                process_memory[process_inst.id] = process_inst
                    hists[var_name] = {process_memory[process_id]: h for process_id, h in merged_hists.items()}
                else:
                    hists[var_name] = hists[var_name][self.config_inst]

                # axis selections and reductions
                _hists = OrderedDict()
                for process_inst in hists[var_name].keys():
                    h = hists[var_name][process_inst]
                    # determine expected shifts from intersection of requested shifts and those known for the process
                    process_shifts = (
                        process_shift_map[process_inst.name]
                        if process_inst.name in process_shift_map
                        else {"nominal"}
                    )
                    expected_shifts = (process_shifts & plot_shift_names) or (process_shifts & {"nominal"})
                    if not expected_shifts:
                        raise Exception(f"no shifts to plot found for process {process_inst.name}")
                    # selections
                    h = h[{
                        "category": [
                            hist.loc(c.name)
                            for c in leaf_category_insts
                            if c.name in h.axes["category"]
                        ],
                        "shift": [
                            hist.loc(s_name)
                            for s_name in expected_shifts
                            if s_name in h.axes["shift"]
                        ],
                    }]
                    # reductions
                    h = h[{"category": sum}]
                    # store
                    _hists[process_inst] = h
                hists[var_name] = _hists

            # copy process instances once so that their auxiliary data fields can be used as a storage for
            # process-specific plot parameters later on in plot scripts without affecting the original instances
            fake_root = od.Process(
                name=f"{hex(id(object()))[2:]}",
                id="+",
                processes=list(set.union(*[set(_hists.keys()) for _hists in hists.values()])),
            ).copy()
            process_map = {proc_inst.name: proc_inst for proc_inst in fake_root.processes.values()}
            fake_root.processes.clear()
            for var_name, _hists in hists.items():
                hists[var_name] = {process_map[proc_inst.name]: h for proc_inst, h in _hists.items()}

            # helper to get variable instances per variable name in tuples (split in case of n-d plots)
            get_var_insts = lambda var_name: list(map(self.config_inst.get_variable, self.variable_tuples[var_name]))

            # temporarily use a merged luminostiy value, assigned to the first config
            config_inst = self.config_insts[0]
            lumi = sum([_config_inst.x.luminosity for _config_inst in self.config_insts])
            with law.util.patch_object(config_inst.x, "luminosity", lumi):
                # call the plot function
                fig, _ = self.call_plot_func(
                    self.plot_function,
                    hists=hists if self.multi_variable else hists[variables[0]],
                    config_inst=config_inst,
                    category_inst=category_inst.copy_shallow(),
                    variable_insts=(
                        {var_name: get_var_insts(var_name) for var_name in variables}
                        if self.multi_variable
                        else get_var_insts(variables[0])
                    ),
                    shift_insts=plot_shifts,
                    **self.get_plot_parameters(),
                )

            # save the plot
            for outp in self.output()["plots"]:
                outp.dump(fig, formatter="mpl")


class PlotVariablesBaseSingleShift(
    ShiftTask,
    PlotVariablesBase,
):
    # use the MergeHistograms task to trigger upstream TaskArrayFunction initialization
    resolution_task_cls = MergeHistograms

    exclude_index = True

    reqs = Requirements(
        PlotVariablesBase.reqs,
        MergeHistograms=MergeHistograms,
    )

    def requires(self):
        reqs = {}

        if self.is_branch() and self.bypass_branch_requirements:
            return reqs

        for config_inst, datasets in zip(self.config_insts, self.datasets):
            reqs[config_inst.name] = {}
            for d in datasets:
                if d not in config_inst.datasets:
                    continue
                reqs[config_inst.name][d] = self.reqs.MergeHistograms.req_different_branching(
                    self,
                    config=config_inst.name,
                    shift=self.global_shift_insts[config_inst].name,
                    dataset=d,
                    branch=-1,
                    _prefer_cli={"variables"},
                )

        return reqs

    def plot_parts(self) -> law.util.InsertableDict:
        parts = super().plot_parts()

        parts["processes"] = f"proc_{self.processes_repr}"
        parts["category"] = f"cat_{self.branch_data.category}"
        if self.multi_variable:
            parts["variables"] = f"vars_{self.variables_repr}"
        else:
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
        return [get_shift_from_configs(self.config_insts, self.shift)]


class PlotVariables1D(
    PlotVariablesBaseSingleShift,
    PlotBase1D,
):
    plot_function = PlotBase.plot_function.copy(
        default="columnflow.plotting.plot_functions_1d.plot_variable_stack",
        add_default_to_description=True,
    )


class PlotVariablesPerConfig1D(
    PlotVariables1D,
    law.WrapperTask,
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
    PlotVariables1D,
    law.WrapperTask,
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
    ShiftSourcesMixin,
    PlotVariablesBase,
):
    legend_title = luigi.Parameter(
        default=law.NO_STR,
        significant=False,
        description="sets the title of the legend; when empty and only one process is present in "
        "the plot, the process_inst label is used; empty default",
    )

    # always ensure the nominal shift is present in shift sources
    enforce_nominal_shift_source = True

    # whether this task creates a single plot combining all shifts or one plot per shift
    combine_shifts = True

    # use the MergeHistograms task to trigger upstream TaskArrayFunction initialization
    resolution_task_cls = MergeHistograms

    exclude_index = True

    # upstream requirements
    reqs = Requirements(
        PlotVariablesBase.reqs,
        MergeHistograms=MergeHistograms,
        MergeShiftedHistograms=MergeShiftedHistograms,
    )

    def create_branch_map(self) -> list[DotDict]:
        keys = ["category"]
        seqs = [self.categories]
        if not self.multi_variable:
            keys.append("variable")
            seqs.append(self.variables)
        if not self.combine_shifts:
            seqs.append(self.shift_sources)
            keys.append("shift_source")
        return [DotDict(zip(keys, vals)) for vals in itertools.product(*seqs)]

    def requires(self):
        reqs = {}

        if self.is_branch() and self.bypass_branch_requirements:
            return reqs

        def hist_req(config_inst, dataset_name, **kwargs):
            # return simple merged histograms for data
            if config_inst.get_dataset(dataset_name).is_data:
                return self.reqs.MergeHistograms.req(self, **kwargs)
            # for mc, return shifted histograms
            return self.reqs.MergeShiftedHistograms.req(self, **kwargs)

        for config_inst, datasets in zip(self.config_insts, self.datasets):
            reqs[config_inst.name] = {}
            for d in datasets:
                if d not in config_inst.datasets:
                    continue
                reqs[config_inst.name][d] = hist_req(
                    config_inst,
                    d,
                    config=config_inst.name,
                    dataset=d,
                    branch=-1,
                    _exclude={"branches"},
                    _prefer_cli={"variables"},
                )

        return reqs

    def plot_parts(self) -> law.util.InsertableDict:
        parts = super().plot_parts()

        parts["processes"] = f"proc_{self.processes_repr}"
        parts["category"] = f"cat_{self.branch_data.category}"
        if self.multi_variable:
            parts["variables"] = f"vars_{self.variables_repr}"
        else:
            parts["variable"] = f"var_{self.branch_data.variable}"

        # shift source or sources
        parts["shift_source"] = (
            f"shifts_{self.shift_sources_repr}"
            if self.combine_shifts
            else f"shift_{self.branch_data.shift_source}"
        )

        # hooks
        if (hooks_repr := self.hist_hooks_repr):
            parts["hook"] = f"hooks_{hooks_repr}"

        return parts

    def output(self):
        return {
            "plots": [self.target(name) for name in self.get_plot_names("plot")],
        }

    def get_plot_shifts(self) -> list[od.Shift]:
        # only to be called by branch tasks
        if self.is_workflow():
            raise Exception("calls to get_plots_shifts are forbidden for workflow tasks")

        # gather sources, and expand to up/down shifts
        sources = self.shift_sources if self.combine_shifts else [self.branch_data.shift_source]
        shifts = list(map(functools.partial(get_shift_from_configs, self.config_insts), expand_shift_sources(sources)))

        return shifts

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
        default="columnflow.plotting.plot_functions_1d.plot_variable_stack",
        add_default_to_description=True,
    )


class PlotShiftedVariablesPerShift1D(
    PlotBase1D,
    PlotVariablesBaseMultiShifts,
):
    # this tasks creates one plot per shift
    combine_shifts = False
    plot_function = PlotBase.plot_function.copy(
        default="columnflow.plotting.plot_functions_1d.plot_shifted_variable",
        add_default_to_description=True,
    )


class PlotShiftedVariablesPerConfig1D(
    PlotShiftedVariables1D,
    law.WrapperTask,
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


class PlotShiftedVariablesPerShiftAndProcess1D(law.WrapperTask):

    # upstream requirements
    reqs = Requirements(
        PlotShiftedVariablesPerShift1D.reqs,
        PlotShiftedVariablesPerShift1D=PlotShiftedVariablesPerShift1D,
    )

    def requires(self):
        return {
            process: self.reqs.PlotShiftedVariablesPerShift1D.req(self, processes=(process,))
            for process in self.processes
        }
