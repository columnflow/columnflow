# coding: utf-8

"""
Tasks to plot different types of histograms.
"""

from __future__ import annotations

import itertools
import functools
import threading
import collections
from abc import abstractmethod

import law
import luigi
import order as od

from columnflow.tasks.framework.base import Requirements, ShiftTask
from columnflow.tasks.framework.mixins import (
    CalibratorClassesMixin, SelectorClassMixin, ReducerClassMixin, ProducerClassesMixin, HistProducerClassMixin,
    DatasetsProcessesMixin, CategoriesMixin, ShiftSourcesMixin, HistHookMixin, MLModelsMixin,
)
from columnflow.tasks.framework.plotting import (
    PlotBase, PlotBase1D, PlotBase2D, PlotBase1DWithErrorBands, ProcessPlotSettingMixin, VariablePlotSettingMixin,
)
from columnflow.tasks.framework.inference import InferenceModelUser
from columnflow.tasks.framework.decorators import view_output_plots
from columnflow.tasks.framework.remote import RemoteWorkflow
from columnflow.tasks.histograms import MergeHistograms, MergeShiftedHistograms
from columnflow.inference import ParameterType
from columnflow.inference.transformation import ShapeTransformer
from columnflow.plotting import check_multi_variable_support, check_multi_category_support
from columnflow.util import DotDict, dev_sandbox, maybe_import, pattern_matcher
from columnflow.hist_util import (
    add_missing_shifts, sum_hists, select_category_bins, insert_axis_values, ensure_bin_exists,
)
from columnflow.config_util import get_shift_from_configs, expand_shift_sources
from columnflow.types import TYPE_CHECKING, TypeAlias, Any

if TYPE_CHECKING:
    hist = maybe_import("hist")


# type aliases for more verbose type hints
CatVarPair: TypeAlias = tuple[str, str]
ProcHists: TypeAlias = dict[od.Process, "hist.Hist"]
ConfigHists: TypeAlias = dict[od.Config, ProcHists]
HistDicts: TypeAlias = dict[CatVarPair, ConfigHists]
MergedHistDicts: TypeAlias = dict[CatVarPair, ProcHists]


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
    """
    Base class for all variable plots.

    Note that instances of this class require attributes ``datatsets`` and ``processes`` which are not defined yet. In
    most cases, this is achieved by simply inheriting from :py:class:`DatasetsProcessesMixin`. However, this is not
    done by default to allow other tasks to define these attributes in different ways (e.g. dynamically, depending on
    other configurations).
    """

    multi_variable = luigi.BoolParameter(
        default=False,
        description="whether a single plot for all variables should be created; this requires that the used plot "
        "function is decorated with '@columnflow.plotting.supports_multi_variable' and accepts a nested dictionary "
        "for the 'hists' argument with all variable and process histograms as an input; default: False",
    )
    multi_category = luigi.BoolParameter(
        default=False,
        description="whether a single plot for all categories should be created; this requires that the used plot "
        "function is decorated with '@columnflow.plotting.supports_multi_category' and accepts a list of categories "
        "for the 'category_inst' argument; cannot be used in conjunction with --multi-variable; default: False",
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

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

        # check multi flags
        self._check_multi_flags()

        # the plot function support for multi-flags
        plot_func = self.get_plot_func(self.plot_function)
        if self.multi_variable and not check_multi_variable_support(plot_func):
            raise Exception(
                f"plot function '{self.plot_function}' does not support multi-variable plotting; please change the "
                "plot function or, if it actually has multi-variable support, decorate it with "
                "@columnflow.plotting.supports_multi_variable",
            )
        if self.multi_category and not check_multi_category_support(plot_func):
            raise Exception(
                f"plot function '{self.plot_function}' does not support multi-category plotting; please change the "
                "plot function or, if it actually has multi-category support, decorate it with "
                "@columnflow.plotting.supports_multi_category",
            )

    def _check_multi_flags(self) -> None:
        if self.multi_variable and self.multi_category:
            raise Exception("cannot use --multi-variable and --multi-category at the same time")

    def create_branch_map(self):
        self._check_multi_flags()
        keys = []
        seqs = []
        if not self.multi_category:
            keys.append("category")
            seqs.append(self.categories)
        if not self.multi_variable:
            keys.append("variable")
            seqs.append(self.variables)
        return [DotDict(zip(keys, vals)) for vals in itertools.product(*seqs)]

    @abstractmethod
    def requires_histograms(self, config_inst: od.Config, dataset_name: str, **kwargs) -> Any:
        ...

    def requires(self):
        reqs = {}

        if self.is_branch() and self.bypass_branch_requirements:
            return reqs

        for config_inst, datasets in zip(self.config_insts, self.datasets):
            reqs[config_inst.name] = {
                d: self.requires_histograms(
                    config_inst=config_inst,
                    dataset_name=d,
                    branch=-1,
                    _prefer_cli={"variables"},
                )
                for d in datasets
                if d in config_inst.datasets
            }

        return reqs

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

    def store_parts(self) -> law.util.InsertableDict:
        parts = super().store_parts()
        if (datasets_repr := self.datasets_repr):
            parts.insert_before("version", "datasets", f"datasets_{datasets_repr}")
        return parts

    def plot_parts(self) -> law.util.InsertableDict:
        parts = super().plot_parts()

        self._check_multi_flags()

        if (processes_repr := self.processes_repr):
            parts["processes"] = f"proc_{processes_repr}"

        if self.multi_category:
            parts["category"] = f"cats_{self.categories_repr}"
        else:
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

    @abstractmethod
    def get_plot_shifts(self):
        ...

    def update_hists_after_hooks(
        self,
        hists: ConfigHists,
        category_name: str,
        variable_name: str,
    ) -> ConfigHists:
        # hook to update histograms right after hist hooks have been applied
        return hists

    def update_hists_before_config_merging(
        self,
        hists: ConfigHists,
        category_name: str,
        variable_name: str,
    ) -> ConfigHists:
        # hook to update histograms right before merging across different config instances
        return hists

    def update_hists_before_plotting(
        self,
        hists: MergedHistDicts,
    ) -> MergedHistDicts:
        # hook to update histograms right before plotting
        return hists

    def update_shifts_before_plotting(
        self,
        shifts: list[od.Shift],
        hists: MergedHistDicts,
    ) -> list[od.Shift]:
        # hook to update shifts right before plotting
        return shifts

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
        process_shift_map = collections.defaultdict(set)

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

        self._check_multi_flags()

        # prepare other config objects
        categories = list(self.categories) if self.multi_category else [self.branch_data.category]
        variables = list(self.variables) if self.multi_variable else [self.branch_data.variable]
        category_variable_combis = list(itertools.product(categories, variables))
        plot_shifts = self.get_plot_shifts()
        plot_shift_names = set(shift_inst.name for shift_inst in plot_shifts) | {"nominal"}

        # get assignment of processes to datasets and shifts
        config_process_map, process_shift_map = self.get_config_process_map()

        # read histograms per variable name, config and process
        hists: HistDicts = {tpl: {} for tpl in category_variable_combis}
        with self.publish_step(f"plotting {','.join(variables)} in {','.join(categories)}"):
            inputs = self.input() or self.workflow_input().merged_hists
            for cat_name, var_name in category_variable_combis:
                hist_key: CatVarPair = (cat_name, var_name)
                for i, (config, dataset_dict) in enumerate(inputs.items()):
                    config_inst = self.config_insts[i]
                    category_inst = config_inst.get_category(cat_name)

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

                            # skip empty histograms right away
                            if h.empty():
                                continue

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
                    hists[hist_key][config_inst]: ProcHists = {
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
                hists[hist_key] = self.invoke_hist_hooks(
                    hists[hist_key],
                    hook_kwargs={"category_name": cat_name, "variable_name": var_name},
                )

                # update histograms after hooks
                hists[hist_key] = self.update_hists_after_hooks(
                    hists=hists[hist_key],
                    category_name=cat_name,
                    variable_name=var_name,
                )

                # axis selections and reductions
                for config_inst, proc_hists in hists[hist_key].items():
                    for process_inst, h in proc_hists.items():
                        # determine expected shifts from intersection of requested shifts and those known for the process
                        process_shifts = (
                            process_shift_map[process_inst.name]
                            if process_inst.name in process_shift_map
                            else {"nominal"}
                        )
                        expected_shifts = (process_shifts & plot_shift_names) or (process_shifts & {"nominal"})
                        if not expected_shifts:
                            raise Exception(f"no shifts to plot found for process {process_inst.name}")
                        # select shifts
                        h = h[{"shift": [hist.loc(s_name) for s_name in expected_shifts if s_name in h.axes["shift"]]}]
                        # select and reduce categories
                        h = select_category_bins(h, category_inst, use_leaves=True, prefer_parents=True, reduce=True)
                        # replace
                        proc_hists[process_inst] = h

                # update histograms before config merging
                hists[hist_key] = self.update_hists_before_config_merging(
                    hists=hists[hist_key],
                    category_name=cat_name,
                    variable_name=var_name,
                )

                # merge configs
                if len(self.config_insts) != 1:
                    process_memory = {}
                    merged_hists = {}
                    for _hists in hists[hist_key].values():
                        for process_inst, h in _hists.items():
                            if process_inst.id in merged_hists:
                                merged_hists[process_inst.id] += h
                            else:
                                merged_hists[process_inst.id] = h
                                process_memory[process_inst.id] = process_inst
                    hists[hist_key] = {process_memory[process_id]: h for process_id, h in merged_hists.items()}
                else:
                    hists[hist_key] = hists[hist_key][self.config_inst]

            # update histograms and shifts before being passed to plot function
            hists = self.update_hists_before_plotting(hists)
            plot_shifts = self.update_shifts_before_plotting(plot_shifts, hists)

            # copy process instances once so that their auxiliary data fields can be used as a storage for
            # process-specific plot parameters later on in plot scripts without affecting the original instances
            fake_root = od.Process(
                name=f"{hex(id(object()))[2:]}",
                id="+",
                processes=list(set.union(*[set(_hists.keys()) for _hists in hists.values()])),
            ).copy()
            process_map = {proc_inst.name: proc_inst for proc_inst in fake_root.processes.values()}
            fake_root.processes.clear()
            for hist_key, _hists in hists.items():
                hists[hist_key] = {process_map[proc_inst.name]: h for proc_inst, h in _hists.items()}

            # helper to get variable instances per variable name in tuples (split in case of n-d plots)
            get_var_insts = lambda var_name: list(map(self.config_inst.get_variable, self.variable_tuples[var_name]))

            # prepare dynamic plot arguments
            if self.multi_category:
                plot_content = {
                    "hists": {cat_name: hists[(cat_name, variables[0])] for cat_name in categories},
                    "category_inst": [self.config_inst.get_category(cat_name).copy_shallow() for cat_name in categories],
                    "variable_insts": get_var_insts(variables[0]),
                }
            elif self.multi_variable:
                plot_content = {
                    "hists": {var_name: hists[(categories[0], var_name)] for var_name in variables},
                    "category_inst": self.config_inst.get_category(categories[0]).copy_shallow(),
                    "variable_insts": {var_name: get_var_insts(var_name) for var_name in variables},
                }
            else:
                plot_content = {
                    "hists": hists[(categories[0], variables[0])],
                    "category_inst": self.config_inst.get_category(categories[0]).copy_shallow(),
                    "variable_insts": get_var_insts(variables[0]),
                }

            # temporarily use a merged luminostiy value, assigned to the first config
            config_inst = self.config_insts[0]
            if not config_inst.has_aux("lumi_plot_lock"):
                config_inst.x.lumi_plot_lock = threading.RLock()
            lumi = sum([_config_inst.x.luminosity for _config_inst in self.config_insts])

            with law.util.patch_object(config_inst.x, "luminosity", lumi, lock=config_inst.x.lumi_plot_lock):
                # call the plot function
                fig, _ = self.call_plot_func(
                    self.plot_function,
                    **plot_content,
                    config_inst=config_inst,
                    shift_insts=plot_shifts,
                    **self.get_plot_parameters(),
                )

            # save the plot
            for outp in self.output()["plots"]:
                outp.dump(fig, formatter="mpl")


class PlotVariablesBaseSingleShift(
    ShiftTask,
    DatasetsProcessesMixin,
    PlotVariablesBase,
):
    # use the MergeHistograms task to trigger upstream TaskArrayFunction initialization
    resolution_task_cls = MergeHistograms

    exclude_index = True

    reqs = Requirements(
        PlotVariablesBase.reqs,
        MergeHistograms=MergeHistograms,
    )

    def requires_histograms(self, config_inst: od.Config, dataset_name: str, **kwargs) -> Any:
        kwargs |= {
            "config": config_inst.name,
            "dataset": dataset_name,
            "shift": self.global_shift_insts[config_inst].name,
        }

        return self.reqs.MergeHistograms.req_different_branching(self, **kwargs)

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
    DatasetsProcessesMixin,
    PlotVariablesBase,
):
    # always ensure the nominal shift is present in shift sources
    enforce_nominal_shift_source = True

    # whether this task creates a single plot combining all shifts or one plot per shift
    combine_shifts = True

    # use the MergeHistograms task to trigger upstream TaskArrayFunction initialization
    resolution_task_cls = MergeHistograms

    # upstream requirements
    reqs = Requirements(
        PlotVariablesBase.reqs,
        MergeHistograms=MergeHistograms,
        MergeShiftedHistograms=MergeShiftedHistograms,
    )

    exclude_index = True

    def requires_histograms(self, config_inst: od.Config, dataset_name: str, **kwargs) -> Any:
        kwargs |= {"config": config_inst.name, "dataset": dataset_name}

        # return simple merged histograms for data
        if config_inst.get_dataset(dataset_name).is_data:
            return self.reqs.MergeHistograms.req_different_branching(self, **kwargs)

        # for mc, return shifted histograms
        return self.reqs.MergeShiftedHistograms.req_different_branching(self, **kwargs)

    def create_branch_map(self) -> list[DotDict]:
        branch_data = super().create_branch_map()

        if not self.combine_shifts:
            branch_data = [
                {**d, "shift_source": source}
                for d in branch_data
                for source in self.shift_sources
                if source != "nominal"
            ]

        return branch_data

    def store_parts(self) -> law.util.InsertableDict:
        parts = super().store_parts()
        if "shift_sources" in parts:
            parts.insert_before("datasets", "shift_sources", parts.pop("shift_sources"))
        return parts

    def plot_parts(self) -> law.util.InsertableDict:
        parts = super().plot_parts()

        # shift source or sources
        shift_source_repr = (
            f"shifts_{self.shift_sources_repr}"
            if self.combine_shifts
            else f"shift_{self.branch_data.shift_source}"
        )
        parts.insert_before("hook", "shift_source", shift_source_repr)

        return parts

    def get_plot_shifts(self) -> list[od.Shift]:
        # only to be called by branch tasks
        if self.is_workflow():
            raise Exception("calls to get_plots_shifts are forbidden for workflow tasks")

        # gather sources, and expand to up/down shifts
        sources = self.shift_sources if self.combine_shifts else [self.branch_data.shift_source]
        shifts = list(map(functools.partial(get_shift_from_configs, self.config_insts), expand_shift_sources(sources)))

        return shifts


class PlotShiftedVariables1D(
    PlotBase1DWithErrorBands,
    PlotVariablesBaseMultiShifts,
):
    plot_function = PlotBase.plot_function.copy(
        default="columnflow.plotting.plot_functions_1d.plot_variable_stack",
        add_default_to_description=True,
    )


class PlotShiftedVariablesPerShift1D(
    PlotBase1DWithErrorBands,
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


class PlotShiftedVariablesPerShiftAndProcess1D(
    law.WrapperTask,
):

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


class PlotVariablesBaseShiftsFromModel(
    PlotVariablesBase,
    InferenceModelUser,
):
    variables = PlotVariablesBase.variables.copy(
        default=(),
        add_default_to_description=True,
    )
    categories = law.CSVParameter(
        default=(),
        description="comma-separated category names or patterns to select from the inference model; to request a "
        "category _not_ used by the model, a category string should have the format 'ANALYSIS_CATEGORY:MODEL_CATEGORY' "
        "which will serve a mapping between them; if empty, all categories defined by the model will be used; default: "
        "empty",
        brace_expand=True,
    )
    skip_processes = law.CSVParameter(
        default=(),
        brace_expand=True,
        description="names or patterns of processes to skip, based on all processes extract from the inference model; "
        "default: empty",
    )
    nuisances = law.CSVParameter(
        default=(),
        brace_expand=True,
        description="names or patterns of nuisance parameters to use from the inference model; if empty, all nuisances "
        "defined by the model will be used; default: empty",
    )
    skip_nuisances = law.CSVParameter(
        default=(),
        brace_expand=True,
        description="names or patterns of nuisance parameters to skip after evaluating --nuisances; default: empty",
    )
    split_nuisances = luigi.BoolParameter(
        default=False,
        description="whether to split the nuisance parameters and create plots for each nuisances; default: False",
    )
    # fix some upstream parameters
    multi_variable = False
    multi_category = False

    # arguments to be passed to the internally used ShapeTransformer
    shape_transformer_kwargs: dict[str, Any] = {}

    # class-level settings from upstream tasks
    allow_empty_categories = True
    allow_empty_variables = True
    resolution_task_cls = MergeShiftedHistograms

    transfer_params_to_inst = {"category_map"}

    @classmethod
    def resolve_param_values_post_init(cls, params: dict[str, Any]) -> dict[str, Any]:
        # skip category resolution
        categories_orig = params.get("categories")
        params = super().resolve_param_values_post_init(params)
        params["categories"] = categories_orig

        # model specific defaults and validations
        if (config_insts := params.get("config_insts")) and (inference_model_inst := params.get("inference_model_inst")):
            combined_config_data = params[cls._combined_config_data_attr]  # provided by InferenceModelUser

            # expand / default variables
            if (variables := params.get("variables")):
                variables = cls.find_config_objects(
                    names=variables,
                    container=config_insts,
                    object_cls=od.Variable,
                    groups_str="variable_groups",
                    multi_strategy="intersection",
                )
            else:
                variables = sorted(law.util.make_unique(law.util.flatten(
                    config_data["variables"] for config_data in combined_config_data.values()
                )))
            params["variables"] = tuple(variables)

            # before evaluating categories, build a map "model category -> config categories"
            full_catogory_map_rev = {
                cat_obj.name: {
                    config_data.category
                    for config_name, config_data in cat_obj.config_data.items()
                    if config_name in params["configs"]
                }
                for cat_obj in inference_model_inst.categories
            }
            full_category_map = {
                cat_name: model_cat_name
                for model_cat_name, cat_names in full_catogory_map_rev.items()
                for cat_name in cat_names
            }

            # expand / default categories
            category_map = {}
            if (categories := params.get("categories")):
                for cat_expr in categories:
                    cat_pattern, model_cat_pattern = cat_expr.split(":", 1) if ":" in cat_expr else (cat_expr, None)

                    # when model cat is a pattern, it should expand to exactly one actual name
                    model_cat_name = None
                    if model_cat_pattern:
                        matcher = pattern_matcher(model_cat_pattern)
                        for _model_cat_name in full_catogory_map_rev.keys():
                            if matcher(_model_cat_name):
                                if model_cat_name is not None:
                                    raise Exception(
                                        f"model category pattern '{model_cat_pattern}' matches multiple model "
                                        f"categories ('{model_cat_name}' and '{_model_cat_name}'); please specify a "
                                        "more specific pattern or use an exact name",
                                    )
                                model_cat_name = _model_cat_name
                        if model_cat_name is None:
                            raise Exception(
                                f"model category pattern '{model_cat_pattern}' does not match any model category",
                            )

                    # expand category pattern
                    cat_names = cls.find_config_objects(
                        names=cat_pattern,
                        container=config_insts,
                        object_cls=od.Category,
                        groups_str="category_groups",
                        multi_strategy="intersection",
                    )

                    # fill the map
                    for cat_name in cat_names:
                        if model_cat_name is None and cat_name not in full_category_map:
                            raise Exception(
                                f"category '{cat_name}' is not defined in the inference model; using the syntax "
                                "'CATEGORY:MODEL_CATEGORY', as MODEL_CATEGORY, specify any of "
                                f"{','.join(full_catogory_map_rev)}",
                            )
                        category_map[cat_name] = model_cat_name or full_category_map[cat_name]
            else:
                category_map.update(full_category_map)
            params["categories"] = tuple(sorted(category_map.keys()))
            params["category_map"] = {cat_name: category_map[cat_name] for cat_name in params["categories"]}

        return params

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

        # constraint: since the plotting requires actual process instances with labels, etc, it is currently not
        # supported for the inference model to use the same config process multiple times within the same config object
        # to compose different model processes, e.g.
        #   proc obj a
        #     -> config x
        #       -> process tt
        #   proc obj b
        #     -> config x
        #       -> process tt
        for category in self.categories:
            cat_obj = self.inference_model_inst.get_category(self.category_map[category])
            config_processes = collections.defaultdict(lambda: collections.defaultdict(set))
            for proc_obj in cat_obj.processes:
                for config_name, proc_data in proc_obj.config_data.items():
                    config_processes[config_name][proc_data.process].add(proc_obj.name)
            for config_name, proc_map in config_processes.items():
                for proc_name, proc_obj_names in proc_map.items():
                    if len(proc_obj_names) > 1:
                        raise Exception(
                            f"in category '{category}' (model category '{self.cartegory_map[category]}') for config "
                            f"'{config_name}', process '{proc_name}' is used by multiple model processes "
                            f"({','.join(proc_obj_names)}); this is currently not supported for plotting",
                        )

    @property
    def processes_repr(self) -> str:
        return DatasetsProcessesMixin._processes_repr(self.processes)

    @property
    def datasets_repr(self) -> str:
        return DatasetsProcessesMixin._datasets_repr(self.datasets)

    @classmethod
    def _nuisances_repr(cls, nuisances: set[str]) -> str:
        return cls._multi_sequence_repr(nuisances, sort=True)

    @property
    def nuisances_repr(self) -> str:
        return (
            self._nuisances_repr(self.nuisance_map[self.branch_data.category])
            if self.is_branch()
            else self._nuisances_repr(set.union(*self.nuisance_map.values()))
        )

    @law.workflow_property(cache=True)
    def processes(self) -> tuple[tuple[str]]:
        config_data = self.combined_config_data
        exclude = pattern_matcher(self.skip_processes) if self.skip_processes else (lambda proc_name: False)
        include = lambda proc_name: not exclude(proc_name)
        return tuple(
            tuple(filter(include, set.union(*(mc_data.proc_names for mc_data in config_data.mc_datasets.values()))))
            for config_inst, config_data in config_data.items()
        )

    @law.workflow_property(cache=True)
    def nuisance_map(self) -> dict[str, set[str]]:
        # collect list of all nuisance parameters that need to be accounted for, mapped to categories
        # (merely for filtering and constructing requirements)
        nuisance_map = {cat_name: set() for cat_name in self.categories}

        # collect all nuisances from the model
        category_map_rev = dict(zip(self.category_map.values(), self.category_map.keys()))
        supported_parameter_types = {ParameterType.rate_gauss, ParameterType.rate_uniform, ParameterType.shape}
        for cat_obj_name, _, param_obj in self.inference_model_inst.iter_parameters(category=self.category_map.values()):
            # consider only specific parameter types
            if param_obj.type in supported_parameter_types:
                nuisance_map[category_map_rev[cat_obj_name]].add(param_obj.name)
            else:
                self.logger.warning(
                    f"parameter '{param_obj.name}' has unsupported type '{param_obj.type}', skipping it for nuisance "
                    f"parameter selection; supported types are {supported_parameter_types}",
                )

        # per category, filter with inclusion and exclusion lists
        if self.nuisances or self.skip_nuisances:
            for cat_name, nuisances in nuisance_map.items():
                if self.nuisances:
                    include = pattern_matcher(self.nuisances)
                    nuisances = set(filter(include, nuisances))
                if self.skip_nuisances:
                    exclude = pattern_matcher(self.skip_nuisances)
                    not_exclude = lambda n: not exclude(n)
                    nuisances = set(filter(not_exclude, nuisances))
                nuisance_map[cat_name] = nuisances

        # raise on empty nuisance sets
        for cat_name, nuisances in nuisance_map.items():
            if not nuisances:
                raise Exception(f"no nuisance parameters found for category '{cat_name}'")

        return nuisance_map

    @law.workflow_property(cache=True)
    def shift_sources(self) -> dict[od.Config, dict[str, str]]:
        # determine shift sources to request per config, mapping from nuisance name to source name
        shift_sources = {config_inst: {} for config_inst in self.config_insts}

        # get all shift sources
        config_map = {config_inst.name: config_inst for config_inst in self.config_insts}
        for cat_name, nuisances in self.nuisance_map.items():
            for _, _, param_obj in self.inference_model_inst.iter_parameters(category=self.category_map[cat_name]):
                # skip if parameter is not in the nuisances for that category
                if param_obj.name not in self.nuisance_map[cat_name]:
                    continue
                # skip if no shift is required to model the parameter effect
                from_shift = (
                    (param_obj.type.is_shape and not param_obj.transformations.any_from_rate) or
                    (param_obj.type.is_rate and param_obj.transformations.any_from_shape)
                )
                if not from_shift:
                    continue
                # add to shift sources
                for config_name, config_data in param_obj.config_data.items():
                    shift_sources[config_map[config_name]][param_obj.name] = config_data.shift_source

        return shift_sources

    @law.workflow_property(cache=True)
    def datasets(self) -> list[list[str]]:
        # define which datasets are required per config, potentially filtered by selected processes to show
        # (not a dict, same order as config_insts as per structural design of parent classes)
        all_datasets = []
        for (config_inst, config_data), processes in zip(self.combined_config_data.items(), self.processes):
            datasets = []

            # add mc datasets
            proc_insts = set(map(config_inst.get_process, processes))
            procs_match = lambda p1, p2: p1 == p2 or p1.has_process(p2) or p2.has_process(p1)
            for dataset_name in config_data.mc_datasets:
                dataset_proc_inst = config_inst.get_dataset(dataset_name).processes.get_first()
                if any(procs_match(dataset_proc_inst, proc_inst) for proc_inst in proc_insts):
                    datasets.append(dataset_name)

            # add data datasets
            datasets.extend(list(config_data.data_datasets))
            all_datasets.append(datasets)

        return all_datasets

    def create_branch_map(self) -> list[DotDict]:
        branch_data = super().create_branch_map()

        if self.split_nuisances:
            branch_data = [
                DotDict({**d, "nuisance": nuisance})
                for d in branch_data
                for nuisance in sorted(self.nuisance_map[d["category"]])
            ]

        return branch_data

    def req_workflow(self, **kwargs) -> PlotVariablesBaseShiftsFromModel:
        kwargs["categories"] = tuple(":".join(pair) for pair in self.category_map.items())
        return super().req_workflow(**kwargs)

    def req_branch(self, branch: int, **kwargs) -> PlotVariablesBaseShiftsFromModel:
        kwargs["categories"] = tuple(":".join(pair) for pair in self.category_map.items())
        return super().req_branch(branch, **kwargs)

    def requires_histograms(self, config_inst: od.Config, dataset_name: str, **kwargs) -> Any:
        dataset_is_mc = config_inst.get_dataset(dataset_name).is_mc
        kwargs |= {
            "config": config_inst.name,
            "dataset": dataset_name,
            "shift_sources": ("nominal", *(self.shift_sources[config_inst].values() if dataset_is_mc else ())),
        }
        return self.reqs.MergeShiftedHistograms.req_different_branching(self, **kwargs)

    def plot_parts(self) -> law.util.InsertableDict:
        parts = super().plot_parts()

        # nuisance(s)
        nuisance_repr = ""
        if self.split_nuisances:
            nuisance_repr = f"nparam_{self.branch_data.nuisance}"
        elif (nuisances_repr := self.nuisances_repr):
            nuisance_repr = f"nparams_{nuisances_repr}"
        if nuisance_repr:
            parts.insert_before("hook", "nuisance", nuisance_repr)

        return parts

    def get_plot_parameters(self) -> dict[str, Any]:
        params = super().get_plot_parameters()

        # add nuisance parameter name if split_nuisances is enabled
        if self.split_nuisances and self.is_branch():
            params["syst_error_label"] = f"{self.branch_data.nuisance} unc."

        return params

    def get_plot_shifts(self) -> list[od.Shift]:
        # only to be called by branch tasks
        if self.is_workflow():
            raise Exception("calls to get_plots_shifts are forbidden for workflow tasks")

        # gather all actual shifts and expand to up/down shifts
        shifts = [self.config_insts[0].shifts.n.nominal]
        seen = set()
        for config_inst, source_map in self.shift_sources.items():
            if not self.split_nuisances:
                _shift_names = expand_shift_sources(source_map.values())
            elif self.branch_data.nuisance in source_map:
                _shift_names = expand_shift_sources(source_map[self.branch_data.nuisance])
            else:
                continue
            for _shift in map(functools.partial(get_shift_from_configs, [config_inst]), _shift_names):
                if _shift.name not in seen:
                    shifts.append(_shift)
                    seen.add(_shift.name)

        return shifts

    def update_hists_before_config_merging(
        self,
        hists: ConfigHists,
        category_name: str,
        variable_name: str,
    ) -> ConfigHists:
        import hist

        # determine which nuisances to consider for this category
        nuisances = {self.branch_data.nuisance} if self.split_nuisances else self.nuisance_map[category_name]

        # build mapping "proc name -> config name -> arameter objects"
        param_map = collections.defaultdict(dict)
        for proc_objs in self.inference_model_inst.get_processes(category=self.category_map[category_name]).values():
            for proc_obj in proc_objs:
                for config_name, config_data in proc_obj.config_data.items():
                    param_map[config_data.process][config_name] = [
                        param_obj
                        for param_obj in proc_obj.parameters
                        if param_obj.name in nuisances
                    ]

        # create a shape transformer helper, for now with defaults
        transformer = ShapeTransformer(**(self.shape_transformer_kwargs or {}))

        for config_inst, proc_hists in hists.items():
            for proc_inst, h_all in proc_hists.items():
                # pick the nominal histogram only, but keep the shift axis which is extended below
                h = h_all[{"shift": [hist.loc("nominal")]}]

                # check which nuisances should be added through parameter objects as shifts
                param_objs = param_map.get(proc_inst.name, {})[config_inst.name]
                if param_objs:
                    # ensure that the shift axis contains all parameters
                    h = ensure_bin_exists(h, "shift", expand_shift_sources(param_obj.name for param_obj in param_objs))

                    # prepare nominal and varied histograms for the transformer
                    h_nom = h[{"shift": hist.loc("nominal")}]
                    h_varied = {}
                    for param_obj in param_objs:
                        if param_obj.config_data and config_inst.name in param_obj.config_data:
                            source = param_obj.config_data[config_inst.name].shift_source
                            down_up_hists = []
                            for shift in expand_shift_sources(source, down_first=True):
                                if shift not in h_all.axes["shift"]:
                                    raise Exception(
                                        f"histogram for process '{proc_inst.name}' in config '{config_inst.name}' does "
                                        f"not contain shift '{shift}' as required for parameter '{param_obj.name}'",
                                    )
                                down_up_hists.append(h_all[{"shift": hist.loc(shift)}])
                            h_varied[param_obj.name] = tuple(down_up_hists)
                        else:
                            h_varied[param_obj.name] = None

                    # perform transformations
                    output = transformer.apply_parameters(
                        param_objs=param_objs,
                        h_nom=h_nom,
                        h_varied=h_varied,
                        output_type=ShapeTransformer.OutputType.convert_to_shapes,
                    )

                    # when the nominal hist was updated, inject it's values back into h
                    # (can happen in certain circumstances depending on some transformations)
                    if output.nominal_changed:
                        insert_axis_values(h, "shift", "nominal", output.h_nom)

                    # insert varied shapes into full hist
                    for param_name, (h_down, h_up) in output.h_varied.items():
                        insert_axis_values(h, "shift", f"{param_name}_{od.Shift.UP}", h_up)
                        insert_axis_values(h, "shift", f"{param_name}_{od.Shift.DOWN}", h_down)

                # store the updated histogram
                proc_hists[proc_inst] = h

        return hists

    def update_shifts_before_plotting(self, shifts: list[od.Shift], hists: MergedHistDicts) -> list[od.Shift]:
        # hists has only one entry, so unpack values
        proc_hists = next(iter(hists.values()))

        # collect all shifts from histograms (they were introduced in update_hists_before_config_merging and resemble
        # nuisance parameters rather than actual shifts that exist in the config)
        shift_names = set.union(*(set(h.axes["shift"]) for h in proc_hists.values()))

        # create fake shift objects with nominal id 0, even up and odd down ids
        shift_names = ["nominal"] + sorted(shift_names - {"nominal"})[::-1]
        shifts = [od.Shift(name=shift_name, id=i) for i, shift_name in enumerate(shift_names)]

        return shifts


class PlotShiftedVariablesFromModel1D(
    PlotBase1DWithErrorBands,
    PlotVariablesBaseShiftsFromModel,
):
    plot_function = PlotBase.plot_function.copy(
        default="columnflow.plotting.plot_functions_1d.plot_variable_stack",
        add_default_to_description=True,
    )
