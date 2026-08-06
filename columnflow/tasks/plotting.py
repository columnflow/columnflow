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
from columnflow.tasks.framework.decorators import view_output_plots
from columnflow.tasks.framework.remote import RemoteWorkflow
from columnflow.tasks.histograms import MergeHistograms, MergeShiftedHistograms
from columnflow.plotting import check_multi_variable_support, check_multi_category_support,check_multi_version_support
from columnflow.util import DotDict, dev_sandbox, dict_add_strict
from columnflow.hist_util import add_missing_shifts, sum_hists, select_category_bins
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
    # NEW
    multi_version = luigi.BoolParameter(
        default=False,
        description="whether a single plot combining histograms produced under multiple upstream task "
        "'--version' values should be created; requires '--hist-versions' to be set and the plot function to be "
        "decorated with '@columnflow.plotting.supports_multi_version'; can be combined with --multi-category or "
        "--multi-variable (but not both); default: False",
    )
    # NEW
    hist_versions = law.CSVParameter(
        default=(),
        description="comma-separated list of upstream task versions (e.g. of MergeHistograms) to compare when "
          "--multi-version is set; ignored otherwise",
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
        # resolve default hist_versions to this task's own version when not explicitly given
        if self.multi_version and not check_multi_version_support(plot_func):
            raise Exception(
                f"plot function '{self.plot_function}' does not support multi-version plotting; please change the "
                "plot function or, if it actually has multi-version support, decorate it with "
                "@columnflow.plotting.supports_multi_version",
            )


    def _check_multi_flags(self) -> None:
        if self.multi_variable and self.multi_category:
            raise Exception("cannot use --multi-variable and --multi-category at the same time")
        if self.multi_version and not self.hist_versions:
            raise Exception("--multi-version requires --hist-versions to be set")

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
        hist_versions = list(self.hist_versions) if self.multi_version else [self.version]
        for config_inst, datasets in zip(self.config_insts, self.datasets):
            reqs[config_inst.name] = {
                hv: {  # NEW: nest by hist_version, unconditionally
                    d: self.requires_histograms(
                        config_inst=config_inst,
                        dataset_name=d,
                        branch=-1,
                        version=hv,  # NEW: override the upstream requirement's version
                        _prefer_cli={"variables"},
                    )
                    for d in datasets
                    if d in config_inst.datasets
                }
                for hv in hist_versions  # NEW
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
        if self.multi_version:
            parts["hist_versions"] = f"hvers_{'_'.join(self.hist_versions)}"

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
        hist_versions = list(self.hist_versions) if self.multi_version else [self.version]

        config_process_map = {config_inst: {} for config_inst in self.config_insts}
        process_shift_map = collections.defaultdict(set)

        for i, config_inst in enumerate(self.config_insts):
            process_insts = [config_inst.get_process(p) for p in self.processes[i]]
            dataset_insts = [config_inst.get_dataset(d) for d in self.datasets[i]]

            requested_shifts_per_dataset: dict[od.Dataset, list[str]] = {}
            for dataset_inst in dataset_insts:
                # NEW: gather shifts across all compared hist_versions; shift structure is assumed
                # consistent across versions, so use the first one
                shifts_per_version = []
                for hv in hist_versions:
                    _req = reqs[config_inst.name][hv][dataset_inst.name]
                    if isinstance(_req, ShiftTask) and _req.shift:
                        # when a shift is found, use it
                        shifts_per_version.append([_req.shift])
                    elif isinstance(_req, ShiftSourcesMixin):
                        # when no shift is found, check for shift sources and expand to up/down variations
                        shifts_per_version.append(expand_shift_sources(_req.shift_sources))
                    else:
                        raise Exception(
                            f"no shift or shift source found in requirements for dataset {dataset_inst.name} "
                            f"of config {config_inst.name}",
                        )
                requested_shifts_per_dataset[dataset_inst] = shifts_per_version[0]

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
        hist_versions = list(self.hist_versions) if self.multi_version else [self.version]
        combis = list(itertools.product(categories, variables, hist_versions))  # NEW: triple product
        plot_shifts = self.get_plot_shifts()
        plot_shift_names = set(shift_inst.name for shift_inst in plot_shifts) | {"nominal"}

        # get assignment of processes to datasets and shifts
        config_process_map, process_shift_map = self.get_config_process_map()

        # read histograms per variable name, config and process
        hists: dict[tuple[str, str, str], dict[od.Config, dict[od.Process, hist.Hist]]] = {  # NEW: 3-tuple key
            tpl: {}
            for tpl in combis
        }
        with self.publish_step(f"plotting {','.join(variables)} in {','.join(categories)}"):
            inputs = self.input() or self.workflow_input().merged_hists

            for cat_name, var_name, hv in combis:  # NEW: unpack hv
                hist_key = (cat_name, var_name, hv)
                for i, (config, hv_dict) in enumerate(inputs.items()):  # NEW: renamed for clarity
                    config_inst = self.config_insts[i]
                    category_inst = config_inst.get_category(cat_name)
                    dataset_dict = hv_dict[hv]  # NEW: index through hist_version first

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
                        " - requested variable requires columns that were missing during histogramming\n"
                        " - selected --processes did not match any value on the input histogram process axis",
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

                # axis selections and reductions
                _hists = OrderedDict()
                for process_inst in hists[hist_key].keys():
                    h = hists[hist_key][process_inst]

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

                    # store
                    _hists[process_inst] = h
                hists[hist_key] = _hists

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
            # NEW: multi_version branch, combinable with multi_category / multi_variable
            if self.multi_version:
                if self.multi_category:
                    plot_content = {
                        "hists": {
                            hv: {cat_name: hists[(cat_name, variables[0], hv)] for cat_name in categories}
                            for hv in hist_versions
                        },
                        "category_inst": [
                            self.config_inst.get_category(cat_name).copy_shallow() for cat_name in categories
                        ],
                        "variable_insts": get_var_insts(variables[0]),
                    }
                elif self.multi_variable:
                    plot_content = {
                        "hists": {
                            hv: {var_name: hists[(categories[0], var_name, hv)] for var_name in variables}
                            for hv in hist_versions
                        },
                        "category_inst": self.config_inst.get_category(categories[0]).copy_shallow(),
                        "variable_insts": {var_name: get_var_insts(var_name) for var_name in variables},
                    }
                else:
                    plot_content = {
                        "hists": {hv: hists[(categories[0], variables[0], hv)] for hv in hist_versions},
                        "category_inst": self.config_inst.get_category(categories[0]).copy_shallow(),
                        "variable_insts": get_var_insts(variables[0]),
                    }
            elif self.multi_category:
                plot_content = {
                    "hists": {
                        cat_name: hists[(cat_name, variables[0], hist_versions[0])] for cat_name in categories
                    },
                    "category_inst": [
                        self.config_inst.get_category(cat_name).copy_shallow() for cat_name in categories
                    ],
                    "variable_insts": get_var_insts(variables[0]),
                }
            elif self.multi_variable:
                plot_content = {
                    "hists": {
                        var_name: hists[(categories[0], var_name, hist_versions[0])] for var_name in variables
                    },
                    "category_inst": self.config_inst.get_category(categories[0]).copy_shallow(),
                    "variable_insts": {var_name: get_var_insts(var_name) for var_name in variables},
                }
            else:
                plot_content = {
                    "hists": hists[(categories[0], variables[0], hist_versions[0])],
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
                DotDict.wrap({**d, "shift_source": source})
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
