# coding: utf-8

"""
CMS specific plotting tasks.
"""

from __future__ import annotations

import os
import threading

import luigi
import law
import order as od

from columnflow.tasks.framework.mixins import DatasetsProcessesMixin, HistHookMixin
from columnflow.tasks.framework.plotting import PlotBase, ProcessPlotSettingMixin, PlotBase1DWithErrorBands
from columnflow.tasks.framework.inference import InferenceModelUser
from columnflow.tasks.framework.decorators import view_output_plots
from columnflow.tasks.plotting import ProcHists, PlotVariablesBaseShiftsFromModel
from columnflow.hist_util import sum_hists, create_hist_from_variables
from columnflow.cms_util import HarvesterShapes
from columnflow.util import maybe_import, expand_path, pattern_matcher, DotDict
from columnflow.types import TYPE_CHECKING, Any

np = maybe_import("numpy")
if TYPE_CHECKING:
    uproot = maybe_import("uproot")


class _PlotPostfitFromModel(
    PlotBase1DWithErrorBands,
    ProcessPlotSettingMixin,
    InferenceModelUser,
    HistHookMixin,
):
    """
    Base classes for :py:class:`PlotPostfitFromModel`.
    """


class PlotPostfitFromModel1D(_PlotPostfitFromModel):
    """
    Task to create postfit plots from previously created shapes using combine/harvester. For more info, see
    https://cms-analysis.github.io/CombineHarvester/post-fit-shapes-ws.html
    """

    task_namespace = "cf.cms"

    shapes_file = luigi.Parameter(
        description="path to the shapes file (create by CMS Combine Harvester, see "
        "https://cms-analysis.github.io/CombineHarvester/post-fit-shapes-ws.html)",
    )
    shapes_name = luigi.Parameter(
        default=law.NO_STR,
        description="custom name for the --shapes-file that will be written into plot file names; when empty, the "
        "first 8 characters of the sha1 hash of the file contents will be used; default: empty",
    )
    prefit = luigi.BoolParameter(
        default=False,
        description="if True, prefit plots will be created rather than postfit plots (provided they exist in "
        "--shapes-file); default: False",
    )
    uncertainty = luigi.ChoiceParameter(
        default="bkg",
        choices=["bkg", "all", "none"],
        description="which uncertainty to show in the ratio plot; choices: bkg,all,none; default: bkg",
    )
    plot_function = PlotBase.plot_function.copy(
        default="columnflow.plotting.plot_functions_1d.plot_variable_stack",
        add_default_to_description=True,
    )
    categories = PlotVariablesBaseShiftsFromModel.categories
    merge_processes = PlotVariablesBaseShiftsFromModel.merge_processes
    skip_processes = PlotVariablesBaseShiftsFromModel.skip_processes

    # disable some upstream features
    resolution_task_cls = law.no_value
    hide_stat_errors = None
    merge_stat_errors = None
    shape_norm = None

    transfer_params_to_inst = {"category_map"}

    @classmethod
    def resolve_param_values(cls, params: dict[str, Any]) -> dict[str, Any]:
        params = super().resolve_param_values(params)

        # expand shapes_file path
        params["shapes_file"] = os.path.abspath(expand_path(params["shapes_file"]))

        # default shapes_name
        if params["shapes_name"] in {"", None, law.NO_STR}:
            params["shapes_name"] = law.util.compute_sha1_hash(params["shapes_file"])[:8]

        # expand categories
        existing_categories = HarvesterShapes.read_categories(params["shapes_file"], prefit=bool(params["prefit"]))
        category_map = PlotVariablesBaseShiftsFromModel.resolve_model_category_map(
            inference_model_inst=params["inference_model_inst"],
            config_insts=params["config_insts"],
            categories=params["categories"],
        )
        category_map = {
            cat_name: cat_obj_name
            for cat_name, cat_obj_name in category_map.items()
            if cat_obj_name in existing_categories
        }
        params["categories"] = tuple(sorted(category_map.keys()))
        params["category_map"] = {cat_name: category_map[cat_name] for cat_name in params["categories"]}

        # expand merge processes
        if (merge_processes := params.get("merge_processes")):
            merge_processes = cls.find_config_objects(
                names=merge_processes,
                container=params["config_insts"],
                object_cls=od.Process,
                groups_str="process_groups",
                multi_strategy="intersection",
            )
            params["merge_processes"] = tuple(merge_processes)

        return params

    @law.workflow_property(cache=True)
    def shape_data(self) -> dict[str, HarvesterShapes]:
        return HarvesterShapes.from_file(self.shapes_file, prefit=self.prefit)

    def create_branch_map(self):
        def get_first_config_variable(cat_name):
            cat_obj = self.inference_model_inst.get_category(self.category_map[cat_name])
            return self.config_insts[0].get_variable(cat_obj.config_data[self.config_insts[0].name].variable)

        return [
            DotDict(
                category=cat_name,
                variable=get_first_config_variable(cat_name).name,
            )
            for cat_name in self.categories
        ]

    @property
    def processes_repr(self) -> str:
        return DatasetsProcessesMixin._processes_repr(self.merge_processes) if self.merge_processes else ""

    def store_parts(self) -> law.util.InsertableDict:
        parts = super().store_parts()
        parts.insert_before("version", "shapes_name", f"shapes_{self.shapes_name}")
        return parts

    def plot_parts(self) -> law.util.InsertableDict:
        parts = super().plot_parts()

        parts["category"] = f"cat_{self.branch_data.category}"
        parts["variable"] = f"var_{self.branch_data.variable}"
        parts["unc"] = f"unc_{self.uncertainty}"

        if (processes_repr := self.processes_repr):
            parts["processes"] = f"proc_{processes_repr}"

        hooks_repr = self.hist_hooks_repr
        if hooks_repr:
            parts["hook"] = f"hooks_{hooks_repr}"

        return parts

    def output(self):
        return {
            "plots": [self.target(name) for name in self.get_plot_names("plot")],
        }

    def get_plot_parameters(self, *args, **kwargs) -> dict[str, Any]:
        params = super().get_plot_parameters(*args, **kwargs)
        params.pop("hide_stat_errors", None)
        params.pop("merge_stat_errors", None)
        params.pop("shape_norm", None)
        return params

    @law.decorator.log
    @view_output_plots
    def run(self):
        import hist

        # prepare config objects, relative to the first config instance
        config_inst = self.config_insts[0]
        cat_obj = self.inference_model_inst.get_category(self.category_map[self.branch_data.category])
        category_inst = config_inst.get_category(self.branch_data.category)
        variable_inst = config_inst.get_variable(self.branch_data.variable).copy_shallow()
        shapes = self.shape_data[self.category_map[self.branch_data.category]]
        exclude_matcher = pattern_matcher(self.skip_processes) if self.skip_processes else (lambda proc_name: False)

        # helper to sanitize histograms coming out of harvester shapes file
        def sanitize_hist(h: hist.Hist) -> hist.Hist:
            # check if bin edges align
            edges_mismatch = (
                h.axes[0].size != variable_inst.n_bins or
                not np.allclose(h.axes[0].edges, variable_inst.bin_edges)
            )
            if edges_mismatch:
                raise ValueError(
                    f"edges of histogram for process '{proc_inst.name}' do not match the binning of variable "
                    f"'{variable_inst.name}' in category '{category_inst.name}'"
                    f"\nhistogram: {h.axes[0].edges}\nvariable : {variable_inst.bin_edges}",
                )
            # create a copy to
            # - update variable name and label
            # - add shift axis and use current values as "nominal" bin
            # - remove variances that represent syst. uncs. but would be interpreted as stat. uncs. during plotting
            h2 = create_hist_from_variables(
                variable_inst,
                categorical_axes=[("shift", "strcat", ["nominal"])],
                weight=True,
            )
            h2.view(flow=True).value[...] = h.view(flow=True).value
            return h2

        # create a mapping of process instance to process object in the inference model
        proc_map: dict[od.Process, DotDict] = {}
        for proc_obj_name in shapes.processes:
            proc_obj = self.inference_model_inst.get_process(process=proc_obj_name, category=cat_obj.name)
            proc_inst = config_inst.get_process(proc_obj.config_data[config_inst.name].process)
            proc_map[proc_inst] = proc_obj

        # helper to show warnings in case a process is not used but actually contributed to the requested uncertainty
        def maybe_warn_unused_process(proc_inst: od.Process, skip: bool = False) -> None:
            # never warn about data
            if proc_inst.is_data:
                return
            # never warn in case "none" uncertainty is requested
            if self.uncertainty == "none":
                return
            # do not warn for signals when only "bkg" uncertainty is requested
            if self.uncertainty == "bkg" and proc_map[proc_inst].is_signal:
                return
            # warn
            if skip:
                action = "skipped"
                extra = ""
            else:
                action = "not merged"
                extra = f"; merge processes: {','.join(self.merge_processes)}"
            self.logger.warning(
                f"shape of process '{proc_inst.name}' was {action} for plotting in category '{category_inst.name}', "
                f"but was likely considered for the computation of the requested uncertainty type '{self.uncertainty}' "
                f"which is inconsistent and might be misleading{extra}",
            )

        # to be consistent to other plotting tasks, create a mapping "process -> hist"
        proc_hists: ProcHists = {}
        for proc_inst, proc_obj in proc_map.items():
            if exclude_matcher(proc_inst.name):
                maybe_warn_unused_process(proc_inst, skip=True)
            else:
                proc_hists[proc_inst] = sanitize_hist(shapes.hist(proc_obj.name))
        if not exclude_matcher("data"):
            proc_hists[config_inst.get_process("data")] = sanitize_hist(shapes.data_hist)

        # optional process merging and selection
        if self.merge_processes:
            orig_hists = proc_hists.copy()
            proc_hists.clear()
            for proc_name in self.merge_processes:
                proc_inst = config_inst.get_process(proc_name)
                _hists = []
                for _proc_inst, h in list(orig_hists.items()):
                    if _proc_inst == proc_inst or proc_inst.has_process(_proc_inst, deep=True):
                        orig_hists.pop(_proc_inst)
                        _hists.append(h)
                if _hists:
                    proc_hists[proc_inst] = sum_hists(_hists)
                else:
                    self.logger.warning(
                        f"no process shape found to merge into process histogram '{proc_name}'; existing processes "
                        f"were {','.join(p.name for p in orig_hists)}",
                    )
            # potentially warn about unmerged, left-over processes
            for proc_inst in orig_hists:
                maybe_warn_unused_process(proc_inst, skip=False)

        # update histograms using custom hooks
        proc_hists = self.invoke_hist_hooks(
            {config_inst: proc_hists},
            hook_kwargs={"category_name": category_inst.name, "variable_name": variable_inst.name},
        )[config_inst]

        # prepare error band
        error_kwargs = {}
        if self.uncertainty in {"bkg", "all"}:
            h_unc = shapes.background_hist if self.uncertainty == "bkg" else shapes.total_hist
            error_kwargs["custom_errors"] = h_unc.view().variance**0.5
            error_kwargs["custom_error_label"] = "MC unc. (postfit)"
            error_kwargs["custom_hatch_style"] = "black"

        # copy process instances once so that their auxiliary data fields can be used as a storage for
        # process-specific plot parameters later on in plot scripts without affecting the original instances
        fake_root = od.Process(
            name=f"{hex(id(object()))[2:]}",
            id="+",
            processes=list(proc_hists.keys()),
        ).copy()
        process_map = {proc_inst.name: proc_inst for proc_inst in fake_root.processes.values()}
        fake_root.processes.clear()
        proc_hists = {process_map[proc_inst.name]: h for proc_inst, h in proc_hists.items()}

        # temporarily use a merged luminostiy value, assigned to the first config
        if not config_inst.has_aux("lumi_plot_lock"):
            config_inst.x.lumi_plot_lock = threading.RLock()
        lumi = sum([_config_inst.x.luminosity for _config_inst in self.config_insts])

        with law.util.patch_object(config_inst.x, "luminosity", lumi, lock=config_inst.x.lumi_plot_lock):
            # call the plot function
            fig, _ = self.call_plot_func(
                self.plot_function,
                hists=proc_hists,
                category_inst=category_inst,
                variable_insts=[variable_inst],
                config_inst=config_inst,
                shift_insts=[config_inst.get_shift("nominal")],
                hide_stat_errors=True,
                merge_stat_errors=False,
                **error_kwargs,
                **self.get_plot_parameters(),
            )

        # save the plot
        for outp in self.output()["plots"]:
            outp.dump(fig, formatter="mpl")
