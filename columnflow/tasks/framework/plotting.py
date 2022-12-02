# coding: utf-8

"""
Base classes for different types of plotting tasks.
"""

import importlib
from typing import Any, Callable

import law
import luigi

from columnflow.tasks.framework.parameters import SettingsParameter
from columnflow.tasks.framework.base import AnalysisTask
from columnflow.tasks.framework.mixins import DatasetsProcessesMixin, VariablesMixin
from columnflow.util import test_float, DotDict, dict_add_strict


class PlotBase(AnalysisTask):
    """
    Base class for all plotting tasks.
    """

    file_types = law.CSVParameter(
        default=("pdf",),
        significant=True,
        description="comma-separated list of file extensions to produce; default: pdf",
    )
    plot_suffix = luigi.Parameter(
        default=law.NO_STR,
        significant=True,
        description="adds a suffix to the output file name of a plot; empty default",
    )
    view_cmd = luigi.Parameter(
        default=law.NO_STR,
        significant=False,
        description="a command to execute after the task has run to visualize plots right in the "
        "terminal; no default",
    )
    skip_legend = luigi.BoolParameter(
        default=False,
        significant=False,
        description="when True, no legend is drawn; default: False",
    )
    skip_cms = luigi.BoolParameter(
        default=False,
        significant=False,
        description="when True, no CMS logo is drawn; default: False",
    )

    def get_plot_parameters(self) -> DotDict:
        # convert parameters to usable values during plotting
        params = DotDict({})
        dict_add_strict(params, "skip_legend", self.skip_legend)
        dict_add_strict(params, "skip_cms", self.skip_cms)
        return params

    def get_plot_names(self, name: str) -> list[str]:
        """
        Returns a list of basenames for created plots given a file *name* for all configured file
        types, plus the additional plot suffix.
        """
        suffix = ""
        if self.plot_suffix and self.plot_suffix != law.NO_STR:
            suffix = f"__{self.plot_suffix}"

        return [
            f"{name}{suffix}.{ft}"
            for ft in self.file_types
        ]

    def get_plot_func(self, func_name: str) -> Callable:
        """
        Returns a function, imported from a module given *func_name* which should have the format
        ``<module_to_import>.<function_name>``.
        """
        # prepare names
        if "." not in func_name:
            raise ValueError(f"invalid func_name format: {func_name}")
        module_id, name = func_name.rsplit(".", 1)

        # import the module
        try:
            mod = importlib.import_module(module_id)
        except ImportError as e:
            raise ImportError(f"cannot import plot function {name} from module {module_id}: {e}")

        # get the function
        func = getattr(mod, name, None)
        if func is None:
            raise Exception(f"module {module_id} does not contain plot function {name}")

        return func

    def call_plot_func(self, func_name: str, **kwargs) -> Any:
        """
        Gets the plot function referred to by *func_name* via :py:meth:`get_plot_func`, calls it
        with all *kwargs* and returns its result. *kwargs* are updated through the
        :py:meth:`update_plot_kwargs` hook first.
        """
        return self.get_plot_func(func_name)(**(self.update_plot_kwargs(kwargs)))

    def update_plot_kwargs(self, kwargs: dict) -> dict:
        """
        Hook to update keyword arguments *kwargs* used for plotting in :py:meth:`call_plot_func`.
        """
        return kwargs


@law.decorator.factory(accept_generator=True)
def view_output_plots(fn, opts, task, *args, **kwargs):
    def before_call():
        return None

    def call(state):
        return fn(task, *args, **kwargs)

    def after_call(state):
        view_cmd = getattr(task, "view_cmd", None)
        if not view_cmd or view_cmd == law.NO_STR:
            return

        # prepare the view command
        if "{}" not in view_cmd:
            view_cmd += " {}"

        # collect all paths to view
        view_paths = []
        outputs = law.util.flatten(task.output())
        while outputs:
            output = outputs.pop(0)
            if isinstance(output, law.TargetCollection):
                outputs.extend(output._flat_target_list)
                continue
            if not getattr(output, "path", None):
                continue
            if output.path.endswith((".pdf", ".png")):
                if not isinstance(output, law.LocalTarget):
                    task.logger.warning(f"cannot show non-local plot at '{output.path}'")
                    continue
                elif output.path not in view_paths:
                    view_paths.append(output.path)

        # loop through paths and view them
        for path in view_paths:
            task.publish_message("showing {}".format(path))
            law.util.interruptable_popen(view_cmd.format(path), shell=True, executable="/bin/bash")

    return before_call, call, after_call


PlotBase.view_output_plots = view_output_plots


class PlotBase1D(PlotBase):
    """
    Base class for plotting tasks creating 1-dimensional plots.
    """

    plot_function_1d = luigi.Parameter(
        default="columnflow.plotting.example.plot_variable_per_process",
        significant=False,
        description="name of the 1D plot function; default: 'columnflow.plotting.example.plot_variable_per_process'",
    )
    skip_ratio = luigi.BoolParameter(
        default=False,
        significant=False,
        description="when True, no ratio (usually Data/Bkg ratio) is drawn in the lower panel; "
        "default: False",
    )
    yscale = luigi.ChoiceParameter(
        choices=(law.NO_STR, "linear", "log"),
        default=law.NO_STR,
        significant=False,
        description="string parameter to define the y-axis scale of the plot in the upper panel; "
        "choices: NO_STR,linear,log; no default",
    )
    shape_norm = luigi.BoolParameter(
        default=False,
        significant=False,
        description="when True, each process is normalized on it's integral in the upper panel; "
        "default: False",
    )

    def get_plot_parameters(self) -> DotDict:
        # convert parameters to usable values during plotting
        params = super().get_plot_parameters()
        dict_add_strict(params, "skip_ratio", self.skip_ratio)
        dict_add_strict(params, "yscale", None if self.yscale == law.NO_STR else self.yscale)
        dict_add_strict(params, "shape_norm", self.shape_norm)
        return params


class PlotBase2D(PlotBase):
    """
    Base class for plotting tasks creating 2-dimensional plots.
    """

    plot_function_2D = luigi.Parameter(
        default="columnflow.plotting.plot2d.plot_2d",
        significant=False,
        description="name of the 2D plot function; default: 'columnflow.plotting.plot2d.plot_2d'",
    )
    zscale = luigi.ChoiceParameter(
        choices=(law.NO_STR, "linear", "log"),
        default=law.NO_STR,
        significant=False,
        description="string parameter to define the z-axis scale of the plot; "
        "choices: NO_STR,linear,log; no default",
    )
    shape_norm = luigi.BoolParameter(
        default=False,
        significant=False,
        description="when True, the overall bin content is normalized on its integral; "
        "default: False",
    )

    def get_plot_parameters(self) -> DotDict:
        # convert parameters to usable values during plotting
        params = super().get_plot_parameters()
        dict_add_strict(params, "zscale", None if self.zscale == law.NO_STR else self.zscale)
        dict_add_strict(params, "shape_norm", self.shape_norm)
        return params


class ProcessPlotSettingMixin(
    DatasetsProcessesMixin,
    PlotBase,
):
    """
    Mixin class for tasks creating plots where contributions of different processes are shown.
    """

    process_settings = law.MultiCSVParameter(
        default=(),
        significant=False,
        description="parameter for changing different process settings; Format: "
        "'process1,option1=value1,option3=value3:process2,option2=value2'; options implemented: "
        "scale, unstack, label; can also be the key of a mapping defined in 'process_settings_groups; "
        "default: value of the 'default_process_settings' if defined, else empty default",
        brace_expand=True,
    )

    def get_plot_parameters(self) -> DotDict:
        # convert parameters to usable values during plotting
        params = super().get_plot_parameters()

        def parse_setting(setting: str):
            pair = setting.split("=", 1)
            key, value = pair if len(pair) == 2 else (pair[0], "True")
            if test_float(value):
                value = float(value)
            elif value.lower() == "true":
                value = True
            elif value.lower() == "false":
                value = False
            return (key, value)

        process_settings = {
            proc_settings[0]: dict(map(parse_setting, proc_settings[1:]))
            for proc_settings in self.process_settings
        }
        dict_add_strict(params, "process_settings", process_settings)

        return params

    @classmethod
    def modify_param_values(cls, params):
        params = super().modify_param_values(params)

        if "config_inst" not in params:
            return params
        config_inst = params["config_inst"]

        # resolve process_settings
        if "process_settings" in params:
            # when empty and default process_settings are defined, use them instead
            if not params["process_settings"] and config_inst.x("default_process_settings", ()):
                params["process_settings"] = tuple(config_inst.x("default_process_settings", ()))

            # when process_settings are a key to a process_settings_groups, use this instead
            groups = config_inst.x("process_settings_groups", {})
            if params["process_settings"] and params["process_settings"][0][0] in groups.keys():
                params["process_settings"] = groups[params["process_settings"][0][0]]

        return params

    def store_parts(self):
        parts = super().store_parts()
        part = f"datasets_{self.datasets_repr}__processes_{self.processes_repr}"
        parts.insert_before("version", "plot", part)
        return parts


class VariablePlotSettingMixin(
        VariablesMixin,
        PlotBase,
):
    """
    Mixin class for tasks creating plots for multiple variables.
    """

    variable_settings = SettingsParameter(
        default=(),
        significant=False,
        description="parameter for changing different variable settings; Format: "
        "'var1,option1=value1,option3=value3:var2,option2=value2'; options implemented: "
        "rebin; can also be the key of a mapping defined in 'variable_settings_groups; "
        "default: value of the 'default_variable_settings' if defined, else empty default",
        brace_expand=True,
    )

    def get_plot_parameters(self) -> DotDict:
        # convert parameters to usable values during plotting
        params = super().get_plot_parameters()
        dict_add_strict(params, "variable_settings", self.variable_settings)

        return params

    @classmethod
    def modify_param_values(cls, params):
        params = super().modify_param_values(params)
        if "config_inst" not in params:
            return params
        config_inst = params["config_inst"]

        # resolve variable_settings
        if "variable_settings" in params:
            settings = params["variable_settings"]
            # when empty and default variable_settings are defined, use them instead
            if not settings and config_inst.x("default_variable_settings", ()):
                settings = config_inst.x("default_variable_settings", ())
                if isinstance(settings, tuple):
                    settings = cls.variable_settings.parse(settings)

            # when variable_settings are a key to a variable_settings_groups, use them instead
            groups = config_inst.x("variable_settings_groups", {})

            if settings and cls.variable_settings.serialize(settings) in groups.keys():
                settings = groups[cls.variable_settings.serialize(settings)]
                if isinstance(settings, tuple):
                    settings = cls.variable_settings.parse(settings)

            params["variable_settings"] = settings
        return params
