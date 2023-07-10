# coding: utf-8

"""
Base classes for different types of plotting tasks.
"""

import importlib
from typing import Any, Callable

import law
import luigi

from columnflow.tasks.framework.base import ConfigTask
from columnflow.tasks.framework.mixins import DatasetsProcessesMixin, VariablesMixin
from columnflow.tasks.framework.parameters import SettingsParameter, MultiSettingsParameter
from columnflow.util import DotDict, dict_add_strict


class PlotBase(ConfigTask):
    """
    Base class for all plotting tasks.
    """

    plot_function = luigi.Parameter(
        significant=False,
        description="location of the plot function to use in the format 'module.function_name'",
    )
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
    general_settings = SettingsParameter(
        default=DotDict(),
        significant=False,
        description="Parameter to set a list of custom plotting parameters. Format: "
        "'option1=val1,option2=val2,...'",
    )
    skip_legend = law.OptionalBoolParameter(
        default=None,
        significant=False,
        description="when True, no legend is drawn; default: None",
    )
    cms_label = luigi.Parameter(
        default="wip",
        significant=False,
        description="postfix to add behind the CMS logo; when 'skip', no CMS logo is shown at all; "
        "the following special values are expanded into the usual postfixes: wip, pre, pw, sim, "
        "simwip, simpre, simpw, od, odwip, public; default: 'wip'",
    )

    @classmethod
    def resolve_param_values(cls, params):
        params = super().resolve_param_values(params)

        if "config_inst" not in params:
            return params
        config_inst = params["config_inst"]

        # resolve variable_settings
        if "general_settings" in params:
            settings = params["general_settings"]
            # when empty and default general_settings are defined, use them instead
            if not settings and config_inst.x("default_general_settings", ()):
                settings = config_inst.x("default_general_settings", ())
                if isinstance(settings, tuple):
                    settings = cls.general_settings.parse(settings)

            # when general_settings are a key to a general_settings_groups, use them instead
            groups = config_inst.x("general_settings_groups", {})
            if settings and list(settings.keys())[0] in groups.keys():
                settings = groups[list(settings.keys())[0]]
                if isinstance(settings, tuple):
                    settings = cls.general_settings.parse(settings)

            params["general_settings"] = settings

        return params

    def get_plot_parameters(self) -> DotDict:
        # convert parameters to usable values during plotting
        params = DotDict()
        dict_add_strict(params, "skip_legend", self.skip_legend)
        dict_add_strict(params, "cms_label", self.cms_label)
        dict_add_strict(params, "general_settings", self.general_settings)
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
        # remove None entrys from plot kwargs
        for key, value in list(kwargs.items()):
            if value is None:
                kwargs.pop(key)

        # set items of general_settings in kwargs if corresponding key is not yet present
        general_settings = kwargs.get("general_settings", {})
        for key, value in general_settings.items():
            kwargs.setdefault(key, value)

        return kwargs


class PlotBase1D(PlotBase):
    """
    Base class for plotting tasks creating 1-dimensional plots.
    """

    skip_ratio = law.OptionalBoolParameter(
        default=None,
        significant=False,
        description="when True, no ratio (usually Data/Bkg ratio) is drawn in the lower panel; "
        "default: None",
    )
    density = law.OptionalBoolParameter(
        default=None,
        significant=False,
        description="when True, the number of entries is scaled to the bin width; default: None",
    )
    yscale = luigi.ChoiceParameter(
        choices=(law.NO_STR, "linear", "log"),
        default=law.NO_STR,
        significant=False,
        description="string parameter to define the y-axis scale of the plot in the upper panel; "
        "choices: NO_STR,linear,log; no default",
    )
    shape_norm = law.OptionalBoolParameter(
        default=None,
        significant=False,
        description="when True, each process is normalized on it's integral in the upper panel; "
        "default: None",
    )
    hide_errors = law.OptionalBoolParameter(
        default=None,
        significant=False,
        description="when True, no error bars/bands on histograms are drawn; default: None",
    )

    def get_plot_parameters(self) -> DotDict:
        # convert parameters to usable values during plotting
        params = super().get_plot_parameters()
        dict_add_strict(params, "skip_ratio", self.skip_ratio)
        dict_add_strict(params, "density", self.density)
        dict_add_strict(params, "yscale", None if self.yscale == law.NO_STR else self.yscale)
        dict_add_strict(params, "shape_norm", self.shape_norm)
        dict_add_strict(params, "hide_errors", self.hide_errors)
        return params


class PlotBase2D(PlotBase):
    """
    Base class for plotting tasks creating 2-dimensional plots.
    """

    zscale = luigi.ChoiceParameter(
        choices=(law.NO_STR, "linear", "log"),
        default=law.NO_STR,
        significant=False,
        description="string parameter to define the z-axis scale of the plot; "
        "choices: NO_STR,linear,log; no default",
    )
    density = law.OptionalBoolParameter(
        default=None,
        significant=False,
        description="when True, the number of entries is scaled to the bin width; default: None",
    )
    shape_norm = law.OptionalBoolParameter(
        default=None,
        significant=False,
        description="when True, the overall bin content is normalized on its integral; "
        "default: None",
    )
    colormap = luigi.Parameter(
        default=law.NO_STR,
        significant=False,
        description="name of the matplotlib colormap to use for the colorbar; "
        "if not set, an appropriate colormap will be chosen by the plotting function",
    )
    zlim = law.CSVParameter(
        default=("min", "max"),
        significant=False,
        min_len=2,
        max_len=2,
        description="the range of values covered by the colorbar; this optional CSV parameter "
        "accepts exactly two values, indicating the lower and upper bound of the colorbar. The special "
        "specifiers 'min' and 'max' can be used in place of floats to indicate the minimum or maximum value "
        "of the data. Similarly, 'maxabs' and 'minabs' indicate the minimum and maximum of the absolute "
        "value of the data. A hyphen ('-') may be prepended to any specifier to indicate the negative of the "
        "corresponding value. If no 'zlim' is given, the limits are inferred from the data (equivalent to 'min,max').",
    )
    extremes = luigi.ChoiceParameter(
        default="color",
        choices=("color", "clip", "hide"),
        significant=False,
        description="how to handle extreme values outside `zlim`; valid choices are: 'color' (extreme values are "
        "shown in a different color), 'clip' (extreme values are clipped to the closest allowed values) and 'hide' "
        "(extreme values are treated as missing); default: 'color'",
    )
    extreme_colors = law.CSVParameter(
        default=(),
        significant=False,
        description="the colors to use for marking extreme values; this optional CSV parameter accepts exactly two "
        "values, indicating the color to use for values below and above the range covered by the colorbar.",
    )

    @classmethod
    def modify_param_values(cls, params: dict) -> dict:
        params = super().modify_param_values(params)

        params["zlim"] = tuple(
            float(v) if law.util.is_float(v) else v
            for v in params["zlim"]
        )

        return params

    def get_plot_parameters(self) -> DotDict:
        # convert parameters to usable values during plotting
        params = super().get_plot_parameters()
        dict_add_strict(params, "zscale", None if self.zscale == law.NO_STR else self.zscale)
        dict_add_strict(params, "zlim", None if not self.zlim else self.zlim)
        dict_add_strict(params, "extremes", self.extremes)
        dict_add_strict(params, "extreme_colors", None if not self.extreme_colors else self.extreme_colors)
        dict_add_strict(params, "colormap", None if self.colormap == law.NO_STR else self.colormap)
        dict_add_strict(params, "density", self.density)
        dict_add_strict(params, "shape_norm", self.shape_norm)
        return params


class ProcessPlotSettingMixin(
    DatasetsProcessesMixin,
    PlotBase,
):
    """
    Mixin class for tasks creating plots where contributions of different processes are shown.
    """

    process_settings = MultiSettingsParameter(
        default=DotDict(),
        significant=False,
        description="parameter for changing different process settings; format: "
        "'process1,option1=value1,option3=value3:process2,option2=value2'; options implemented: "
        "scale, unstack, label; can also be the key of a mapping defined in 'process_settings_groups; "
        "default: value of the 'default_process_settings' if defined, else empty default",
        brace_expand=True,
    )

    @classmethod
    def resolve_param_values(cls, params):
        params = super().resolve_param_values(params)

        if "config_inst" not in params:
            return params
        config_inst = params["config_inst"]

        # resolve process_settings
        if "process_settings" in params:
            settings = params["process_settings"]
            # when empty and default process_settings are defined, use them instead
            if not settings and config_inst.x("default_process_settings", ()):
                settings = config_inst.x("default_process_settings", ())
                if isinstance(settings, tuple):
                    settings = cls.process_settings.parse(settings)

            # when process_settings are a key to a process_settings_groups, use them instead
            groups = config_inst.x("process_settings_groups", {})
            if settings and cls.process_settings.serialize(settings) in groups.keys():
                settings = groups[cls.process_settings.serialize(settings)]
                if isinstance(settings, tuple):
                    settings = cls.process_settings.parse(settings)

            params["process_settings"] = settings

        return params

    def get_plot_parameters(self) -> DotDict:
        # convert parameters to usable values during plotting
        params = super().get_plot_parameters()
        dict_add_strict(params, "process_settings", self.process_settings)

        return params


class VariablePlotSettingMixin(
        VariablesMixin,
        PlotBase,
):
    """
    Mixin class for tasks creating plots for multiple variables.
    """

    variable_settings = MultiSettingsParameter(
        default=DotDict(),
        significant=False,
        description="parameter for changing different variable settings; format: "
        "'var1,option1=value1,option3=value3:var2,option2=value2'; options implemented: "
        "rebin; can also be the key of a mapping defined in 'variable_settings_groups; "
        "default: value of the 'default_variable_settings' if defined, else empty default",
        brace_expand=True,
    )

    @classmethod
    def resolve_param_values(cls, params):
        params = super().resolve_param_values(params)

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

    def get_plot_parameters(self) -> DotDict:
        # convert parameters to usable values during plotting
        params = super().get_plot_parameters()
        dict_add_strict(params, "variable_settings", self.variable_settings)

        return params
