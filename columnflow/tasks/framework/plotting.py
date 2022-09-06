# coding: utf-8

"""
Base tasks for different types of plotting tasks
"""

import law
import luigi

from columnflow.tasks.framework.mixins import PlotMixin, CategoriesMixin, DatasetsProcessesMixin
from columnflow.util import test_float


class PlotBase(PlotMixin):
    """
    Base class for all plotting tasks.
    """

    plot_suffix = luigi.Parameter(
        default=law.NO_STR,
        significant=True,
        description="adds a suffix to the output file name of a plot",
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

    def get_plot_parameters(self):
        # convert parameters to usable values during plotting
        return {
            "skip_legend": self.skip_legend,
            "skip_cms": self.skip_cms,
        }


class PlotBase1D(PlotBase):
    """
    Base class for plotting tasks creating 1-dimensional plots.
    """

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

    def get_plot_parameters(self):
        # convert parameters to usable values during plotting
        params = super().get_plot_parameters()

        params["skip_ratio"] = self.skip_ratio
        params["yscale"] = None if self.yscale == law.NO_STR else self.yscale
        params["shape_norm"] = self.shape_norm

        return params


class ProcessPlotBase(
    CategoriesMixin,
    DatasetsProcessesMixin,
    PlotBase1D,
):
    """
    Base class for tasks creating plots where contributions of different processes are shown.
    """

    # TODO: allow default_process_settings in config
    process_settings = law.MultiCSVParameter(
        default=(),
        significant=False,
        description="e.g. (signal,scale=10,unstack=True:tt,scale=1,label=top antitop); "
        "implemented settings: scale,unstack,label,color; empty default", # TODO make good description
        brace_expand=True,
    )

    def get_plot_parameters(self):
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

        params["process_settings"] = {
            proc_settings[0]: dict(map(parse_setting, proc_settings[1:]))
            for proc_settings in self.process_settings
        }
        return params

    def store_parts(self):
        parts = super().store_parts()
        part = f"datasets_{self.datasets_repr}__processes_{self.processes_repr}"
        parts.insert_before("version", "plot", part)
        return parts


class PlotBase2D(PlotBase):
    """
    Base class for plotting tasks creating 2-dimensional plots.
    """

    z_scale = luigi.ChoiceParameter(
        choices=(law.NO_STR, "linear", "log"),
        default=law.NO_STR,
        significant=False,
        description="string parameter to define the z-axis scale of the plot in the upper panel; "
        "choices: NO_STR,linear,log; no default",
    )

    def get_plot_parameters(self):
        # convert parameters to usable values during plotting
        params = super().get_plot_parameters()

        params["z_scale"] = None if self.z_scale == law.NO_STR else self.z_scale

        return params
