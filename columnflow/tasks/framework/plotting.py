# coding: utf-8

"""
Base tasks for different types of plotting tasks
"""

import law
import luigi

from columnflow.tasks.framework.mixins import PlotMixin, CategoriesMixin, DatasetsProcessesMixin


class PlotBase(PlotMixin):
    """
    Base class for all plotting tasks.
    """

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
    y_scale = luigi.ChoiceParameter(
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
        params["y_scale"] = None if self.y_scale == law.NO_STR else self.y_scale
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

    process_lines = law.CSVParameter(
        default=(),
        significant=False,
        description="comma-separated process names or patterns to plot as individual lines; "
        "can also be the key of a mapping defined in the 'process_groups' auxiliary data of "
        "the config; uses no process when empty; empty default",
    )

    scale_process = law.CSVParameter(
        default=(),
        significant=False,
        description="comma-seperated process names and values with which to scale the process, "
        "e.g. ('signal:100'); empty default",
    )

    def get_plot_parameters(self):
        # convert parameters to usable values during plotting
        params = super().get_plot_parameters()

        params["process_lines"] = self.process_lines
        params["scale_process"] = {i[0]: i[1] for i in [s.split(":") for s in self.scale_process]}

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
