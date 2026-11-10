# coding: utf-8

"""
Tasks to produce yield tables.
"""

import math
from collections import defaultdict

import law
import luigi
import order as od
from scinum import Number

from columnflow.tasks.framework.base import Requirements
from columnflow.tasks.framework.histograms import HistogramsUserSingleShiftBase
from columnflow.tasks.framework.remote import RemoteWorkflow
from columnflow.tasks.histograms import MergeHistograms
from columnflow.util import dev_sandbox, try_int
from columnflow.tasks.framework.decorators import view_output_plots
from columnflow.tasks.framework.plotting import PlotBase

logger = law.logger.get_logger(__name__)


class _CreateYieldTable(
    HistogramsUserSingleShiftBase,
    law.LocalWorkflow,
    RemoteWorkflow,
):
    """
    Base classes for :py:class:`CreateYieldTable`.
    """

    single_config = False
    resolution_task_cls = MergeHistograms


class CreateYieldTable(_CreateYieldTable):
    """
    Task that creates a yield table for the specified processes and categories.
    Also produces a horizontal bar plot of the yields per category for each process.
    """
    sandbox = dev_sandbox(law.config.get("analysis", "default_columnar_sandbox"))

    variables = HistogramsUserSingleShiftBase.variables.copy(
        default=("event",),
        description="Variable to use for the yields table. Should be a single variable",
        add_default_to_description=True,
    )

    table_format = luigi.Parameter(
        default="fancy_grid",
        significant=False,
        description="format of the yield table; accepts all formats of the tabulate package; "
        "default: fancy_grid",
    )
    number_format = luigi.Parameter(
        default="pdg",
        significant=False,
        description="rounding format of each number in the yield table; accepts all formats "
        "understood by scinum.Number.str(), e.g. 'pdg', 'publication', '%.1f' or an integer "
        "(number of signficant digits); default: pdg",
    )
    skip_uncertainties = luigi.BoolParameter(
        default=False,
        significant=False,
        description="when True, uncertainties are not displayed in the table; default: False",
    )
    normalize_yields = luigi.ChoiceParameter(
        choices=(law.NO_STR, "per_process", "per_category", "all"),
        default=law.NO_STR,
        significant=False,
        description="string parameter to define the normalization of the yields; "
        "choices: '', per_process, per_category, all; empty default",
    )
    output_suffix = luigi.Parameter(
        default=law.NO_STR,
        description="Adds a suffix to the output name of the yields table; empty default",
    )
    transpose = luigi.BoolParameter(
        default=False,
        significant=False,
        description="Transpose the yield table; default: False",
    )
    ratio = law.CSVParameter(
        default=("data", "background"),
        significant=False,
        description="Ratio of two processes to be calculated and added to the table",
    )
    ratio_modes = law.CSVParameter(
        default=("ratio", "subtract", "s_over_b", "s_over_sqrt_b"),
        significant=False,
        description="Modes for the ratio calculation; "
        "options: 'ratio', 'subtract', 's_over_b', 's_over_sqrt_b'; "
        "if 'ratio', the ratio of the two processes is calculated, "
        "if 's_over_b', S/B is calculated, using all processes with the "
        "tag 'is_signal' as S and the process 'background' as B, "
        "if 's_over_sqrt_b', S/sqrt(B) is calculated, "
        "if 'subtract', (p[0] - p[1] + p[2]) / p[2] is calculated, ",
    )

    # TODO: order of categories is not always maintained, see: https://github.com/columnflow/columnflow/issues/717
    keep_categories_order = luigi.BoolParameter(
        default=True,
        significant=False,
        description="Keep the order of categories as specified in the input; "
        "this option disables the ability to use placeholders and groups for categories; "
        "default: True",
    )

    # plot parameters
    cms_label = PlotBase.cms_label.copy()
    skip_legend = PlotBase.skip_legend.copy()
    view_cmd = PlotBase.view_cmd.copy()

    # upstream requirements
    reqs = Requirements(
        RemoteWorkflow.reqs,
        MergeHistograms=MergeHistograms,
    )

    # dummy branch map
    def create_branch_map(self):
        return [0]

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

        # datasets/processes instances
        self.process_insts = {
            config_inst: list(map(config_inst.get_process, processes))
            for config_inst, processes in zip(self.config_insts, self.processes)
        }
        self.dataset_insts = {
            config_inst: list(map(config_inst.get_dataset, datasets))
            for config_inst, datasets in zip(self.config_insts, self.datasets)
        }

    @classmethod
    def resolve_param_values(cls, params):
        # get initial categories
        initial_categories = params.get("categories", [])

        params = super().resolve_param_values(params)

        if params.get("keep_categories_order") and initial_categories and initial_categories[0] != "DEFAULT":
            # restore initial categories after being modified by parent class
            # NOTE: this does not work if the user requests categories via groups or placeholders
            params["categories"] = initial_categories
        else:
            # if using default categories, sort them for consistent order
            params["categories"] = sorted(params.get("categories", []))

        if len(params.get("variables")) != 1:
            raise ValueError("The yields table requires exactly one variable to be specified.")

        if "number_format" in params and try_int(params["number_format"]):
            # convert 'number_format' in integer if possible
            params["number_format"] = int(params["number_format"])

        return params

    def output(self):
        suffix = ""
        if self.output_suffix and self.output_suffix != law.NO_STR:
            suffix = f"__{self.output_suffix}"

        return {
            "table": self.target(f"table__proc_{self.processes_repr}__cat_{self.categories_repr}{suffix}.txt"),
            "csv": self.target(f"table__proc_{self.processes_repr}__cat_{self.categories_repr}{suffix}.csv"),
            "yields": self.target(f"yields__proc_{self.processes_repr}__cat_{self.categories_repr}{suffix}.json"),
            "plot": self.target(f"plot__proc_{self.processes_repr}__cat_{self.categories_repr}{suffix}.pdf"),
        }

    @property
    def category_insts(self):
        """
        Returns the category instances for the first config.
        """
        return list(map(self.config_insts[0].get_category, self.categories))

    @property
    def yields_variable(self):
        """
        Returns the variable to be used for the yields table.
        This is the first variable in the variables list.
        """
        return self.variables[0]

    @law.decorator.notify
    @law.decorator.log
    @view_output_plots
    def run(self):
        with self.publish_step(f"Creating yields for processes {self.processes}, categories {self.categories}"):
            yields, processes, _ = self.prepare_yields()
            derived, derived_procs = self.apply_ratio(yields, processes)
            raw_yields, yields_str = self.prepare_yields_str(yields, processes, derived, derived_procs)
            self.make_table(raw_yields, yields_str)
            self.make_yields_plot(raw_yields, processes)

    def background_mask(self, processes: list[od.Process]) -> list[bool]:
        """
        Returns a mask that is True for all processes that are considered as (non-overlapping) background, i.e.
        MC processes without the "is_signal" tag that do not contain any other requested process. These processes
        are used to compute normalization factors and are stacked in the yields plot.
        """
        def is_background(proc):
            if proc.is_data or proc.has_tag("is_signal"):
                return False
            # skip aggregate processes (e.g. "background") when any of their sub processes are requested as well
            return not any(p is not proc and proc.has_process(p, deep=True) for p in processes)

        return list(map(is_background, processes))

    def prepare_yields(self):
        inputs = self.input()
        import hist
        if len(set(self.processes)) != 1:
            raise NotImplementedError("The yields table task currently assumes same processes for all configs.")

        yields_hist = hist.Hist.new.StrCat(
            [], name="process", growth=True,
        ).StrCat(
            [], name="category", growth=True,
        ).Weight()

        for process in self.processes[0]:
            for category in self.categories:
                yields_hist.fill(process, category, weight=0)

        for i, config_inst in enumerate(self.config_insts):
            hist_per_config = None
            sub_processes = self.processes[i]
            for dataset in self.datasets[i]:
                # sum over all histograms of the same variable and config
                if hist_per_config is None:
                    hist_per_config = self.load_histogram(inputs, config_inst, dataset, self.yields_variable)
                else:
                    hist_per_config += self.load_histogram(inputs, config_inst, dataset, self.yields_variable)

            # slice histogram per config according to the sub_processes and categories
            hist_per_config = self.slice_histogram(
                histogram=hist_per_config,
                config_inst=config_inst,
                processes=sub_processes,
                categories=self.categories,
                # NOTE: this could lead to double-counting if (for some reason) one event is assigned to both nominal
                # and varied shift; should not happen in practice though
                shifts={self.shift, "nominal"},
            )
            hist_per_config = hist_per_config[{"shift": sum, self.yields_variable: sum}]

            for process in sub_processes:
                for category in self.categories:
                    h_cat = self.slice_histogram(
                        histogram=hist_per_config,
                        config_inst=config_inst,
                        processes=[process],
                        categories=[category],
                    )
                    yields_hist[{"process": process, "category": category}] += h_cat.sum()

        yields = defaultdict(list)
        processes = []

        # use process and category insts from 1st config
        for process_inst in self.process_insts[self.config_insts[0]]:
            if process_inst.name not in yields_hist.axes["process"]:
                logger.warning(f"Process {process_inst.name} not found in histogram axes, skipping.")
                continue
            processes.append(process_inst)
            for category_inst in self.category_insts:
                if category_inst.name not in yields_hist.axes["category"]:
                    logger.warning(f"Category {category_inst.name} not found in histogram axes, skipping.")
                    continue

                # get the histogram for the process and category
                h_cat = yields_hist[{"process": process_inst.name, "category": category_inst.name}]

                # create a Number object for the yield
                value = Number(h_cat.value)
                if not self.skip_uncertainties:
                    # set a unique uncertainty name for correct propagation below
                    value.set_uncertainty(
                        f"mcstat_{process_inst.name}_{category_inst.name}",
                        math.sqrt(h_cat.variance),
                    )
                yields[category_inst].append(value)

        return yields, processes, yields_hist

    def apply_ratio(self, yields, processes):
        """
        Function that derives additional quantities (ratios, S/B, ...) from the yields.
        The derived quantities are kept separate from the process yields so that they neither enter the
        normalization nor the yields plot. Returns a dictionary mapping category instances to lists of derived
        values and a list of (dummy) process instances describing each derived quantity.
        If requested processes are not available, a warning is logged and the respective calculation is skipped.
        """
        derived = defaultdict(list)
        derived_procs = []
        if not self.ratio:
            return derived, derived_procs

        proc_idx = {proc.name: i for i, proc in enumerate(processes)}
        missing_for_ratio = set(self.ratio[:2]) - set(proc_idx)
        if missing_for_ratio:
            logger.warning(f"Cannot derive ratio, missing requested processes: {', '.join(missing_for_ratio)}")
            return derived, derived_procs

        def add_derived(proc_inst, func):
            derived_procs.append(proc_inst)
            for category_inst, category_yields in yields.items():
                derived[category_inst].append(func(category_yields))

        # default ratio
        num_idx, den_idx = proc_idx[self.ratio[0]], proc_idx[self.ratio[1]]
        if "ratio" in self.ratio_modes:
            add_derived(
                od.Process("Ratio", id=-9870, label=f"{self.ratio[0]} / {self.ratio[1]}"),
                lambda y: y[num_idx] / y[den_idx],
            )

        if "s_over_sqrt_b" in self.ratio_modes or "s_over_b" in self.ratio_modes:
            sig_idxs = [i for i, proc in enumerate(processes) if proc.has_tag("is_signal")]
            bkg_idx = proc_idx.get("background")
            if not sig_idxs:
                logger.warning("Cannot calculate S/B or S/sqrt(B), no signal process found.")
            elif bkg_idx is None:
                logger.warning("Cannot calculate S/B or S/sqrt(B), no background process found.")
            else:
                if "s_over_b" in self.ratio_modes:
                    add_derived(
                        od.Process("S_over_B", id=-9871, label="S / B"),
                        lambda y: sum(y[i] for i in sig_idxs) / y[bkg_idx],
                    )
                if "s_over_sqrt_b" in self.ratio_modes:
                    add_derived(
                        od.Process("S_over_sqrt_B", id=-9872, label="S / sqrt(B)"),
                        lambda y: sum(y[i] for i in sig_idxs) / y[bkg_idx] ** 0.5,
                    )

        if len(self.ratio) >= 3 and "subtract" in self.ratio_modes:
            for i, estimate_proc in enumerate(self.ratio[2:]):
                if estimate_proc not in proc_idx:
                    logger.warning(f"Cannot derive subtraction ratio, missing requested process: {estimate_proc}")
                    continue
                est_idx = proc_idx[estimate_proc]
                # subtract the second process from the first one
                add_derived(
                    od.Process(
                        f"Ratio_diff_{estimate_proc}",
                        id=-9873 - i,
                        label=f"({self.ratio[0]} - {self.ratio[1]} + {estimate_proc}) / {estimate_proc}",
                    ),
                    lambda y: (y[num_idx] - y[den_idx] + y[est_idx]) / y[est_idx],
                )

        return derived, derived_procs

    def prepare_yields_str(self, yields, processes, derived, derived_procs):
        """
        Function that prepares the yields for the table output.
        It normalizes the yields according to the specified normalization method and formats them into strings.
        For "per_category" and "all", normalization factors are computed from background processes only (see
        :py:meth:`background_mask`), so that the backgrounds add up to one while data and signal are expressed
        relative to the total background. Derived quantities (ratios, ...) are appended without normalization.
        It returns a dictionary of raw yields and a dictionary of formatted yields strings.
        """
        from columnflow.plotting.plot_util import remove_label_placeholders

        bkg_idxs = [i for i, is_bkg in enumerate(self.background_mask(processes)) if is_bkg]
        if self.normalize_yields in ("per_category", "all") and not bkg_idxs:
            raise ValueError(f"cannot normalize yields '{self.normalize_yields}' without any background process")

        def bkg_sum(category):
            return sum(yields[category][i] for i in bkg_idxs)

        # obtain normalizaton factors per category and process
        n_procs = len(processes)
        norm_factors = {category: [1] * n_procs for category in yields}
        if self.normalize_yields == "all":
            total = sum(bkg_sum(category) for category in yields)
            norm_factors = {category: [total] * n_procs for category in yields}
        elif self.normalize_yields == "per_process":
            totals = [sum(category_yields[i] for category_yields in yields.values()) for i in range(n_procs)]
            norm_factors = {category: totals for category in yields}
        elif self.normalize_yields == "per_category":
            norm_factors = {category: [bkg_sum(category)] * n_procs for category in yields}

        def format_value(value):
            value_str = value.str(
                combine_uncs="all",
                format=self.number_format,
                style="latex" if "latex" in self.table_format else "plain",
            )
            return f"${value_str}$" if "latex" in self.table_format else value_str

        # initialize dicts
        main_label = "Category" if self.transpose else "Process"
        yields_str = defaultdict(list, {
            main_label: [remove_label_placeholders(proc.label) for proc in processes + derived_procs],
        })
        raw_yields = defaultdict(dict, {})

        # apply normalization and format
        for category, category_yields in yields.items():
            cat_label = remove_label_placeholders(category.label).replace("\n", ", ")
            for proc, value, norm_factor in zip(processes, category_yields, norm_factors[category]):
                value = value / norm_factor
                raw_yields[category.name][proc.name] = value.nominal
                yields_str[cat_label].append(format_value(value))

            # derived quantities are not normalized
            for proc, value in zip(derived_procs, derived[category]):
                raw_yields[category.name][proc.name] = value.nominal
                yields_str[cat_label].append(format_value(value))

        return raw_yields, yields_str

    def make_table(self, raw_yields, yields_str):
        from tabulate import tabulate
        data = [list(yields_str.keys())] + list(zip(*yields_str.values()))
        if self.transpose:
            data = list(zip(*data))

        headers = data[0]

        # create, print and save the yield table
        yield_table = tabulate(data[1:], headers=headers, tablefmt=self.table_format)

        with_hlines = True
        if with_hlines and self.table_format == "latex_raw":
            # identify line breaks and add hlines after every line break
            yield_table = yield_table.replace("\\\\", "\\\\ \\hline")
        self.publish_message(yield_table)

        outputs = self.output()
        outputs["table"].dump(yield_table, formatter="text")
        outputs["yields"].dump(raw_yields, formatter="json")

        outputs["csv"].touch()
        with open(outputs["csv"].abspath, "w", newline="") as csvfile:
            import csv
            writer = csv.writer(csvfile)
            writer.writerows(data)

    def make_yields_plot(self, raw_yields, processes, skip_labels=True):
        """
        Creates a horizontal bar plot of the yields per category. Background processes (see
        :py:meth:`background_mask`) are stacked, data is shown as markers on top of the stack and signal processes
        are shown unstacked in a separate panel on the right.

        :param raw_yields: Dictionary containing the raw yields for each category and process. e.g.
            {"category1": {"process1": 0.5, "process2": 0.3}, "category2": {"process1": 0.4, "process2": 0.6}}
        :param processes: List of process instances corresponding to the yields.
        :param skip_labels: If True, no xlabels and ylabels will be added to the plot.
        """
        import matplotlib.pyplot as plt
        import numpy as np
        import mplhep
        plt.style.use(mplhep.style.CMS)
        from columnflow.plotting.plot_util import get_cms_label, remove_label_placeholders

        bkg_procs = [proc for proc, is_bkg in zip(processes, self.background_mask(processes)) if is_bkg]
        data_procs = [proc for proc in processes if proc.is_data]
        sig_procs = [proc for proc in processes if proc.has_tag("is_signal")]

        category_insts = [cat for cat in self.category_insts[::-1] if cat.name in raw_yields]
        y_pos = np.arange(len(category_insts))
        cat_labels = [remove_label_placeholders(cat.label) for cat in category_insts]
        as_percentage = self.normalize_yields in ("per_category", "all")

        def get_yields(proc):
            return np.array([raw_yields[cat.name][proc.name] for cat in category_insts])

        def format_significant(value, digits):
            # round to significant digits without switching to scientific notation
            if value == 0:
                return "0"
            decimals = max(0, digits - 1 - math.floor(math.log10(abs(value))))
            return f"{value:.{decimals}f}"

        # label formats ordered from most to least precise; the first one that fits into the bar is used
        if as_percentage:
            label_formatters = [lambda v, d=d: f"{format_significant(v * 100, d)}%" for d in (3, 2, 1)]
        else:
            label_formatters = [lambda v, d=d: format_significant(v, d) for d in (3, 2, 1)]

        # labels are only added after the final layout is fixed, since their fit is checked in display coordinates
        bkg_label_requests = []
        sig_label_requests = []

        def add_fitting_labels(ax, bars, values, outside=False, fontsize=12, pad_px=4):
            """
            Adds a label to each bar using the first of *label_formatters* that fits. Labels are centered inside
            the bar, or placed right of the bar end when *outside* is True. Bars without fitting label stay empty.
            """
            renderer = ax.figure.canvas.get_renderer()
            ax_bbox = ax.get_window_extent(renderer)
            for bar, value in zip(bars, values):
                bar_bbox = bar.get_window_extent(renderer)
                y = bar.get_y() + bar.get_height() / 2
                for fmt in label_formatters:
                    if outside:
                        text = ax.annotate(
                            fmt(value),
                            xy=(bar.get_x() + bar.get_width(), y),
                            xytext=(pad_px, 0),
                            textcoords="offset pixels",
                            ha="left",
                            va="center",
                            fontsize=fontsize,
                        )
                        text_bbox = text.get_window_extent(renderer)
                        fits = text_bbox.x1 <= ax_bbox.x1 and text_bbox.height <= bar_bbox.height
                    else:
                        text = ax.text(
                            bar.get_x() + bar.get_width() / 2,
                            y,
                            fmt(value),
                            ha="center",
                            va="center",
                            fontsize=fontsize,
                        )
                        text_bbox = text.get_window_extent(renderer)
                        fits = (
                            text_bbox.width + 2 * pad_px <= bar_bbox.width and
                            text_bbox.height <= bar_bbox.height
                        )
                    if fits:
                        break
                    text.remove()

        if sig_procs:
            fig, (ax, ax_sig) = plt.subplots(
                1, 2,
                figsize=(12, 6),
                sharey=True,
                gridspec_kw={"width_ratios": [3, 1]},
            )
        else:
            fig, ax = plt.subplots(figsize=(9, 6))

        # stacked bar plot of backgrounds with one bar per category, different colors for different processes
        bkg_yields = {proc: get_yields(proc) for proc in bkg_procs[::-1]}
        lefts = np.zeros(len(category_insts))
        for proc, yields in bkg_yields.items():
            bars = ax.barh(
                y_pos,
                yields,
                left=lefts,
                color=proc.color,
                label=remove_label_placeholders(proc.label),
            )
            bkg_label_requests.append((bars, yields))
            lefts = lefts + yields

        # data as markers on top of the background stack
        for proc in data_procs:
            ax.plot(
                get_yields(proc),
                y_pos,
                linestyle="none",
                marker="o",
                color="black",
                label=remove_label_placeholders(proc.label),
            )

        # signals unstacked (outlines only) in a separate panel, grouped side by side per category
        if sig_procs:
            sig_height = 0.8 / len(sig_procs)
            for i, proc in enumerate(sig_procs):
                # first signal on top within each category group, matching the legend order
                offset = ((len(sig_procs) - 1) / 2 - i) * sig_height
                yields = get_yields(proc)
                bars = ax_sig.barh(
                    y_pos + offset,
                    yields,
                    height=sig_height,
                    fill=False,
                    edgecolor=proc.color,
                    linewidth=2,
                    label=remove_label_placeholders(proc.label),
                )
                sig_label_requests.append((bars, yields))
            ax_sig.margins(x=0.3)
            ax_sig.tick_params(axis="x", labelsize=12)
            if not self.skip_legend:
                ax_sig.legend(fontsize=11, loc="upper right", framealpha=0.8, frameon=True)

        ax.set_yticks(y_pos, cat_labels)
        if not skip_labels:
            ax.set_xlabel("Yield")
            ax.set_ylabel("Category")
        ax.tick_params(axis="y", labelsize=14)

        if not self.skip_legend:
            ax.legend(
                framealpha=0.8,
                fontsize=14 if skip_labels else 13,
                loc="upper right",
                frameon=True,
            )

        lumi = sum([_config_inst.x.luminosity for _config_inst in self.config_insts])

        com = self.config_insts[0].campaign.ecm
        if self.cms_label != "skip":
            cms_label_kwargs = get_cms_label(ax, self.cms_label)
            cms_label_kwargs.update({
                "com": com,
                "lumi": f"{0.001 * lumi.get('nominal'):.1f}",  # /pb -> /fb
                "fontsize": 18,
            })
            mplhep.cms.label(**cms_label_kwargs)
        plt.tight_layout()

        # add bar labels now that the layout is fixed
        for bars, yields in bkg_label_requests:
            add_fitting_labels(ax, bars, yields)
        for bars, yields in sig_label_requests:
            add_fitting_labels(ax_sig, bars, yields, outside=True, fontsize=10)

        # save the plot
        outputs = self.output()
        outputs["plot"].dump(fig, formatter="mpl")
