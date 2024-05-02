# coding: utf-8

"""
Useful plot functions for ML Evaluation
"""

from __future__ import annotations

import re

from columnflow.types import Sequence
from columnflow.util import maybe_import
from columnflow.plotting.plot_util import get_cms_label

ak = maybe_import("awkward")
od = maybe_import("order")
np = maybe_import("numpy")
sci = maybe_import("scinum")
plt = maybe_import("matplotlib.pyplot")
hep = maybe_import("mplhep")
colors = maybe_import("matplotlib.colors")

# define a CF custom color maps
cf_colors = {
    "cf_green_cmap": colors.ListedColormap([
        "#212121", "#242723", "#262D25", "#283426", "#2A3A26", "#2C4227", "#2E4927",
        "#305126", "#325A25", "#356224", "#386B22", "#3B7520", "#3F7F1E", "#43891B",
        "#479418", "#4C9F14", "#52AA10", "#58B60C", "#5FC207", "#67cf02",
    ]),
    "cf_ygb_cmap": colors.ListedColormap([
        "#003675", "#005B83", "#008490", "#009A83", "#00A368", "#00AC49", "#00B428",
        "#00BC06", "#0CC300", "#39C900", "#67cf02", "#72DB02", "#7EE605", "#8DF207",
        "#9CFD09", "#AEFF0B", "#C1FF0E", "#D5FF10", "#EBFF12", "#FFFF14",
    ]),
    "cf_cmap": colors.ListedColormap([
        "#002C9C", "#00419F", "#0056A2", "#006BA4", "#0081A7", "#0098AA", "#00ADAB",
        "#00B099", "#00B287", "#00B574", "#00B860", "#00BB4C", "#00BD38", "#00C023",
        "#00C20D", "#06C500", "#1EC800", "#36CA00", "#4ECD01", "#67cf02",
    ]),
    "viridis": colors.ListedColormap([
        "#263DA8", "#1652CC", "#1063DB", "#1171D8", "#1380D5", "#0E8ED0", "#089DCC",
        "#0DA7C2", "#1DAFB3", "#2DB7A3", "#52BA91", "#73BD80", "#94BE71", "#B2BC65",
        "#D0BA59", "#E1BF4A", "#F4C53A", "#FCD12B", "#FAE61C", "#F9F90E",
    ]),
}


def create_sample_weights(
    sample_weights: Sequence[float] | bool,
    events: dict,
    true_labels: Sequence[str],
) -> dict:
    """
    Helper function to create the sample weights for the events, if needed.

    :param sample_weights: sample weights of the events. If an explicit array is not given,
        the weights are calculated based on the number of events.
    :param events: dictionary with the true labels as keys and the model output of the events as values.
    :param true_labels: true labels of the events
    :return: sample weights of the events
    :raises ValueError: If both predictions and labels have mismatched shapes, or
        if *weights* is not *None* and its shape doesn't match the predictions length.
    """
    if not sample_weights:
        return {label: 1 for label in true_labels}
    if not (isinstance(sample_weights, bool) or (len(sample_weights) == len(true_labels))):
        raise ValueError(f"Shape of sample_weights {len(sample_weights)} does not match "
            f"shape of predictions {len(true_labels)}")
    if isinstance(sample_weights, bool):
        size = {label: len(event) for label, event in events.items()}
        mean = np.mean(list(size.values()))
        return {label: mean / length for label, length in size.items()}
    return dict(zip(true_labels, sample_weights))


def plot_cm(
    events: dict,
    config_inst: od.Config,
    category_inst: od.Category,
    sample_weights: list | bool = False,
    normalization: str = "row",
    skip_uncertainties: bool = False,
    x_labels: list[str] | None = None,
    y_labels: list[str] | None = None,
    cms_rlabel: str = "",
    cms_llabel: str = "pw",
    *args,
    **kwargs,
) -> tuple[list[plt.Figure], np.ndarray]:
    """
    Generates the figure of the confusion matrix given the output of the nodes
    and an array of true labels. The Cronfusion matrix can also be weighted.

    :param events: Dictionary with the true labels as keys and the model output of the events as values.
    :param config_inst: The used config instance, for which the plot is created.
    :param category_inst: The used category instance, for which the plot is created.
    :param sample_weights: Sample weights applied to the confusion matrix the events.
        If an explicit array is not given, the weights are calculated based on the number of events when set to *True*.
    :param normalization: The type of normalization of the confusion matrix.
        This parameter takes 'row', 'col' or '' (empty string) as argument.
        If not provided, the matrix is row normalized.
    :param skip_uncertainties: If true, no uncertainty of the cells will be shown in the plot.
    :param x_labels: The labels for the x-axis. If not provided, the labels will be 'out<i>'
    :param y_labels: The labels for the y-axis. If not provided, the dataset names are used.
    :param *args: Additional arguments to pass to the function.
    :param **kwargs: Additional keyword arguments to pass to the function.

    :return: Returns the resulting plot and the confusion matrix.

    :raises AssertionError: If both predictions and labels have mismatched shapes, or if *weights*
        is not *None* and its shape doesn't match *predictions*.
    :raises AssertionError: If *normalization* is not one of *None*, "row", "column".
    """
    # defining some useful properties and output shapes
    true_labels = list(events.keys())
    pred_labels = [s.removeprefix("score_") for s in list(events.values())[0].fields]
    return_type = np.float32 if sample_weights else np.int32
    mat_shape = (len(true_labels), len(pred_labels))

    def get_conf_matrix(sample_weights, *args, **kwargs) -> np.ndarray:
        result = np.zeros(shape=mat_shape, dtype=return_type)
        counts = np.zeros(shape=mat_shape, dtype=return_type)
        sample_weights = create_sample_weights(sample_weights, events, true_labels)

        # looping over the datasets
        for ind, (dataset, pred) in enumerate(events.items()):
            # remove awkward structure to use the numpy logic
            pred = ak.to_numpy(pred)
            pred = pred.view(float).reshape((pred.size, len(pred_labels)))

            # create predictions of the model output
            pred = np.argmax(pred, axis=-1)

            for index, count in zip(*np.unique(pred, return_counts=True)):
                result[ind, index] += count * sample_weights[dataset]
                counts[ind, index] += count

        if not skip_uncertainties:
            vecNumber = np.vectorize(lambda n, count: sci.Number(n, float(n / np.sqrt(count) if count else 0)))
            result = vecNumber(result, counts)

        # normalize Matrix if needed
        if normalization:
            valid = {"row": 1, "column": 0}
            if normalization not in valid.keys():
                raise ValueError(
                    f"'{normalization}' is not a valid argument for normalization. If given, normalization "
                    "should only take 'row' or 'column'",
                )

            row_sums = result.sum(axis=valid.get(normalization))
            result = result / row_sums[:, np.newaxis] if valid.get(normalization) else result / row_sums

        return result

    def plot_confusion_matrix(
        cm: np.ndarray,
        colormap: str = "cf_cmap",
        cmap_label: str = "Accuracy",
        digits: int = 3,
        x_labels: list[str] | None = None,
        y_labels: list[str] | None = None,
        cms_rlabel: str = "",
        cms_llabel: str = "private work",
        *args,
        **kwargs,
    ) -> plt.figure:
        """
        Plots a confusion matrix.

        :param cm: The confusion matrix to plot.
        :param title: The title of the plot, displayed in the top right corner.
        :param colormap: The name of the colormap to use. Can be selected from the following:
            "cf_cmap", "cf_green_cmap", "cf_ygb_cmap", "viridis".
        :param cmap_label: The label of the colorbar.
        :param digits: The number of digits to display for each value in the matrix.
        :param x_labels: The labels for the x-axis. If not provided, the labels will be "out<i>"
        :param y_labels: The labels for the y-axis. If not provided, the dataset names are used.
        :param *args: Additional arguments to pass to the function.
        :param **kwargs: Additional keyword arguments to pass to the function.

        :return: The resulting plot.
        """
        from mpl_toolkits.axes_grid1 import make_axes_locatable

        def calculate_font_size():
            # get cell width
            bbox = ax.get_window_extent().transformed(fig.dpi_scale_trans.inverted())
            width, height = fig.dpi * bbox.width, fig.dpi * bbox.height

            # size of each cell in pixels
            cell_width = width / n_classes
            cell_height = height / n_processes

            # calculate the font size based on the cell size to ensure font is not too large
            font_size = min(cell_width, cell_height) / 10
            font_size = max(min(font_size, 18), 8)

            return font_size

        def get_errors(matrix):
            """
            Useful for seperating the error from the data
            """
            if matrix.dtype.name == "object":
                get_errors_vec = np.vectorize(lambda x: x.get(sci.UP, unc=True))
                return get_errors_vec(matrix)
            return np.zeros_like(matrix)

        def value_text(i, j):
            """
            Format the inputs as 'Number +- Uncertainty'
            """
            def fmt(v):
                s = "{{:.{}f}}".format(digits).format(v)
                return s if re.sub(r"(0|\.)", "", s) else ("<" + s[:-1] + "1")
            if skip_uncertainties:
                return fmt(values[i][j])
            return "{}\n\u00B1{}".format(fmt(values[i][j]), fmt(np.nan_to_num(uncs[i][j])))

        # create the plot
        plt.style.use(hep.style.CMS)
        fig, ax = plt.subplots(dpi=300)

        # some useful variables and functions
        n_processes = cm.shape[0]
        n_classes = cm.shape[1]
        cmap = cf_colors.get(colormap, cf_colors["cf_cmap"])
        x_labels = x_labels if x_labels else [f"out{i}" for i in range(n_classes)]
        y_labels = y_labels if y_labels else true_labels
        font_ax = 20
        font_label = 20
        font_text = calculate_font_size()

        # get values and (if available) their uncertenties
        values = cm.astype(np.float32)
        uncs = get_errors(cm)

        # remove Major ticks and edit minor ticks
        minor_tick_length = max(int(120 / n_classes), 12) / 2
        minor_tick_width = max(6 / n_classes, 0.6)
        xtick_marks = np.arange(n_classes)
        ytick_marks = np.arange(n_processes)

        # plot the data
        im = ax.imshow(values, interpolation="nearest", cmap=cmap)

        # plot settings
        thresh = values.max() / 2.
        ax.tick_params(axis="both", which="major", bottom=False, top=False, left=False, right=False)
        ax.tick_params(
            axis="both",
            which="minor",
            bottom=True,
            top=True,
            left=True,
            right=True,
            length=minor_tick_length,
            width=minor_tick_width,
        )
        ax.set_xticks(xtick_marks + 0.5, minor=True)
        ax.set_yticks(ytick_marks + 0.5, minor=True)
        ax.set_xticks(xtick_marks)
        ax.set_xticklabels(x_labels, rotation=0, fontsize=font_label)
        ax.set_yticks(ytick_marks)
        ax.set_yticklabels(y_labels, fontsize=font_label)
        ax.set_xlabel("Predicted process", loc="right", labelpad=10, fontsize=font_ax)
        ax.set_ylabel("True process", loc="top", labelpad=15, fontsize=font_ax)

        # adding a color bar on a new axis and adjusting its values
        divider = make_axes_locatable(ax)
        cax = divider.append_axes("right", size="5%", pad=0.10)
        colorbar = fig.colorbar(im, cax=cax)
        colorbar.set_label(label=cmap_label, fontsize=font_ax)
        colorbar.ax.tick_params(labelsize=font_ax - 5)
        im.set_clim(0, max(1, values.max()))

        # add Matrix Elemtns
        for i in range(values.shape[0]):
            for j in range(values.shape[1]):
                ax.text(
                    j,
                    i,
                    value_text(i, j),
                    fontdict={"size": font_text},
                    horizontalalignment="center",
                    verticalalignment="center",
                    color="white" if values[i, j] < thresh else "black",
                )

        # final touches
        if cms_llabel != "skip":
            cms_label_kwargs = get_cms_label(ax=ax, llabel=cms_llabel)
            cms_label_kwargs["rlabel"] = cms_rlabel
            hep.cms.label(**cms_label_kwargs)
        plt.tight_layout()

        return fig

    cm = get_conf_matrix(sample_weights, *args, **kwargs)

    fig = plot_confusion_matrix(
        cm,
        x_labels=x_labels,
        y_labels=y_labels,
        cms_llabel=cms_llabel,
        cms_rlabel=cms_rlabel,
        *args,
        **kwargs,
    )
    print("Confusion matrix plotted!")

    return [fig], cm


def plot_roc(
    events: dict,
    config_inst: od.Config,
    category_inst: od.Category,
    sample_weights: Sequence | bool = False,
    n_thresholds: int = 200 + 1,
    skip_discriminators: list[str] | None = None,
    evaluation_type: str = "OvR",
    cms_rlabel: str = "",
    cms_llabel: str = "private work",
    *args,
    **kwargs,
) -> tuple[list[plt.Figure], dict]:
    """
    Generates the figure of the ROC curve given the output of the nodes
    and an array of true labels. The ROC curve can also be weighted.

    :param events: Dictionary with the true labels as keys and the model output of the events as values.
    :param config_inst: Used configuration for the plot.
    :param category_inst: Used category instance, for which the plot is created.
    :param sample_weights: Sample weights of the events. If an explicit array is not given, the weights are
        calculated based on the number of events.
    :param n_thresholds: Number of thresholds to use for the ROC curve.
    :param skip_discriminators: List of discriminators to skip.
    :param evaluation_type: Type of evaluation to use for the ROC curve. If not provided, the type is 'OvR'.
    :param cms_rlabel: Right label of the CMS label.
    :param cms_llabel: Left label of the CMS label.
    :param *args: Additional arguments to pass to the function.
    :param **kwargs: Additional keyword arguments to pass to the function.

    :return: The resulting plot and the ROC curve.

    :raises ValueError: If both predictions and labels have mismatched shapes, or if *weights*
        is not *None* and its shape doesn't match *predictions*.
    :raises ValueError: If *normalization* is not one of *None*, 'row', 'column'.
    """
    # defining some useful properties and output shapes
    thresholds = np.linspace(0, 1, n_thresholds)
    weights = create_sample_weights(sample_weights, events, list(events.keys()))
    discriminators = list(events.values())[0].fields
    figs = []
    if skip_discriminators is None:
        skip_discriminators = []

    if evaluation_type not in ["OvO", "OvR"]:
        raise ValueError(
            "Illeagal Argument! Evaluation Type can only be choosen as 'OvO' (One vs One)"
            "or 'OvR' (One vs Rest)",
        )

    def create_histograms(events: dict, sample_weights: dict, *args, **kwargs) -> dict[str, dict[str, np.ndarray]]:
        """
        Helper function to create histograms for the different discriminators and classes.
        """
        hists = {}
        for disc in discriminators:
            hists[disc] = {}
            for cls, predictions in events.items():
                hists[disc][cls] = (
                    sample_weights[cls] *
                    ak.to_numpy(np.histogram(predictions[disc], bins=thresholds)[0])
                )
        return hists

    def binary_roc_data(
        positiv_hist: np.ndarray,
        negativ_hist: np.ndarray,
        *args,
        **kwargs,
    ) -> tuple[np.ndarray, np.ndarray]:
        """
        Compute binary Receiver operating characteristic (ROC) values.
        Used as a helper function for the multi-dimensional ROC curve
        """
        # calculate the different rates
        fn = np.cumsum(positiv_hist)
        tn = np.cumsum(negativ_hist)
        tp = fn[-1] - fn
        fp = tn[-1] - tn

        tpr = tp / (tp + fn)
        fpr = fp / (fp + tn)

        return fpr, tpr

    def roc_curve_data(
        evaluation_type: str,
        histograms: dict,
        *args,
        **kwargs,
    ) -> dict[str, dict[str, np.ndarray]]:
        """
        Compute Receiver operating characteristic (ROC) values for a multi-dimensional output.
        """
        result = {}

        for disc in discriminators:
            if disc in skip_discriminators:
                continue

            tmp = {}

            # choose the evaluation type
            if (evaluation_type == "OvO"):
                for pos_cls, pos_hist in histograms[disc].items():
                    for neg_cls, neg_hist in histograms[disc].items():
                        if (pos_cls == neg_cls):
                            continue
                        fpr, tpr = binary_roc_data(
                            positiv_hist=pos_hist,
                            negativ_hist=neg_hist,
                            *args,
                            **kwargs,
                        )
                        tmp[f"{pos_cls}_vs_{neg_cls}"] = {"fpr": fpr, "tpr": tpr}

            elif (evaluation_type == "OvR"):
                for pos_cls, pos_hist in histograms[disc].items():
                    neg_hist = sum(
                        (
                            neg_pred
                            for neg_cls, neg_pred in histograms[disc].items()
                            if neg_cls != pos_cls
                        ),
                        np.zeros_like(pos_hist),
                    )

                    fpr, tpr = binary_roc_data(
                        positiv_hist=pos_hist,
                        negativ_hist=neg_hist,
                        *args,
                        **kwargs,
                    )
                    tmp[f"{pos_cls}_vs_rest"] = {"fpr": fpr, "tpr": tpr}

            result[disc] = tmp

        return result

    def plot_roc_curve(
        roc_data: dict,
        title: str,
        cms_rlabel: str = "",
        cms_llabel: str = "pw",
        *args,
        **kwargs,
    ) -> plt.figure:
        """
        Plots a ROC curve.
        """
        def auc_score(fpr: list, tpr: list, *args) -> np.float64:
            """
            Compute the area under the curve using the trapezoidal rule.
            """
            sign = 1
            if np.any(np.diff(fpr) < 0):
                if np.all(np.diff(fpr) > 0):
                    raise ValueError(f"x is neither increasing nor decreasing : {fpr}.")
                sign = -1

            return sign * np.trapz(tpr, fpr)

        fpr = roc_data["fpr"]
        tpr = roc_data["tpr"]

        plt.style.use(hep.style.CMS)
        fig, ax = plt.subplots(dpi=300)
        ax.set_xlabel("FPR", loc="right", labelpad=10, fontsize=25)
        ax.set_ylabel("TPR", loc="top", labelpad=15, fontsize=25)
        ax.set_xlim(0, 1)
        ax.set_ylim(0, 1)

        ax.plot(
            fpr,
            tpr,
            color="#67cf02",
            label=f"AUC = {auc_score(roc_data['fpr'], roc_data['tpr']):.3f}",
        )
        ax.plot([0, 1], [0, 1], color="black", linestyle="--")

        # setting titles and legend
        ax.legend(loc="lower right", fontsize=15)
        ax.set_title(title, wrap=True, pad=50, fontsize=30)
        if cms_llabel != "skip":
            cms_label_kwargs = get_cms_label(ax=ax, llabel=cms_llabel)
            cms_label_kwargs["rlabel"] = cms_rlabel
            hep.cms.label(**cms_label_kwargs)
        plt.tight_layout()

        return fig

    # create historgrams and calculate FPR and TPR
    histograms = create_histograms(events, weights, *args, **kwargs)

    results = roc_curve_data(evaluation_type, histograms, *args, **kwargs)

    # plotting
    for disc, roc in results.items():
        for cls, roc_data in roc.items():
            title = rf"{cls.replace('_vs_', ' VS ')} with {disc}"
            figs.append(plot_roc_curve(
                roc_data=roc_data,
                title=title,
                cms_rlabel=cms_rlabel,
                cms_llabel=cms_llabel,
                *args,
                **kwargs,
            ))
    print("ROC curves plotted!")

    results["thresholds"] = thresholds

    return figs, results
