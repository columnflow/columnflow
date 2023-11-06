# coding: utf-8

"""
Useful plot functions for ML Evaluation
"""

from __future__ import annotations
from typing import Iterable

from columnflow.util import maybe_import

ak = maybe_import("awkward")
od = maybe_import("order")
np = maybe_import("numpy")
sci = maybe_import("scinum")
plt = maybe_import("matplotlib.pyplot")
hep = maybe_import("mplhep")
colors = maybe_import("matplotlib.colors")

# Define a CF custom color map
cf_colors = {
    "cf_green_cmap": colors.ListedColormap(["#212121", "#242723", "#262D25", "#283426", "#2A3A26", "#2C4227", "#2E4927",
                                            "#305126", "#325A25", "#356224", "#386B22", "#3B7520", "#3F7F1E", "#43891B",
                                            "#479418", "#4C9F14", "#52AA10", "#58B60C", "#5FC207", "#67cf02"]),
    "cf_ygb_cmap": colors.ListedColormap(["#003675", "#005B83", "#008490", "#009A83", "#00A368", "#00AC49", "#00B428",
                                          "#00BC06", "#0CC300", "#39C900", "#67cf02", "#72DB02", "#7EE605", "#8DF207",
                                          "#9CFD09", "#AEFF0B", "#C1FF0E", "#D5FF10", "#EBFF12", "#FFFF14"]),
    "cf_cmap": colors.ListedColormap(["#002C9C", "#00419F", "#0056A2", "#006BA4", "#0081A7", "#0098AA", "#00ADAB",
                                      "#00B099", "#00B287", "#00B574", "#00B860", "#00BB4C", "#00BD38", "#00C023",
                                      "#00C20D", "#06C500", "#1EC800", "#36CA00", "#4ECD01", "#67cf02"]),
    "viridis": colors.ListedColormap(["#263DA8", "#1652CC", "#1063DB", "#1171D8", "#1380D5", "#0E8ED0", "#089DCC",
                                      "#0DA7C2", "#1DAFB3", "#2DB7A3", "#52BA91", "#73BD80", "#94BE71", "#B2BC65",
                                      "#D0BA59", "#E1BF4A", "#F4C53A", "#FCD12B", "#FAE61C", "#F9F90E"]),
}


def plot_ml_evaluation(
        events: ak.Array,
        config_inst: od.Config,
        category_inst: od.Category,
        **kwargs,
) -> plt.Figure:
    return None


def create_sample_weights(sample_weights: Iterable | bool | None,
                          events: dict,
                          true_labels: Iterable,
                          ) -> dict:
    """ Helper function to creates the sample weights for the events, if needed.

            Args:
                sample_weights (np.ndarray or bool, optional): sample weights of the events. If an explicit array is not
                                givin the weights are calculated based on the number of eventsDefaults to None.
                events (dict): dictionary with the true labels as keys and the model output of \
                    the events as values.
                true_labels (np.ndarray): true labels of the events

            Returns:
                dict: sample weights of the events

            Raises:
                AssertionError: If both predictions and labels have mismatched shapes, \
                    or if `weights` is not `None` and its shape doesn't match `predictions`.
        """
    if not sample_weights:
        return {label: 1 for label in true_labels}
    else:
        assert (isinstance(sample_weights, bool) or (len(sample_weights) == len(true_labels))), (
            f"Shape of sample_weights {sample_weights.shape} does not match "
            f"shape of predictions {len(true_labels)}")
        if isinstance(sample_weights, bool):
            size = {label: len(event) for label, event in events.items()}
            mean = np.mean(list(size.values()))
            sample_weights = {label: mean / length for label, length in size.items()}
        return sample_weights


def plot_cm(
        events: dict,
        config_inst: od.Config,
        category_inst: od.Category,
        sample_weights: list | bool = False,
        normalization: str = "row",
        skip_uncertainties: bool = False,
        x_labels: list[str] = None,
        y_labels: list[str] = None,
        *args,
        **kwargs,
) -> tuple[plt.Figure, np.ndarray]:
    """ Generates the figure of the confusion matrix given the output of the nodes
    and a true labels array. The Cronfusion matrix can also be weighted

        Args:
            events (dict): dictionary with the true labels as keys and the model output of \
                the events as values.
            config_inst (od.Config): used configuration for the plot
            category_inst (od.Category): used category instance, for which the plot is created
            sample_weights (np.ndarray or bool, optional): sample weights of the events. If an explicit array is not
                            givin the weights are calculated based on the number of eventsDefaults to None.
            normalization (str, optional): type of normalization of the confusion matrix. Defaults to "row".
            skip_uncertainties (bool, optional): calculate errors of the cm elements. Defaults to False.
            x_labels (list[str], optional): labels for the x-axis. Defaults to None.
            y_labels (list[str], optional): labels for the y-axis. Defaults to None.
            *args: Additional arguments to pass to the function.
            **kwargs: Additional keyword arguments to pass to the function.

        Returns:
            tuple[plt.Figure, np.ndarray]: The resulting plot and the confusion matrix.

        Raises:
            AssertionError: If both predictions and labels have mismatched shapes, \
                or if `weights` is not `None` and its shape doesn't match `predictions`.
            AssertionError: If `normalization` is not one of `None`, `"row"`, `"column"`.

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
            vecNumber = np.vectorize(lambda n, count: sci.Number(n, float(n / np.sqrt(count))))
            result = vecNumber(result, counts)

        # Normalize Matrix if needed
        if normalization:
            valid = {"row": 1, "column": 0}
            assert (normalization in valid.keys()), (
                f"\"{normalization}\" is no valid argument for normalization. If givin, normalization "
                "should only take \"row\" or \"column\"")

            row_sums = result.sum(axis=valid.get(normalization))
            result = result / row_sums[:, np.newaxis]

        return result

    def plot_confusion_matrix(cm: np.ndarray,
                              title: str = "",
                              colormap: str = "cf_cmap",
                              cmap_label: str = "Accuracy",
                              digits: int = 3,
                              x_labels: list[str] = None,
                              y_labels: list[str] = None,
                              *args,
                              **kwargs,
                              ) -> plt.figure:
        """
        Plots a confusion matrix.

        Args:
            cm (np.ndarray): The confusion matrix to plot.
            title (str): The title of the plot, displayed in the top right corner. Defaults to ''.
            colormap (str): The name of the colormap to use. Defaults to "cf_cmap".
            cmap_label (str): The label of the colorbar. Defaults to "Accuracy".
            digits (int): The number of digits to display for each value in the matrix. Defaults to 3.
            x_labels (list[str]): The labels for the x-axis. If not provided, the labels will be "out<i>"
            y_labels (list[str]): The labels for the y-axis. If not provided, the dataset labels are used.
            *args: Additional arguments to pass to the function.
            **kwargs: Additional keyword arguments to pass to the function.

        Returns:
            plt.figure: The resulting plot.
        """
        from mpl_toolkits.axes_grid1 import make_axes_locatable

        def calculate_font_size():
            # Get cell width
            bbox = ax.get_window_extent().transformed(fig.dpi_scale_trans.inverted())
            width, height = fig.dpi * bbox.width, fig.dpi * bbox.height

            # Size of each cell in pixels
            cell_width = width / n_classes
            cell_height = height / n_processes

            # Calculate the font size based on the cell size to ensure font is not too large
            font_size = min(cell_width, cell_height) / 10
            font_size = max(min(font_size, 18), 8)

            return font_size

        def get_errors(matrix):
            """Useful for seperating the error from the data"""
            if matrix.dtype.name == "object":
                get_errors_vec = np.vectorize(lambda x: x.get(sci.UP, unc=True))
                return get_errors_vec(matrix)
            else:
                return np.zeros_like(matrix)

        def value_text(i, j):
            """Format the inputs as 'Number +- Uncertainty' """
            import re
            def fmt(v):
                s = "{{:.{}f}}".format(digits).format(v)
                return s if re.sub(r"(0|\.)", "", s) else ("<" + s[:-1] + "1")
            if skip_uncertainties:
                return fmt(values[i][j])
            else:
                return "{}\n\u00B1{}".format(fmt(values[i][j]), fmt(np.nan_to_num(uncs[i][j])))

        # create the plot
        plt.style.use(hep.style.CMS)
        fig, ax = plt.subplots(dpi=300)

        # Some useful variables and functions
        n_processes = cm.shape[0]
        n_classes = cm.shape[1]
        cmap = cf_colors.get(colormap, cf_colors["cf_cmap"])
        x_labels = x_labels if x_labels else [f"out{i}" for i in range(n_classes)]
        y_labels = y_labels if y_labels else true_labels
        font_ax = 20
        font_label = 20
        font_text = calculate_font_size()

        # Get values and (if available) their uncertenties
        values = cm.astype(np.float32)
        uncs = get_errors(cm)

        # Remove Major ticks and edit minor ticks
        minor_tick_length = max(int(120 / n_classes), 12) / 2
        minor_tick_width = max(6 / n_classes, 0.6)
        xtick_marks = np.arange(n_classes)
        ytick_marks = np.arange(n_processes)

        # plot the data
        im = ax.imshow(values, interpolation="nearest", cmap=cmap)

        # Plot settings
        thresh = values.max() / 2.
        ax.tick_params(axis="both", which="major",
                       bottom=False, top=False, left=False, right=False)
        ax.tick_params(axis="both", which="minor",
                       bottom=True, top=True, left=True, right=True,
                       length=minor_tick_length, width=minor_tick_width)
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

        # Add Matrix Elemtns
        for i in range(values.shape[0]):
            for j in range(values.shape[1]):
                ax.text(j, i, value_text(i, j), fontdict={"size": font_text},
                        horizontalalignment="center", verticalalignment="center",
                        color="white" if values[i, j] < thresh else "black")

        # final touches
        hep.cms.label(ax=ax, llabel="private work",
                    rlabel=title if title else "")
        plt.tight_layout()

        return fig

    cm = get_conf_matrix(sample_weights, *args, **kwargs)
    fig = plot_confusion_matrix(cm, x_labels=x_labels, y_labels=y_labels, *args, **kwargs)

    return [fig], cm
