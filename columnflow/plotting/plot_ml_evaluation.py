# coding: utf-8

"""
Useful plot functions for ML Evaluation
"""

from __future__ import annotations

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


def plot_cm(
        events: dict,
        config_inst: od.Config,
        category_inst: od.Category,
        sample_weights: np.ndarray = None,
        normalization: str = "row",
        skip_uncertainties: bool = False,
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
            sample_weights (np.ndarray, optional): sample weights of the events. Defaults to None.
            normalization (str, optional): type of normalization of the confusion matrix. Defaults to "row".
            skip_uncertainties (bool, optional): calculate errors of the cm elements. Defaults to False.

        Returns:
            plt.Figure: The plot to be saved in the task. The matrix has

        Raises:
            AssertionError: If both predictions and labels have mismatched shapes, \
                or if `weights` is not `None` and its shape doesn't match `predictions`.
    """

    # defining some useful properties and output shapes
    true_lables = list(events.keys())
    pred_lables = [s.removeprefix('score_') for s in list(events.values())[0].fields]
    return_type = np.float32 if sample_weights else np.int32
    mat_shape = (len(true_lables), len(pred_lables))

    def get_conf_matrix() -> np.ndarray:
        # TODO implement weights assertion and processing

        result = np.zeros(shape=mat_shape, dtype=return_type)
        counts = np.zeros(shape=mat_shape, dtype=return_type)

        # looping over the datasets
        for ind, pred in enumerate(events.values()):
            # remove awkward structure to use the numpy logic
            pred = ak.to_numpy(pred)
            pred = pred.view(float).reshape((pred.size, len(pred_lables)))

            # create predictions of the model output
            pred = np.argmax(pred, axis=-1)

            for index, count in zip(*np.unique(pred, return_counts=True)):
                result[ind, index] += count
                counts[ind, index] += count

        if not skip_uncertainties:
            vecNumber = np.vectorize(lambda n, count: sci.Number(n, float(n / np.sqrt(count))))
            result = vecNumber(result, counts)

        # Normalize Matrix if needed
        if normalization is not None:
            valid = {"row": 1, "column": 0}
            assert (normalization in valid.keys()), (
                f"\"{normalization}\" is no valid argument for normalization. If givin, normalization \
                    should only take \"row\" or \"column\"")

            row_sums = result.sum(axis=valid.get(normalization))
            result = result / row_sums[:, np.newaxis]

        return result

    def plot_confusion_matrix(cm: np.ndarray,
                          title="",
                          colormap: str = "cf_cmap",
                          cmap_label: str = "Accuracy",
                          digits: int = 3,
                              ) -> plt.figure:
        """plots a givin confusion matrix

        Args:
            cm (np.ndarray): _description_
            title (str, optional): _description_. Defaults to "Confusion matrix".
            colormap (str, optional): _description_. Defaults to "cf_cmap".
            cmap_label (str, optional): _description_. Defaults to "Accuracy".
            digits (int, optional): _description_. Defaults to 3.

        Returns:
            plt.figure: _description_
        """

        # Some useful variables and functions
        n_processes = cm.shape[0]
        n_classes = cm.shape[1]
        cmap = cf_colors.get(colormap, cf_colors["cf_cmap"])

        def scale_font(class_number: int) -> int:
            """function (defined emperically) to scale the font"""
            if class_number > 10:
                return max(8, int(- 8 / 10 * class_number + 23))
            else:
                return int(class_number / 14 * (9 * class_number - 177) + 510 / 7)

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

        # Get values and (if available) their uncertenties
        values = cm.astype(np.float32)
        uncs = get_errors(cm)


        # Setting some plotting values
        thresh = values.max() / 2.
        font_size = scale_font(n_classes)

        # Remove Major ticks and edit minor ticks
        # plt.style.use(hep.style.CMS)
        # hep.cms.label(llabel="private work",
        #             rlabel=title if title is not None else "")
        minor_tick_length = max(int(120 / n_classes), 12)
        minor_tick_width = max(6 / n_classes, 0.6)
        xtick_marks = np.arange(n_classes)
        ytick_marks = np.arange(n_processes)
        plt.tick_params(axis="both", which="major",
                        bottom=False, top=False, left=False, right=False)
        plt.tick_params(axis="both", which="minor",
                        bottom=True, top=True, left=True, right=True,
                        length=minor_tick_length, width=minor_tick_width)
        plt.xticks(xtick_marks + 0.5, minor=True)
        plt.yticks(ytick_marks + 0.49, minor=True)
        plt.xticks(xtick_marks, pred_lables, rotation=0)#, fontsize=font_size)
        plt.yticks(ytick_marks, true_lables)#, fontsize=font_size)
        plt.xlabel("Predicted process", loc="right", labelpad=10) #,fontsize=font_size + 3)
        plt.ylabel("True process", loc="top", labelpad=15) #, fontsize=font_size)
        plt.tight_layout()

        # plotting
        plt.imshow(values, interpolation="nearest", cmap=cmap)

        # Justify Color bar
        colorbar = plt.colorbar(fraction=0.0471, pad=0.01)
        colorbar.set_label(label=cmap_label)#, fontsize=font_size + 3)
        # colorbar.ax.tick_params(labelsize=font_size)
        plt.clim(0, max(1, values.max()))

        # Add Matrix Elemtns
        # offset = 0.12 if len(class_labels) > 2 and len(class_labels) < 6 else 0.1
        # size_offset = 1 if len(class_labels) > 5 else 3
        for i in range(values.shape[0]):
            for j in range(values.shape[1]):
                plt.text(j, i, value_text(i, j), #fontdict={"size": font_size},
                        horizontalalignment="center", verticalalignment="center",
                        color="white" if values[i, j] < thresh else "black")

        # Add Axes and plot labels
        from IPython import embed; embed()

        return plt.gcf()

    cm = get_conf_matrix()
    fig = plot_confusion_matrix(cm, *args, **kwargs)
