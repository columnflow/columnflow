"""
Test the plot_ml_evaluation module.
"""

__all__ = ["TestPlotUtil", "TestPlotCM", "TestPlotROC"]

import io
import unittest
from unittest.mock import MagicMock
from contextlib import redirect_stdout

from columnflow.util import maybe_import


np = maybe_import("numpy")
ak = maybe_import("awkward")
plt = maybe_import("matplotlib.pyplot")
hist = maybe_import("hist")


class TestPlotUtil(unittest.TestCase):
    def setUp(self):
        # create dummy histogram similar to default columnflow histograms
        self.hist = (
            hist.Hist.new
            .IntCat([], name="category", growth=True)
            .IntCat([], name="process", growth=True)
            .IntCat([], name="shift", growth=True)
            .Var(np.arange(0, 1.01, 1 / 40), name="var1", label="var1")
            .Var(np.arange(0, 100.1, 10), name="var2", label="var2")
            .Weight()
        )

        # fill histogram with dummy values such that variable axis typically contain entries in flow bins
        self.hist.fill(
            category=np.random.choice([1, 2], size=1000),
            process=np.random.choice([1, 2], size=1000),
            shift=np.random.choice([0], size=1000),
            var1=np.random.normal(loc=0, scale=1, size=1000),
            var2=np.random.normal(loc=50, scale=100, size=1000),
            weight=np.random.normal(loc=1, scale=0.1, size=1000),
        )

    def test_use_flow_bins(self):
        from columnflow.plotting.plot_util import use_flow_bins
        input_hist_copy = self.hist.copy()
        flow_hist = use_flow_bins(self.hist, "var1")

        # input hist should be unaffected
        self.assertEqual(self.hist, input_hist_copy)

        # moving flow bins should not affect the overall sum
        self.assertAlmostEqual(self.hist.sum(flow=True).value, flow_hist.sum(flow=True).value)
        self.assertAlmostEqual(self.hist.sum(flow=True).variance, flow_hist.sum(flow=True).variance)

        # hists reduced to axis "var1"
        reduced_pre = self.hist[::sum, ::sum, ::sum, :, ::sum].view(flow=True)
        reduced_post = flow_hist[::sum, ::sum, ::sum, :, ::sum].view(flow=True)

        # check that underflow values have been moved correctly
        self.assertEqual(reduced_post[0].value, 0)
        self.assertEqual(reduced_post[0].variance, 0)
        self.assertAlmostEqual(reduced_post[1].value, reduced_pre[0].value + reduced_pre[1].value)
        self.assertAlmostEqual(reduced_post[1].variance, reduced_pre[0].variance + reduced_pre[1].variance)

        # check that overflow values have been moved correctly
        self.assertEqual(reduced_post[-1].value, 0)
        self.assertEqual(reduced_post[-1].variance, 0)
        self.assertAlmostEqual(reduced_post[-2].value, reduced_pre[-1].value + reduced_pre[-2].value)
        self.assertAlmostEqual(reduced_post[-2].variance, reduced_pre[-1].variance + reduced_pre[-2].variance)

        # when using flow bins from all axes, all flow bins should be empty
        flow_hist_both_axes = use_flow_bins(flow_hist, "var2")
        self.assertAlmostEqual(self.hist.sum(flow=True).value, flow_hist_both_axes.sum(flow=False).value)
        self.assertAlmostEqual(self.hist.sum(flow=True).variance, flow_hist_both_axes.sum(flow=False).variance)

        # nothing is done when both overflow and underflow are False
        self.assertEqual(self.hist, use_flow_bins(self.hist, "var1", underflow=False, overflow=False))

        # setting overflow to False leaves everything except the underflow + first bin unaffected
        self.assertEqual(
            self.hist[:, :, :, 1:, :],
            use_flow_bins(self.hist, "var1", overflow=False)[:, :, :, 1:, :],
        )

        # setting underflow to False leaves everything except the overflow + last bin unaffected
        self.assertEqual(
            self.hist[:, :, :, :(self.hist.shape[3] - 1), :],
            use_flow_bins(self.hist, "var1", underflow=False)[:, :, :, :(self.hist.shape[3] - 1), :],
        )

        # raises Expection when hist does not contain flow bins
        hist_without_flow = hist.Hist.new.Var(range(10), name="var", flow=False).Weight()
        with self.assertRaises(Exception):
            use_flow_bins(hist_without_flow, "var")


class TestPlotCM(unittest.TestCase):

    def setUp(self):
        from columnflow.plotting.plot_ml_evaluation import plot_cm
        self.plot_cm = plot_cm
        self.events = {
            "dataset_1": ak.Array({
                "out1": [0.1, 0.1, 0.3, 0.5],
                "out2": [0.2, 0.2, 0.4, 0.2],
                "out3": [0.7, 0.7, 0.3, 0.3],
            }),
            "dataset_2": ak.Array({
                "out1": [0.2, 0.2, 0.4, 0.3],
                "out2": [0.3, 0.3, 0.3, 0.2],
                "out3": [0.5, 0.5, 0.3, 0.5],
            }),
        }
        self.config_inst = MagicMock()
        self.category_inst = MagicMock()
        self.sample_weights = [1, 2]
        self.normalization = "row"
        self.skip_uncertainties = False
        self.x_labels = ["out1", "out2", "out3"]
        self.y_labels = ["dataset_1", "dataset_2"]

        # The following results are calculated by hand
        self.weighted_matrix = np.array([[0.25, 0.25, 0.5], [0.25, 0, 0.75]])
        self.unweighted_matrix = np.array([[0.25, 0.25, 0.5], [0.25, 0, 0.75]])
        self.not_normalized_matrix = np.array([[1, 1, 2], [1, 0, 3]])
        self.column_normalized_matrix = np.array([[0.5, 1, 0.4], [0.5, 0, 0.6]])
        self.text_trap = io.StringIO()

    def test_plot_cm(self):
        with redirect_stdout(self.text_trap):
            fig, cm = self.plot_cm(
                events=self.events,
                config_inst=self.config_inst,
                category_inst=self.category_inst,
                sample_weights=self.sample_weights,
                normalization=self.normalization,
                skip_uncertainties=self.skip_uncertainties,
                x_labels=self.x_labels,
                y_labels=self.y_labels,
            )
        self.assertIsInstance(fig, list)
        self.assertIsInstance(fig[0], plt.Figure)
        self.assertIsInstance(cm, np.ndarray)
        self.assertTupleEqual(cm.shape, (2, 3))
        self.assertListEqual(cm.tolist(), self.weighted_matrix.tolist())

    def test_plot_cm_no_weights(self):
        with redirect_stdout(self.text_trap):
            fig, cm = self.plot_cm(
                events=self.events,
                config_inst=self.config_inst,
                category_inst=self.category_inst,
                normalization=self.normalization,
                skip_uncertainties=self.skip_uncertainties,
                x_labels=self.x_labels,
                y_labels=self.y_labels,
            )
        self.assertIsInstance(fig, list)
        self.assertIsInstance(fig[0], plt.Figure)
        self.assertIsInstance(cm, np.ndarray)
        self.assertTupleEqual(cm.shape, (2, 3))
        self.assertListEqual(cm.tolist(), self.unweighted_matrix.tolist())

    def test_plot_cm_skip_uncertainties(self):
        with redirect_stdout(self.text_trap):
            fig, cm = self.plot_cm(
                events=self.events,
                config_inst=self.config_inst,
                category_inst=self.category_inst,
                sample_weights=self.sample_weights,
                normalization=self.normalization,
                skip_uncertainties=True,
                x_labels=self.x_labels,
                y_labels=self.y_labels,
            )
        self.assertIsInstance(fig, list)
        self.assertIsInstance(fig[0], plt.Figure)
        self.assertIsInstance(cm, np.ndarray)
        self.assertTupleEqual(cm.shape, (2, 3))
        self.assertListEqual(cm.tolist(), self.weighted_matrix.tolist())

    def test_plot_cm_no_labels(self):
        with redirect_stdout(self.text_trap):
            fig, cm = self.plot_cm(
                events=self.events,
                config_inst=self.config_inst,
                category_inst=self.category_inst,
                sample_weights=self.sample_weights,
                normalization=self.normalization,
                skip_uncertainties=self.skip_uncertainties,
            )
        x_labels = ["out0", "out1", "out2"]
        y_labels = ["dataset_1", "dataset_2"]
        self.assertListEqual([t.get_text() for t in fig[0].axes[0].get_xticklabels()], x_labels)
        self.assertListEqual([t.get_text() for t in fig[0].axes[0].get_yticklabels()], y_labels)

    def test_plot_cm_labels(self):
        x_labels = ["vbf", "ggf", "other"]
        y_labels = ["Higgs", "Graviton"]
        with redirect_stdout(self.text_trap):
            fig, cm = self.plot_cm(
                events=self.events,
                config_inst=self.config_inst,
                category_inst=self.category_inst,
                sample_weights=self.sample_weights,
                normalization=self.normalization,
                skip_uncertainties=self.skip_uncertainties,
                x_labels=x_labels,
                y_labels=y_labels,
            )
        self.assertListEqual([t.get_text() for t in fig[0].axes[0].get_xticklabels()], x_labels)
        self.assertListEqual([t.get_text() for t in fig[0].axes[0].get_yticklabels()], y_labels)

    def test_plot_cm_invalid_normalization(self):
        with self.assertRaises(ValueError), redirect_stdout(self.text_trap):
            self.plot_cm(
                events=self.events,
                config_inst=self.config_inst,
                category_inst=self.category_inst,
                sample_weights=self.sample_weights,
                normalization="invalid",
                skip_uncertainties=self.skip_uncertainties,
                x_labels=self.x_labels,
                y_labels=self.y_labels,
            )

    def test_plot_cm_no_normalization(self):
        with redirect_stdout(self.text_trap):
            fig, cm = self.plot_cm(
                events=self.events,
                config_inst=self.config_inst,
                category_inst=self.category_inst,
                normalization=None,
                skip_uncertainties=self.skip_uncertainties,
                x_labels=self.x_labels,
                y_labels=self.y_labels,
            )
        self.assertTupleEqual(cm.shape, (2, 3))
        self.assertListEqual(cm.tolist(), self.not_normalized_matrix.tolist())

    def test_plot_cm_column_normalization(self):
        with redirect_stdout(self.text_trap):
            fig, cm = self.plot_cm(
                events=self.events,
                config_inst=self.config_inst,
                category_inst=self.category_inst,
                normalization="column",
                skip_uncertainties=self.skip_uncertainties,
                x_labels=self.x_labels,
                y_labels=self.y_labels,
            )
        self.assertTupleEqual(cm.shape, (2, 3))
        self.assertListEqual(cm.tolist(), self.column_normalized_matrix.tolist())

    def test_plot_cm_mismatched_weights_shape(self):
        sample_weights = [1, 2, 3]
        with self.assertRaises(ValueError), redirect_stdout(self.text_trap):
            self.plot_cm(
                events=self.events,
                config_inst=self.config_inst,
                category_inst=self.category_inst,
                sample_weights=sample_weights,
                normalization=self.normalization,
                skip_uncertainties=self.skip_uncertainties,
                x_labels=self.x_labels,
                y_labels=self.y_labels,
            )


class TestPlotROC(unittest.TestCase):

    def setUp(self):
        from columnflow.plotting.plot_ml_evaluation import plot_roc
        self.plot_roc = plot_roc
        self.events = {
            "dataset_1": ak.Array({
                "out1": [0.9, 0.9, 0.7, 0.4],
                "out2": [0.1, 0.1, 0.3, 0.6],
            }),
            "dataset_2": ak.Array({
                "out1": [0.2, 0.2, 0.4, 0.8],
                "out2": [0.8, 0.8, 0.6, 0.2],
            }),
        }
        self.n_discriminators = 2
        self.config_inst = MagicMock()
        self.category_inst = MagicMock()
        # The following results are calculated by hand
        self.results_dataset1_as_signal = {
            "out1": {
                "fpr": [0.5, 0.25, 0.25, 0],
                "tpr": [1, 0.75, 0.5, 0],
            },
            "out2": {
                "fpr": [0.75, 0.75, 0.5, 0],
                "tpr": [0.5, 0.25, 0, 0],
            },
        }
        self.text_trap = io.StringIO()

    def test_plot_roc_returns_figures_and_results(self):
        with redirect_stdout(self.text_trap):
            figs, results = self.plot_roc(self.events, self.config_inst, self.category_inst)
        self.assertIsInstance(figs, list)
        self.assertIsInstance(results, dict)

    def test_plot_roc_returns_correct_number_of_figures(self):
        with redirect_stdout(self.text_trap):
            figs_ovr, _ = self.plot_roc(self.events, self.config_inst, self.category_inst, evaluation_type="OvR")
            figs_ovo, _ = self.plot_roc(self.events, self.config_inst, self.category_inst, evaluation_type="OvO")

        self.assertEqual(len(figs_ovr), self.n_discriminators * len(self.events))
        self.assertEqual(len(figs_ovo), self.n_discriminators * len(self.events) * (len(self.events) - 1))

    def test_plot_roc_returns_correct_results(self):
        with redirect_stdout(self.text_trap):
            _, results = self.plot_roc(
                self.events,
                self.config_inst,
                self.category_inst,
                n_thresholds=5,
                evaluation_type="OvR",
            )
        fpr_out1 = results["out1"]["dataset_1_vs_rest"]["fpr"].tolist()
        tpr_out1 = results["out1"]["dataset_1_vs_rest"]["tpr"].tolist()
        fpr_out2 = results["out2"]["dataset_1_vs_rest"]["fpr"].tolist()
        tpr_out2 = results["out2"]["dataset_1_vs_rest"]["tpr"].tolist()
        self.assertListEqual(fpr_out1, self.results_dataset1_as_signal["out1"]["fpr"])
        self.assertListEqual(tpr_out1, self.results_dataset1_as_signal["out1"]["tpr"])
        self.assertListEqual(fpr_out2, self.results_dataset1_as_signal["out2"]["fpr"])
        self.assertListEqual(tpr_out2, self.results_dataset1_as_signal["out2"]["tpr"])

    def test_plot_roc_raises_value_error_for_invalid_evaluation_type(self):
        with self.assertRaises(ValueError), redirect_stdout(self.text_trap):
            self.plot_roc(self.events, self.config_inst, self.category_inst, evaluation_type="InvalidType")
