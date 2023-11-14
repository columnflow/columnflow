"""
Test the plot_ml_evaluation module.
"""

__all__ = ["TestPlotCM"]

import unittest
from unittest.mock import MagicMock

from columnflow.util import maybe_import
from columnflow.plotting.plot_ml_evaluation import plot_cm


np = maybe_import("numpy")
ak = maybe_import("awkward")
plt = maybe_import("matplotlib.pyplot")


class TestPlotCM(unittest.TestCase):

    def setUp(self):
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
        self.weighted_matrix = np.array([[0.25, 0.25, 0.5], [0.25, 0, 0.75]])
        self.unweighted_matrix = np.array([[0.25, 0.25, 0.5], [0.25, 0, 0.75]])
        self.not_normalized_matrix = np.array([[1, 1, 2], [1, 0, 3]])
        self.column_normalized_matrix = np.array([[0.5, 1, 0.4], [0.5, 0, 0.6]])

    def test_plot_cm(self):
        fig, cm = plot_cm(
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
        self.assertEqual(cm.shape, (2, 3))
        self.assertEqual(cm.tolist(), self.weighted_matrix.tolist())

    def test_plot_cm_no_weights(self):
        fig, cm = plot_cm(
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
        self.assertEqual(cm.shape, (2, 3))
        self.assertEqual(cm.tolist(), self.unweighted_matrix.tolist())

    def test_plot_cm_skip_uncertainties(self):
        fig, cm = plot_cm(
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
        self.assertEqual(cm.shape, (2, 3))
        self.assertEqual(cm.tolist(), self.weighted_matrix.tolist())

    def test_plot_cm_no_labels(self):
        fig, cm = plot_cm(
            events=self.events,
            config_inst=self.config_inst,
            category_inst=self.category_inst,
            sample_weights=self.sample_weights,
            normalization=self.normalization,
            skip_uncertainties=self.skip_uncertainties,
        )
        x_labels = ["out0", "out1", "out2"]
        y_labels = ["dataset_1", "dataset_2"]
        self.assertEqual([t.get_text() for t in fig[0].axes[0].get_xticklabels()], x_labels)
        self.assertEqual([t.get_text() for t in fig[0].axes[0].get_yticklabels()], y_labels)

    def test_plot_cm_labels(self):
        x_labels = ["vbf", "ggf", "other"]
        y_labels = ["Higgs", "Graviton"]
        fig, cm = plot_cm(
            events=self.events,
            config_inst=self.config_inst,
            category_inst=self.category_inst,
            sample_weights=self.sample_weights,
            normalization=self.normalization,
            skip_uncertainties=self.skip_uncertainties,
            x_labels=x_labels,
            y_labels=y_labels,
        )
        self.assertEqual([t.get_text() for t in fig[0].axes[0].get_xticklabels()], x_labels)
        self.assertEqual([t.get_text() for t in fig[0].axes[0].get_yticklabels()], y_labels)

    def test_plot_cm_invalid_normalization(self):
        with self.assertRaises(ValueError):
            plot_cm(
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
        fig, cm = plot_cm(
            events=self.events,
            config_inst=self.config_inst,
            category_inst=self.category_inst,
            normalization=None,
            skip_uncertainties=self.skip_uncertainties,
            x_labels=self.x_labels,
            y_labels=self.y_labels,
        )
        self.assertEqual(cm.shape, (2, 3))
        self.assertEqual(cm.tolist(), self.not_normalized_matrix.tolist())

    def test_plot_cm_column_normalization(self):
        fig, cm = plot_cm(
            events=self.events,
            config_inst=self.config_inst,
            category_inst=self.category_inst,
            normalization="column",
            skip_uncertainties=self.skip_uncertainties,
            x_labels=self.x_labels,
            y_labels=self.y_labels,
        )
        self.assertEqual(cm.shape, (2, 3))
        self.assertEqual(cm.tolist(), self.column_normalized_matrix.tolist())

    def test_plot_cm_mismatched_weights_shape(self):
        sample_weights = [1, 2, 3]
        with self.assertRaises(ValueError):
            plot_cm(
                events=self.events,
                config_inst=self.config_inst,
                category_inst=self.category_inst,
                sample_weights=sample_weights,
                normalization=self.normalization,
                skip_uncertainties=self.skip_uncertainties,
                x_labels=self.x_labels,
                y_labels=self.y_labels,
            )


if __name__ == "__main__":
    unittest.main()
