# coding: utf-8

"""
Example plot functions for ML Evaluation
"""

from __future__ import annotations

from columnflow.util import maybe_import

ak = maybe_import("awkward")
od = maybe_import("order")
plt = maybe_import("matplotlib.pyplot")

def plot_ml_evaluation(
        events: ak.Array,
        config_inst: od.Config,
        category_inst: od.Category,
        **kwargs,
) -> plt.Figure:

    return None
