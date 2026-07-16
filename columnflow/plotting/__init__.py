# coding: utf-8

from __future__ import annotations

from columnflow.types import Any, Callable


# helpers to decorate plot functions to assign them inspectable features
def _add_plot_feature(plot_func: Callable, feature_name: str, feature_value: Any | None = None) -> None:
    if getattr(plot_func, "_plot_features", None) is None:
        plot_func._plot_features = {}
    plot_func._plot_features[feature_name] = feature_value


def _get_plot_features(plot_func: Callable) -> dict[str, Any]:
    return getattr(plot_func, "_plot_features", {})


def supports_multi_variable(plot_func: Callable) -> Callable:
    _add_plot_feature(plot_func, "multi_variable")
    return plot_func


def check_multi_variable_support(plot_func: Callable) -> bool:
    return "multi_variable" in _get_plot_features(plot_func)


def supports_multi_category(plot_func: Callable) -> Callable:
    _add_plot_feature(plot_func, "multi_category")
    return plot_func


def check_multi_category_support(plot_func: Callable) -> bool:
    return "multi_category" in _get_plot_features(plot_func)
