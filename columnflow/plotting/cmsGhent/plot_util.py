from __future__ import annotations
import order as od
from columnflow.util import maybe_import

hist = maybe_import("hist")
np = maybe_import("numpy")


def cumulate(h: np.ndarray | hist.Hist, direction="below", axis: str | int | od.Variable = 0):
    idx_slice = np.s_[::-1] if direction == "above" else np.s_[:]
    arr = h if isinstance(h, np.ndarray) else h.view(flow=False)
    if isinstance(axis, od.Variable):
        axis = axis.name
    if isinstance(axis, str):
        assert isinstance(h, hist.Hist), "str axis only allowed for hist.Hist"
        ax_names = [ax.name for ax in h.axes]
        assert axis in ax_names, f"requested axis not available in histrogram axes {', '.join(ax_names)}"
        axis = ax_names.index(axis)
    arr = np.cumsum(arr[idx_slice], axis=axis)[idx_slice]
    if isinstance(h, np.ndarray):
        return arr
    h_cum = h.copy()
    h_cum.view(flow=False)[:] = arr
    return h_cum
