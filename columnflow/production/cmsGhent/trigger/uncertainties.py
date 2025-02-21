from __future__ import annotations
from typing import Callable, Sequence
from collections.abc import Collection

from columnflow.util import maybe_import
from columnflow.production.cmsGhent.trigger.Koopman_test import koopman_confint
import columnflow.production.cmsGhent.trigger.util as util

import numpy as np

np = maybe_import("numpy")
hist = maybe_import("hist")

Hist = hist.Hist


def calc_stat(
    histograms: dict[str, Hist],
    trigger: str,
    ref_trigger: str,
    store_hists: dict,
    error_func: Callable[[float, float, float, float], tuple[float, float]] = koopman_confint
) -> Hist:
    """
    get scale factor uncertainties for efficiencies determined by the given suffix. Allow
    for one additional auxiliary variable in the binning.
    """
    out_hist = util.syst_hist(
        [ax for ax in histograms["data"].axes if ax.name not in (trigger, ref_trigger)],
        syst_name="stat",
    )
    for idx, hist_bins in util.loop_hists(histograms["data"], histograms["mc"], exclude_axes=trigger):
        if not idx.pop(ref_trigger):
            continue
        inputs = [h[bn].value for h in hist_bins for bn in [1, sum]]
        out_hist[idx].values()[:] = error_func(*inputs)
    return out_hist


def calc_corr(
    histograms: dict[str, Hist],
    trigger: str,
    ref_trigger: str,
    store_hists: dict,
    corr_variables: Sequence[str] = tuple(),
    corr_func: Callable[[Hist], tuple[float, float]] = util.correlation_efficiency_bias,
    tag=None,
) -> Hist:
    # histograms = util.collect_hist(histograms)
    mc_hist = histograms["mc"]
    triggers = (trigger, ref_trigger)

    # histogram to store correlation for each bin (before reducing)
    unred_corr_hist = mc_hist[{t: sum for t in triggers}]

    # reduce axes
    mc_hist = util.reduce_hist(mc_hist, exclude=[*triggers, *corr_variables])

    # histogram to store correlation for each bin (after reducing)
    corr_vars = [ax.name for ax in mc_hist.axes if ax.name not in triggers]
    if corr_vars:
        corr_hist = mc_hist.project(*corr_vars)
    else:
        corr_hist = hist.Hist.new.IntCategory([0]).Weight()
    corr_hist.name = f"correlation bias" + (f"({', '.join(corr_vars)})" if corr_vars else "")
    corr_hist.label = (
        f"correlation bias for {trigger} trigger with reference {ref_trigger} "
        f"(binned in {', '.join(corr_vars)})" if corr_vars else "(inclusive)"
    )

    for idx, hist_bin in util.loop_hists(mc_hist, exclude_axes=triggers):
        # calculate correlation (tuple central, variance or central)
        c = corr_func(hist_bin.project(*triggers))

        # convert tuple to structured array
        if isinstance(c, tuple):
            dtype = np.dtype([("value", float), ("variance", float)])
            c = np.array(corr_func(hist_bin.project(*triggers)), dtype=dtype)

        # store correlation
        corr_hist[idx] = c if idx else [c]

        # broadcast for unreduced hist
        unred_corr_hist[idx] = np.full_like(unred_corr_hist[idx], c)

    # store correlation histogram for plotting by itself
    if tag is None:
        tag = "corr" + ("" if not corr_vars else ("_" + "_".join(corr_vars)))
    store_hists[tag] = corr_hist

    # calculate up and down variations on scale factors
    eff = {dt: util.calculate_efficiency(histograms[dt], *triggers) for dt in histograms}
    sf = eff["data"].values() / eff["mc"].values()
    sf_vars = [sf - sf * unred_corr_hist.values(), sf + sf * unred_corr_hist.values()]
    sf_vars = util.syst_hist(unred_corr_hist.axes, syst_name=tag, arrays=sf_vars)

    return sf_vars


dev_funcs = {
    "max_dev_sym": lambda dev, idx: np.max(np.abs(dev), axis=idx),
    "max_dev": lambda dev, idx: (np.abs(np.min(dev, axis=idx)), np.abs(np.max(dev, axis=idx))),
    "std": lambda dev, idx: np.std(np.abs(dev), axis=idx),
}


def calc_auxiliary_unc(
    histograms: dict[str, Hist],
    trigger: str,
    ref_trigger: str,
    store_hists: dict,
    auxiliaries: list[str],
    dev_func: str | Callable[
        [np.ndarray, Collection[int]],  # an array, indices of auxilaray indices
        np.ndarray | tuple[np.ndarray, np.ndarray]  # symmetric or down, up
    ] = "max_dev_sym"
):
    triggers = (trigger, ref_trigger)
    if isinstance(dev_func, str):
        dev_func = dev_funcs[dev_func]

    nom_hist = {dt: util.reduce_hist(histograms[dt], reduce=auxiliaries) for dt in histograms}
    eff = {dt: util.calculate_efficiency(nom_hist[dt], *triggers) for dt in nom_hist}
    sf = eff["data"] / eff["mc"].values()

    eff_aux = {dt: util.calculate_efficiency(histograms[dt], *triggers) for dt in histograms}
    sf_aux = eff_aux["data"] / eff_aux["mc"].values()

    aux_idx = [sf_aux.axes.name.index(vr) for vr in auxiliaries]
    dev = dev_func(sf_aux.values() - np.expand_dims(sf.values(), axis=aux_idx), aux_idx)
    if not isinstance(dev, tuple):
        dev = (dev, dev)

    return util.syst_hist(sf.axes, syst_name="corr", arrays=[sf.values() - dev[0], sf.values() + dev[1]])
