# coding: utf-8

"""
Externalized functions for plotting tasks
"""

from ap.util import maybe_import

np = maybe_import("numpy")
hist = maybe_import("hist")
plt = maybe_import("matplotlib.pyplot")
mplhep = maybe_import("mplhep")


def draw_error_stairs(ax, h, kwargs={}):
    norm = kwargs.pop("norm", 1)
    values = h.values() / norm
    error = np.sqrt(h.variances()) / norm
    defaults = {
        "edges": h.axes[0].edges,
        "baseline": values - error,
        "values": values + error,
        "hatch": "///",
        "facecolor": "none",
        "linewidth": 0,
        "color": "black",
    }
    defaults.update(kwargs)
    ax.stairs(**defaults)


def draw_from_stack(ax, h, kwargs={}):
    norm = kwargs.pop("norm", 1)
    # how to normalize stack?
    # h = h / norm
    defaults = {
        "ax": ax,
        "stack": True,
        "histtype": "fill",
    }
    defaults.update(kwargs)
    h.plot(**defaults)


def draw_from_hist(ax, h, kwargs={}):
    norm = kwargs.pop("norm", 1)
    h = h / norm
    defaults = {
        "ax": ax,
        "stack": False,
        "histtype": "step",
    }
    defaults.update(kwargs)
    h.plot1d(**defaults)


def draw_errorbars(ax, h, kwargs={}):
    norm = kwargs.pop("norm", 1)
    values = h.values() / norm
    variances = np.sqrt(h.variances()) / norm
    # compute asymmetric poisson errors for data
    # TODO: passing the output of poisson_interval to as yerr to mpl.plothist leads to
    #       buggy error bars and the documentation is clearly wrong (mplhep 0.3.12,
    #       hist 2.4.0), so adjust the output to make up for that, but maybe update or
    #       remove the next lines if this is fixed to not correct it "twice"
    from hist.intervals import poisson_interval
    yerr = poisson_interval(values, variances)
    yerr[np.isnan(yerr)] = 0
    yerr[0] = values - yerr[0]
    yerr[1] -= values

    defaults = {
        "x": h.axes[0].centers,
        "y": values,
        "yerr": yerr,
        "color": "k",
        "linestyle": "none",
        "marker": "o",
        "elinewidth": 1,
    }
    defaults.update(kwargs)
    ax.errorbar(**defaults)


def plot_all(plot_cfgs, style_cfgs, ratio=True):
    """
    plot_cfgs expects dictionaries with fields:
    "method": str, identical to the name of a function defined above,
    "hist": hist.Hist or hist.Stack,
    "kwargs": dict (optional),
    "ratio_kwargs": dict (optional),

    style_cfgs expects fields (all optional):
    "ax_cfg": dict,
    "rax_cfg": dict,
    "legend_cfg": dict,
    "CMS_label_cfg": dict,
    """

    plt.style.use(mplhep.style.CMS)

    rax = None
    if ratio:
        fig, (ax, rax) = plt.subplots(2, 1, gridspec_kw=dict(height_ratios=[3, 1], hspace=0), sharex=True)
    else:
        fig, ax = plt.subplots()

    for key in plot_cfgs:
        cfg = plot_cfgs[key]
        if "method" not in cfg:
            raise ValueError("No method given in plot_cfg entry {key}")
        method = cfg["method"]

        if "hist" not in cfg:
            raise ValueError("No histogram(s) given in plot_cfg entry {key}")
        hist = cfg["hist"]
        kwargs = cfg.get("kwargs", {})
        print(globals)
        globals()[method](ax, hist, kwargs)

        if ratio and "ratio_kwargs" in cfg:
            # take ratio_method if the ratio plot requires a different plotting method
            method = cfg.get("ratio_method", method)
            globals()[method](rax, hist, cfg["ratio_kwargs"])

    # axis styling
    ax_kwargs = {
        "ylabel": "Counts",
        "xlabel": "variable",
        "yscale": "linear",
    }
    ax_kwargs.update(style_cfgs.get("ax_cfg", {}))
    if ax_kwargs["yscale"] == "linear":
        ax_kwargs["ylim"] = 0.000001
    ax.set(**ax_kwargs)

    if ratio:
        # hard-coded line at 1
        rax.axhline(y=1.0, linestyle="dashed", color="gray")
        rax_kwargs = {
            "ylim": (0.75, 1.25),
            "ylabel": "Ratio",
            "xlabel": "Variable",
            "yscale": "linear",
        }
        rax_kwargs.update(style_cfgs.get("rax_cfg", {}))
        rax.set(**rax_kwargs)

    # legend
    legend_kwargs = {
        "title": "Processes",
        "ncol": 1,
        "loc": "upper right",
    }
    legend_kwargs.update(style_cfgs.pop("legend_cfg", {}))
    ax.legend(**legend_kwargs)

    CMS_label_kwargs = {
        "ax": ax,
        "label": "Work in Progress",
        "fontsize": 22,
    }
    CMS_label_kwargs.update(style_cfgs.pop("CMS_label_cfg", {}))
    mplhep.cms.label(**CMS_label_kwargs)

    plt.tight_layout()

    return fig
