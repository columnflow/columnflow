import law
from collections import OrderedDict
from columnflow.util import maybe_import

plt = maybe_import("matplotlib.pyplot")
np = maybe_import("numpy")
od = maybe_import("order")
mtrans = maybe_import("matplotlib.transforms")
mplhep = maybe_import("mplhep")

from columnflow.plotting.plot_all import make_plot_2d
from columnflow.plotting.plot_util import (
    apply_variable_settings,
    remove_residual_axis,
    prepare_plot_config_2d,
    prepare_style_config_2d,
    fix_cbar_minor_ticks,
)


def plot_migration_matrices(
    hists: OrderedDict,
    config_inst: od.Config,
    category_inst: od.Process,  # per process plots
    variable_insts: list[od.Variable],
    style_config: dict = None,
    shape_norm: bool = False,
    yscale: str = None,
    hide_errors: bool = None,
    variable_settings: dict = None,
    initial: str = "Initial",
    label_numbers: bool = False,
    cms_label: str = "wip",
    **kwargs,
):
    plt.style.use(mplhep.style.CMS)
    fig, axes = plt.subplots(
        2, 3,
        figsize=(10, 10),
        gridspec_kw=dict(width_ratios=[1, 4, 0.5], height_ratios=[4, 1], hspace=0, wspace=0),
    )
    axes[0, 1].sharex(axes[1, 1])
    axes[0, 1].sharey(axes[0, 0])

    remove_residual_axis(hists, "shift")
    hists = apply_variable_settings(hists, variable_insts, variable_settings)
    initial_hist = hists.pop(initial)
    [(category, hist_2d)] = hists.items()

    # add all processes into 1 histogram
    projections = [hist_2d.project(v.name) for v in variable_insts]

    migrations = hist_2d / projections[1].values(flow=True)[None]

    plot_config = prepare_plot_config_2d(
        {category_inst: migrations},
        shape_norm=shape_norm,
        zscale="linear",
    )

    # will add cbar separately!
    plot_config["kwargs"]["cbar"] = False
    plot_config["cbar_kwargs"] |= dict(
        cax=axes[0, 2],
        fraction=1,
    )

    default_style_config = prepare_style_config_2d(
        config_inst=config_inst,
        category_inst=config_inst.get_category(category),
        process_insts=[category_inst],
        variable_insts=variable_insts,
        cms_label=cms_label,
    )

    del default_style_config["legend_cfg"]
    default_style_config["annotate_cfg"]["bbox"] = dict(alpha=0.5, facecolor="white")

    style_config = law.util.merge_dicts(default_style_config, style_config, deep=True)

    # make main central migration plot
    make_plot_2d(plot_config, style_config, figaxes=(fig, axes[0, 1]))
    if label_numbers:
        for i, x in enumerate(migrations.axes[0].centers):
            for j, y in enumerate(migrations.axes[1].centers):
                if abs(i - j) <= 1:
                    axes[0, 1].text(x, y, f"{migrations[i, j].value:.2f}", ha="center", va="center", size="large")

    cbar = plt.colorbar(axes[0, 1].collections[0], **plot_config["cbar_kwargs"])
    fix_cbar_minor_ticks(cbar)

    # make purity plot
    purity = hist_2d / projections[0].values(flow=True)[:, None]
    purity_diagonal = purity * np.eye(*[len(a) for a in hist_2d.axes[1:]])
    purity_diagonal = purity_diagonal[:, sum]
    purity_diagonal.plot1d(ax=axes[1, 1])
    trans = mtrans.blended_transform_factory(axes[1, 1].transData, axes[1, 1].transAxes)
    if label_numbers:
        for i, x in enumerate(purity_diagonal.axes[0].centers):
            axes[1, 1].text(x, 0.5, f"{purity_diagonal[i].value * 100:.1f}%", rotation="vertical",
                            ha="center", va="center", size="medium", transform=trans)
    axes[1, 1].set_xlabel(axes[0, 1].get_xlabel(), size="medium")
    axes[1, 1].set_ylabel("purity", size="small", loc="bottom")
    axes[1, 1].tick_params(labelleft=False)

    # make efficiency plot
    efficiency = projections[1] / initial_hist.project(variable_insts[1].name).values()
    trans = mtrans.Affine2D().scale(1, -1).rotate_deg(90) + axes[0, 0].transData
    efficiency.plot1d(ax=axes[0, 0], transform=trans)
    trans = mtrans.blended_transform_factory(axes[0, 0].transAxes, axes[0, 0].transData)
    if label_numbers:
        for i, x in enumerate(efficiency.axes[0].centers):
            axes[0, 0].text(0.5, x, f"{efficiency[i].value * 100:.1f}%", rotation="horizontal",
                            ha="center", va="center", size="medium", transform=trans)
    axes[0, 0].tick_params(labelbottom=False)
    axes[0, 0].set_ylabel(axes[0, 1].get_ylabel(), size="medium")
    axes[0, 0].set_xlabel("efficiency", size="small", loc="left")

    # condition number
    cond = np.linalg.cond(migrations.values())
    axes[1, 0].text(
        0.05, 0.05,
        f"condition\nnumber\n{cond:.1f}",
        transform=axes[1, 0].transAxes,
        size="small",
        va="bottom",
        ha="left",
        color="red",
    )

    # finally remove redundant stuff
    for i in [0, 2]:
        axes[1, i].set_axis_off()
    axes[0, 1].tick_params(labelbottom=False, labelleft=False)
    axes[0, 1].set_ylabel(None)
    axes[0, 1].set_xlabel(None)
    return fig, axes
