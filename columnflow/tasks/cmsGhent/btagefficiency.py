from __future__ import annotations

import hist

import law
import order as od
from collections import OrderedDict
from itertools import product

from columnflow.tasks.framework.base import Requirements
from columnflow.tasks.framework.mixins import (
    CalibratorsMixin, VariablesMixin, SelectorMixin,
)
from columnflow.tasks.framework.plotting import (
    PlotBase, PlotBase2D,
)
from columnflow.tasks.cmsGhent.selection_hists import SelectionEfficiencyHistMixin

from columnflow.tasks.framework.remote import RemoteWorkflow
from columnflow.util import dev_sandbox, dict_add_strict, DotDict


class BTagEfficiencyBase:
    tag_name = "btag"
    flav_name = "hadronFlavour"
    flavours = {0: "light", 4: "charm", 5: "bottom"}
    wps = ["L", "M", "T"]

    sandbox = dev_sandbox(law.config.get("analysis", "default_columnar_sandbox"))


class BTagEfficiency(
    BTagEfficiencyBase,
    SelectionEfficiencyHistMixin,
    VariablesMixin,
    SelectorMixin,
    CalibratorsMixin,
    law.LocalWorkflow,
    RemoteWorkflow,
):
    def output(self):
        return {
            "json": self.target(f"{self.tag_name}_efficiency.json"),
            "hist": self.target(f"{self.tag_name}_efficiency.pickle"),
        }

    def get_plot_parameters(self):
        # convert parameters to usable values during plotting
        params = super().get_plot_parameters()
        dict_add_strict(params, "legend_title", "Processes")
        return params


    @law.decorator.log
    def run(self):
        import hist
        import correctionlib
        import correctionlib.convert
        from columnflow.plotting.cmsGhent.plot_util import cumulate

        variable_insts = list(map(self.config_inst.get_variable, self.variables))
        histograms = self.read_hist(variable_insts)
        sum_histogram = sum(histograms.values())

        # combine tagged and inclusive histograms to an efficiency histogram
        cum_histogram = cumulate(sum_histogram, direction="above", axis=f"{self.tag_name}_wp")
        incl = cum_histogram[{f"{self.tag_name}_wp": slice(0, 1)}].values()

        axes = OrderedDict(zip(cum_histogram.axes.name, cum_histogram.axes))
        axes[f"{self.tag_name}_wp"] = hist.axis.StrCategory(self.wps, name=f"{self.tag_name}_wp", label="working point")

        selected_counts = hist.Hist(*axes.values(), name=sum_histogram.name, storage=hist.storage.Weight())
        selected_counts.view()[:] = cum_histogram[{"btag_wp": slice(1, None)}].view()

        efficiency_hist = self.efficiency(selected_counts, incl)

        # save as pickle hist
        self.output()["hist"].dump(efficiency_hist, formatter="pickle")

        # save as correctionlib file
        efficiency_hist.label = "out"
        description = f"{self.tag_name} efficiencies of jets for {efficiency_hist.name} algorithm"
        clibcorr = correctionlib.convert.from_histogram(efficiency_hist[{"systematic": "central"}])
        clibcorr.description = description

        cset = correctionlib.schemav2.CorrectionSet(schema_version=2, description=description, corrections=[clibcorr])
        self.output()["json"].dump(cset.dict(exclude_unset=True), indent=4, formatter="json")

        if not self.make_plots:
            return



class BTagEfficiencyPlot(
    BTagEfficiencyBase,
    PlotBase2D,
):
    reqs = Requirements(BTagEfficiency=BTagEfficiency)

    def create_branch_map(self):
        return [
            DotDict({"flav": flav, "wp": wp})
            for flav in self.flavours.values()
            for wp in self.wps
        ]

    def requires(self):
        return {
            d: self.reqs.MergeHistograms.req(
                self,
                dataset=d,
                branch=-1,
                _exclude={"branches"},
            )
            for d in self.datasets
        }

    def output(self):
        return [
            [
                self.target(name)
                for name in self.get_plot_names(
                    f"{self.tag_name}_eff__{self.branch_data.wp}_{self.flav_name}"
                    f"__wp_{self.branch_data.wp}" +
                    (f"__err_{dr}" if dr != "central" else ""),
                )
            ]
            for dr in ["central", "down", "up"]
        ]

    def run(self):
        import numpy as np

        variable_insts = list(map(self.config_inst.get_variable, self.variables))

        # plot efficiency for each hadronFlavour and wp
        efficiency_hist = self.input()["hist"].load(formatter="pickle")

        for i, sys in enumerate(["central", "down", "up"]):
            # create a dummy histogram dict for plotting with the first process
            # TODO change process name to the relevant process group
            h = efficiency_hist[{
                 self.flav_name: hist.loc(self.branch_data.flav),
                 f"{self.tag_name}_wp": self.branch_data.wp,
            }]

            h_sys = h[{"systematics": sys}]
            if sys != "central":
                h_sys -= h[{"systematics": "central"}].values()

            proc = self.config_inst.get_process(self.processes[-1])

            # create a dummy category for plotting
            cat = od.Category(
                name=self.flav_name,
                label=self.flavours[self.branch_data.flav],
            )

            # custom styling:
            label_values = np.round(h_sys.values() * 100, decimals=1)
            style_config = {"plot2d_cfg": {"cmap": "PiYG", "labels": label_values}}
            # call the plot function
            fig, _ = self.call_plot_func(
                self.plot_function,
                hists={proc: h_sys},
                config_inst=self.config_inst,
                category_inst=cat.copy_shallow(),
                variable_insts=[var_inst.copy_shallow() for var_inst in variable_insts],
                style_config=style_config,
                **self.get_plot_parameters(),
            )
            for p in self.output()[i]:
                p.dump(fig, formatter="mpl")