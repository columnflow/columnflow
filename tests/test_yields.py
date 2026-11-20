# coding: utf-8

"""
Tests for the :py:class:`~columnflow.tasks.yields.CreateYieldTable` task.
"""

__all__ = ["CreateYieldTableTests"]

import math
import unittest

import law
import order as od
from scinum import Number

from columnflow.util import maybe_import
from columnflow.tasks.framework.histograms import HistogramsUserBase
from columnflow.tasks.yields import CreateYieldTable

np = maybe_import("numpy")
hist = maybe_import("hist")


class YieldTableStub(object):
    """
    Lightweight stand-in for :py:class:`CreateYieldTable` that borrows its methods, so that they can be tested
    without resolving task parameters and upstream requirements.
    """

    background_mask = CreateYieldTable.background_mask
    prepare_yields = CreateYieldTable.prepare_yields
    apply_ratio = CreateYieldTable.apply_ratio
    prepare_yields_str = CreateYieldTable.prepare_yields_str
    slice_histogram = HistogramsUserBase.slice_histogram

    defaults = {
        "shift": "nominal",
        "yields_variable": "event",
        "ratio": (),
        "ratio_modes": ("ratio", "subtract", "s_over_b", "s_over_sqrt_b"),
        "normalize_yields": law.NO_STR,
        "skip_uncertainties": False,
        "number_format": "pdg",
        "table_format": "fancy_grid",
        "transpose": False,
    }

    def __init__(self, **kwargs):
        super().__init__()
        self.__dict__.update({**self.defaults, **kwargs})
        self.hists = {}

    def input(self):
        return {}

    def load_histogram(self, inputs, config_inst, dataset, variable):
        return self.hists[config_inst.name][dataset].copy()


class CreateYieldTableTests(unittest.TestCase):

    processes = ("data", "background", "tt", "dy", "hh")
    datasets = ("ds_data", "ds_tt", "ds_dy", "ds_hh")

    # nominal entries per dataset, process and category, given as (number of entries, weight per entry)
    entries = {
        ("ds_data", "data", "c1"): (110, 1.0),
        ("ds_data", "data", "c2"): (52, 1.0),
        ("ds_tt", "tt", "c1"): (30, 2.0),
        ("ds_tt", "tt", "c2"): (5, 2.0),
        ("ds_dy", "dy", "c1"): (40, 1.0),
        ("ds_dy", "dy", "c2"): (40, 1.0),
        ("ds_hh", "hh", "c1"): (10, 0.1),
        ("ds_hh", "hh", "c2"): (5, 0.1),
    }

    # expected nominal yields per category, ordered as in *processes*
    expected = {
        "c1": [110.0, 100.0, 60.0, 40.0, 1.0],
        "c2": [52.0, 50.0, 10.0, 40.0, 0.5],
    }

    def setUp(self):
        self.config_inst = self.create_config("config", 1)

    @staticmethod
    def create_config(name, id):
        cfg = od.Config(name=name, id=id)

        cfg.add_shift(name="nominal", id=0)
        cfg.add_shift(name="jec_up", id=1)

        cfg.add_process(name="data", id=1, is_data=True, label="Data")
        bkg = cfg.add_process(name="background", id=2, label="Background")
        bkg.add_process(name="tt", id=3, label="TT")
        bkg.add_process(name="dy", id=4, label="DY")
        cfg.add_process(name="hh", id=5, label="HH", tags={"is_signal"})

        incl = cfg.add_category(name="incl", id=1, label="inclusive")
        incl.add_category(name="c1", id=2, label="cat 1")
        incl.add_category(name="c2", id=3, label="cat 2")

        return cfg

    @classmethod
    def create_histograms(cls, scale=1.0):
        """
        Creates one histogram per dataset, mimicking the output of MergeHistograms. The "jec_up" shift is filled
        with large weights to detect if it leaks into the nominal yields.
        """
        hists = {}
        for dataset in cls.datasets:
            h = (
                hist.Hist.new
                .StrCat([], name="process", growth=True)
                .StrCat([], name="category", growth=True)
                .StrCat([], name="shift", growth=True)
                .Integer(0, 1, name="event")
                .Weight()
            )

            for (_dataset, process, category), (n, weight) in cls.entries.items():
                if _dataset != dataset:
                    continue
                for shift, shift_scale in (("nominal", 1.0), ("jec_up", 10.0)):
                    h.fill(
                        process=[process] * n,
                        category=[category] * n,
                        shift=[shift] * n,
                        event=np.zeros(n, dtype=int),
                        weight=np.full(n, weight * scale * shift_scale),
                    )
            hists[dataset] = h

        return hists

    def create_task(self, config_insts=None, processes=None, categories=("c1", "c2"), scales=None, **kwargs):
        config_insts = config_insts or [self.config_inst]
        processes = processes or self.processes
        scales = scales or [1.0] * len(config_insts)

        task = YieldTableStub(**kwargs)
        task.config_insts = config_insts
        task.processes = tuple(tuple(processes) for _ in config_insts)
        task.datasets = tuple(self.datasets for _ in config_insts)
        task.categories = tuple(categories)
        task.process_insts = {cfg: [cfg.get_process(p) for p in processes] for cfg in config_insts}
        task.category_insts = [config_insts[0].get_category(c) for c in categories]
        task.hists = {cfg.name: self.create_histograms(scale) for cfg, scale in zip(config_insts, scales)}

        return task

    def create_yields(self, processes=None):
        """
        Creates yields as returned by :py:meth:`CreateYieldTable.prepare_yields`, with Poisson-like uncertainties.
        """
        processes = processes or self.processes
        process_insts = [self.config_inst.get_process(p) for p in processes]

        yields = {}
        for category, category_values in self.expected.items():
            category_inst = self.config_inst.get_category(category)
            yields[category_inst] = [
                Number(v, {f"mcstat_{p}_{category}": math.sqrt(v)})
                for p, v in zip(self.processes, category_values)
                if p in processes
            ]

        return yields, process_insts

    #
    # background_mask
    #

    def test_background_mask(self):
        task = self.create_task()
        _, processes = self.create_yields()

        # data, signal and the aggregate "background" (whose sub processes are requested) are no background
        self.assertEqual(task.background_mask(processes), [False, False, True, True, False])

    def test_background_mask_aggregate_only(self):
        task = self.create_task()
        _, processes = self.create_yields(processes=("data", "background", "hh"))

        # without requested sub processes, the aggregate process is the background
        self.assertEqual(task.background_mask(processes), [False, True, False])

    #
    # prepare_yields
    #

    def test_prepare_yields(self):
        task = self.create_task()
        yields, _, _ = task.prepare_yields()

        for category_inst, category_yields in yields.items():
            for value, expected in zip(category_yields, self.expected[category_inst.name]):
                # shifted entries must not enter the yields
                self.assertAlmostEqual(value.nominal, expected)

    def test_prepare_yields_uncertainties(self):
        task = self.create_task()
        yields, processes, _ = task.prepare_yields()
        c1 = self.config_inst.get_category("c1")

        # variance of the aggregate process is the sum of sub process variances: 30 * 2^2 + 40 * 1^2
        bkg = yields[c1][processes.index(self.config_inst.get_process("background"))]
        self.assertAlmostEqual(bkg.get_uncertainty("mcstat_background_c1", direction="up"), math.sqrt(160))

        # variance for weighted entries: 10 * 0.1^2
        hh = yields[c1][processes.index(self.config_inst.get_process("hh"))]
        self.assertAlmostEqual(hh.get_uncertainty("mcstat_hh_c1", direction="up"), math.sqrt(0.1))

    def test_prepare_yields_parent_category(self):
        task = self.create_task(categories=("incl", "c1"))
        yields, processes, _ = task.prepare_yields()
        incl, c1 = task.category_insts

        # the parent category is the sum of its leaf categories
        for i in range(len(processes)):
            self.assertAlmostEqual(yields[incl][i].nominal, self.expected["c1"][i] + self.expected["c2"][i])
            self.assertAlmostEqual(yields[c1][i].nominal, self.expected["c1"][i])

    def test_prepare_yields_shift(self):
        task = self.create_task(shift="jec_up")

        # histograms of shifted MergeHistograms tasks only contain the requested shift
        for dataset_hists in task.hists.values():
            for dataset, h in dataset_hists.items():
                dataset_hists[dataset] = h[{"shift": [hist.loc("jec_up")]}]

        yields, _, _ = task.prepare_yields()
        c1 = self.config_inst.get_category("c1")

        # for a shifted task, the shifted entries are used (scaled by 10 in the test histograms)
        for value, expected in zip(yields[c1], self.expected["c1"]):
            self.assertAlmostEqual(value.nominal, 10 * expected)

    def test_prepare_yields_multi_config(self):
        config_inst2 = self.create_config("config2", 2)
        task = self.create_task(config_insts=[self.config_inst, config_inst2], scales=[1.0, 2.0])
        yields, _, _ = task.prepare_yields()

        # yields of all configs are summed
        for category_inst, category_yields in yields.items():
            for value, expected in zip(category_yields, self.expected[category_inst.name]):
                self.assertAlmostEqual(value.nominal, 3 * expected)

    #
    # apply_ratio
    #

    def test_apply_ratio(self):
        task = self.create_task(ratio=("data", "background", "tt"))
        yields, processes = self.create_yields()
        derived, derived_procs = task.apply_ratio(yields, processes)

        # exactly one value per derived quantity and category
        for category_values in derived.values():
            self.assertEqual(len(category_values), len(derived_procs))

        c1 = self.config_inst.get_category("c1")
        values = {proc.name: value.nominal for proc, value in zip(derived_procs, derived[c1])}
        self.assertAlmostEqual(values["Ratio"], 110 / 100)
        self.assertAlmostEqual(values["S_over_B"], 1 / 100)
        self.assertAlmostEqual(values["S_over_sqrt_B"], 1 / 10)
        self.assertAlmostEqual(values["Ratio_diff_tt"], (110 - 100 + 60) / 60)

        # input yields are not modified
        self.assertEqual(len(yields[c1]), len(processes))

    def test_apply_ratio_uncertainty_propagation(self):
        task = self.create_task(ratio=("data", "background"), ratio_modes=("s_over_sqrt_b",))
        yields, processes = self.create_yields()
        derived, _ = task.apply_ratio(yields, processes)

        c1 = self.config_inst.get_category("c1")
        s_over_sqrt_b = derived[c1][0]

        # S / sqrt(B) with S = 1 +- 1 and B = 100 +- 10
        self.assertAlmostEqual(s_over_sqrt_b.get_uncertainty("mcstat_hh_c1", direction="up"), 1 / 10)
        self.assertAlmostEqual(
            s_over_sqrt_b.get_uncertainty("mcstat_background_c1", direction="up"),
            0.5 * 1 * 100 ** -1.5 * 10,
        )

    #
    # normalization
    #

    def prepare_raw_yields(self, **kwargs):
        task = self.create_task(**kwargs)
        yields, processes = self.create_yields()
        derived, derived_procs = task.apply_ratio(yields, processes)
        raw_yields, _ = task.prepare_yields_str(yields, processes, derived, derived_procs)
        return raw_yields

    def test_normalization_none(self):
        raw_yields = self.prepare_raw_yields(ratio=("data", "background"), ratio_modes=("ratio",))

        for category, expected in self.expected.items():
            for process, value in zip(self.processes, expected):
                self.assertAlmostEqual(raw_yields[category][process], value)
        self.assertAlmostEqual(raw_yields["c1"]["Ratio"], 1.1)

    def test_normalization_per_category(self):
        raw_yields = self.prepare_raw_yields(
            normalize_yields="per_category",
            ratio=("data", "background"),
            ratio_modes=("s_over_b",),
        )
        c1 = raw_yields["c1"]

        # backgrounds sum up to one, data and signal are relative to the total background
        self.assertAlmostEqual(c1["tt"] + c1["dy"], 1.0)
        self.assertAlmostEqual(c1["tt"], 0.6)
        self.assertAlmostEqual(c1["background"], 1.0)
        self.assertAlmostEqual(c1["data"], 1.1)
        self.assertAlmostEqual(c1["hh"], 0.01)
        self.assertAlmostEqual(raw_yields["c2"]["tt"], 0.2)

        # derived quantities are not normalized
        self.assertAlmostEqual(c1["S_over_B"], 0.01)

    def test_normalization_all(self):
        raw_yields = self.prepare_raw_yields(normalize_yields="all")

        # total background over all categories: 100 + 50
        self.assertAlmostEqual(raw_yields["c1"]["tt"], 60 / 150)
        self.assertAlmostEqual(raw_yields["c2"]["dy"], 40 / 150)
        self.assertAlmostEqual(raw_yields["c1"]["data"], 110 / 150)
        total_bkg = sum(raw_yields[c][p] for c in ("c1", "c2") for p in ("tt", "dy"))
        self.assertAlmostEqual(total_bkg, 1.0)

    def test_normalization_per_process(self):
        raw_yields = self.prepare_raw_yields(normalize_yields="per_process")

        for process in self.processes:
            self.assertAlmostEqual(raw_yields["c1"][process] + raw_yields["c2"][process], 1.0)
        self.assertAlmostEqual(raw_yields["c1"]["tt"], 60 / 70)
