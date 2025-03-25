# coding: utf-8


__all__ = ["AnalysisTaskTests"]

import unittest

import order as od

from columnflow.tasks.framework.base import AnalysisTask, RESOLVE_DEFAULT, ShiftTask, DatasetTask
from columnflow.tasks.framework.mixins import (
    VariablesMixin, CategoriesMixin, DatasetsProcessesMixin,
)


class AnalysisTaskTests(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.analysis_inst = ana = od.Analysis("analysis", 1)
        self.config_inst1 = cfg1 = ana.add_config(name="config", id=1)
        self.config_inst2 = cfg2 = ana.add_config(name="config2", id=2)
        self.base_params = {
            "analysis_inst": ana,
            "config_inst": cfg1,
            "config_insts": tuple(ana.configs),
        }
        # setup calibrators, selectors, producers
        ana.x.default_calibrator = "calib"
        ana.x.default_selector = "sel"
        ana.x.default_producer = ["A", "B", "C"]
        ana.x.producer_groups = {
            "A": ["a", "b"],
            "C": ["c", "d"],
        }

        # setup categories and variables
        cfg1.add_shift("nominal", id=0)
        for i in range(5):
            cfg1.add_category(name=f"cat{i}", id=i)
            cfg1.add_variable(name=f"var{i}", id=i)
            p = cfg1.add_process(name=f"proc{i}", id=i)
            cfg1.add_shift(name=f"shift{i}_up", id=2 * i + 1)
            cfg1.add_shift(name=f"shift{i}_down", id=2 * i + 2)
            cfg1.add_dataset(name=f"ds{i}", processes=[p], id=i, info={
                "nominal": [], f"shift{i}_up": [], f"shift{i}_down": []})

        cat1 = cfg1.get_category("cat1")
        cat1.add_category(name="cat1_1", id=11)
        cat1.add_category(name="cat1_2", id=12)

        cfg1.x.default_categories = ("cg1", "cat1_2", "not_existing")
        cfg1.x.category_groups = {
            "cg1": ["cat0", "cat1_1"],
            "cg2": ["cat2", "cat3"],
        }

        cfg1.x.default_variables = ("vg1", "vg_2d", "var4", "not_existing")
        cfg1.x.variable_groups = {
            "vg1": ["var0", "var1"],
            "vg2": ["var2", "var3", "not_existing"],
            "vg_2d": ["var0-var1", "var0-var2", "var1-var2", "var3"],
        }

        # setup for MultiConfig
        cfg2.add_shift("nominal", id=0)
        for i in range(3, 7):
            cfg2.add_category(name=f"cat{i}", id=i)
            cfg2.add_variable(name=f"var{i}", id=i)
            cfg2.add_shift(name=f"shift{i}_up", id=2 * i + 1)
            cfg2.add_shift(name=f"shift{i}_down", id=2 * i + 2)
            p = cfg2.add_process(name=f"proc{i}", id=i)
            cfg2.add_dataset(name=f"ds{i}", processes=[p], id=i, info={
                "nominal": [], f"shift{i}_up": [], f"shift{i}_down": []})

        # same proess group, different processes
        cfg1.x.process_groups = {
            "pg1": ("proc1", "proc2"),
        }
        cfg2.x.process_groups = {
            "pg1": ("proc3", "proc4"),
        }

        # same calibrator, different producer
        cfg1.x.default_calibrator = ("calib",)
        cfg2.x.default_calibrator = ("calib",)
        cfg1.x.default_producer = ("A", "B", "C")
        cfg2.x.default_producer = ("B", "C", "D")

    def test_resolve_config_default(self):
        # single config
        resolved_calibrator = AnalysisTask.resolve_config_default(
            param=(RESOLVE_DEFAULT,),
            task_params=self.base_params,
            container=self.analysis_inst,
            default_str="default_calibrator",
            multi_strategy="first",
        )
        self.assertEqual(resolved_calibrator, ("calib",))

        resolved_selector = AnalysisTask.resolve_config_default(
            param=RESOLVE_DEFAULT,
            task_params=self.base_params,
            container=self.analysis_inst,
            default_str="default_selector",
            multi_strategy="first",
        )
        self.assertEqual(resolved_selector, "sel")

        resolved_selector_steps = AnalysisTask.resolve_config_default(
            param=(RESOLVE_DEFAULT,),
            task_params=self.base_params,
            container=self.analysis_inst,
            default_str="default_selector_steps",  # does note exist --> should resolve to empty tuple
            multi_strategy="first",
        )
        self.assertEqual(resolved_selector_steps, ())

        resolved_producer = AnalysisTask.resolve_config_default(
            param=RESOLVE_DEFAULT,
            task_params=self.base_params,
            container=self.analysis_inst,
            default_str="default_producer",
            multi_strategy="first",
        )
        self.assertEqual(resolved_producer, "A")

        resolved_producers = AnalysisTask.resolve_config_default(
            param=(RESOLVE_DEFAULT,),
            task_params=self.base_params,
            container=self.analysis_inst,
            default_str="default_producer",
            multi_strategy="first",
        )
        self.assertEqual(resolved_producers, ("A", "B", "C"))

        resolved_producer_groups = AnalysisTask.resolve_config_default_and_groups(
            param=(RESOLVE_DEFAULT,),
            task_params=self.base_params,
            container=self.analysis_inst,
            default_str="default_producer",
            groups_str="producer_groups",
            multi_strategy="first",
        )
        self.assertEqual(resolved_producer_groups, ("b", "a", "B", "d", "c"))  # TODO: order reversed

        # multi config
        for multi_strategy, expected_producer in (
            ("all", {self.config_inst1: ("A", "B", "C"), self.config_inst2: ("B", "C", "D")}),
            ("first", ("A", "B", "C")),
            ("union", ("A", "B", "C", "D")),
            ("intersection", ("B", "C")),
        ):
            resolved_producer = AnalysisTask.resolve_config_default(
                param=(RESOLVE_DEFAULT,),
                task_params=self.base_params,
                container=tuple(self.analysis_inst.configs),
                default_str="default_producer",
                multi_strategy=multi_strategy,
            )
            # TODO: remove set() when order is fixed
            self.assertEqual(set(resolved_producer), set(expected_producer))

        # "same" strategy
        resolved_calibrator = AnalysisTask.resolve_config_default(
            param=(RESOLVE_DEFAULT,),
            task_params=self.base_params,
            container=tuple(self.analysis_inst.configs),
            default_str="default_calibrator",
            multi_strategy="same",
        )
        self.assertEqual(resolved_calibrator, ("calib",))
        with self.assertRaises(ValueError):
            AnalysisTask.resolve_config_default(
                param=(RESOLVE_DEFAULT,),
                task_params=self.base_params,
                container=tuple(self.analysis_inst.configs),
                default_str="default_producer",
                multi_strategy="same",
            )

    def test_find_config_objects(self):
        config = AnalysisTask.find_config_objects(
            names=self.config_inst1.name,
            container=self.analysis_inst,
            object_cls=od.Config,
        )
        self.assertEqual(config, [self.config_inst1.name])
        configs = AnalysisTask.find_config_objects(
            names=(*self.analysis_inst.configs.names(), "not_existing"),
            container=self.analysis_inst,
            object_cls=od.Config,
        )
        self.assertEqual(configs, list(self.analysis_inst.configs.names()))

        variables = AnalysisTask.find_config_objects(
            names=("var1", "var2", "var3", "not_existing"),
            container=self.config_inst1,
            object_cls=od.Variable,
        )
        self.assertEqual(variables, ["var1", "var2", "var3"])

        categories = AnalysisTask.find_config_objects(
            names=("cat1", "cat1_1", "cat1_2", "cat2", "cat3", "not_existing"),
            container=self.config_inst1,
            object_cls=od.Category,
            deep=True,
        )
        self.assertEqual(categories, ["cat1", "cat1_1", "cat1_2", "cat2", "cat3"])

        categories = AnalysisTask.find_config_objects(
            names=("cat1", "cat1_1", "cat1_2", "cat2", "cat3", "not_existing"),
            container=self.config_inst1,
            object_cls=od.Category,
            deep=False,
        )
        self.assertEqual(categories, ["cat1", "cat2", "cat3"])

    def test_resolve_categories(self):
        # TODO: order of resolved categories is still messed up
        # testing with single config
        CategoriesMixin.single_config = True

        for input_categories, expected_categories in (
            ((RESOLVE_DEFAULT,), ("cat0", "cat1_1", "cat1_2")),
            (("cg1", "cg2", "cat4", "not_existing"), ("cat0", "cat1_1", "cat2", "cat3", "cat4")),
        ):
            input_params = {
                **self.base_params,
                "categories": input_categories,
            }
            resolved_params = CategoriesMixin.modify_param_values(params=input_params)
            # TODO: remove set() when order is fixed
            self.assertEqual(set(resolved_params["categories"]), set(expected_categories))

    def test_resolve_variables(self):
        # testing with single config
        VariablesMixin.single_config = True

        for input_variables, expected_variables in (
            ((RESOLVE_DEFAULT,), ("var0", "var1", "var0-var1", "var0-var2", "var1-var2", "var3", "var4")),
            (("vg1", "vg2", "var4-var1", "var4-missing"), ("var0", "var1", "var2", "var3", "var4-var1")),
        ):
            input_params = {
                **self.base_params,
                "variables": input_variables,
            }
            resolved_params = VariablesMixin.modify_param_values(params=input_params)
            # TODO: remove set() when order is fixed
            self.assertEqual(set(resolved_params["variables"]), set(expected_variables))

    def test_resolve_datasets_processes(self):
        DatasetsProcessesMixin.single_config = False
        DatasetsProcessesMixin.resolution_task_class = DatasetTask
        for input_processes, expected_processes in (
            ((("proc4",),), (("proc4",), ("proc4",))),
            ((("proc4",), ("proc5")), (("proc4",), ("proc5",))),
            ((("proc4", "proc5", "proc6"),), (("proc4",), ("proc4", "proc5", "proc6"))),
            ((("proc1", "proc2"), ("proc5", "proc6")), (("proc1", "proc2"), ("proc5", "proc6"))),
            ((("pg1"),), (("proc1", "proc2"), ("proc3", "proc4"))),
            # default (empty tuple) is resolved to all processes
            ((), tuple(tuple(proc.name for proc in cfg.processes) for cfg in self.analysis_inst.configs)),
        ):
            input_params = {
                **self.base_params,
                "processes": input_processes,
                "datasets": (),
            }
            resolved_params = DatasetsProcessesMixin.modify_param_values(params=input_params)
            self.assertEqual(resolved_params["processes"], expected_processes)

            # since there is a 1-to-1 mapping between processes and datasets, we can infer the datasets as well
            self.assertEqual(
                resolved_params["datasets"],
                tuple(tuple(proc_name.replace("proc", "ds") for proc_name in inner) for inner in expected_processes),
            )

    def test_resolve_dataset(self):
        DatasetTask.single_config = True

        base_params = {
            **self.base_params,
            "shift": "nominal",
        }

        resolved_params = DatasetTask.modify_param_values(params={**base_params, "dataset": "ds0"})
        self.assertEqual(resolved_params["dataset"], "ds0")

        with self.assertRaises(ValueError):
            DatasetTask.modify_param_values({**base_params, "dataset": "not_existing"})

    def test_resolve_shifts(self):
        # single config

        for input_shift, input_dataset, expected_shift in (
            ("nominal", "ds0", "nominal"),
            ("shift0_up", "ds0", "shift0_up"),  # implemented upstream from dataset "ds0"
            ("shift1_up", "ds0", "nominal"),  # not implemented upstream --> fallback to "nominal"
            ("shift1_up", "ds1", "shift1_up"),  # implemented upstream from dataset "ds0"
        ):
            input_params = {
                **self.base_params,
                "dataset": input_dataset,
                "shift": input_shift,
            }
            resolved_params = DatasetTask.modify_param_values(params=input_params)
            self.assertEqual(resolved_params["shift"], expected_shift)

        with self.assertRaises(ValueError):
            DatasetTask.modify_param_values({
                **self.base_params,
                "dataset": "ds0",
                "shift": "not_existing",
            })

    def test_modify_shifts_multi_config(self):
        # multi config
        class ShiftTaskAllUpstream(ShiftTask):
            """
            Exemplary shift declaration task that collects all known shifts
            from all config instances as upstream shifts.
            """
            single_config = False
            @classmethod
            def get_known_shifts(
                cls,
                params: dict,
                shifts,
            ) -> None:
                super().get_known_shifts(params, shifts)
                for config_inst in params.get("config_insts", {}):
                    shifts.upstream.update(config_inst.shifts.names())

        class ShiftTaskAllLocal(ShiftTask):
            """
            Exemplary shift declaration task that collects all known shifts
            from all config instances as local shifts.
            """
            single_config = False
            @classmethod
            def get_known_shifts(
                cls,
                params: dict,
                shifts,
            ) -> None:
                super().get_known_shifts(params, shifts)
                for config_inst in params.get("config_insts", {}):
                    shifts.local.update(config_inst.shifts.names())

        for input_shift, expected_shift, expected_shift_cfg1, expected_shift_cfg2 in (
            ("nominal", "nominal", "nominal", "nominal"),
            ("shift1_up", "shift1_up", "shift1_up", "nominal"),  # known to cfg1
            ("shift3_up", "shift3_up", "shift3_up", "shift3_up"),  # known to cfg1 and cfg2
            ("shift6_up", "shift6_up", "nominal", "shift6_up"),  # known to cfg2
        ):
            # upstream shifts (local shifts should always resolve to "nominal")
            input_params = {
                **self.base_params,
                "shift": input_shift,
            }

            expected_shift_insts = {
                self.config_inst1: self.config_inst1.get_shift(expected_shift_cfg1),
                self.config_inst2: self.config_inst2.get_shift(expected_shift_cfg2),
            }

            resolved_params_upstream = ShiftTaskAllUpstream.modify_param_values(params=input_params)
            self.assertEqual(resolved_params_upstream["local_shift"], "nominal")
            self.assertEqual(
                resolved_params_upstream["local_shift_insts"],
                {cfg: cfg.get_shift("nominal") for cfg in self.analysis_inst.configs},
            )
            self.assertEqual(resolved_params_upstream["shift"], expected_shift)
            self.assertEqual(resolved_params_upstream["global_shift_insts"], expected_shift_insts)

            # local shifts (upstream shifts should be identical to local shifts)
            input_params = {
                **self.base_params,
                "shift": input_shift,
            }
            resolved_params_local = ShiftTaskAllLocal.modify_param_values(params=input_params)
            self.assertEqual(resolved_params_local["local_shift"], expected_shift)
            self.assertEqual(resolved_params_local["local_shift_insts"], expected_shift_insts)
            self.assertEqual(resolved_params_local["shift"], expected_shift)
            self.assertEqual(resolved_params_local["global_shift_insts"], expected_shift_insts)

        # resolving non-existing shifts should raise an error
        for task in (ShiftTaskAllUpstream, ShiftTaskAllLocal):
            with self.assertRaises(ValueError):
                task.modify_param_values({**self.base_params, "shift": "not_existing"})
