# coding: utf-8


__all__ = ["AnalysisTaskTests"]

import unittest

import order as od

from columnflow.tasks.framework.base import AnalysisTask, RESOLVE_DEFAULT


class AnalysisTaskTests(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.analysis_inst = ana = od.Analysis("analysis", 1)

        ana.x.default_calibrator = "calib"
        ana.x.default_selector = "sel"
        ana.x.default_producer = ["A", "B", "C"]

        ana.x.producer_groups = {
            "A": ["a", "b"],
            "C": ["c", "d"],
        }

        self.params = {

        }

    def test_resolve_config_default(self):

        resolved_calibrator = AnalysisTask.resolve_config_default(
            param=(RESOLVE_DEFAULT,),
            task_params={},
            container=self.analysis_inst,
            default_str="default_calibrator",
        )
        resolved_selector = AnalysisTask.resolve_config_default(
            param=RESOLVE_DEFAULT,
            task_params={},
            container=self.analysis_inst,
            default_str="default_selector",
        )
        resolved_selector_steps = AnalysisTask.resolve_config_default(
            param=(RESOLVE_DEFAULT,),
            task_params={},
            container=self.analysis_inst,
            default_str="default_selector_steps",  # does note exist --> should resolve to empty tuple
        )
        resolved_producer = AnalysisTask.resolve_config_default(
            param=RESOLVE_DEFAULT,
            task_params={},
            container=self.analysis_inst,
            default_str="default_producer",
        )
        resolved_producers = AnalysisTask.resolve_config_default(
            param=(RESOLVE_DEFAULT,),
            task_params={},
            container=self.analysis_inst,
            default_str="default_producer",
        )
        resolved_producer_groups = AnalysisTask.resolve_config_default_and_groups(
            param=(RESOLVE_DEFAULT,),
            task_params={},
            container=self.analysis_inst,
            default_str="default_producer",
            groups_str="producer_groups",
            multi_strategy="same",
        )

        self.assertEqual(resolved_calibrator, ("calib",))
        self.assertEqual(resolved_selector, "sel")
        self.assertEqual(resolved_selector_steps, ())
        self.assertEqual(resolved_producer, "A")
        self.assertEqual(resolved_producers, ("A", "B", "C"))
        self.assertEqual(resolved_producer_groups, ("b", "a", "B", "d", "c"))  # TODO: order reversed?
