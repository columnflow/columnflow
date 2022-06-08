# coding: utf-8

"""
Lightweight mixins task classes.
"""

import law
import luigi

from ap.tasks.framework.base import AnalysisTask, ConfigTask


class CalibratorMixin(ConfigTask):

    calibrator = luigi.Parameter(
        default=law.NO_STR,
        description="the name of the calibrator to the applied; default: value of the "
        "'default_calibrator' config",
    )

    @classmethod
    def modify_param_values(cls, params):
        params = super().modify_param_values(params)

        if "config" in params and params.get("calibrator") == law.NO_STR:
            config_inst = cls.get_analysis_inst(cls.analysis).get_config(params["config"])
            if config_inst.x("default_calibrator", None):
                params["calibrator"] = config_inst.x.default_calibrator

        return params

    def store_parts(self):
        parts = super().store_parts()
        parts.insert_before("version", "calibrator", f"calib__{self.calibrator}")
        return parts


class CalibratorsMixin(ConfigTask):

    calibrators = law.CSVParameter(
        default=(),
        description="comma-separated names of calibrators to be applied; default: value of the "
        "'default_calibrator' config in a 1-tuple",
    )

    @classmethod
    def modify_param_values(cls, params):
        params = super().modify_param_values(params)

        if "config" in params and params.get("calibrators") == ():
            config_inst = cls.get_analysis_inst(cls.analysis).get_config(params["config"])
            if config_inst.x("default_calibrator", None):
                params["calibrators"] = (config_inst.x.default_calibrator,)

        return params

    def store_parts(self):
        parts = super().store_parts()

        calib = "__".join(self.calibrators[:5])
        if len(self.calibrators) > 5:
            calib += f"__{law.util.create_hash(self.calibrators[5:])}"
        parts.insert_before("version", "calibrators", f"calib__{calib}")

        return parts


class SelectorMixin(ConfigTask):

    selector = luigi.Parameter(
        default=law.NO_STR,
        description="the name of the selector to the applied; default: value of the "
        "'default_selector' config",
    )

    @classmethod
    def modify_param_values(cls, params):
        params = super().modify_param_values(params)

        if "config" in params and params.get("selector") == law.NO_STR:
            config_inst = cls.get_analysis_inst(cls.analysis).get_config(params["config"])
            if config_inst.x("default_selector", None):
                params["selector"] = config_inst.x.default_selector

        return params

    @classmethod
    def determine_allowed_shifts(cls, config_inst, params):
        shifts = super().determine_allowed_shifts(config_inst, params)

        # add selector dependent shifts
        if "selector" in params:
            from ap.selection import Selector
            shifts |= Selector.get(params["selector"]).all_shifts

        return shifts

    def store_parts(self):
        parts = super().store_parts()
        parts.insert_before("version", "selector", f"sel__{self.selector}")
        return parts


class CalibratorsSelectorMixin(SelectorMixin, CalibratorsMixin):
    pass


class ProducerMixin(ConfigTask):

    producer = luigi.Parameter(
        default=law.NO_STR,
        description="the name of the producer to the applied; default: value of the "
        "'default_producer' config",
    )

    @classmethod
    def modify_param_values(cls, params):
        params = super().modify_param_values(params)

        if "config" in params and params.get("producer") == law.NO_STR:
            config_inst = cls.get_analysis_inst(cls.analysis).get_config(params["config"])
            if config_inst.x("default_producer", None):
                params["producer"] = config_inst.x.default_producer

        return params

    @classmethod
    def determine_allowed_shifts(cls, config_inst, params):
        shifts = super().determine_allowed_shifts(config_inst, params)

        # add producer dependent shifts
        if "producer" in params:
            from ap.production import Producer
            shifts |= Producer.get(params["producer"]).all_shifts

        return shifts

    def store_parts(self):
        parts = super().store_parts()
        producer = f"prod__{self.producer}" if self.producer != law.NO_STR else "none"
        parts.insert_before("version", "producer", producer)
        return parts


class ProducersMixin(ConfigTask):

    producers = law.CSVParameter(
        default=(),
        description="comma-separated names of producers to be applied; empty default",
    )

    @classmethod
    def modify_param_values(cls, params):
        params = super().modify_param_values(params)

        if "config" in params and params.get("producers") == ():
            config_inst = cls.get_analysis_inst(cls.analysis).get_config(params["config"])
            if config_inst.x("default_producer", None):
                params["producers"] = (config_inst.x.default_producer,)

        return params

    def store_parts(self):
        parts = super().store_parts()

        producers = "none"
        if self.producers:
            producers = "__".join(self.producers[:5])
            if len(self.producers) > 5:
                producers += f"__{law.util.create_hash(self.producers[5:])}"
        parts.insert_before("version", "producers", f"prod__{producers}")

        return parts


class PlotMixin(AnalysisTask):

    view_cmd = luigi.Parameter(
        default=law.NO_STR,
        significant=False,
        description="a command to execute after the task has run to visualize plots right in the "
        "terminal; no default",
    )


@law.decorator.factory(accept_generator=True)
def view_output_plots(fn, opts, task, *args, **kwargs):
    def before_call():
        return None

    def call(state):
        return fn(task, *args, **kwargs)

    def after_call(state):
        view_cmd = getattr(task, "view_cmd", None)
        if not view_cmd or view_cmd == law.NO_STR:
            return

        # prepare the view command
        if "{}" not in view_cmd:
            view_cmd += " {}"

        # collect all paths to view
        view_paths = []
        outputs = law.util.flatten(task.output())
        while outputs:
            output = outputs.pop(0)
            if isinstance(output, law.TargetCollection):
                outputs.extend(output._flat_target_list)
                continue
            if not getattr(output, "path", None):
                continue
            if output.path.endswith((".pdf", ".png")) and output.path not in view_paths:
                view_paths.append(output.path)

        # loop through paths and view them
        for path in view_paths:
            task.publish_message("showing {}".format(path))
            law.util.interruptable_popen(view_cmd.format(path), shell=True, executable="/bin/bash")

    return before_call, call, after_call


PlotMixin.view_output_plots = view_output_plots
