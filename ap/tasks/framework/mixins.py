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

        # add the default calibrator when empty
        if "config" in params and params.get("calibrator") == law.NO_STR:
            config_inst = cls.get_analysis_inst(cls.analysis).get_config(params["config"])
            if config_inst.x("default_calibrator", None):
                params["calibrator"] = config_inst.x.default_calibrator

        return params

    @classmethod
    def determine_allowed_shifts(cls, config_inst, params):
        shifts = super().determine_allowed_shifts(config_inst, params)

        # get the calibrator, update it and add its shifts
        if params.get("calibrator") not in (None, law.NO_STR):
            calibrator_func = cls.get_calibrator_func(
                params["calibrator"],
                **cls.get_calibrator_kwargs(**params),
            )
            shifts |= calibrator_func.all_shifts

        return shifts

    @classmethod
    def get_calibrator_func(cls, calibrator, copy=True, **update_kwargs):
        from ap.calibration import Calibrator

        func = Calibrator.get(calibrator, copy=copy)
        if update_kwargs:
            func.run_update(**update_kwargs)

        return func

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # cache for calibrator func
        self._calibrator_func = None

    @property
    def calibrator_func(self):
        if self._calibrator_func is None:
            # store a copy of the updated calibrator
            self._calibrator_func = self.get_calibrator_func(
                self.calibrator,
                **self.get_calibrator_kwargs(self),
            )
        return self._calibrator_func

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

    @classmethod
    def determine_allowed_shifts(cls, config_inst, params):
        shifts = super().determine_allowed_shifts(config_inst, params)

        # get the calibrators, update them and add their shifts
        if params.get("calibrators") not in (None, law.NO_STR):
            calibrator_kwargs = cls.get_calibrator_kwargs(**params)
            for calibrator in params["calibrators"]:
                calibrator_func = CalibratorMixin.get_calibrator_func(calibrator, **calibrator_kwargs)
                shifts |= calibrator_func.all_shifts

        return shifts

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

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # cache for selector func
        self._selector_func = None

    @classmethod
    def modify_param_values(cls, params):
        params = super().modify_param_values(params)

        # add the default selector when empty
        if "config" in params and params.get("selector") == law.NO_STR:
            config_inst = cls.get_analysis_inst(cls.analysis).get_config(params["config"])
            if config_inst.x("default_selector", None):
                params["selector"] = config_inst.x.default_selector

        return params

    @classmethod
    def determine_allowed_shifts(cls, config_inst, params):
        shifts = super().determine_allowed_shifts(config_inst, params)

        # get the selector, update it and add its shifts
        if params.get("selector") not in (None, law.NO_STR):
            selector_func = cls.get_selector_func(
                params["selector"],
                **cls.get_selector_kwargs(**params),
            )
            shifts |= selector_func.all_shifts

        return shifts

    @classmethod
    def get_selector_func(cls, selector, copy=True, **update_kwargs):
        from ap.selection import Selector

        func = Selector.get(selector, copy=copy)
        if update_kwargs:
            func.run_update(**update_kwargs)

        return func

    @property
    def selector_func(self):
        if self._selector_func is None:
            # store a copy of the selector and update it
            self._selector_func = self.get_selector_func(
                self.selector,
                **self.get_selector_kwargs(self),
            )
        return self._selector_func

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

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # cache for producer func
        self._producer_func = None

    @classmethod
    def modify_param_values(cls, params):
        params = super().modify_param_values(params)

        # add the default producer when empty
        if "config" in params and params.get("producer") == law.NO_STR:
            config_inst = cls.get_analysis_inst(cls.analysis).get_config(params["config"])
            if config_inst.x("default_producer", None):
                params["producer"] = config_inst.x.default_producer

        return params

    @classmethod
    def determine_allowed_shifts(cls, config_inst, params):
        shifts = super().determine_allowed_shifts(config_inst, params)

        # get the producer, update it and add its shifts
        if params.get("producer") not in (None, law.NO_STR):
            producer_func = cls.get_producer_func(
                params["producer"],
                **cls.get_producer_kwargs(**params),
            )
            shifts |= producer_func.all_shifts

        return shifts

    @classmethod
    def get_producer_func(cls, producer, copy=True, **update_kwargs):
        from ap.production import Producer

        func = Producer.get(producer, copy=copy)
        if update_kwargs:
            func.run_update(**update_kwargs)

        return func

    @property
    def producer_func(self):
        if self._producer_func is None:
            # store a copy of the producer and update it
            self._producer_func = self.get_producer_func(
                self.producer,
                **self.get_producer_kwargs(self),
            )
        return self._producer_func

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

    @classmethod
    def determine_allowed_shifts(cls, config_inst, params):
        shifts = super().determine_allowed_shifts(config_inst, params)

        # get the producers, update them and add their shifts
        if params.get("producers") not in (None, law.NO_STR):
            producer_kwargs = cls.get_producer_kwargs(**params)
            for producer in params["producers"]:
                producer_func = ProducerMixin.get_producer_func(producer, **producer_kwargs)
                shifts |= producer_func.all_shifts

        return shifts

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
