# coding: utf-8

"""
Lightweight mixins task classes.
"""

from typing import Union, Sequence, List, Set, Dict, Any, Optional

import law
import luigi
import order as od

from ap.tasks.framework.base import AnalysisTask, ConfigTask
from ap.calibration import Calibrator
from ap.selection import Selector
from ap.production import Producer
from ap.ml import MLModel
from ap.inference import InferenceModel


class CalibratorMixin(ConfigTask):

    calibrator = luigi.Parameter(
        default=law.NO_STR,
        description="the name of the calibrator to the applied; default: value of the "
        "'default_calibrator' config",
    )

    update_calibrator = False

    @classmethod
    def modify_param_values(cls, params):
        params = super().modify_param_values(params)

        # add the default calibrator when empty
        if "config_inst" in params and params.get("calibrator") == law.NO_STR:
            config_inst = params["config_inst"]
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
                **(cls.get_calibrator_kwargs(**params) if cls.update_calibrator else {}),
            )
            shifts |= calibrator_func.all_shifts

        return shifts

    @classmethod
    def get_calibrator_func(cls, calibrator, copy=True, **update_kwargs):
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
            # store a copy of the calibrator
            self._calibrator_func = self.get_calibrator_func(
                self.calibrator,
                **(self.get_calibrator_kwargs(self) if self.update_calibrator else {}),
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

    update_calibrators = False

    @classmethod
    def modify_param_values(cls, params):
        params = super().modify_param_values(params)

        if "config_inst" in params and params.get("calibrators") == ():
            config_inst = params["config_inst"]
            if config_inst.x("default_calibrator", None):
                params["calibrators"] = (config_inst.x.default_calibrator,)

        return params

    @classmethod
    def determine_allowed_shifts(cls, config_inst, params):
        shifts = super().determine_allowed_shifts(config_inst, params)

        # get the calibrators, update them and add their shifts
        if params.get("calibrators") not in (None, law.NO_STR):
            calibrator_kwargs = cls.get_calibrator_kwargs(**params) if cls.update_calibrators else {}
            for calibrator in params["calibrators"]:
                calibrator_func = cls.get_calibrator_func(calibrator, **calibrator_kwargs)
                shifts |= calibrator_func.all_shifts

        return shifts

    @classmethod
    def get_calibrator_func(cls, calibrator, copy=True, **update_kwargs):
        func = Calibrator.get(calibrator, copy=copy)
        if update_kwargs:
            func.run_update(**update_kwargs)

        return func

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # cache for calibrator funcs
        self._calibrator_funcs = None

    @property
    def calibrator_funcs(self):
        if self._calibrator_funcs is None:
            # store copies of all calibrators
            calibrator_kwargs = self.get_calibrator_kwargs(self) if self.update_calibrators else {}
            self._calibrator_funcs = [
                self.get_calibrator_func(calibrator, **calibrator_kwargs)
                for calibrator in self.calibrators
            ]
        return self._calibrator_funcs

    def store_parts(self):
        parts = super().store_parts()

        part = "__".join(self.calibrators[:5])
        if len(self.calibrators) > 5:
            part += f"__{law.util.create_hash(self.calibrators[5:])}"
        parts.insert_before("version", "calibrators", f"calib__{part}")

        return parts


class SelectorMixin(ConfigTask):

    selector = luigi.Parameter(
        default=law.NO_STR,
        description="the name of the selector to the applied; default: value of the "
        "'default_selector' config",
    )

    update_selector = False

    @classmethod
    def modify_param_values(cls, params):
        params = super().modify_param_values(params)

        # add the default selector when empty
        if "config_inst" in params and params.get("selector") == law.NO_STR:
            config_inst = params["config_inst"]
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
                **(cls.get_selector_kwargs(**params) if cls.update_selector else {}),
            )
            shifts |= selector_func.all_shifts

        return shifts

    @classmethod
    def get_selector_func(cls, selector, copy=True, **update_kwargs):
        func = Selector.get(selector, copy=copy)
        if not func.exposed:
            raise RuntimeError(f"cannot use unexposed selector '{selector}' in {cls.__name__}")
        if update_kwargs:
            func.run_update(**update_kwargs)

        return func

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # cache for selector func
        self._selector_func = None

    @property
    def selector_func(self):
        if self._selector_func is None:
            # store a copy of the selector
            self._selector_func = self.get_selector_func(
                self.selector,
                **(self.get_selector_kwargs(self) if self.update_selector else {}),
            )
        return self._selector_func

    def store_parts(self):
        parts = super().store_parts()
        parts.insert_before("version", "selector", f"sel__{self.selector}")
        return parts


class SelectorStepsMixin(SelectorMixin):

    selector_steps = law.CSVParameter(
        default=(),
        description="a subset of steps of the selector to apply; uses all steps when empty; "
        "empty default",
    )

    selector_steps_order_sensitive = False

    @classmethod
    def modify_param_values(cls, params: Dict[str, Any]) -> Dict[str, Any]:
        params = super().modify_param_values(params)

        # expand selector step groups
        if "config_inst" in params and len(params.get("selector_steps", ())) == 1:
            config_inst = params["config_inst"]
            step_group = params["selector_steps"][0]
            if step_group in config_inst.x("selector_step_groups", {}):
                params["selector_steps"] = tuple(config_inst.x.selector_step_groups[step_group])

        # sort selector steps when the order does not matter
        if not cls.selector_steps_order_sensitive and "selector_steps" in params:
            params["selector_steps"] = tuple(sorted(params["selector_steps"]))

        return params

    def store_parts(self) -> law.util.InsertableDict:
        parts = super().store_parts()

        steps = self.selector_steps
        if not self.selector_steps_order_sensitive:
            steps = sorted(steps)
        if steps:
            parts["selector"] += "__steps_" + "_".join(steps)

        return parts


class ProducerMixin(ConfigTask):

    producer = luigi.Parameter(
        default=law.NO_STR,
        description="the name of the producer to the applied; default: value of the "
        "'default_producer' config",
    )

    update_producer = False

    @classmethod
    def modify_param_values(cls, params):
        params = super().modify_param_values(params)

        # add the default producer when empty
        if "config_inst" in params and params.get("producer") == law.NO_STR:
            config_inst = params["config_inst"]
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
                **(cls.get_producer_kwargs(**params) if cls.update_producer else {}),
            )
            shifts |= producer_func.all_shifts

        return shifts

    @classmethod
    def get_producer_func(cls, producer, copy=True, **update_kwargs):
        func = Producer.get(producer, copy=copy)
        if update_kwargs:
            func.run_update(**update_kwargs)

        return func

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # cache for producer func
        self._producer_func = None

    @property
    def producer_func(self):
        if self._producer_func is None:
            # store a copy of the producer
            self._producer_func = self.get_producer_func(
                self.producer,
                **(self.get_producer_kwargs(self) if self.update_producer else {}),
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

    update_producers = False

    @classmethod
    def modify_param_values(cls, params):
        params = super().modify_param_values(params)

        if "config_inst" in params and params.get("producers") == ():
            config_inst = params["config_inst"]
            if config_inst.x("default_producer", None):
                params["producers"] = (config_inst.x.default_producer,)

        return params

    @classmethod
    def determine_allowed_shifts(cls, config_inst, params):
        shifts = super().determine_allowed_shifts(config_inst, params)

        # get the producers, update them and add their shifts
        if params.get("producers") not in (None, law.NO_STR):
            producer_kwargs = cls.get_producer_kwargs(**params) if cls.update_producers else {}
            for producer in params["producers"]:
                producer_func = cls.get_producer_func(producer, **producer_kwargs)
                shifts |= producer_func.all_shifts

        return shifts

    @classmethod
    def get_producer_func(cls, producer, copy=True, **update_kwargs):
        func = Producer.get(producer, copy=copy)
        if update_kwargs:
            func.run_update(**update_kwargs)

        return func

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # cache for producer funcs
        self._producer_funcs = None

    @property
    def producer_funcs(self):
        if self._producer_funcs is None:
            # store copies of all producers
            producer_kwargs = self.get_producer_kwargs(self) if self.update_producers else {}
            self._producer_funcs = [
                self.get_producer_func(producer, **producer_kwargs)
                for producer in self.producers
            ]
        return self._producer_funcs

    def store_parts(self):
        parts = super().store_parts()

        if self.producers:
            part = "__".join(self.producers[:5])
            if len(self.producers) > 5:
                part += f"__{law.util.create_hash(self.producers[5:])}"
            parts.insert_before("version", "producers", f"prod__{part}")

        return parts


class MLModelMixin(ConfigTask):

    ml_model = luigi.Parameter(
        default=law.NO_STR,
        description="the name of the ML model to the applied; default: value of the "
        "'default_ml_model' config",
    )

    @classmethod
    def modify_param_values(cls, params: Dict[str, Any]) -> Dict[str, Any]:
        params = super().modify_param_values(params)

        # add the default ml model when empty
        if "config_inst" in params:
            config_inst = params["config_inst"]
            if params.get("ml_model") == law.NO_STR and config_inst.x("default_ml_model", None):
                params["ml_model"] = config_inst.x.default_ml_model

            # initialize it once to trigger its set_config hook which might, in turn,
            # add objects to the config itself
            if params.get("ml_model") not in (law.NO_STR, None):
                cls.get_ml_model_inst(params["ml_model"], config_inst)

        return params

    @classmethod
    def get_ml_model_inst(
        cls,
        ml_model: str,
        config_inst: Optional[od.Config] = None,
        copy: bool = True,
    ) -> MLModel:
        ml_model_inst = MLModel.get(ml_model, copy=True)
        if config_inst:
            ml_model_inst.set_config(config_inst)

        return ml_model_inst

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # get the ML model instance
        self.ml_model_inst = self.get_ml_model_inst(self.ml_model, self.config_inst)

    def events_used_in_training(self, dataset_inst: od.Dataset, shift_inst: od.Shift) -> bool:
        # evaluate whether the events for the combination of dataset_inst and shift_inst
        # shall be used in the training
        return (
            dataset_inst in self.ml_model_inst.used_datasets and
            not shift_inst.x("disjoint_from_nominal", False)
        )

    def store_parts(self) -> law.util.InsertableDict:
        parts = super().store_parts()
        if self.ml_model != law.NO_STR:
            parts.insert_before("version", "ml_model", f"ml__{self.ml_model}")
        return parts


class MLModelsMixin(ConfigTask):

    ml_models = law.CSVParameter(
        default=(),
        description="comma-separated names of ML models to be applied; empty default",
    )

    @classmethod
    def modify_param_values(cls, params: Dict[str, Any]) -> Dict[str, Any]:
        params = super().modify_param_values(params)

        if "config_inst" in params:
            config_inst = params["config_inst"]
            if params.get("ml_models") == () and config_inst.x("default_ml_model", None):
                params["ml_models"] = (config_inst.x.default_ml_model,)

            # initialize them once to trigger their set_config hook
            if params.get("ml_models"):
                for ml_model in params["ml_models"]:
                    MLModelMixin.get_ml_model_inst(ml_model, config_inst)

        return params

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # get the ML model instances
        self.ml_model_insts = [
            MLModelMixin.get_ml_model_inst(ml_model, self.config_inst)
            for ml_model in self.ml_models
        ]

    def store_parts(self) -> law.util.InsertableDict:
        parts = super().store_parts()

        if self.ml_models:
            part = "__".join(self.ml_models)
            parts.insert_before("version", "ml_models", f"ml__{part}")

        return parts


class InferenceModelMixin(ConfigTask):

    inference_model = luigi.Parameter(
        default=law.NO_STR,
        description="the name of the inference model to the used; default: value of the "
        "'default_inference_model' config",
    )

    @classmethod
    def modify_param_values(cls, params: Dict[str, Any]) -> Dict[str, Any]:
        params = super().modify_param_values(params)

        # add the default inference model when empty
        if "config_inst" in params:
            config_inst = params["config_inst"]
            if params.get("inference_model") == law.NO_STR and config_inst.x("default_inference_model", None):
                params["inference_model"] = config_inst.x.default_inference_model

        return params

    @classmethod
    def get_inference_model_inst(
        cls,
        inference_model: str,
        config_inst: Optional[od.Config] = None,
        copy: bool = True,
    ) -> InferenceModel:
        inference_model_inst = InferenceModel.get(inference_model, copy=True)
        if config_inst:
            inference_model_inst.set_config(config_inst)

        return inference_model_inst

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # get the inference model instance
        self.inference_model_inst = self.get_inference_model_inst(self.inference_model, self.config_inst)

    def store_parts(self) -> law.util.InsertableDict:
        parts = super().store_parts()
        if self.inference_model != law.NO_STR:
            parts.insert_before("version", "inf_model", f"inf__{self.inference_model}")
        return parts


class CategoriesMixin(ConfigTask):

    categories = law.CSVParameter(
        default=("1mu",),
        description="comma-separated category names or patterns to select; can also be the key of "
        "a mapping defined in 'category_groups' auxiliary data of the config; default: ('1mu',)",
    )

    @classmethod
    def modify_param_values(cls, params):
        params = super().modify_param_values(params)

        if "config_inst" not in params:
            return params
        config_inst = params["config_inst"]

        # resolve categories
        if "categories" in params:
            categories = cls.find_config_objects(
                params["categories"],
                config_inst,
                od.Category,
                config_inst.x("category_groups", {}),
                deep=True,
            )
            params["categories"] = tuple(categories)

        return params

    @property
    def categories_repr(self):
        if len(self.categories) == 1:
            return self.categories[0]

        return f"{len(self.categories)}_{law.util.create_hash(sorted(self.categories))}"


class VariablesMixin(ConfigTask):

    variables = law.CSVParameter(
        default=(),
        description="comma-separated variable names or patterns to select; can also be the key of "
        "a mapping defined in the 'variable_group' auxiliary data of the config; when empty, uses "
        "all variables of the config; empty default",
    )

    @classmethod
    def modify_param_values(cls, params):
        params = super().modify_param_values(params)

        if "config_inst" not in params:
            return params
        config_inst = params["config_inst"]

        # resolve variables
        if "variables" in params:
            if params["variables"]:
                variables = cls.find_config_objects(
                    params["variables"],
                    config_inst,
                    od.Variable,
                    config_inst.x("variable_groups", {}),
                )
            else:
                variables = config_inst.variables.names()
            params["variables"] = tuple(variables)

        return params

    @property
    def variables_repr(self):
        if len(self.variables) == 1:
            return self.variables[0]

        return f"{len(self.variables)}_{law.util.create_hash(sorted(self.variables))}"


class DatasetsProcessesMixin(ConfigTask):

    datasets = law.CSVParameter(
        default=(),
        description="comma-separated dataset names or patters to select; can also be the key of a "
        "mapping defined in the 'dataset_groups' auxiliary data of the config; when empty, uses "
        "all datasets registered in the config that contain any of the selected --processes; empty "
        "default",
    )
    processes = law.CSVParameter(
        default=(),
        description="comma-separated process names or patterns for filtering processes; can also "
        "be the key of a mapping defined in the 'process_groups' auxiliary data of the config; "
        "uses all processes of the config when empty; empty default",
    )

    @classmethod
    def modify_param_values(cls, params):
        params = super().modify_param_values(params)

        if "config_inst" not in params:
            return params
        config_inst = params["config_inst"]

        # resolve processes
        if "processes" in params:
            if params["processes"]:
                processes = cls.find_config_objects(
                    params["processes"],
                    config_inst,
                    od.Process,
                    config_inst.x("process_groups", {}),
                    deep=True,
                )
            else:
                processes = config_inst.processes.names()
            params["processes"] = tuple(processes)

        # resolve datasets
        if "datasets" in params:
            if params["datasets"]:
                datasets = cls.find_config_objects(
                    params["datasets"],
                    config_inst,
                    od.Dataset,
                    config_inst.x("dataset_groups", {}),
                )
            elif "processes" in params:
                # pick all datasets that contain any of the requested (sub) processes
                sub_process_insts = sum((
                    [proc for proc, _, _ in process_inst.walk_processes(include_self=True)]
                    for process_inst in map(config_inst.get_process, params["processes"])
                ), [])
                datasets = (
                    dataset_inst.name
                    for dataset_inst in config_inst.datasets
                    if any(map(dataset_inst.has_process, sub_process_insts))
                )
            params["datasets"] = tuple(datasets)

        return params

    @property
    def datasets_repr(self):
        if len(self.datasets) == 1:
            return self.datasets[0]

        return f"{len(self.datasets)}_{law.util.create_hash(sorted(self.datasets))}"

    @property
    def processes_repr(self):
        if len(self.processes) == 1:
            return self.processes[0]

        return f"{len(self.processes)}_{law.util.create_hash(self.processes)}"


class ShiftSourcesMixin(ConfigTask):

    shift_sources = law.CSVParameter(
        default=(),
        description="comma-separated shift source names (without direction) or patterns to select; "
        "can also be the key of a mapping defined in the 'shift_group' auxiliary data of the "
        "config; default: ()",
    )

    @classmethod
    def modify_param_values(cls, params):
        params = super().modify_param_values(params)

        if "config_inst" not in params:
            return params
        config_inst = params["config_inst"]

        # resolve shift sources
        if "shift_sources" in params:
            # convert to full shift first to do the object finding
            shifts = cls.find_config_objects(
                cls.expand_shift_sources(params["shift_sources"]),
                config_inst,
                od.Shift,
                config_inst.x("shift_groups", {}),
            )
            # convert back to sources
            params["shift_sources"] = tuple(cls.reduce_shifts(shifts))

        return params

    @classmethod
    def expand_shift_sources(cls, sources: Union[Sequence[str], Set[str]]) -> List[str]:
        return sum(([f"{s}_up", f"{s}_down"] for s in sources), [])

    @classmethod
    def reduce_shifts(cls, shifts: Union[Sequence[str], Set[str]]) -> List[str]:
        return list(set(od.Shift.split_name(shift)[0] for shift in shifts))

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.shifts = self.expand_shift_sources(self.shift_sources)

    @property
    def shift_sources_repr(self):
        if len(self.shift_sources) == 1:
            return self.shift_sources[0]

        return f"{len(self.shift_sources)}_{law.util.create_hash(sorted(self.shift_sources))}"


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
