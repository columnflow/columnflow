# coding: utf-8

"""
Lightweight mixins task classes.
"""

import importlib
from typing import Union, Sequence, List, Set, Dict, Any, Callable

import law
import luigi
import order as od

from columnflow.tasks.framework.base import AnalysisTask, ConfigTask
from columnflow.calibration import Calibrator
from columnflow.selection import Selector
from columnflow.production import Producer
from columnflow.ml import MLModel
from columnflow.inference import InferenceModel


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
            calibrator_inst = cls.get_calibrator_inst(
                params["calibrator"],
                cls.get_calibrator_kwargs(**params),
            )
            shifts |= calibrator_inst.all_shifts

        return shifts

    @classmethod
    def get_calibrator_inst(cls, calibrator, inst_dict=None):
        return Calibrator.get_cls(calibrator)(inst_dict=inst_dict)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # cache for calibrator inst
        self._calibrator_inst = None

    @property
    def calibrator_inst(self):
        if self._calibrator_inst is None:
            # store a calibrator instance
            self._calibrator_inst = self.get_calibrator_inst(
                self.calibrator,
                self.get_calibrator_kwargs(self),
            )
        return self._calibrator_inst

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
            calibrator_kwargs = cls.get_calibrator_kwargs(**params)
            for calibrator in params["calibrators"]:
                calibrator_inst = cls.get_calibrator_inst(calibrator, calibrator_kwargs)
                shifts |= calibrator_inst.all_shifts

        return shifts

    @classmethod
    def get_calibrator_inst(cls, calibrator, inst_dict=None):
        return Calibrator.get_cls(calibrator)(inst_dict=inst_dict)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # cache for calibrator insts
        self._calibrator_insts = None

    @property
    def calibrator_insts(self):
        if self._calibrator_insts is None:
            # store instances of all calibrators
            calibrator_kwargs = self.get_calibrator_kwargs(self)
            self._calibrator_insts = [
                self.get_calibrator_inst(calibrator, calibrator_kwargs)
                for calibrator in self.calibrators
            ]
        return self._calibrator_insts

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
            selector_inst = cls.get_selector_inst(
                params["selector"],
                cls.get_selector_kwargs(**params),
            )
            shifts |= selector_inst.all_shifts

        return shifts

    @classmethod
    def get_selector_inst(cls, selector, inst_dict=None):
        selector_cls = Selector.get_cls(selector)

        if not selector_cls.exposed:
            raise RuntimeError(f"cannot use unexposed selector '{selector}' in {cls.__name__}")

        return selector_cls(inst_dict=inst_dict)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # cache for selector inst
        self._selector_inst = None

    @property
    def selector_inst(self):
        if self._selector_inst is None:
            # store a selector instance
            self._selector_inst = self.get_selector_inst(
                self.selector,
                self.get_selector_kwargs(self),
            )
        return self._selector_inst

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
            producer_inst = cls.get_producer_inst(
                params["producer"],
                cls.get_producer_kwargs(**params),
            )
            shifts |= producer_inst.all_shifts

        return shifts

    @classmethod
    def get_producer_inst(cls, producer, inst_dict=None):
        return Producer.get_cls(producer)(inst_dict=inst_dict)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # cache for producer inst
        self._producer_inst = None

    @property
    def producer_inst(self):
        if self._producer_inst is None:
            # store a producer instance
            self._producer_inst = self.get_producer_inst(
                self.producer,
                self.get_producer_kwargs(self),
            )
        return self._producer_inst

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
            producer_kwargs = cls.get_producer_kwargs(**params)
            for producer in params["producers"]:
                producer_inst = cls.get_producer_inst(producer, producer_kwargs)
                shifts |= producer_inst.all_shifts

        return shifts

    @classmethod
    def get_producer_inst(cls, producer, inst_dict=None):
        return Producer.get_cls(producer)(inst_dict=inst_dict)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # cache for producer insts
        self._producer_insts = None

    @property
    def producer_insts(self):
        if self._producer_insts is None:
            # store instances of all producers
            producer_kwargs = self.get_producer_kwargs(self)
            self._producer_insts = [
                self.get_producer_inst(producer, producer_kwargs)
                for producer in self.producers
            ]
        return self._producer_insts

    def store_parts(self):
        parts = super().store_parts()

        part = "none"
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
    def get_ml_model_inst(cls, ml_model: str, config_inst: od.Config) -> MLModel:
        return MLModel.get_cls(ml_model)(config_inst)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # get the ML model instance
        self.ml_model_inst = None
        if self.ml_model != law.NO_STR:
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
        if self.ml_model_inst:
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

            # special case: initialize them once to trigger their set_config hook
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
        if self.ml_model_insts:
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
    def get_inference_model_inst(cls, inference_model: str, config_inst: od.Config) -> InferenceModel:
        return InferenceModel.get_cls(inference_model)(config_inst)

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

    allow_empty_categories = False

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

            # complain when no categories were found
            if not categories and not cls.allow_empty_categories:
                raise ValueError(f"no categories found matching '{params['categories']}'")

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

    default_variables = None
    allow_empty_variables = False

    @classmethod
    def modify_param_values(cls, params):
        params = super().modify_param_values(params)

        if "config_inst" not in params:
            return params
        config_inst = params["config_inst"]

        # resolve variables
        if "variables" in params:
            # when empty and default variables are defined, use them instead
            if not params["variables"] and cls.default_variables:
                params["variables"] = tuple(cls.default_variables)

            if params["variables"]:
                # resolve variable names
                variables = cls.find_config_objects(
                    params["variables"],
                    config_inst,
                    od.Variable,
                    config_inst.x("variable_groups", {}),
                )
            else:
                # fallback to using all known variables
                variables = config_inst.variables.names()

            # complain when no variables were found
            if not variables and not cls.allow_empty_variables:
                raise ValueError(f"no variables found matching '{params['variables']}'")

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

    allow_empty_datasets = False
    allow_empty_processes = False

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

            # complain when no processes were found
            if not processes and not cls.allow_empty_processes:
                raise ValueError(f"no processes found matching '{params['processes']}'")

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

            # complain when no datasets were found
            if not datasets and not cls.allow_empty_datasets:
                raise ValueError(f"no datasets found matching '{params['datasets']}'")

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

    allow_empty_shift_sources = False

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

            # complain when no shifts were found
            if not shifts and not cls.allow_empty_shift_sources:
                raise ValueError(f"no shifts found matching '{params['shift_sources']}'")

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
    skip_ratio = luigi.BoolParameter(
        default=False,
        significant=False,
        description="when True, no ratio (usually Data/Bkg ratio) is drawn in the lower panel; "
        "default: False",
    )
    shape_norm = luigi.BoolParameter(
        default=False,
        significant=False,
        description="when True, each process is normalized on it's integral in the upper panel; "
        "default: False",
    )
    skip_legend = luigi.BoolParameter(
        default=False,
        significant=False,
        description="when True, no legend is drawn; default: False",
    )
    skip_cms = luigi.BoolParameter(
        default=False,
        significant=False,
        description="when True, no CMS logo is drawn; default: False",
    )
    y_scale = luigi.ChoiceParameter(
        choices=(law.NO_STR, "linear", "log"),
        default=law.NO_STR,
        significant=False,
        description="string parameter to define the y-axis scale of the plot in the upper panel; "
        "choices: NO_STR,linear,log; no default",
    )

    def get_plot_parameters(self):
        # convert parameters to usable values during plotting
        return {
            "skip_ratio": self.skip_ratio,
            "shape_norm": self.shape_norm,
            "skip_legend": self.skip_legend,
            "skip_cms": self.skip_cms,
            "yscale": None if self.y_scale == law.NO_STR else self.y_scale,
        }

    def get_plot_func(self, func_name: str) -> Callable:
        """
        Returns a function, imported from a module given *func_name* which should have the format
        ``<module_to_import>.<function_name>``.
        """
        # prepare names
        if "." not in func_name:
            raise ValueError(f"invalid func_name format: {func_name}")
        module_id, name = func_name.rsplit(".", 1)

        # import the module
        try:
            mod = importlib.import_module(module_id)
        except ImportError as e:
            raise ImportError(f"cannot import plot function {name} from module {module_id}: {e}")

        # get the function
        func = getattr(mod, name, None)
        if func is None:
            raise Exception(f"module {module_id} does not contain plot function {name}")

        return func

    def call_plot_func(self, func_name: str, **kwargs) -> Any:
        """
        Gets the plot function referred to by *func_name* via :py:meth:`get_plot_func`, calls it
        with all *kwargs* and returns its result. *kwargs* are updated through the
        :py:meth:`update_plot_kwargs` hook first.
        """
        return self.get_plot_func(func_name)(**(self.update_plot_kwargs(kwargs)))

    def update_plot_kwargs(self, kwargs: dict) -> dict:
        """
        Hook to update keyword arguments *kwargs* used for plotting in :py:meth:`call_plot_func`.
        """
        return kwargs


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
            if output.path.endswith((".pdf", ".png")):
                if not isinstance(output, law.LocalTarget):
                    task.logger.warning(f"cannot show non-local plot at '{output.path}'")
                    continue
                elif output.path not in view_paths:
                    view_paths.append(output.path)

        # loop through paths and view them
        for path in view_paths:
            task.publish_message("showing {}".format(path))
            law.util.interruptable_popen(view_cmd.format(path), shell=True, executable="/bin/bash")

    return before_call, call, after_call


PlotMixin.view_output_plots = view_output_plots
