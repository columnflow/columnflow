# coding: utf-8

"""
Basic objects for defining statistical inference models.
"""

from __future__ import annotations

import enum
import copy as _copy

import law
import order as od
import yaml

from columnflow.types import Generator, Callable, TextIO, Sequence, Any
from columnflow.util import (
    DerivableMeta, Derivable, DotDict, is_pattern, is_regex, pattern_matcher,
)


class ParameterType(enum.Enum):
    """
    Parameter type flag.
    """

    rate_gauss = "rate_gauss"
    rate_uniform = "rate_uniform"
    rate_unconstrained = "rate_unconstrained"
    shape = "shape"

    def __str__(self: ParameterType) -> str:
        return self.value

    @property
    def is_rate(self: ParameterType) -> bool:
        return self in (
            self.rate_gauss,
            self.rate_uniform,
            self.rate_unconstrained,
        )

    @property
    def is_shape(self: ParameterType) -> bool:
        return self in (
            self.shape,
        )


class ParameterTransformation(enum.Enum):
    """
    Flags denoting transformations to be applied on parameters.
    """

    none = "none"
    centralize = "centralize"
    symmetrize = "symmetrize"
    asymmetrize = "asymmetrize"
    asymmetrize_if_large = "asymmetrize_if_large"
    normalize = "normalize"
    effect_from_shape = "effect_from_shape"
    effect_from_rate = "effect_from_rate"

    def __str__(self: ParameterTransformation) -> str:
        return self.value

    @property
    def from_shape(self: ParameterTransformation) -> bool:
        return self in (
            self.effect_from_shape,
        )

    @property
    def from_rate(self: ParameterTransformation) -> bool:
        return self in (
            self.effect_from_rate,
        )


class ParameterTransformations(tuple):
    """
    Container around a sequence of :py:class:`ParameterTransformation`'s with a few convenience
    methods.
    """

    def __new__(
        cls,
        transformations: Sequence[ParameterTransformation | str],
    ) -> ParameterTransformations:
        # TODO: at this point one could object / complain in case incompatible transfos are used
        transformations = [
            (t if isinstance(t, ParameterTransformation) else ParameterTransformation[t])
            for t in transformations
        ]

        # initialize
        return super().__new__(cls, transformations)

    @property
    def any_from_shape(self: ParameterTransformations) -> bool:
        return any(t.from_shape for t in self)

    @property
    def any_from_rate(self: ParameterTransformations) -> bool:
        return any(t.from_rate for t in self)


class InferenceModel(Derivable):
    """
    Interface to statistical inference models with connections to config objects (such as
    py:class:`order.Config` or :py:class:`order.Dataset`).

    The internal structure to describe a model looks as follows (in yaml style) and is accessible
    through :py:attr:`model` as well as property access to its top-level objects.

    .. code-block:: yaml

        categories:
          - name: cat1
            config_category: 1e
            config_variable: ht
            config_data_datasets: [data_mu_a]
            data_from_processes: []
            mc_stats: 10
            processes:
              - name: HH
                config_process: hh
                is_signal: True
                config_mc_datasets: [hh_ggf]
                scale: 1.0
                parameters:
                  - name: lumi
                    type: rate_gauss
                    effect: 1.02
                    config_shift_source: null
                  - name: pu
                    type: rate_gauss
                    effect: [0.97, 1.02]
                    config_shift_source: null
                  - name: pileup
                    type: shape
                    effect: 1.0
                    config_shift_source: minbias_xs
              - name: tt
                is_signal: False
                config_process: ttbar
                config_mc_datasets: [tt_sl, tt_dl, tt_fh]
                scale: 1.0
                parameters:
                  - name: lumi
                    type: rate_gauss
                    effect: 1.02
                    config_shift_source: null

          - name: cat2
            ...

        parameter_groups:
          - name: rates
            parameters_names: [lumi, pu]
          - ...

    .. py:attribute:: name

        type: str

        The unique name of this model.

    .. py:attribute:: config_inst

        type: order.Config, None

        Reference to the :py:class:`order.Config` object.

    .. py:attribute:: config_callbacks

        type: list

        A list of callables that are invoked after :py:meth:`set_config` was called.

    .. py:attribute:: model

        type: DotDict

        The internal data structure representing the model.
    """

    # optional initialization method
    init_func = None

    class YamlDumper(yaml.SafeDumper or object):
        """
        YAML dumper for statistical inference models with ammended representers to serialize
        internal, structured objects as safe, standard objects.
        """

        def __init__(self: InferenceModel.YamlDumper, *args, **kwargs) -> None:
            super().__init__(*args, **kwargs)

            # ammend representers
            map_repr = lambda dumper, data: dumper.represent_mapping("tag:yaml.org,2002:map", data.items())
            self.add_representer(DotDict, map_repr)

            list_repr = lambda dumper, data: dumper.represent_list(list(data))
            self.add_representer(tuple, list_repr)

            str_repr = lambda dumper, data: dumper.represent_str(str(data))
            self.add_representer(ParameterType, str_repr)

        def ignore_aliases(self: InferenceModel.YamlDumper, *args, **kwargs) -> bool:
            return True

    @classmethod
    def inference_model(
        cls,
        func: Callable | None = None,
        bases: tuple[type] = (),
        **kwargs,
    ) -> DerivableMeta | Callable:
        """
        Decorator for creating a new :py:class:`InferenceModel` subclass with additional, optional
        *bases* and attaching the decorated function to it as ``init_func``. All additional *kwargs* are
        added as class members of the new subclasses.
        """
        def decorator(func: Callable) -> DerivableMeta:
            # create the class dict
            cls_dict = {"init_func": func}
            cls_dict.update(kwargs)

            # create the subclass
            subclass = cls.derive(func.__name__, bases=bases, cls_dict=cls_dict)

            return subclass

        return decorator(func) if func else decorator

    @classmethod
    def model_spec(cls) -> DotDict:
        """
        Returns a dictionary representing the top-level structure of the model.

            - *categories*: List of :py:meth:`category_spec` objects.
            - *parameter_groups*: List of :py:meth:`paramter_group_spec` objects.
        """
        return DotDict([
            ("categories", []),
            ("parameter_groups", []),
        ])

    @classmethod
    def category_spec(
        cls,
        name: str,
        config_category: str | None = None,
        config_variable: str | None = None,
        config_data_datasets: Sequence[str] | None = None,
        data_from_processes: Sequence[str] | None = None,
        mc_stats: float | tuple | None = None,
        empty_bin_value: float = 1e-5,
    ) -> DotDict:
        """
        Returns a dictionary representing a category (interchangeably called bin or channel in other
        tools), forwarding all arguments.

            - *name*: The name of the category in the model.
            - *config_category*: The name of the source category in the config to use.
            - *config_variable*: The name of the variable in the config to use.
            - *config_data_datasets*: List of names of datasets in the config to use for real data.
            - *data_from_processes*: Optional list of names of :py:meth:`process_spec` objects that,
              when *config_data_datasets* is not defined, make of a fake data contribution.
            - *mc_stats*: Either *None* to disable MC stat uncertainties, or a float or tuple of
              floats to control the options of MC stat options.
            - *empty_bin_value*: When bins are no content, they are filled with this value.
        """
        return DotDict([
            ("name", str(name)),
            ("config_category", str(config_category) if config_category else None),
            ("config_variable", str(config_variable) if config_variable else None),
            ("config_data_datasets", list(map(str, config_data_datasets or []))),
            ("data_from_processes", list(map(str, data_from_processes or []))),
            ("mc_stats", mc_stats),
            ("empty_bin_value", empty_bin_value),
            ("processes", []),
        ])

    @classmethod
    def process_spec(
        cls,
        name: str,
        config_process: str | None = None,
        is_signal: bool = False,
        config_mc_datasets: Sequence[str] | None = None,
        scale: float | int = 1.0,
    ) -> DotDict:
        """
        Returns a dictionary representing a process, forwarding all arguments.

            - *name*: The name of the process in the model.
            - *is_signal*: A boolean flag deciding whether this process describes signal.
            - *config_process*: The name of the source process in the config to use.
            - *config_mc_datasets*: List of names of MC datasets in the config to use.
            - *scale*: A float value to scale the process, defaulting to 1.0.
        """
        return DotDict([
            ("name", str(name)),
            ("is_signal", bool(is_signal)),
            ("config_process", str(config_process) if config_process else None),
            ("config_mc_datasets", list(map(str, config_mc_datasets or []))),
            ("scale", float(scale)),
            ("parameters", []),
        ])

    @classmethod
    def parameter_spec(
        cls,
        name: str,
        type: ParameterType | str,
        transformations: Sequence[ParameterTransformation | str] = (ParameterTransformation.none,),
        config_shift_source: str | None = None,
        effect: Any | None = 1.0,
    ) -> DotDict:
        """
        Returns a dictionary representing a (nuisance) parameter, forwarding all arguments.

            - *name*: The name of the parameter in the model.
            - *type*: A :py:class:`ParameterType` instance describing the type of this parameter.
            - *transformations*: A sequence of :py:class:`ParameterTransformation` instances
              describing transformations to be applied to the effect of this parameter.
            - *config_shift_source*: The name of a systematic shift source in the config that this
              parameter corresponds to.
            - *effect*: An arbitrary object describing the effect of the parameter (e.g. float for
              symmetric rate effects, 2-tuple for down/up variation, etc).
        """
        return DotDict([
            ("name", str(name)),
            ("type", type if isinstance(type, ParameterType) else ParameterType[type]),
            ("transformations", ParameterTransformations(transformations)),
            ("config_shift_source", str(config_shift_source) if config_shift_source else None),
            ("effect", effect),
        ])

    @classmethod
    def parameter_group_spec(
        cls,
        name: str,
        parameter_names: Sequence[str] | None = None,
    ) -> DotDict:
        """
        Returns a dictionary representing a group of parameter names.

            - *name*: The name of the parameter group in the model.
            - *parameter_names*: Names of parameter objects this group contains.
        """
        return DotDict([
            ("name", str(name)),
            ("parameter_names", list(map(str, parameter_names or []))),
        ])

    @classmethod
    def require_shapes_for_parameter(self: InferenceModel, param_obj: dict) -> bool:
        """
        Returns *True* if for a certain parameter object *param_obj* varied shapes are needed, and
        *False* otherwise.
        """
        if param_obj.type.is_shape:
            # the shape might be build from a rate, in which case input shapes are not required
            if param_obj.transformations.any_from_rate:
                return False
            # in any other case, shapes are required
            return True

        if param_obj.type.is_rate:
            # when the rate effect is extracted from shapes, they are required
            if param_obj.transformations.any_from_shape:
                return True
            # in any other case, shapes are not required
            return False

        # other cases are not supported
        raise Exception(
            f"shape requirement cannot be evaluated of parameter '{param_obj.name}' with type " +
            f"'{param_obj.type}' and transformations {param_obj.transformations}",
        )

    def __init__(self: InferenceModel, config_inst: od.Config) -> None:
        super().__init__()

        # store attributes
        self.config_inst = config_inst

        # model info
        self.model = self.model_spec()

        # custom init function when set, always followed by the cleanup
        if callable(self.init_func):
            self.init_func()
            self.cleanup()

    def to_yaml(self: InferenceModel, stream: TextIO | None = None) -> str | None:
        """
        Writes the content of the :py:attr:`model` into a file-like object *stream* when given, and
        returns a string representation otherwise.
        """
        return yaml.dump(self.model, stream=stream, Dumper=self.YamlDumper)

    def pprint(self: InferenceModel) -> None:
        """
        Pretty-prints the content of the :py:attr:`model` in yaml-style.
        """
        print(self.to_yaml())

    #
    # property access to top-level objects
    #

    @property
    def categories(self: InferenceModel) -> DotDict:
        return self.model.categories

    @property
    def parameter_groups(self: InferenceModel) -> DotDict:
        return self.model.parameter_groups

    #
    # handling of categories
    #

    def get_categories(
        self: InferenceModel,
        category: str | Sequence[str] | None = None,
        only_names: bool = False,
    ) -> list[DotDict | str]:
        """
        Returns a list of categories whose name match *category*. *category* can be a string, a
        pattern, or sequence of them. When *only_names* is *True*, only names of categories are
        returned rather than structured dictionaries.
        """
        # rename arguments to make their meaning explicit
        category_pattern = category

        match = pattern_matcher(category_pattern or "*")
        return [
            (category.name if only_names else category)
            for category in self.categories
            if match(category.name)
        ]

    def get_category(
        self: InferenceModel,
        category: str | Sequence[str],
        only_name: bool = False,
        silent: bool = False,
    ) -> DotDict | str:
        """
        Returns a single category whose name matches *category*. *category* can be a string, a
        pattern, or sequence of them. An exception is raised if no or more than one category is
        found, unless *silent* is *True* in which case *None* is returned. When *only_name* is
        *True*, only the name of the category is returned rather than a structured dictionary.
        """
        # rename arguments to make their meaning explicit
        category_name = category

        categories = self.get_categories(category_name, only_names=only_name)

        # length checks
        if not categories:
            if silent:
                return None
            raise ValueError(f"no category named '{category_name}' found")
        if len(categories) > 1:
            if silent:
                return None
            raise ValueError(f"category name '{category_name}' matches more than one category")

        return categories[0]

    def has_category(
        self: InferenceModel,
        category: str | Sequence[str],
    ) -> bool:
        """
        Returns *True* if a category whose name matches *category* is existing, and *False*
        otherwise. *category* can be a string, a pattern, or sequence of them.
        """
        # rename arguments to make their meaning explicit
        category_pattern = category

        # simple length check
        return len(self.get_categories(category_pattern)) > 0

    def add_category(self: InferenceModel, *args, **kwargs) -> None:
        """
        Adds a new category with all *args* and *kwargs* used to create the structured category
        dictionary via :py:meth:`category_spec`. If a category with the same name already exists, an
        exception is raised.
        """
        # create the object
        category = self.category_spec(*args, **kwargs)

        # checks
        if self.has_category(category.name):
            raise ValueError(f"category named '{category.name}' already registered")

        # add it
        self.categories.append(category)

    def remove_category(
        self: InferenceModel,
        category: str | Sequence[str],
    ) -> bool:
        """
        Removes one or more categories whose names match *category*. Returns *True* if at least one
        category was removed, and *False* otherwise. *category* can be a string, a pattern, or
        sequence of them.
        """
        # rename arguments to make their meaning explicit
        category_pattern = category

        match = pattern_matcher(category_pattern)
        n_before = len(self.categories)
        self.categories[:] = [
            category
            for category in self.categories
            if not match(category.name)
        ]

        # stop early if nothing was removed
        removed_any = len(self.categories) != n_before
        if not removed_any:
            return False

        return True

    #
    # handling of processes
    #

    def get_processes(
        self: InferenceModel,
        process: str | Sequence[str] | None = None,
        category: str | Sequence[str] | None = None,
        only_names: bool = False,
        flat: bool = False,
    ) -> dict[str, DotDict | str] | list[str]:
        """
        Returns a dictionary of processes whose names match *process*, mapped to the name of the
        category they belong to. Categories can optionally be filtered through *category*. Both
        *process* and *category* can be a string, a pattern, or sequence of them.

        When *only_names* is *True*, only names of processes are returned rather than structured
        dictionaries. When *flat* is *True*, a flat, unique list of process names is returned.
        """
        # rename arguments to make their meaning explicit
        process_pattern = process
        category_pattern = category

        # when flat is True, one can only return names
        if flat:
            only_names = True

        # get matching categories first
        categories = self.get_categories(category_pattern)

        # look for the process pattern in each of them
        match = pattern_matcher(process_pattern or "*")
        pairs = (
            (category.name, [
                (process.name if only_names else process)
                for process in category.processes
                if match(process.name)
            ])
            for category in categories
        )

        # only store entries with processes
        processes = DotDict.wrap(pair for pair in pairs if pair[1])

        # flatten
        if flat:
            processes = law.util.make_unique(sum(processes.values(), []))

        return processes

    def get_process(
        self: InferenceModel,
        process: str | Sequence[str],
        category: str | Sequence[str] | None = None,
        only_name: bool = False,
        silent: bool = False,
    ) -> DotDict | str:
        """
        Returns a single process whose name matches *process*, and optionally, whose category's name
        matches *category*. Both *process* and *category* can be a string, a pattern, or sequence of
        them.

        An exception is raised if no or more than one process is found, unless
        *silent* is *True* in which case *None* is returned. When *only_name* is *True*, only the
        name of the process is returned rather than a structured dictionary.
        """
        # rename arguments to make their meaning explicit
        process_name = process
        category_pattern = category

        processes = self.get_processes(
            process_name,
            category=category_pattern,
            only_names=only_name,
        )

        # checks
        if not processes:
            if silent:
                return None
            err = f"no process named '{process_name}' found"
            if category_pattern:
                err += f" in category '{category_pattern}'"
            raise ValueError(err)
        if len(processes) > 1:
            if silent:
                return None
            raise ValueError(
                f"process '{process_name}' found in more than one category: " + ",".join(processes),
            )

        category, processes = list(processes.items())[0]
        if len(processes) > 1:
            if silent:
                return None
            names = processes if only_name else [p.name for p in processes]
            raise ValueError(
                f"process '{process_name}' found more than once in category '{category}': " +
                ",".join(names),
            )

        return processes[0]

    def has_process(
        self: InferenceModel,
        process: str | Sequence[str],
        category: str | Sequence[str] | None = None,
    ) -> bool:
        """
        Returns *True* if a process whose name matches *process*, and optionally whose category's
        name matches *category*, is existing, and *False* otherwise. Both *process* and *category*
        can be a string, a pattern, or sequence of them.
        """
        # rename arguments to make their meaning explicit
        process_pattern = process
        category_pattern = category

        # simple length check
        return len(self.get_processes(process_pattern, category=category_pattern)) > 0

    def add_process(
        self: InferenceModel,
        *args,
        category: str | Sequence[str] | None = None,
        silent: bool = False,
        **kwargs,
    ) -> None:
        """
        Adds a new process to all categories whose names match *category*, with all *args* and
        *kwargs* used to create the structured process dictionary via :py:meth:`process_spec`.
        *category* can be a string, a pattern, or sequence of them.

        If a process with the same name already exists in one of the categories, an exception is
        raised unless *silent* is *True*.
        """
        # rename arguments to make their meaning explicit
        category_pattern = category

        process = self.process_spec(*args, **kwargs)

        # get categories the process should be added to
        categories = self.get_categories(category_pattern)

        # check for duplicates
        target_categories = []
        for category in categories:
            if self.has_process(process.name, category=category.name):
                if silent:
                    continue
                raise ValueError(
                    f"process named '{process.name}' already registered in category " +
                    f"'{category.name}'",
                )
            target_categories.append(category)

        # add independent copies to categories
        for category in target_categories:
            category.processes.append(_copy.deepcopy(process))

    def remove_process(
        self: InferenceModel,
        process: str | Sequence[str],
        category: str | Sequence[str] | None = None,
    ) -> bool:
        """
        Removes one or more processes whose names match *process*, and optionally whose category's
        name match *category*. Both *process* and *category* can be a string, a pattern, or sequence
        of them. Returns *True* if at least one process was removed, and *False* otherwise.
        """
        # rename arguments to make their meaning explicit
        process_pattern = process
        category_pattern = category

        # get categories the process should be removed from
        categories = self.get_categories(category_pattern)

        match = pattern_matcher(process_pattern)
        removed_any = False
        for category in categories:
            n_before = len(category.processes)
            category.processes[:] = [
                process
                for process in category.processes
                if not match(process.name)
            ]
            removed_any |= len(category.processes) != n_before

        # stop early if nothing was removed
        if not removed_any:
            return False

        return True

    #
    # handling of parameters
    #

    def get_parameters(
        self: InferenceModel,
        parameter: str | Sequence[str] | None = None,
        process: str | Sequence[str] | None = None,
        category: str | Sequence[str] | None = None,
        only_names: bool = False,
        flat: bool = False,
    ) -> dict[str, dict[str, DotDict | str]] | list[str]:
        """
        Returns a dictionary of parameter whose names match *parameter*, mapped twice to the name of
        the category and the name of the process they belong to. Categories and processes can
        optionally be filtered through *category* and *process*. All three, *parameter*, *process*
        and *category* can be a string, a pattern, or sequence of them.

        When *only_names* is *True*, only names of parameters are returned rather than structured
        dictionaries. When *flat* is *True*, a flat, unique list of parameter names is returned.
        """
        # rename arguments to make their meaning explicit
        parameter_pattern = parameter
        process_pattern = process
        category_pattern = category

        # when flat is True, one can only return names
        if flat:
            only_names = True

        # get matching processes (mapped to matching categories)
        processes = self.get_processes(process=process_pattern, category=category_pattern)

        # look for the process pattern in each pair
        match = pattern_matcher(parameter_pattern or "*")
        parameters = DotDict()
        for category_name, _processes in processes.items():
            pairs = (
                (process.name, [
                    (parameter.name if only_names else parameter)
                    for parameter in process.parameters
                    if match(parameter.name)
                ])
                for process in _processes
            )

            # only store entries with parameters
            _parameters = DotDict.wrap(pair for pair in pairs if pair[1])
            if _parameters:
                parameters[category_name] = _parameters

        # flatten
        if flat:
            parameters = law.util.make_unique(sum((
                sum(_parameters.values(), [])
                for _parameters in parameters.values()
            ), []))

        return parameters

    def get_parameter(
        self: InferenceModel,
        parameter: str | Sequence[str],
        process: str | Sequence[str] | None = None,
        category: str | Sequence[str] | None = None,
        only_name: bool = False,
        silent: bool = False,
    ) -> DotDict | str:
        """
        Returns a single parameter whose name matches *parameter*, and optionally, whose category's
        and process' name matches *category* and *process*. All three, *parameter*, *process* and
        *category* can be a string, a pattern, or sequence of them.

        An exception is raised if no or more than one parameter is found, unless *silent* is *True*
        in which case *None* is returned. When *only_name* is *True*, only the name of the parameter
        is returned rather than a structured dictionary.
        """
        # rename arguments to make their meaning explicit
        parameter_name = parameter
        process_pattern = process
        category_pattern = category

        parameters = self.get_parameters(
            parameter_name,
            process=process_pattern,
            category=category_pattern,
            only_names=only_name,
        )

        # checks
        if not parameters:
            if silent:
                return None
            err = f"no parameter named '{parameter_name}' found"
            if process_pattern:
                err += f" for process '{process_pattern}'"
            if category_pattern:
                err += f" in category '{category_pattern}'"
            raise ValueError(err)
        if len(parameters) > 1:
            if silent:
                return None
            raise ValueError(
                f"parameter '{parameter_name}' found in more than one category: " +
                ",".join(parameters),
            )

        category, parameters = list(parameters.items())[0]
        if len(parameters) > 1:
            if silent:
                return None
            raise ValueError(
                f"parameter '{parameter_name}' found in more than one process in category " +
                f"'{category}': " + ",".join(parameters),
            )

        process, parameters = list(parameters.items())[0]
        if len(parameters) > 1:
            if silent:
                return None
            names = parameters if only_name else [p.name for p in parameters]
            raise ValueError(
                f"parameter '{parameter_name}' found more than once in category '{category}' and " +
                f"process '{process}': " + ",".join(names),
            )

        return parameters[0]

    def has_parameter(
        self: InferenceModel,
        parameter: str | Sequence[str],
        process: str | Sequence[str] | None = None,
        category: str | Sequence[str] | None = None,
    ) -> bool:
        """
        Returns *True* if a parameter whose name matches *parameter*, and optionally whose
        category's and process' name match *category* and *process*, is existing, and *False*
        otherwise. All three, *parameter*, *process* and *category* can be a string, a pattern, or
        sequence of them.
        """
        # rename arguments to make their meaning explicit
        parameter_pattern = parameter
        process_pattern = process
        category_pattern = category

        # simple length check
        return len(self.get_parameters(
            parameter_pattern,
            process=process_pattern,
            category=category_pattern,
        )) > 0

    def add_parameter(
        self: InferenceModel,
        *args,
        process: str | Sequence[str] | None = None,
        category: str | Sequence[str] | None = None,
        **kwargs,
    ) -> DotDict:
        """
        Adds a new parameter to all categories and processes whose names match *category* and
        *process*, with all *args* and *kwargs* used to create the structured parameter dictionary
        via :py:meth:`parameter_spec`. Both *process* and *category* can be a string, a pattern, or
        sequence of them.

        If a parameter with the same name already exists in one of the processes throughout the
        categories, an exception is raised.
        """
        # rename arguments to make their meaning explicit
        process_pattern = process
        category_pattern = category

        parameter = self.parameter_spec(*args, **kwargs)

        # get processes (mapped to categories) the parameter should be added to
        processes = self.get_processes(process=process_pattern, category=category_pattern)

        # check for duplicates
        for category_name, _processes in processes.items():
            for process in _processes:
                if self.has_parameter(parameter.name, process=process.name, category=category_name):
                    raise ValueError(
                        f"parameter named '{parameter.name}' already registered for process " +
                        f"'{process.name}' in category '{category_name}'",
                    )

        # add independent copies to processes
        for category_name, _processes in processes.items():
            for process in _processes:
                process.parameters.append(_copy.deepcopy(parameter))

        return parameter

    def remove_parameter(
        self: InferenceModel,
        parameter: str | Sequence[str],
        process: str | Sequence[str] | None = None,
        category: str | Sequence[str] | None = None,
    ) -> bool:
        """
        Removes one or more parameters whose names match *parameter*, and optionally whose
        category's and process' name match *category* and *process*. All three, *parameter*,
        *process* and *category* can be a string, a pattern, or sequence of them. Returns *True* if
        at least one parameter was removed, and *False* otherwise.
        """
        # rename arguments to make their meaning explicit
        parameter_pattern = parameter
        process_pattern = process
        category_pattern = category

        # get processes (mapped to categories) the parameter should be removed from
        processes = self.get_processes(process=process_pattern, category=category_pattern)

        match = pattern_matcher(parameter_pattern)
        removed_any = False
        for _processes in processes.values():
            for process in _processes:
                n_before = len(process.parameters)
                process.parameters[:] = [
                    parameter
                    for parameter in process.parameters
                    if not match(parameter.name)
                ]
                removed_any |= len(process.parameters) != n_before

        # stop early if nothing was removed
        if not removed_any:
            return False

        return True

    #
    # handling of parameter groups
    #

    def get_parameter_groups(
        self: InferenceModel,
        group: str | Sequence[str] | None = None,
        only_names: bool = False,
    ) -> list[DotDict | str]:
        """
        Returns a list of parameter group whose name match *group*. *group* can be a string, a
        pattern, or sequence of them.

        When *only_names* is *True*, only names of parameter groups are returned rather than
        structured dictionaries.
        """
        # rename arguments to make their meaning explicit
        group_pattern = group

        match = pattern_matcher(group_pattern or "*")
        return [
            (group.name if only_names else group)
            for group in self.parameter_groups
            if match(group.name)
        ]

    def get_parameter_group(
        self: InferenceModel,
        group: str | Sequence[str],
        only_name: bool = False,
    ) -> DotDict | str:
        """
        Returns a single parameter group whose name matches *group*. *group* can be a string, a
        pattern, or sequence of them.

        An exception is raised in case no or more than one parameter group is found. When
        *only_name* is *True*, only the name of the parameter group is returned rather than a
        structured dictionary.
        """
        # rename arguments to make their meaning explicit
        group_name = group

        groups = self.get_parameter_groups(group_name, only_names=only_name)

        # checks
        if not groups:
            raise ValueError(f"no parameter group named '{group_name}' found")
        if len(groups) > 1:
            raise ValueError(f"parameter group name '{group_name}' matches more than one category")

        return groups[0]

    def has_parameter_group(
        self: InferenceModel,
        group: str | Sequence[str],
    ) -> bool:
        """
        Returns *True* if a parameter group whose name matches *group* is existing, and *False*
        otherwise. *group* can be a string, a pattern, or sequence of them.
        """
        # rename arguments to make their meaning explicit
        group_pattern = group

        # simeple length check
        return len(self.get_parameter_groups(group_pattern)) > 0

    def add_parameter_group(self: InferenceModel, *args, **kwargs) -> None:
        """
        Adds a new parameter group with all *args* and *kwargs* used to create the structured
        parameter group dictionary via :py:meth:`parameter_group_spec`. If a group with the same
        name already exists, an exception is raised.
        """
        # create the instance
        group = self.parameter_group_spec(*args, **kwargs)

        # checks
        if self.has_parameter_group(group.name):
            raise ValueError(f"parameter group named '{group.name}' already registered")

        self.parameter_groups.append(group)

    def remove_parameter_group(
        self: InferenceModel,
        group: str | Sequence[str],
    ) -> bool:
        """
        Removes one or more parameter groups whose names match *group*. *group* can be a string, a
        pattern, or sequence of them. Returns *True* if at least one group was removed, and *False*
        otherwise.
        """
        # rename arguments to make their meaning explicit
        group_pattern = group

        match = pattern_matcher(group_pattern)
        n_before = len(self.parameter_groups)
        self.parameter_groups[:] = [
            group
            for group in self.parameter_groups
            if not match(group.name)
        ]
        removed_any = len(self.parameter_groups) != n_before

        return removed_any

    def add_parameter_to_group(
        self: InferenceModel,
        parameter: str | Sequence[str],
        group: str | Sequence[str],
    ) -> bool:
        """
        Adds a parameter named *parameter* to one or multiple parameter groups whose name match
        *group*. *group* can be a string, a pattern, or sequence of them. When *parameter* is a
        pattern or regular expression, all previously added, matching parameters are added.
        Otherwise, *parameter* is added as as. If a parameter was added to at least one group,
        *True* is returned and *False* otherwise.
        """
        # rename arguments to make their meaning explicit
        parameter_pattern = parameter
        group_pattern = group

        # stop when there are no matching groups
        groups = self.get_parameter_groups(group_pattern)
        if not groups:
            return False

        # when parameter(s) contain any pattern, get flat list of all existing parameters first
        _is_pattern = lambda s: is_pattern(s) or is_regex(s)
        parameter_pattern = law.util.make_list(parameter_pattern)
        if any(map(_is_pattern, parameter_pattern)):
            parameter_names = self.get_parameters(parameter_pattern, flat=True)
        else:
            parameter_names = parameter_pattern

        # add names
        added_any = False
        for group in groups:
            for parameter_name in parameter_names:
                if parameter_name not in group.parameter_names:
                    group.parameter_names.append(parameter_name)
                    added_any = True

        return added_any

    def remove_parameter_from_groups(
        self: InferenceModel,
        parameter: str | Sequence[str],
        group: str | Sequence[str] | None = None,
    ) -> bool:
        """
        Removes all parameters matching *parameter* from parameter groups whose names match *group*.
        Both *parameter* and *group* can be a string, a pattern, or sequence of them. Returns *True*
        if at least one parameter was removed, and *False* otherwise.
        """
        # rename arguments to make their meaning explicit
        parameter_pattern = parameter
        group_pattern = group

        # stop when there are no matching groups
        groups = self.get_parameter_groups(group_pattern)
        if not groups:
            return False

        match = pattern_matcher(parameter_pattern)
        removed_any = False
        for group in groups:
            n_before = len(group.parameter_names)
            group.parameter_names[:] = [
                parameter_name
                for parameter_name in group.parameter_names
                if not match(parameter_name)
            ]
            removed_any |= len(group.parameter_names) != n_before

        return removed_any

    #
    # reverse lookups
    #

    def get_categories_with_process(
        self: InferenceModel,
        process: str | Sequence[str],
    ) -> list[str]:
        """
        Returns a flat list of category names that contain processes matching *process*. *process*
        can be a string, a pattern, or sequence of them.
        """
        # rename arguments to make their meaning explicit
        process_pattern = process

        # plain name lookup
        return list(self.get_processes(process=process_pattern, only_names=True).keys())

    def get_processes_with_parameter(
        self: InferenceModel,
        parameter: str | Sequence[str],
        category: str | Sequence[str] | None = None,
        flat: bool = True,
    ) -> list[str] | dict[str, list[str]]:
        """
        Returns a dictionary of names of processes that contain a parameter whose names match
        *parameter*, mapped to categories names. Categories can optionally be filtered through
        *category*. Both *parameter* and *category* can be a string, a pattern, or sequence of them.

        When *flat* is *True*, a flat, unique list of process names is returned.
        """
        # rename arguments to make their meaning explicit
        parameter_pattern = parameter
        category_pattern = category

        processes = self.get_parameters(
            parameter=parameter_pattern,
            category=category_pattern,
            only_names=True,
        )

        # restructure
        processes = DotDict(
            (category_name, list(_processes.keys()))
            for category_name, _processes in processes.items()
        )

        # flatten
        if flat:
            processes = law.util.make_unique(sum(processes.values(), []))

        return processes

    def get_categories_with_parameter(
        self: InferenceModel,
        parameter: str | Sequence[str],
        process: str | Sequence[str] | None = None,
        flat: bool = True,
    ) -> list[str] | dict[str, list[str]]:
        """
        Returns a dictionary of category names mapping to process names that contain parameters
        whose name match *parameter*. Processes can optionally be filtered through *process*. Both
        *parameter* and *process* can be a string, a pattern, or sequence of them.

        When *flat* is *True*, a flat, unique list of category names is returned.
        """
        # rename arguments to make their meaning explicit
        parameter_pattern = parameter
        process_pattern = process

        categories = self.get_parameters(
            parameter=parameter_pattern,
            process=process_pattern,
            only_names=True,
        )

        # restructure
        categories = DotDict(
            (category_name, list(_processes.keys()))
            for category_name, _processes in categories.items()
        )

        # flatten
        if flat:
            categories = list(categories)

        return categories

    def get_groups_with_parameter(
        self: InferenceModel,
        parameter: str | Sequence[str],
    ) -> list[str]:
        """
        Returns a list of names of parameter groups that contain a parameter whose name matches
        *parameter*, which can be a string, a pattern, or sequence of them.
        """
        # rename arguments to make their meaning explicit
        parameter_pattern = parameter

        match = pattern_matcher(parameter_pattern)
        return [
            group.name
            for group in self.parameter_groups
            if any(map(match, group.parameter_names))
        ]

    #
    # removal of empty and dangling objects
    #

    def cleanup(self: InferenceModel) -> None:
        """
        Cleans the internal model structure by removing empty and dangling objects by calling
        :py:meth:`remove_empty_categories`, :py:meth:`remove_dangling_parameters_from_groups` and
        :py:meth:`remove_empty_parameter_groups` in that order.
        """
        self.remove_empty_categories()
        self.remove_dangling_parameters_from_groups()
        self.remove_empty_parameter_groups()

    def remove_empty_categories(self: InferenceModel) -> None:
        """
        Removes all categories that contain no processes.
        """
        self.categories[:] = [
            category
            for category in self.categories
            if category.processes
        ]

    def remove_dangling_parameters_from_groups(self: InferenceModel) -> None:
        """
        Removes names of parameters from parameter groups that are not assigned to any process in
        any category.
        """
        # get a list of all parameters
        parameter_names = self.get_parameters("*", flat=True)

        # go through groups and remove dangling parameters
        for group in self.parameter_groups:
            group.parameter_names[:] = [
                parameter_name
                for parameter_name in group.parameter_names
                if parameter_name in parameter_names
            ]

    def remove_empty_parameter_groups(self: InferenceModel) -> None:
        """
        Removes parameter groups that contain no parameter names.
        """
        self.parameter_groups[:] = [
            group
            for group in self.parameter_groups
            if group.parameter_names
        ]

    #
    # generators
    #

    def iter_processes(
        self: InferenceModel,
        process: str | Sequence[str] | None = None,
        category: str | Sequence[str] | None = None,
    ) -> Generator[tuple[DotDict, DotDict], None, None]:
        """
        Generator that iteratively yields all processes whose names match *process*, optionally
        in all categories whose names match *category*. The yielded value is a 2-tuple containing
        the cagegory name and the process object.
        """
        processes = self.get_processes(process=process, category=category)
        for category_name, processes in processes.items():
            for process in processes:
                yield (category_name, process)

    def iter_parameters(
        self: InferenceModel,
        parameter: str | Sequence[str] | None = None,
        process: str | Sequence[str] | None = None,
        category: str | Sequence[str] | None = None,
    ) -> Generator[tuple[DotDict, DotDict, DotDict], None, None]:
        """
        Generator that iteratively yields all parameters whose names match *parameter*, optionally
        in all processes and categories whose names match *process* and *category*. The yielded
        value is a 3-tuple containing the cagegory name, the process name and the parameter object.
        """
        parameters = self.get_parameters(parameter=parameter, process=process, category=category)
        for category_name, parameters in parameters.items():
            for process_name, parameters in parameters.items():
                for parameter in parameters:
                    yield (category_name, process_name, parameter)

    #
    # other helpers
    #

    def scale_process(
        self: InferenceModel,
        scale: int | float,
        process: str | Sequence[str] | None = None,
        category: str | Sequence[str] | None = None,
    ) -> bool:
        """
        Sets the scale attribute of all processes whose names match *process*, optionally in all
        categories whose names match *category*, to *scale*. Returns *True* if at least one process
        was found and scale, and *False* otherwise.
        """
        found = False
        for _, process in self.iter_processes(process=process, category=category):
            process.scale = float(scale)
            found = True
        return found


# shorthand
inference_model = InferenceModel.inference_model
