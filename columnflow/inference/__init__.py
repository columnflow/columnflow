# coding: utf-8

"""
Basic objects for defining statistical inference models.
"""

import enum
import copy as _copy
from typing import Optional, Tuple, List, Union, Dict, Generator, Callable, TextIO, Sequence, Any

import law
import order as od

from columnflow.util import (
    DerivableMeta, Derivable, maybe_import, DotDict, is_pattern, is_regex, pattern_matcher,
)


yaml = maybe_import("yaml")


class ParameterType(enum.Enum):
    """
    Parameter type flag.
    """

    rate_gauss = "rate_gauss"
    rate_uniform = "rate_uniform"
    rate_unconstrained = "rate_unconstrained"
    shape = "shape"

    def __str__(self):
        return self.value

    @property
    def is_rate(self) -> bool:
        return self in (
            self.rate_gauss,
            self.rate_uniform,
            self.rate_unconstrained,
        )

    @property
    def is_shape(self) -> bool:
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

    def __str__(self):
        return self.value

    @property
    def from_shape(self) -> bool:
        return self in (
            self.effect_from_shape,
        )

    @property
    def from_rate(self) -> bool:
        return self in (
            self.effect_from_rate,
        )


class ParameterTransformations(tuple):
    """
    Container around a sequence of :py:class:`ParameterTransformation`'s with a few convenience
    methods.
    """

    def __new__(cls, transformations):
        # TODO: at this point one could interfere and complain in case incompatible transfos are used
        transformations = [
            (t if isinstance(t, ParameterTransformation) else ParameterTransformation[t])
            for t in transformations
        ]

        # initialize
        return super().__new__(cls, transformations)

    @property
    def any_from_shape(self) -> bool:
        return any(t.from_shape for t in self)

    @property
    def any_from_rate(self) -> bool:
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
            category: 1e
            variable: ht
            data_datasets: [data_mu_a]
            data_from_processes: []
            mc_stats: 10
            processes:
              - name: HH
                process: hh
                signal: True
                mc_datasets: [hh_ggf]
                parameters:
                  - name: lumi
                    type: rate_gauss
                    effect: 1.02
                    shift_source: null
                  - name: pu
                    type: rate_gauss
                    effect: [0.97, 1.02]
                    shift_source: null
                  - name: pileup
                    type: shape
                    effect: 1.0
                    shift_source: minbias_xs
              - name: tt
                signal: False
                process: ttbar
                mc_datasets: [tt_sl, tt_dl, tt_fh]
                parameters:
                  - name: lumi
                    type: rate_gauss
                    effect: 1.02
                    shift_source: null

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

        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)

            # ammend representers
            map_repr = lambda dumper, data: dumper.represent_mapping("tag:yaml.org,2002:map", data.items())
            self.add_representer(DotDict, map_repr)

            list_repr = lambda dumper, data: dumper.represent_list(list(data))
            self.add_representer(tuple, list_repr)

            str_repr = lambda dumper, data: dumper.represent_str(str(data))
            self.add_representer(ParameterType, str_repr)

        def ignore_aliases(self, *args, **kwargs) -> bool:
            return True

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
        category: Optional[str] = None,
        variable: Optional[str] = None,
        data_datasets: Optional[Sequence[str]] = None,
        data_from_processes: Optional[Sequence[str]] = None,
        mc_stats: Optional[Union[float, tuple]] = None,
    ) -> DotDict:
        """
        Returns a dictionary representing a category (interchangeably called bin or channel in other
        tools), forwarding all arguments.

            - *name*: The name of the category in the model.
            - *category*: The name of the source category in the config to use. Note the possibly
              ambiguous yet consistent naming.
            - *variable*: The name of the variable in the config to use.
            - *data_datasets*: List of names of datasets in the config to use for real data.
            - *data_from_processes*: Optional list of names of :py:meth:`process_spec` objects that,
              when *data_datasets* is not defined, make of a fake data contribution.
            - *mc_stats*: Either *None* to disable MC stat uncertainties, or a float or tuple of
              floats to control the options of MC stat options.
        """
        return DotDict([
            ("name", str(name)),
            ("category", str(category) if category else None),
            ("variable", str(variable) if variable else None),
            ("data_datasets", list(map(str, data_datasets or []))),
            ("data_from_processes", list(map(str, data_from_processes or []))),
            ("mc_stats", mc_stats),
            ("processes", []),
        ])

    @classmethod
    def process_spec(
        cls,
        name: str,
        process: Optional[str] = None,
        signal: bool = False,
        mc_datasets: Optional[Sequence[str]] = None,
    ) -> DotDict:
        """
        Returns a dictionary representing a process, forwarding all arguments.

            - *name*: The name of the process in the model.
            - *process*: The name of the source process in the config to use. Note the possibly
              ambiguous yet consistent naming.
            - *signal*: A boolean flag deciding whether this process describes signal.
            - *mc_datasets*: List of names of MC datasets in the config to use.
        """
        return DotDict([
            ("name", str(name)),
            ("process", str(process) if process else None),
            ("signal", bool(signal)),
            ("mc_datasets", list(map(str, mc_datasets or []))),
            ("parameters", []),
        ])

    @classmethod
    def parameter_spec(
        cls,
        name: str,
        type: Union[ParameterType, str],
        transformations: Sequence[Union[ParameterTransformation, str]] = (ParameterTransformation.none,),
        shift_source: Optional[str] = None,
        effect: Union[Any] = 1.0,
    ) -> DotDict:
        """
        Returns a dictionary representing a (nuisance) parameter, forwarding all arguments.

            - *name*: The name of the parameter in the model.
            - *type*: A :py:class:`ParameterType` instance describing the type of this parameter.
            - *transformations*: A sequence of :py:class:`ParameterTransformation` instances
              describing transformations to be applied to the effect of this parameter.
            - *shift_source*: The name of a systematic shift source in the config that this
              parameter corresponds to.
            - *effect*: An arbitrary object describing the effect of the parameter (e.g. float for
              symmetric rate effects, 2-tuple for down/up variation, etc).
        """
        return DotDict([
            ("name", str(name)),
            ("type", type if isinstance(type, ParameterType) else ParameterType[type]),
            ("transformations", ParameterTransformations(transformations)),
            ("shift_source", str(shift_source) if shift_source else None),
            ("effect", effect),
        ])

    @classmethod
    def parameter_group_spec(
        cls,
        name: str,
        parameter_names: Optional[Sequence[str]] = None,
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
    def require_shapes_for_parameter(self, param_obj: dict) -> bool:
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
            f"shape requirement cannot be evaluated of parameter '{param_obj.name}' with type "
            f"'{param_obj.type}' and transformations {param_obj.transformations}",
        )

    def __init__(self, config_inst: od.Config):
        super().__init__()

        # store attributes
        self.config_inst = config_inst

        # model info
        self.model = self.model_spec()

        # custom init function when set
        if callable(self.init_func):
            self.init_func()

    def to_yaml(self, stream: Optional[TextIO] = None) -> Union[None, str]:
        """
        Writes the content of the :py:attr:`model` into a file-like object *stream* when given, and
        returns a string representation otherwise.
        """
        return yaml.dump(self.model, stream=stream, Dumper=self.YamlDumper)

    def pprint(self) -> None:
        """
        Pretty-prints the content of the :py:attr:`model` in yaml-style.
        """
        print(self.to_yaml())

    #
    # property access to top-level objects
    #

    @property
    def categories(self) -> DotDict:
        return self.model.categories

    @property
    def parameter_groups(self) -> DotDict:
        return self.model.parameter_groups

    #
    # handling of categories
    #

    def get_categories(
        self,
        category: Optional[Union[str, Sequence[str]]] = None,
        only_names: bool = False,
    ) -> List[Union[DotDict, str]]:
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
        self,
        category: Union[str, Sequence[str]],
        only_name: bool = False,
        silent: bool = False,
    ) -> Union[DotDict, str]:
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
        self,
        category: Union[str, Sequence[str]],
    ) -> bool:
        """
        Returns *True* if a category whose name matches *category* is existing, and *False*
        otherwise. *category* can be a string, a pattern, or sequence of them.
        """
        # rename arguments to make their meaning explicit
        category_pattern = category

        # simple length check
        return len(self.get_categories(category_pattern)) > 0

    def add_category(self, *args, **kwargs) -> None:
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
        self,
        category: Union[str, Sequence[str]],
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
        self,
        process: Optional[Union[str, Sequence[str]]] = None,
        category: Optional[Union[str, Sequence[str]]] = None,
        only_names: bool = False,
        flat: bool = False,
    ) -> Union[Dict[str, Union[DotDict, str]], List[str]]:
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
        self,
        process: Union[str, Sequence[str]],
        category: Optional[Union[str, Sequence[str]]] = None,
        only_name: bool = False,
        silent: bool = False,
    ) -> Union[DotDict, str]:
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
                f"process '{process_name}' found more than once in category '{category}': "
                ",".join(names),
            )

        return processes[0]

    def has_process(
        self,
        process: Union[str, Sequence[str]],
        category: Optional[Union[str, Sequence[str]]] = None,
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
        self,
        *args,
        category: Optional[Union[str, Sequence[str]]] = None,
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
                    f"process named '{process.name}' already registered in category "
                    f"'{category.name}'",
                )
            target_categories.append(category)

        # add independent copies to categories
        for category in target_categories:
            category.processes.append(_copy.deepcopy(process))

    def remove_process(
        self,
        process: Union[str, Sequence[str]],
        category: Optional[Union[str, Sequence[str]]] = None,
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
        self,
        parameter: Optional[Union[str, Sequence[str]]] = None,
        process: Optional[Union[str, Sequence[str]]] = None,
        category: Optional[Union[str, Sequence[str]]] = None,
        only_names: bool = False,
        flat: bool = False,
    ) -> Union[Dict[str, Dict[str, Union[DotDict, str]]], List[str]]:
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
        self,
        parameter: Union[str, Sequence[str]],
        process: Optional[Union[str, Sequence[str]]] = None,
        category: Optional[Union[str, Sequence[str]]] = None,
        only_name: bool = False,
        silent: bool = False,
    ) -> Union[DotDict, str]:
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
                f"parameter '{parameter_name}' found in more than one category: "
                ",".join(parameters),
            )

        category, parameters = list(parameters.items())[0]
        if len(parameters) > 1:
            if silent:
                return None
            raise ValueError(
                f"parameter '{parameter_name}' found in more than one process in category "
                f"'{category}': " + ",".join(parameters),
            )

        process, parameters = list(parameters.items())[0]
        if len(parameters) > 1:
            if silent:
                return None
            names = parameters if only_name else [p.name for p in parameters]
            raise ValueError(
                f"parameter '{parameter_name}' found more than once in category '{category}' and "
                f"process '{process}': " + ",".join(names),
            )

        return parameters[0]

    def has_parameter(
        self,
        parameter: Union[str, Sequence[str]],
        process: Optional[Union[str, Sequence[str]]] = None,
        category: Optional[Union[str, Sequence[str]]] = None,
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
        self,
        *args,
        process: Optional[Union[str, Sequence[str]]] = None,
        category: Optional[Union[str, Sequence[str]]] = None,
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
                        f"parameter named '{parameter.name}' already registered for process "
                        f"'{process.name}' in category '{category_name}'",
                    )

        # add independent copies to processes
        for category_name, _processes in processes.items():
            for process in _processes:
                process.parameters.append(_copy.deepcopy(parameter))

        return parameter

    def remove_parameter(
        self,
        parameter: Union[str, Sequence[str]],
        process: Optional[Union[str, Sequence[str]]] = None,
        category: Optional[Union[str, Sequence[str]]] = None,
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
        self,
        group: Optional[Union[str, Sequence[str]]] = None,
        only_names: bool = False,
    ) -> List[Union[DotDict, str]]:
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
        self,
        group: Union[str, Sequence[str]],
        only_name: bool = False,
    ) -> Union[DotDict, str]:
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
        self,
        group: Union[str, Sequence[str]],
    ) -> bool:
        """
        Returns *True* if a parameter group whose name matches *group* is existing, and *False*
        otherwise. *group* can be a string, a pattern, or sequence of them.
        """
        # rename arguments to make their meaning explicit
        group_pattern = group

        # simeple length check
        return len(self.get_parameter_groups(group_pattern)) > 0

    def add_parameter_group(self, *args, **kwargs) -> None:
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
        self,
        group: Union[str, Sequence[str]],
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
        self,
        parameter: Union[str, Sequence[str]],
        group: Union[str, Sequence[str]],
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
        self,
        parameter: Union[str, Sequence[str]],
        group: Optional[Union[str, Sequence[str]]] = None,
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
        self,
        process: Union[str, Sequence[str]],
    ) -> List[str]:
        """
        Returns a flat list of category names that contain processes matching *process*. *process*
        can be a string, a pattern, or sequence of them.
        """
        # rename arguments to make their meaning explicit
        process_pattern = process

        # plain name lookup
        return list(self.get_processes(process=process_pattern, only_names=True).keys())

    def get_processes_with_parameter(
        self,
        parameter: Union[str, Sequence[str]],
        category: Optional[Union[str, Sequence[str]]] = None,
        flat: bool = True,
    ) -> Union[List[str], Dict[str, List[str]]]:
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
        self,
        parameter: Union[str, Sequence[str]],
        process: Optional[Union[str, Sequence[str]]] = None,
        flat: bool = True,
    ) -> Union[List[str], Dict[str, List[str]]]:
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
        self,
        parameter: Union[str, Sequence[str]],
    ) -> List[str]:
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

    def cleanup(self) -> None:
        """
        Cleans the internal model structure by removing empty and dangling objects by calling
        :py:meth:`remove_empty_categories`, :py:meth:`remove_dangling_parameters_from_groups` and
        :py:meth:`remove_empty_parameter_groups` in that order.
        """
        self.remove_empty_categories()
        self.remove_dangling_parameters_from_groups()
        self.remove_empty_parameter_groups()

    def remove_empty_categories(self) -> None:
        """
        Removes all categories that contain no processes.
        """
        self.categories[:] = [
            category
            for category in self.categories
            if category.processes
        ]

    def remove_dangling_parameters_from_groups(self) -> None:
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

    def remove_empty_parameter_groups(self) -> None:
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
        self,
        process: Optional[Union[str, Sequence[str]]] = None,
        category: Optional[Union[str, Sequence[str]]] = None,
    ) -> Generator[Tuple[DotDict, DotDict], None, None]:
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
        self,
        parameter: Optional[Union[str, Sequence[str]]] = None,
        process: Optional[Union[str, Sequence[str]]] = None,
        category: Optional[Union[str, Sequence[str]]] = None,
    ) -> Generator[Tuple[DotDict, DotDict, DotDict], None, None]:
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


def inference_model(
    func: Optional[Callable] = None,
    bases=(),
    **kwargs,
) -> Union[DerivableMeta, Callable]:
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
        subclass = InferenceModel.derive(func.__name__, bases=bases, cls_dict=cls_dict)

        return subclass

    return decorator(func) if func else decorator
