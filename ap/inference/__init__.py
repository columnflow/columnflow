# coding: utf-8

"""
Basic objects for defining statistical inference models.
"""

import enum
import copy as _copy
from typing import Optional, Any, Tuple, List, Union, Dict, Generator, Callable, TextIO, Sequence

import law
import order as od

from ap.util import maybe_import, DotDict, is_pattern, is_regex, pattern_matcher


yaml = maybe_import("yaml")


class ParameterType(enum.Enum):
    """
    Parameter type flag.
    """

    rate = "rate"
    shape = "shape"
    norm_shape = "norm_shape"
    shape_to_rate = "shape_to_rate"

    def __str__(self):
        return self.value

    @property
    def is_rate(self):
        return self in [self.rate, self.shape_to_rate]

    @property
    def is_shape(self):
        return self in [self.shape, self.norm_shape]


class InferenceModel(object):
    """
    Interface to statistical inference models with connections to config objects (such as
    py:class:`order.Config` or :py:class:`order.Dataset`).

    The internal structure to describe a model looks as follows (in yaml style) and is accessible
    through :py:attr:`model` as well as property access to its top-level objects.

    .. code-block:: yaml

        categories:
          - name: cat1
            source: 1e
            variable: ht
            data: [data_mu_a]
            data_from_processes: []
            mc_stats: True
            processes:
              - name: HH
                signal: True
                source: hh
                mc: [hh_ggf]
                parameters:
                  - name: lumi
                    type: rate
                    effect: 1.02
                    source: null
                  - name: pu
                    type: rate
                    effect: [0.97, 1.02]
                    source: null
                  - name: pileup
                    type: shape
                    effect: 1.0
                    source: minbias_xs
              - name: tt
                signal: False
                source: ttbar
                mc: [tt_sl, tt_dl, tt_fh]
                parameters:
                  - name: lumi
                    type: rate
                    effect: 1.02
                    source: null
            extra: Arbitrary content

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

    # instance cache
    _instances = {}

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
    def new(cls, name: str, *args, **kwargs) -> "InferenceModel":
        """
        Creates a new instance with *name* and all *args* and *kwargs*, adds it to the instance
        cache and returns it. A *ValueError* is raised in case an instance with the same name was
        registered before.
        """
        # check if the instance is already registered
        if name in cls._instances:
            raise ValueError(
                f"an inference model named '{name}' was already registered ({cls.get(name)})",
            )

        # create it
        cls._instances[name] = cls(name, *args, **kwargs)

        return cls._instances[name]

    @classmethod
    def get(cls, name: str, copy: bool = False) -> "InferenceModel":
        """
        Returns a previously registered instance named *name* and optionally copies it when *copy*
        is *True*. A *ValueError* is raised when no instance was found with that name.
        """
        if name not in cls._instances:
            raise ValueError(f"no inference model named '{name}' found")

        return cls._instances[name].copy() if copy else cls._instances[name]

    @classmethod
    def model_spec(cls) -> DotDict:
        """
        Returns a dictionary representing the top-level structure of the model.
        """
        return DotDict([
            ("categories", []),
            ("parameter_groups", []),
        ])

    @classmethod
    def category_spec(
        cls,
        name: str,
        variable: str,
        data: Optional[Sequence[str]] = None,
        data_from_processes: Optional[Sequence[str]] = None,
        mc_stats: bool = False,
        source: Optional[str] = None,
        extra: Optional[Any] = None,
    ) -> DotDict:
        """
        Returns a dictionary representing a category (interchangeably called bin or channel in other
        tools), forwarding all arguments.
        """
        return DotDict([
            ("name", str(name)),
            ("variable", str(variable)),
            ("source", str(source) if source is not None else None),
            ("data", list(map(str, data or []))),
            ("data_from_processes", list(map(str, data or []))),
            ("mc_stats", bool(mc_stats)),
            ("processes", []),
            ("extra", extra),
        ])

    @classmethod
    def process_spec(
        cls,
        name: str,
        signal: bool = False,
        source: Optional[str] = None,
        mc: Optional[Sequence[str]] = None,
    ) -> DotDict:
        """
        Returns a dictionary representing a process, forwarding all arguments.
        """
        return DotDict([
            ("name", str(name)),
            ("signal", bool(signal)),
            ("source", str(source) if source is not None else None),
            ("mc", list(map(str, mc or []))),
            ("parameters", []),
        ])

    @classmethod
    def parameter_spec(
        cls,
        name: str,
        type: Union[ParameterType, str],
        source: Optional[str] = None,
        effect: Union[float, Tuple[float, float]] = 1.0,
    ) -> DotDict:
        """
        Returns a dictionary representing a (nuisance) parameter, forwarding all arguments.
        """
        return DotDict([
            ("name", str(name)),
            ("type", type if isinstance(type, ParameterType) else ParameterType[type]),
            ("source", str(source) if source is not None else None),
            ("effect", float(effect) if isinstance(effect, (int, float)) else tuple(map(float, effect))),
        ])

    @classmethod
    def parameter_group_spec(
        cls,
        name: str,
    ) -> DotDict:
        """
        Returns a dictionary representing a group of parameter names.
        """
        return DotDict([
            ("name", str(name)),
            ("parameter_names", []),
        ])

    def __init__(
        self,
        name: str,
        *,
        config_inst: Optional[od.Config] = None,
        config_callbacks: Optional[Sequence[Callable]] = None,
    ):
        super().__init__()

        # store attributes
        self.name = name
        self.config_inst = None
        self.config_callbacks = list(config_callbacks or [])

        # model info
        self.model = self.model_spec()

        # set the config when given
        if config_inst:
            self.set_config(config_inst)

    def __str__(self) -> str:
        return f"{self.__class__.__name__}__{self.name}"

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__} '{self.name}' at {hex(id(self))}>"

    def copy(self) -> "InferenceModel":
        """
        Returns a deep copy if this instance.
        """
        # create a new instance
        inst = self.__class__(
            self.name,
            config_inst=self.config_inst,
            config_callbacks=self.config_callbacks,
        )

        # overwrite the model with a deep copy
        inst.model = _copy.deepcopy(self.model)

        return inst

    def set_config(self, config_inst: od.Config) -> None:
        """
        Sets the :py:attr:`config_inst` attribute to *config_inst* and, when set, invokes
        :py:attr:`config_callback` bound to this instance.
        """
        self.config_inst = config_inst

        # invoke the optional config callbacks
        for func in self.config_callbacks:
            if callable(func):
                func.__get__(self, self.__class__)()

    def on_config(self, func: Optional[Callable] = None, clear: bool = None) -> None:
        """
        Decorator to register a callable *func* in :py:attr:`config_callbacks` which are invoked
        once a config object is set via :py:meth:`set_config`. When *clear* is *True*, the current
        list of :py:attr:`config_callbacks` is cleared first.
        """
        def decorator(func):
            if clear:
                del self.config_callbacks[:]
            self.config_callbacks.append(func)

        return decorator(func) if func else decorator

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
        category: Optional[str] = None,
        only_names: bool = False,
    ) -> List[Union[DotDict, str]]:
        """
        Returns a list of categories whose name match *category*. When *only_names* is *True*, only
        names of categories are returned rather than structured dictionaries.
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
        category: str,
        only_name: bool = False,
        silent: bool = False,
    ) -> Union[DotDict, str]:
        """
        Returns a single category whose name matches *category*. An exception is raised if no or
        more than one category is found, unless *silent* is *True* in which case *None* is returned.
        When *only_name* is *True*, only the name of the category is returned rather than a
        structured dictionary.
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
        category: str,
    ) -> bool:
        """
        Returns *True* if a category whose name matches *category* is existing, and *False*
        otherwise.
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
        category: str,
    ) -> bool:
        """
        Removes one or more categories whose names match *category*. Returns *True* if at least one
        category was removed, and *False* otherwise.
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
        process: Optional[str] = None,
        category: Optional[str] = None,
        only_names: bool = False,
        flat: bool = False,
    ) -> Union[Dict[str, Union[DotDict, str]], List[str]]:
        """
        Returns a dictionary of processes whose names match *process*, mapped to the name of the
        category they belong to. Categories can optionally be filtered through a *category* pattern.

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
        process: str,
        category: Optional[str] = None,
        only_name: bool = False,
        silent: bool = False,
    ) -> Union[DotDict, str]:
        """
        Returns a single process whose name matches *process*, and optionally, whose category's name
        matches *category*. An exception is raised if no or more than one process is found, unless
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
        process: str,
        category: Optional[str] = None,
    ) -> bool:
        """
        Returns *True* if a process whose name matches *process*, and optionally whose category's
        name matches *category*, is existing, and *False* otherwise.
        """
        # rename arguments to make their meaning explicit
        process_pattern = process
        category_pattern = category

        # simple length check
        return len(self.get_processes(process_pattern, category=category_pattern)) > 0

    def add_process(
        self,
        *args,
        category: Optional[str] = None,
        **kwargs,
    ) -> None:
        """
        Adds a new process to all categories whose names match *category*, with all *args* and
        *kwargs* used to create the structured process dictionary via :py:meth:`process_spec`. If a
        process with the same name already exists in one of the categories, an exception is raised.
        """
        # rename arguments to make their meaning explicit
        category_pattern = category

        process = self.process_spec(*args, **kwargs)

        # get categories the process should be added to
        categories = self.get_categories(category_pattern)

        # check for duplicates
        for category in categories:
            if self.has_process(process.name, category=category.name):
                raise ValueError(
                    f"process named '{process.name}' already registered in category "
                    f"'{category.name}'",
                )

        # add independent copies to categories
        for category in categories:
            category.processes.append(_copy.deepcopy(process))

    def remove_process(
        self,
        process: str,
        category: Optional[str] = None,
    ) -> bool:
        """
        Removes one or more processes whose names match *process*, and optionally whose category's
        name match *category*. Returns *True* if at least one process was removed, and *False*
        otherwise.
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
        parameter: Optional[str] = None,
        process: Optional[str] = None,
        category: Optional[str] = None,
        only_names: bool = False,
        flat: bool = False,
    ) -> Union[Dict[str, Dict[str, Union[DotDict, str]]], List[str]]:
        """
        Returns a dictionary of parameter whose names match *parameter*, mapped twice to the name of
        the category and the name of the process they belong to. Categories and processes can
        optionally be filtered through a *category* pattern and a *process* pattern.

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
        parameter: str,
        process: Optional[str] = None,
        category: Optional[str] = None,
        only_name: bool = False,
        silent: bool = False,
    ) -> Union[DotDict, str]:
        """
        Returns a single parameter whose name matches *parameter*, and optionally, whose category's
        and process' name matches *category* and *process*. An exception is raised if no or more
        than one parameter is found, unless *silent* is *True* in which case *None* is returned.
        When *only_name* is *True*, only the name of the parameter is returned rather than a
        structured dictionary.
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
        parameter: str,
        process: Optional[str] = None,
        category: Optional[str] = None,
    ) -> bool:
        """
        Returns *True* if a parameter whose name matches *parameter*, and optionally whose
        category's and process' name match *category* and *process*, is existing, and *False*
        otherwise.
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
        process: Optional[str] = None,
        category: Optional[str] = None,
        **kwargs,
    ) -> DotDict:
        """
        Adds a new parameter to all categories and processes whose names match *category* and
        *process*, with all *args* and *kwargs* used to create the structured parameter dictionary
        via :py:meth:`parameter_spec`. If a parameter with the same name already exists in one of
        the processes throughout the categories, an exception is raised.
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
        parameter: str,
        process: Optional[str] = None,
        category: Optional[str] = None,
    ) -> bool:
        """
        Removes one or more parameters whose names match *parameter*, and optionally whose
        category's and process' name match *category* and *process*. Returns *True* if at least
        one parameter was removed, and *False* otherwise.
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
        group: Optional[str] = None,
        only_names: bool = False,
    ) -> List[Union[DotDict, str]]:
        """
        Returns a list of parameter group whose name match *group*.

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
        group: str,
        only_name: bool = False,
    ) -> Union[DotDict, str]:
        """
        Returns a single parameter group whose name matches *group*. An exception is raised in case
        no or more than one parameter group is found. When *only_name* is *True*, only the name of
        the parameter group is returned rather than a structured dictionary.
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

    def has_parameter_group(self, group: str) -> bool:
        """
        Returns *True* if a parameter group whose name matches *group* is existing, and *False*
        otherwise.
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

    def remove_parameter_group(self, group: str) -> bool:
        """
        Removes one or more parameter groups whose names match *group*. Returns *True* if at least
        one group was removed, and *False* otherwise.
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
        parameter: str,
        group: str,
    ) -> bool:
        """
        Adds a parameter named *parameter* to one or multiple parameter groups whose name match
        *group*. When *parameter* is a pattern or regular expression, all matcheing, previously
        added parameters are added. Otherwise, *parameter* is added as as. If a parameter was added
        to at least one group, *True* is returned and *False* otherwise.
        """
        # rename arguments to make their meaning explicit
        parameter_pattern = parameter
        group_pattern = group

        # stop when there are no matching groups
        groups = self.get_parameter_groups(group_pattern)
        if not groups:
            return False

        # when a pattern is given, get flat list of all existing parameters first
        if is_pattern(parameter_pattern) or is_regex(parameter_pattern):
            parameter_names = self.get_parameters(parameter_pattern, flat=True)
        else:
            parameter_names = [parameter_pattern]

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
        parameter: str,
        group: Optional[str] = None,
    ) -> bool:
        """
        Removes all parameters matching *parameter* from parameter groups whose names match *group*.
        Returns *True* if at least one parameter was removed, and *False* otherwise.
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
        process: str,
    ) -> List[str]:
        """
        Returns a flat list of category names that contain processes matching *process*.
        """
        # rename arguments to make their meaning explicit
        process_pattern = process

        # plain name lookup
        return list(self.get_processes(process=process_pattern, only_names=True))

    def get_processes_with_parameter(
        self,
        parameter: str,
        category: Optional[str] = None,
        flat: bool = True,
    ) -> Union[List[str], Dict[str, List[str]]]:
        """
        Returns a dictionary of names of processes that contain a parameter whose names match
        *parameter*, mapped to categories names. Categories can optionally be filtered through a
        *category* pattern. When *flat* is *True*, a flat, unique list of process names is returned.
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
        parameter: str,
        process: Optional[str] = None,
        flat: bool = True,
    ) -> Union[List[str], Dict[str, List[str]]]:
        """
        Returns a dictionary of category names mapping to process names that contain parameters
        whose name match *parameter*. Processes can optionally be filtered through a *process*
        pattern. When *flat* is *True*, a flat, unique list of category names is returned.
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
        parameter: str,
    ) -> List[str]:
        """
        Returns a list of names of parameter groups that contain a parameter whose name matches
        *parameter*.
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
        process: Optional[str] = None,
        category: Optional[str] = None,
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
        parameter: Optional[str] = None,
        process: Optional[str] = None,
        category: Optional[str] = None,
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

    #
    # in-place parameter operations
    #

    def flip_parameter_effect(
        self,
        parameter: str,
        process: Optional[str] = None,
        category: Optional[str] = None,
    ) -> bool:
        """
        Flips the effect of all parameters seleted by *parameter*, and optionally *process* and
        *category* patterns.

            - ``1.1`` -> ``0.9``
            - ``(0.9, 1.1)`` -> ``(1.1, 0.9)``
        """
        changed_any = False
        for _, _, parameter in self.iter_parameters(parameter=parameter, process=process, category=category):
            if isinstance(parameter.effect, tuple):
                # simply reverse the order
                parameter.effect = parameter.effect[::-1]
            else:  # float
                # mirror at one
                parameter.effect = 2.0 - parameter.effect
            changed_any = True

        return changed_any

    def asymmetrize_parameter_effect(
        self,
        parameter: str,
        process: Optional[str] = None,
        category: Optional[str] = None,
    ) -> bool:
        """
        Converts symmetric, single-float effects of all parameters seleted by *parameter*, and
        optionally *process* and *category* pattern into asymmetric ones.

            - ``1.1`` -> ``(0.9, 1.1)``
            - ``0.9`` -> ``(1.1, 0.9)``
            - ``(0.9, 1.1)`` -> ``(0.9, 1.1)``, unchanged
        """
        changed_any = False
        for _, _, parameter in self.iter_parameters(parameter=parameter, process=process, category=category):
            if isinstance(parameter.effect, tuple):
                continue

            # split into two parts (e.g. 1.2 -> (0.8, 1.2))
            f = parameter.effect - 1.0
            parameter.effect = (1.0 - f, f + 1.0)
            changed_any = True

        return changed_any

    def symmetrize_parameter_effect(
        self,
        parameter: str,
        process: Optional[str] = None,
        category: Optional[str] = None,
        epsilon: float = 0.001,
        precision: int = 4,
    ) -> bool:
        """
        Converts asymmetric effects of all parameters seleted by *parameter*, and optionally
        *process* and *category* pattern into a single, symmetric one when the difference is below
        *epsilon*. When the difference is non-zero but below epsilon, the average effect is used,
        rounded to *precision* digits.

            - ``(0.9, 1.1)`` -> ``1.1``
            - ``(1.1, 0.9)`` -> ``0.9``
            - ``(0.9, 1.2)`` -> ``(0.9, 1.2)``, unchanged, exceeding epsilon
        """
        changed_any = False
        for _, _, parameter in self.iter_parameters(parameter=parameter, process=process, category=category):
            if not isinstance(parameter.effect, tuple):
                continue

            # check the difference
            f_down = 1.0 - parameter.effect[0]
            f_up = parameter.effect[1] - 1.0
            if abs(f_up - f_down) > epsilon:
                continue

            # merge into one parameter
            f = 0.5 * (f_up + f_down)
            parameter.effect = round(1.0 + f, precision)
            changed_any = True

        return changed_any

    def align_parameter_effect(
        self,
        parameter: str,
        process: Optional[str] = None,
        category: Optional[str] = None,
    ) -> bool:
        """
        Aligns possibly ill-defined effects of all parameters seleted by *parameter*, and optionally
        *process* and *category* pattern such that the plus-1-sigma direction refers to a more
        positive effect that the minus-1-sigma direction.

            - ``1.1`` -> ``1.1``, unchanged
            - ``0.9`` -> ``1.1``
            - ``(0.9, 1.1)`` -> ``(0.9, 1.1)``, unchanged
            - ``(1.1, 0.9)`` -> ``(0.9, 1.1)``
        """
        changed_any = True
        for _, _, parameter in self.iter_parameters(parameter=parameter, process=process, category=category):
            if isinstance(parameter.effect, tuple):
                # simply reverse the order when the first element is larger
                if parameter.effect[0] > parameter.effect[1]:
                    parameter.effect = parameter.effect[::-1]
                    changed_any = True
            else:  # float
                # mirror at one when smaller than one
                if parameter.effect < 1:
                    parameter.effect = 2.0 - parameter.effect
                    changed_any = True

        return changed_any
