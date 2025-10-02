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

from columnflow.types import Generator, Callable, TextIO, Sequence, Any, Hashable, Type, T
from columnflow.util import (
    CachedDerivableMeta, Derivable, DotDict, is_pattern, is_regex, pattern_matcher, get_docs_url,
    freeze,
)

logger = law.logger.get_logger(__name__)


default_dataset = law.config.get_expanded("analysis", "default_dataset")


class ParameterType(enum.Enum):
    """
    Parameter type flag.

    :cvar rate_gauss: Gaussian rate parameter.
    :cvar rate_uniform: Uniform rate parameter.
    :cvar rate_unconstrained: Unconstrained rate parameter.
    :cvar shape: Shape parameter.
    """

    rate_gauss = "rate_gauss"
    rate_uniform = "rate_uniform"
    rate_unconstrained = "rate_unconstrained"
    shape = "shape"

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__}.{self.value}>"

    def __str__(self) -> str:
        return self.value

    @property
    def is_rate(self) -> bool:
        """
        Checks if the parameter type is a rate type.

        :returns: *True* if the parameter type is a rate type, *False* otherwise.
        """
        return self in {
            self.rate_gauss,
            self.rate_uniform,
            self.rate_unconstrained,
        }

    @property
    def is_shape(self) -> bool:
        """
        Checks if the parameter type is a shape type.

        :returns: *True* if the parameter type is a shape type, *False* otherwise.
        """
        return self in {
            self.shape,
        }


class ParameterTransformation(enum.Enum):
    """
    Flags denoting transformations to be applied on parameters.

    Implementation details depend on the routines that apply these transformations, usually as part for a serialization
    processes (such as so-called "datacards" in the CMS context). As such, the exact implementation may also differ
    depending on the type of the parameter that a transformation is applied to (e.g. shape vs rate).

    The general purpose of each transformation is described below.

    :cvar none: No transformation.
    :cvar effect_from_rate: Creates shape variations for a shape-type parameter using the single- or two-valued effect
        usually attributed to rate-type parameters. Only applies to shape-type parameters.
    :cvar effect_from_shape: Derive the effect of a rate-type parameter using the overall, integral effect of shape
        variations. Only applies to rate-type parameters.
    :cvar effect_from_shape_if_flat: Same as :py:attr:`effect_from_shape`, but applies only if both shape variations are
        reasonably flat. The definition of "reasonably flat" can be subject to the serialization routine. Only applies
        to rate-type parameters.
    :cvar symmetrize: The overall (integral) effect of up and down variations is measured and centralized, updating the
        variations such that they are equidistant to the nominal one. Can apply to both rate- and shape-type parameters.
    :cvar asymmetrize: The symmetric effect on a rate-type parameter (usually given as a single value) is converted into
        an asymmetric representation (using two values). Only applies to rate-type parameters.
    :cvar asymmetrize_if_large: Same as :py:attr:`asymmetrize`, but depending on a threshold on the size of the
        symmetric effect which can be subject to the serialization routine. Only applies to rate-type parameters.
    :cvar normalize: Variations of shape-type parameters are changed such that their integral effect identical to the
        nominal one. Should only apply to shape-type parameters.
    :cvar envelope: Builds an evelope of the up and down variations of a shape-type parameter, potentially on a
        bin-by-bin basis. Only applies to shape-type parameters.
    :cvar envelope_if_one_sided: Same as :py:attr:`envelope`, but only if the shape variations are one-sided following
        a definition that can be subject to the serialization routine. Only applies to shape-type parameters.
    :cvar envelope_enforce_two_sided: Same as :py:attr:`envelope`, but it enforces that the up (down) variation of the
        constructed envelope is always above (below) the nominal one. Only applies to shape-type parameters.
    :cvar flip_smaller_if_one_sided: For asymmetric rate effects (usually given by two values) that are found to be
        one-sided (e.g. after applying :py:attr:`effect_from_shape`), flips the smaller effect to the other side of the
        nominal value. Only applies to rate-type parameters.
    :cvar flip_larger_if_one_sided: Same as :py:attr:`flip_smaller_if_one_sided`, but flips the larger effect. Only
        applies to rate-type parameters.
    """

    none = "none"
    effect_from_rate = "effect_from_rate"
    effect_from_shape = "effect_from_shape"
    effect_from_shape_if_flat = "effect_from_shape_if_flat"
    symmetrize = "symmetrize"
    asymmetrize = "asymmetrize"
    asymmetrize_if_large = "asymmetrize_if_large"
    normalize = "normalize"
    envelope = "envelope"
    envelope_if_one_sided = "envelope_if_one_sided"
    envelope_enforce_two_sided = "envelope_enforce_two_sided"
    flip_smaller_if_one_sided = "flip_smaller_if_one_sided"
    flip_larger_if_one_sided = "flip_larger_if_one_sided"

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__}.{self.value}>"

    def __str__(self) -> str:
        return self.value

    @property
    def from_shape(self) -> bool:
        """
        Checks if the transformation is derived from shape.

        :returns: *True* if the transformation is derived from shape, *False* otherwise.
        """
        return self in {
            self.effect_from_shape,
            self.effect_from_shape_if_flat,
        }

    @property
    def from_rate(self) -> bool:
        """
        Checks if the transformation is derived from rate.

        :returns: *True* if the transformation is derived from rate, *False* otherwise.
        """
        return self in {
            self.effect_from_rate,
        }


class ParameterTransformations(tuple):
    """
    Container around a sequence of :py:class:`ParameterTransformation`'s with a few convenience methods.

    :param transformations: A sequence of :py:class:`ParameterTransformation` or their string names.
    """

    def __new__(
        cls,
        transformations: Sequence[ParameterTransformation | str],
    ) -> ParameterTransformations:
        """
        Creates a new instance of :py:class:`ParameterTransformations`.

        :param transformations: A sequence of :py:class:`ParameterTransformation` or their string names.
        :returns: A new instance of :py:class:`ParameterTransformations`.
        """
        # TODO: at this point one could object / complain in case incompatible trafos are used
        transformations = [
            (t if isinstance(t, ParameterTransformation) else ParameterTransformation[t])
            for t in transformations
        ]

        # initialize
        return super().__new__(cls, transformations)

    @property
    def any_from_shape(self) -> bool:
        """
        Checks if any transformation is derived from shape.

        :returns: *True* if any transformation is derived from shape, *False* otherwise.
        """
        return any(t.from_shape for t in self)

    @property
    def any_from_rate(self) -> bool:
        """
        Checks if any transformation is derived from rate.

        :returns: *True* if any transformation is derived from rate, *False* otherwise.
        """
        return any(t.from_rate for t in self)


class FlowStrategy(enum.Enum):
    """
    Strategy to handle over- and underflow bin contents.

    :cvar ignore: Ignore over- and underflow bins.
    :cvar warn: Issue a warning when over- and underflow bins are encountered.
    :cvar remove: Remove over- and underflow bins.
    :cvar move: Move over- and underflow bins to the first and last bin, respectively.
    """

    ignore = "ignore"
    warn = "warn"
    remove = "remove"
    move = "move"

    def __str__(self) -> str:
        return self.value


class InferenceModelMeta(CachedDerivableMeta):

    def _get_inst_cache_key(cls, args: tuple, kwargs: dict) -> Hashable:
        config_insts = args[0]
        config_names = tuple(sorted(config_inst.name for config_inst in config_insts))
        return freeze((cls, config_names, kwargs.get("inst_dict", {})))


class InferenceModel(Derivable, metaclass=InferenceModelMeta):
    """
    Interface to statistical inference models with connections to config objects (such as py:class:`order.Config` or
    :py:class:`order.Dataset`).

    The internal structure to describe a model looks as follows (in yaml style) and is accessible through
    :py:attr:`model` as well as property access to its top-level objects.

    .. code-block:: yaml

        categories:
          - name: cat1
            postfix: null
            config_data:
              22pre_v14:
                category: 1e
                variable: ht
                data_datasets: [data_mu_a]
            data_from_processes: []
            mc_stats: 10
            flow_strategy: warn
            rate_precision: 5
            empty_bin_value: 1e-05
            processes:
              - name: HH
                is_signal: True
                config_data:
                  22pre_v14:
                    process: hh
                    mc_datasets: [hh_ggf]
                scale: 1.0
                is_dynamic: False
                parameters:
                  - name: lumi
                    type: rate_gauss
                    effect: 1.02
                    effect_precision: 4
                    config_data: {}
                    transformations: []
                  - name: pu
                    type: rate_gauss
                    effect: [0.97, 1.02]
                    effect_precision: 4
                    config_data: {}
                    transformations: []
                  - name: pileup
                    type: shape
                    effect: 1.0
                    effect_precision: 4
                    config_data:
                      22pre_v14:
                        shift_source: minbias_xs
                    transformations: []
              - name: tt
                is_signal: False
                config_data:
                  22pre_v14:
                    process: tt
                    mc_datasets: [tt_sl, tt_dl, tt_fh]
                scale: 1.0
                is_dynamic: False
                parameters:
                  - name: lumi
                    type: rate_gauss
                    effect: 1.02
                    effect_precision: 4
                    config_data: {}
                    transformations: []

          - name: cat2
            ...

        parameter_groups:
          - name: rates
            parameters_names: [lumi, pu]
          - ...

    .. py:attribute:: name

        type: str

        The unique name of this model.

    .. py:attribute:: config_insts

        type: list[order.Config]

        Reference to :py:class:`order.Config` objects.

    .. py:attribute:: model

        type: DotDict

        The internal data structure representing the model, see :py:meth:`InferenceModel.model_spec`.
    """

    # optional initialization method
    init_func = None

    class YamlDumper(yaml.SafeDumper):
        """
        YAML dumper for statistical inference models with ammended representers to serialize
        internal, structured objects as safe, standard objects.
        """

        @classmethod
        def _map_repr(cls, dumper: yaml.Dumper, data: dict) -> str:
            return dumper.represent_dict(data if isinstance(data, dict) else dict(data))

        @classmethod
        def _list_repr(cls, dumper: yaml.Dumper, data: list) -> str:
            return dumper.represent_list(data if isinstance(data, list) else list(data))

        @classmethod
        def _str_repr(cls, dumper: yaml.Dumper, data: str) -> str:
            return dumper.represent_str(data if isinstance(data, str) else str(data))

        def __init__(self, *args, **kwargs) -> None:
            super().__init__(*args, **kwargs)

            # ammend representers
            self.add_representer(DotDict, self._map_repr)
            self.add_representer(tuple, self._list_repr)
            self.add_representer(ParameterType, self._str_repr)
            self.add_representer(ParameterTransformation, self._str_repr)
            self.add_representer(ParameterTransformations, self._list_repr)
            self.add_representer(FlowStrategy, self._str_repr)

        def ignore_aliases(self, *args, **kwargs) -> bool:
            return True

    @classmethod
    def inference_model(
        cls: T,
        func: Callable | None = None,
        bases: tuple[type] = (),
        **kwargs,
    ) -> Type[T] | Callable:
        """
        Decorator for creating a new :py:class:`InferenceModel` subclass with additional, optional
        *bases* and attaching the decorated function to it as ``init_func``. All additional *kwargs*
        are added as class members of the new subclass.

        :param func: The function to be decorated and attached as ``init_func``.
        :param bases: Optional tuple of base classes for the new subclass.
        :returns: The new subclass or a decorator function.
        """
        def decorator(func: Callable) -> Type[T]:
            # create the class dict
            cls_dict = {
                **kwargs,
                "init_func": func,
            }

            # create the subclass
            subclass = cls.derive(func.__name__, bases=bases, cls_dict=cls_dict)

            return subclass

        return decorator(func) if func else decorator

    @classmethod
    def used_datasets(cls, config_inst: od.Config) -> list[str]:
        """
        Used datasets for which the `upstream_task_cls.resolve_instances` will be called.
        Defaults to the default dataset.
        """
        return [default_dataset]

    @classmethod
    def model_spec(cls) -> DotDict:
        """
        Returns a dictionary representing the top-level structure of the model.

            - *categories*: List of :py:meth:`category_spec` objects.
            - *parameter_groups*: List of :py:meth:`parameter_group_spec` objects.
        """
        return DotDict([
            ("categories", []),
            ("parameter_groups", []),
        ])

    @classmethod
    def category_spec(
        cls,
        name: str,
        config_data: dict[str, DotDict] | None = None,
        data_from_processes: Sequence[str] | None = None,
        flow_strategy: FlowStrategy | str = FlowStrategy.warn,
        mc_stats: int | float | tuple | None = None,
        postfix: str | None = None,
        empty_bin_value: float = 1e-5,
        rate_precision: int = 5,
    ) -> DotDict:
        """
        Returns a dictionary representing a category (interchangeably called bin or channel in other
        tools), forwarding all arguments.

        :param name: The name of the category in the model.
        :param config_data: Dictionary mapping names of :py:class:`order.Config` objects to dictionaries following the
            :py:meth:`category_config_spec` that wrap settings like category, variable and real datasets in that config.
        :param data_from_processes: Optional list of names of :py:meth:`process_spec` objects that, when
            *config_data_datasets* is not defined, make up a fake data contribution.
        :param flow_strategy: A :py:class:`FlowStrategy` instance describing the strategy to handle over- and underflow
            bin contents.
        :param mc_stats: Either *None* to disable MC stat uncertainties, or an integer, a float or a tuple thereof to
            control the options of MC stat options.
        :param postfix: A postfix that is appended to (e.g.) file names created for this model.
        :param empty_bin_value: When bins have no content, they are filled with this value.
        :param rate_precision: The precision of reported rates.
        :returns: A dictionary representing the category.
        """
        return DotDict([
            ("name", str(name)),
            ("config_data", (
                {k: cls.category_config_spec(**v) for k, v in config_data.items()}
                if config_data
                else {}
            )),
            ("data_from_processes", list(map(str, data_from_processes or []))),
            ("flow_strategy", (
                flow_strategy
                if isinstance(flow_strategy, FlowStrategy)
                else FlowStrategy[flow_strategy]
            )),
            ("mc_stats", mc_stats),
            ("postfix", str(postfix) if postfix else None),
            ("empty_bin_value", empty_bin_value),
            ("rate_precision", rate_precision),
            ("processes", []),
        ])

    @classmethod
    def process_spec(
        cls,
        name: str,
        is_signal: bool = False,
        config_data: dict[str, DotDict] | None = None,
        scale: float | int = 1.0,
        is_dynamic: bool = False,
    ) -> DotDict:
        """
        Returns a dictionary representing a process, forwarding all arguments.

        :param name: The name of the process in the model.
        :param is_signal: A boolean flag deciding whether this process describes signal.
        :param config_data: Dictionary mapping names of :py:class:`order.Config` objects to dictionaries following the
            :py:meth:`process_config_spec` that wrap settings like process and mc datasets in that config.
        :param scale: A float value to scale the process, defaulting to 1.0.
        :param is_dynamic: A boolean flag deciding whether this process is dynamic, i.e., whether it is created
            on-the-fly.
        :returns: A dictionary representing the process.
        """
        return DotDict([
            ("name", str(name)),
            ("is_signal", bool(is_signal)),
            ("config_data", (
                {k: cls.process_config_spec(**v) for k, v in config_data.items()}
                if config_data
                else {}
            )),
            ("scale", float(scale)),
            ("is_dynamic", bool(is_dynamic)),
            ("parameters", []),
        ])

    @classmethod
    def parameter_spec(
        cls,
        name: str,
        type: ParameterType | str,
        transformations: Sequence[ParameterTransformation | str] = (),
        config_data: dict[str, DotDict] | None = None,
        effect: Any | None = 1.0,
        effect_precision: int = 4,
    ) -> DotDict:
        """
        Returns a dictionary representing a (nuisance) parameter, forwarding all arguments.

        :param name: The name of the parameter in the model.
        :param type: A :py:class:`ParameterType` instance describing the type of this parameter.
        :param transformations: A sequence of :py:class:`ParameterTransformation` instances describing transformations
            to be applied to the effect of this parameter.
        :param config_data: Dictionary mapping names of :py:class:`order.Config` objects to dictionaries following the
            :py:meth:`parameter_config_spec` that wrap settings like corresponding shift source in that config.
        :param effect: An arbitrary object describing the effect of the parameter (e.g. float for symmetric rate
            effects, 2-tuple for down/up variation, etc).
        :param effect_precision: The precision of reported effects.
        :returns: A dictionary representing the parameter.
        """
        return DotDict([
            ("name", str(name)),
            ("type", type if isinstance(type, ParameterType) else ParameterType[type]),
            ("transformations", ParameterTransformations(transformations)),
            ("config_data", (
                {k: cls.parameter_config_spec(**v) for k, v in config_data.items()}
                if config_data
                else {}
            )),
            ("effect", effect),
            ("effect_precision", effect_precision),
        ])

    @classmethod
    def parameter_group_spec(
        cls,
        name: str,
        parameter_names: Sequence[str] | None = None,
    ) -> DotDict:
        """
        Returns a dictionary representing a group of parameter names.

        :param name: The name of the parameter group in the model.
        :param parameter_names: Names of parameter objects this group contains.
        :returns: A dictionary representing the group of parameter names.
        """
        return DotDict([
            ("name", str(name)),
            ("parameter_names", list(map(str, parameter_names or []))),
        ])

    @classmethod
    def category_config_spec(
        cls,
        category: str | None = None,
        variable: str | None = None,
        data_datasets: Sequence[str] | None = None,
    ) -> DotDict:
        """
        Returns a dictionary representing configuration specific data, forwarding all arguments.

        :param category: The name of the source category in the config to use.
        :param variable: The name of the variable in the config to use.
        :param data_datasets: List of names or patterns of datasets in the config to use for real data.
        :returns: A dictionary representing category specific settings.
        """
        return DotDict([
            ("category", str(category) if category else None),
            ("variable", str(variable) if variable else None),
            ("data_datasets", list(map(str, data_datasets or []))),
        ])

    @classmethod
    def process_config_spec(
        cls,
        process: str | None = None,
        mc_datasets: Sequence[str] | None = None,
    ) -> DotDict:
        """
        Returns a dictionary representing configuration specific data, forwarding all arguments.

        :param process: The name of the process in the config to use.
        :param mc_datasets: List of names or patterns of datasets in the config to use for mc.
        :returns: A dictionary representing process specific settings.
        """
        return DotDict([
            ("process", str(process) if process else None),
            ("mc_datasets", list(map(str, mc_datasets or []))),
        ])

    @classmethod
    def parameter_config_spec(
        cls,
        shift_source: str | None = None,
    ) -> DotDict:
        """
        Returns a dictionary representing configuration specific data, forwarding all arguments.

        :param shift_source: The name of a systematic shift source in the config.
        :returns: A dictionary representing parameter specific settings.
        """
        return DotDict([
            ("shift_source", str(shift_source) if shift_source else None),
        ])

    def __init__(self, config_insts: list[od.Config]) -> None:
        super().__init__()

        # store attributes
        self.config_insts = config_insts

        # temporary attributes for as long as we issue deprecation warnings
        self.__config_inst = None

        # model info
        self.model = self.model_spec()

        # custom init function when set
        if callable(self.init_func):
            self.init_func()

    def to_yaml(self, stream: TextIO | None = None) -> str | None:
        """
        Writes the content of the :py:attr:`model` into a file-like object *stream* when given, and
        returns a string representation otherwise.

        :param stream: A file-like object to write the model content into.
        :returns: A string representation of the model content if *stream* is not provided.
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

    # !! to be removed in a future release
    @property
    def config_inst(self) -> od.Config:
        if self.__config_inst:
            return self.__config_inst

        # trigger a verbose warning in case the deprecated attribute is accessed
        docs_url = get_docs_url("user_guide", "02_03_transition.html")
        api_url = get_docs_url("api", "inference", "index.html", anchor="columnflow.inference.InferenceModel")
        logger.warning_once(
            "inference_model_deprected_config_inst",
            "access to attribute 'config_inst' in inference model was removed; use 'config_insts' instead; also, make "
            "sure to use the new 'config_data' attribute in 'add_category()' for a more fine-grained control over the "
            f"composition of your inference model categories; see {docs_url} and {api_url} for more info",
        )

        raise AttributeError(f"'{self.__class__.__name__}' object has no attribute 'config_inst'")

    @config_inst.setter
    def config_inst(self, config_inst: od.Config) -> None:
        self.__config_inst = config_inst

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
        category: str | Sequence[str] | None = None,
        only_names: bool = False,
        match_mode: Callable = any,
    ) -> list[DotDict | str]:
        """
        Returns a list of categories whose name match *category*. *category* can be a string, a
        pattern, or sequence of them. When *only_names* is *True*, only names of categories are
        returned rather than structured dictionaries.

        :param category: A string, pattern, or sequence of them to match category names.
        :param only_names: A boolean flag to return only names of categories if set to *True*.
        :param match_mode: Either ``any`` or ``all`` to control the category matching behavior (see
            :py:func:`pattern_matcher`).
        :returns: A list of matching categories or their names.
        """
        # rename arguments to make their meaning explicit
        category_pattern = category

        match = pattern_matcher(category_pattern or "*", mode=match_mode)
        return [
            (category.name if only_names else category)
            for category in self.categories
            if match(category.name)
        ]

    def get_category(
        self,
        category: str | Sequence[str],
        only_name: bool = False,
        match_mode: Callable = any,
        silent: bool = False,
    ) -> DotDict | str:
        """
        Returns a single category whose name matches *category*. *category* can be a string, a
        pattern, or sequence of them. An exception is raised if no or more than one category is
        found, unless *silent* is *True* in which case *None* is returned. When *only_name* is
        *True*, only the name of the category is returned rather than a structured dictionary.

        :param category: A string, pattern, or sequence of them to match category names.
        :param only_name: A boolean flag to return only the name of the category if set to *True*.
        :param match_mode: Either ``any`` or ``all`` to control the category matching behavior (see
            :py:func:`pattern_matcher`).
        :param silent: A boolean flag to return *None* instead of raising an exception if no or more than one category
            is found.
        :returns: A single matching category or its name.
        """
        # rename arguments to make their meaning explicit
        category_name = category

        categories = self.get_categories(category_name, only_names=only_name, match_mode=match_mode)

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
        category: str | Sequence[str],
        match_mode: Callable = any,
    ) -> bool:
        """
        Returns *True* if a category whose name matches *category* is existing, and *False*
        otherwise. *category* can be a string, a pattern, or sequence of them.

        :param category: A string, pattern, or sequence of them to match category names.
        :param match_mode: Either ``any`` or ``all`` to control the category matching behavior (see
            :py:func:`pattern_matcher`).
        :returns: *True* if a matching category exists, *False* otherwise.
        """
        # rename arguments to make their meaning explicit
        category_pattern = category

        # simple length check
        return len(self.get_categories(category_pattern, only_names=True, match_mode=match_mode)) > 0

    def add_category(self, *args, **kwargs) -> None:
        """
        Adds a new category with all *args* and *kwargs* used to create the structured category
        dictionary via :py:meth:`category_spec`. If a category with the same name already exists, an
        exception is raised.

        :raises ValueError: If a category with the same name already exists.
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
        category: str | Sequence[str],
        match_mode: Callable = any,
    ) -> bool:
        """
        Removes one or more categories whose names match *category*.

        :param category: A string, pattern, or sequence of them to match category names.
        :param match_mode: Either ``any`` or ``all`` to control the category matching behavior (see
            :py:func:`pattern_matcher`).
        :returns: *True* if at least one category was removed, *False* otherwise.
        """
        # rename arguments to make their meaning explicit
        category_pattern = category

        match = pattern_matcher(category_pattern, mode=match_mode)
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
        process: str | Sequence[str] | None = None,
        category: str | Sequence[str] | None = None,
        only_names: bool = False,
        flat: bool = False,
        match_mode: Callable = any,
        category_match_mode: Callable = any,
    ) -> dict[str, DotDict | str] | list[str]:
        """
        Returns a dictionary of processes whose names match *process*, mapped to the name of the
        category they belong to. Categories can optionally be filtered through *category*. Both
        *process* and *category* can be a string, a pattern, or sequence of them.

        When *only_names* is *True*, only names of processes are returned rather than structured
        dictionaries. When *flat* is *True*, a flat, unique list of process names is returned.

        :param process: A string, pattern, or sequence of them to match process names.
        :param category: A string, pattern, or sequence of them to filter categories.
        :param only_names: A boolean flag to return only names of processes if set to *True*.
        :param match_mode: Either ``any`` or ``all`` to control the process matching behavior (see
            :py:func:`pattern_matcher`).
        :param category_match_mode: Either ``any`` or ``all`` to control the category matching behavior (see
            :py:func:`pattern_matcher`).
        :param flat: A boolean flag to return a flat, unique list of process names if set to *True*.
        :returns: A dictionary of processes mapped to the category name, or a list of process names.
        """
        # rename arguments to make their meaning explicit
        process_pattern = process
        category_pattern = category

        # when flat is True, one can only return names
        if flat:
            only_names = True

        # get matching categories first
        categories = self.get_categories(category_pattern, match_mode=category_match_mode)

        # look for the process pattern in each of them
        match = pattern_matcher(process_pattern or "*", mode=match_mode)
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
        process: str | Sequence[str],
        category: str | Sequence[str] | None = None,
        only_name: bool = False,
        match_mode: Callable = any,
        category_match_mode: Callable = any,
        silent: bool = False,
    ) -> DotDict | str:
        """
        Returns a single process whose name matches *process*, and optionally, whose category's
        name matches *category*. Both *process* and *category* can be a string, a pattern, or
        sequence of them.

        An exception is raised if no or more than one process is found, unless *silent* is *True*
        in which case *None* is returned. When *only_name* is *True*, only the name of the
        process is returned rather than a structured dictionary.

        :param process: A string, pattern, or sequence of them to match process names.
        :param category: A string, pattern, or sequence of them to match category names.
        :param only_name: A boolean flag to return only the name of the process if set to *True*.
        :param match_mode: Either ``any`` or ``all`` to control the process matching behavior (see
            :py:func:`pattern_matcher`).
        :param category_match_mode: Either ``any`` or ``all`` to control the category matching behavior (see
            :py:func:`pattern_matcher`).
        :param silent: A boolean flag to return *None* instead of raising an exception if no or more than one process is
            found.
        :returns: A single matching process or its name.
        :raises ValueError: If no process or more than one process is found and *silent* is *False*.
        """
        # rename arguments to make their meaning explicit
        process_name = process
        category_pattern = category

        processes = self.get_processes(
            process_name,
            category=category_pattern,
            only_names=only_name,
            match_mode=match_mode,
            category_match_mode=category_match_mode,
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
        self,
        process: str | Sequence[str],
        category: str | Sequence[str] | None = None,
        match_mode: Callable = any,
        category_match_mode: Callable = any,
    ) -> bool:
        """
        Returns *True* if a process whose name matches *process*, and optionally whose category's
        name matches *category*, exists, and *False* otherwise. Both *process* and *category* can
        be a string, a pattern, or sequence of them.

        :param process: A string, pattern, or sequence of them to match process names.
        :param category: A string, pattern, or sequence of them to match category names.
        :param match_mode: Either ``any`` or ``all`` to control the process matching behavior (see
            :py:func:`pattern_matcher`).
        :param category_match_mode: Either ``any`` or ``all`` to control the category matching behavior (see
            :py:func:`pattern_matcher`).
        :returns: *True* if a matching process exists, *False* otherwise.
        """
        # rename arguments to make their meaning explicit
        process_pattern = process
        category_pattern = category

        # simple length check
        return len(self.get_processes(
            process_pattern,
            category=category_pattern,
            only_names=True,
            match_mode=match_mode,
            category_match_mode=category_match_mode,
        )) > 0

    def add_process(
        self,
        *args,
        category: str | Sequence[str] | None = None,
        category_match_mode: Callable = any,
        silent: bool = False,
        **kwargs,
    ) -> None:
        """
        Adds a new process to all categories whose names match *category*, with all *args* and
        *kwargs* used to create the structured process dictionary via :py:meth:`process_spec`.
        *category* can be a string, a pattern, or sequence of them.

        If a process with the same name already exists in one of the categories, an exception is
        raised unless *silent* is *True*.

        :param args: Positional arguments used to create the process.
        :param category: A string, pattern, or sequence of them to match category names.
        :param category_match_mode: Either ``any`` or ``all`` to control the category matching behavior (see
            :py:func:`pattern_matcher`).
        :param silent: A boolean flag to suppress exceptions if a process with the same name already exists.
        :param kwargs: Keyword arguments used to create the process.
        :raises ValueError: If a process with the same name already exists in one of the categories and *silent* is
            *False*.
        """
        # rename arguments to make their meaning explicit
        category_pattern = category

        process = self.process_spec(*args, **kwargs)

        # get categories the process should be added to
        categories = self.get_categories(category_pattern, match_mode=category_match_mode)

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
        self,
        process: str | Sequence[str],
        category: str | Sequence[str] | None = None,
        match_mode: Callable = any,
        category_match_mode: Callable = any,
    ) -> bool:
        """
        Removes one or more processes whose names match *process*, and optionally whose category's
        name matches *category*. Both *process* and *category* can be a string, a pattern, or
        sequence of them. Returns *True* if at least one process was removed, and *False* otherwise.

        :param process: A string, pattern, or sequence of them to match process names.
        :param category: A string, pattern, or sequence of them to match category names.
        :param match_mode: Either ``any`` or ``all`` to control the process matching behavior (see
            :py:func:`pattern_matcher`).
        :param category_match_mode: Either ``any`` or ``all`` to control the category matching behavior (see
            :py:func:`pattern_matcher`).
        :returns: *True* if at least one process was removed, *False* otherwise.
        """
        # rename arguments to make their meaning explicit
        process_pattern = process
        category_pattern = category

        # get categories the process should be removed from
        categories = self.get_categories(category_pattern, match_mode=category_match_mode)

        match = pattern_matcher(process_pattern, mode=match_mode)
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
        parameter: str | Sequence[str] | None = None,
        process: str | Sequence[str] | None = None,
        category: str | Sequence[str] | None = None,
        match_mode: Callable = any,
        category_match_mode: Callable = any,
        process_match_mode: Callable = any,
        only_names: bool = False,
        flat: bool = False,
    ) -> dict[str, dict[str, DotDict | str]] | list[str]:
        """
        Returns a dictionary of parameters whose names match *parameter*, mapped twice to the name
        of the category and the name of the process they belong to. Categories and processes can
        optionally be filtered through *category* and *process*. All three, *parameter*, *process*
        and *category* can be a string, a pattern, or sequence of them.

        When *only_names* is *True*, only names of parameters are returned rather than structured
        dictionaries. When *flat* is *True*, a flat, unique list of parameter names is returned.

        :param parameter: A string, pattern, or sequence of them to match parameter names.
        :param process: A string, pattern, or sequence of them to match process names.
        :param category: A string, pattern, or sequence of them to match category names.
        :param match_mode: Either ``any`` or ``all`` to control the parameter matching behavior (see
            :py:func:`pattern_matcher`).
        :param category_match_mode: Either ``any`` or ``all`` to control the category matching behavior (see
            :py:func:`pattern_matcher`).
        :param process_match_mode: Either ``any`` or ``all`` to control the process matching behavior (see
            :py:func:`pattern_matcher`).
        :param only_names: A boolean flag to return only names of parameters if set to *True*.
        :param flat: A boolean flag to return a flat, unique list of parameter names if set to *True*.
        :returns: A dictionary of parameters mapped to category and process names, or a list of parameter names.
        """
        # rename arguments to make their meaning explicit
        parameter_pattern = parameter
        process_pattern = process
        category_pattern = category

        # when flat is True, one can only return names
        if flat:
            only_names = True

        # get matching processes (mapped to matching categories)
        processes = self.get_processes(
            process=process_pattern,
            category=category_pattern,
            match_mode=process_match_mode,
            category_match_mode=category_match_mode,
        )

        # look for the process pattern in each pair
        match = pattern_matcher(parameter_pattern or "*", mode=match_mode)
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
        parameter: str | Sequence[str],
        process: str | Sequence[str] | None = None,
        category: str | Sequence[str] | None = None,
        match_mode: Callable = any,
        category_match_mode: Callable = any,
        process_match_mode: Callable = any,
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

        :param parameter: A string, pattern, or sequence of them to match parameter names.
        :param process: A string, pattern, or sequence of them to match process names.
        :param category: A string, pattern, or sequence of them to match category names.
        :param match_mode: Either ``any`` or ``all`` to control the parameter matching behavior (see
            :py:func:`pattern_matcher`).
        :param category_match_mode: Either ``any`` or ``all`` to control the category matching behavior (see
            :py:func:`pattern_matcher`).
        :param process_match_mode: Either ``any`` or ``all`` to control the process matching behavior (see
            :py:func:`pattern_matcher`).
        :param only_name: A boolean flag to return only the name of the parameter if set to *True*.
        :param silent: A boolean flag to return *None* instead of raising an exception if no or more than one parameter
            is found.
        :returns: A single matching parameter or its name.
        :raises ValueError: If no parameter or more than one parameter is found and *silent* is *False*.
        """
        # rename arguments to make their meaning explicit
        parameter_name = parameter
        process_pattern = process
        category_pattern = category

        parameters = self.get_parameters(
            parameter_name,
            process=process_pattern,
            category=category_pattern,
            match_mode=match_mode,
            category_match_mode=category_match_mode,
            process_match_mode=process_match_mode,
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
        self,
        parameter: str | Sequence[str],
        process: str | Sequence[str] | None = None,
        category: str | Sequence[str] | None = None,
        match_mode: Callable = any,
        category_match_mode: Callable = any,
        process_match_mode: Callable = any,
    ) -> bool:
        """
        Returns *True* if a parameter whose name matches *parameter*, and optionally whose
        category's and process' name match *category* and *process*, exists, and *False*
        otherwise. All three, *parameter*, *process* and *category* can be a string, a pattern,
        or sequence of them.

        :param parameter: A string, pattern, or sequence of them to match parameter names.
        :param process: A string, pattern, or sequence of them to match process names.
        :param category: A string, pattern, or sequence of them to match category names.
        :param match_mode: Either ``any`` or ``all`` to control the parameter matching behavior (see
            :py:func:`pattern_matcher`).
        :param category_match_mode: Either ``any`` or ``all`` to control the category matching behavior (see
            :py:func:`pattern_matcher`).
        :param process_match_mode: Either ``any`` or ``all`` to control the process matching behavior (see
            :py:func:`pattern_matcher`).
        :returns: *True* if a matching parameter exists, *False* otherwise.
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
            match_mode=match_mode,
            category_match_mode=category_match_mode,
            process_match_mode=process_match_mode,
        )) > 0

    def add_parameter(
        self,
        *args,
        process: str | Sequence[str] | None = None,
        category: str | Sequence[str] | None = None,
        category_match_mode: Callable = any,
        process_match_mode: Callable = any,
        group: str | Sequence[str] | None = None,
        **kwargs,
    ) -> DotDict:
        """
        Adds a parameter to all categories and processes whose names match *category* and *process*, with all *args* and
        *kwargs* used to create the structured parameter dictionary via :py:meth:`parameter_spec`. Both *process* and
        *category* can be a string, a pattern, or sequence of them.

        If a parameter with the same name already exists in one of the processes throughout the categories, an exception
        is raised.

        When *group* is given and the parameter is new, it is added to one or more parameter groups via
        :py:meth:`add_parameter_to_group`.

        :param args: Positional arguments used to create the parameter.
        :param process: A string, pattern, or sequence of them to match process names.
        :param category: A string, pattern, or sequence of them to match category names.
        :param category_match_mode: Either ``any`` or ``all`` to control the category matching behavior (see
            :py:func:`pattern_matcher`).
        :param process_match_mode: Either ``any`` or ``all`` to control the process matching behavior (see
            :py:func:`pattern_matcher`).
        :param group: A string, pattern, or sequence of them to specify parameter groups.
        :param kwargs: Keyword arguments used to create the parameter.
        :returns: The created parameter.
        :raises ValueError: If a parameter with the same name already exists in one of the processes throughout the
            categories.
        """
        # rename arguments to make their meaning explicit
        process_pattern = process
        category_pattern = category

        parameter = self.parameter_spec(*args, **kwargs)

        # get processes (mapped to categories) the parameter should be added to
        processes = self.get_processes(
            process=process_pattern,
            category=category_pattern,
            match_mode=process_match_mode,
            category_match_mode=category_match_mode,
        )

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

        # add to groups if it was added to at least one process
        if group and processes and any(_processes for _processes in processes.values()):
            self.add_parameter_to_group(parameter.name, group)

        return parameter

    def remove_parameter(
        self,
        parameter: str | Sequence[str],
        process: str | Sequence[str] | None = None,
        category: str | Sequence[str] | None = None,
        match_mode: Callable = any,
        category_match_mode: Callable = any,
        process_match_mode: Callable = any,
    ) -> bool:
        """
        Removes one or more parameters whose names match *parameter*, and optionally whose
        category's and process' name match *category* and *process*. All three, *parameter*,
        *process* and *category* can be a string, a pattern, or sequence of them.

        :param parameter: A string, pattern, or sequence of them to match parameter names.
        :param process: A string, pattern, or sequence of them to match process names.
        :param category: A string, pattern, or sequence of them to match category names.
        :param match_mode: Either ``any`` or ``all`` to control the parameter matching behavior (see
            :py:func:`pattern_matcher`).
        :param category_match_mode: Either ``any`` or ``all`` to control the category matching behavior (see
            :py:func:`pattern_matcher`).
        :param process_match_mode: Either ``any`` or ``all`` to control the process matching behavior (see
            :py:func:`pattern_matcher`).
        :returns: *True* if at least one parameter was removed, *False* otherwise.
        """
        # rename arguments to make their meaning explicit
        parameter_pattern = parameter
        process_pattern = process
        category_pattern = category

        # get processes (mapped to categories) the parameter should be removed from
        processes = self.get_processes(
            process=process_pattern,
            category=category_pattern,
            match_mode=process_match_mode,
            category_match_mode=category_match_mode,
        )

        match = pattern_matcher(parameter_pattern, mode=match_mode)
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
        group: str | Sequence[str] | None = None,
        match_mode: Callable = any,
        only_names: bool = False,
    ) -> list[DotDict | str]:
        """
        Returns a list of parameter groups whose names match *group*. *group* can be a string, a
        pattern, or sequence of them.

        When *only_names* is *True*, only names of parameter groups are returned rather than
        structured dictionaries.

        :param group: A string, pattern, or sequence of them to match group names.
        :param match_mode: Either ``any`` or ``all`` to control the parameter group matching behavior (see
            :py:func:`pattern_matcher`).
        :param only_names: A boolean flag to return only names of parameter groups if set to *True*.
        :returns: A list of parameter groups or their names.
        """
        # rename arguments to make their meaning explicit
        group_pattern = group

        match = pattern_matcher(group_pattern or "*", mode=match_mode)
        return [
            (group.name if only_names else group)
            for group in self.parameter_groups
            if match(group.name)
        ]

    def get_parameter_group(
        self,
        group: str | Sequence[str],
        match_mode: Callable = any,
        only_name: bool = False,
    ) -> DotDict | str:
        """
        Returns a single parameter group whose name matches *group*. *group* can be a string, a
        pattern, or sequence of them.

        An exception is raised in case no or more than one parameter group is found. When
        *only_name* is *True*, only the name of the parameter group is returned rather than a
        structured dictionary.

        :param group: A string, pattern, or sequence of them to match group names.
        :param match_mode: Either ``any`` or ``all`` to control the parameter group matching behavior (see
            :py:func:`pattern_matcher`).
        :param only_name: A boolean flag to return only the name of the parameter group if set to *True*.
        :returns: A single matching parameter group or its name.
        :raises ValueError: If no parameter group or more than one parameter group is found.
        """
        # rename arguments to make their meaning explicit
        group_name = group

        groups = self.get_parameter_groups(group_name, match_mode=match_mode, only_names=only_name)

        # checks
        if not groups:
            raise ValueError(f"no parameter group named '{group_name}' found")
        if len(groups) > 1:
            raise ValueError(f"parameter group name '{group_name}' matches more than one category")

        return groups[0]

    def has_parameter_group(
        self,
        group: str | Sequence[str],
        match_mode: Callable = any,
    ) -> bool:
        """
        Returns *True* if a parameter group whose name matches *group* exists, and *False*
        otherwise. *group* can be a string, a pattern, or sequence of them.

        :param group: A string, pattern, or sequence of them to match group names.
        :param match_mode: Either ``any`` or ``all`` to control the parameter group matching behavior (see
            :py:func:`pattern_matcher`).
        :returns: *True* if a matching parameter group exists, *False* otherwise.
        """
        # rename arguments to make their meaning explicit
        group_pattern = group

        # simeple length check
        return len(self.get_parameter_groups(group_pattern, match_mode=match_mode)) > 0

    def add_parameter_group(self, *args, **kwargs) -> None:
        """
        Adds a new parameter group with all *args* and *kwargs* used to create the structured
        parameter group dictionary via :py:meth:`parameter_group_spec`. If a group with the same
        name already exists, an exception is raised.

        :param args: Positional arguments used to create the parameter group.
        :param kwargs: Keyword arguments used to create the parameter group.
        :raises ValueError: If a parameter group with the same name already exists.
        """
        # create the instance
        group = self.parameter_group_spec(*args, **kwargs)

        # checks
        if self.has_parameter_group(group.name):
            raise ValueError(f"parameter group named '{group.name}' already registered")

        self.parameter_groups.append(group)

    def remove_parameter_group(
        self,
        group: str | Sequence[str],
        match_mode: Callable = any,
    ) -> bool:
        """
        Removes one or more parameter groups whose names match *group*. *group* can be a string, a
        pattern, or sequence of them. Returns *True* if at least one group was removed, and *False*
        otherwise.

        :param group: A string, pattern, or sequence of them to match group names.
        :param match_mode: Either ``any`` or ``all`` to control the parameter group matching behavior (see
            :py:func:`pattern_matcher`).
        :returns: *True* if at least one group was removed, *False* otherwise.
        """
        # rename arguments to make their meaning explicit
        group_pattern = group

        match = pattern_matcher(group_pattern, mode=match_mode)
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
        parameter: str | Sequence[str],
        group: str | Sequence[str],
        match_mode: Callable = any,
        parameter_match_mode: Callable = any,
    ) -> bool:
        """
        Adds a parameter named *parameter* to one or multiple parameter groups whose names match
        *group*. *group* can be a string, a pattern, or sequence of them. When *parameter* is a
        pattern or regular expression, all previously added, matching parameters are added.
        Otherwise, *parameter* is added as is. If a parameter was added to at least one group,
        *True* is returned and *False* otherwise.

        :param parameter: A string, pattern, or sequence of them to match parameter names.
        :param group: A string, pattern, or sequence of them to match group names.
        :param match_mode: Either ``any`` or ``all`` to control the parameter group matching behavior (see
            :py:func:`pattern_matcher`).
        :param parameter_match_mode: Either ``any`` or ``all`` to control the parameter matching behavior (see
            :py:func:`pattern_matcher`).
        :returns: *True* if at least one parameter was added to a group, *False* otherwise.
        """
        # rename arguments to make their meaning explicit
        parameter_pattern = parameter
        group_pattern = group

        # stop when there are no matching groups
        groups = self.get_parameter_groups(group_pattern, match_mode=match_mode)
        if not groups:
            return False

        # when parameter(s) contain any pattern, get flat list of all existing parameters first
        _is_pattern = lambda s: is_pattern(s) or is_regex(s)
        parameter_pattern = law.util.make_list(parameter_pattern)
        if any(map(_is_pattern, parameter_pattern)):
            parameter_names = self.get_parameters(parameter_pattern, flat=True, match_mode=parameter_match_mode)
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
        parameter: str | Sequence[str],
        group: str | Sequence[str] | None = None,
        match_mode: Callable = any,
        parameter_match_mode: Callable = any,
    ) -> bool:
        """
        Removes all parameters matching *parameter* from parameter groups whose names match *group*.
        Both *parameter* and *group* can be a string, a pattern, or sequence of them. Returns *True*
        if at least one parameter was removed, and *False* otherwise.

        :param parameter: A string, pattern, or sequence of them to match parameter names.
        :param group: A string, pattern, or sequence of them to match group names.
        :param match_mode: Either ``any`` or ``all`` to control the parameter group matching behavior (see
            :py:func:`pattern_matcher`).
        :param parameter_match_mode: Either ``any`` or ``all`` to control the parameter matching behavior (see
            :py:func:`pattern_matcher`).
        :returns: *True* if at least one parameter was removed, *False* otherwise.
        """
        # rename arguments to make their meaning explicit
        parameter_pattern = parameter
        group_pattern = group

        # stop when there are no matching groups
        groups = self.get_parameter_groups(group_pattern, match_mode=match_mode)
        if not groups:
            return False

        match = pattern_matcher(parameter_pattern, mode=parameter_match_mode)
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
        process: str | Sequence[str],
        match_mode: Callable = any,
    ) -> list[str]:
        """
        Returns a flat list of category names that contain processes matching *process*. *process*
        can be a string, a pattern, or sequence of them.

        :param process: A string, pattern, or sequence of them to match process names.
        :param match_mode: Either ``any`` or ``all`` to control the process matching behavior (see
            :py:func:`pattern_matcher`).
        :returns: A list of category names containing matching processes.
        """
        # rename arguments to make their meaning explicit
        process_pattern = process

        # plain name lookup
        return list(self.get_processes(process=process_pattern, match_mode=match_mode, only_names=True).keys())

    def get_processes_with_parameter(
        self,
        parameter: str | Sequence[str],
        category: str | Sequence[str] | None = None,
        match_mode: Callable = any,
        category_match_mode: Callable = any,
        flat: bool = True,
    ) -> list[str] | dict[str, list[str]]:
        """
        Returns a dictionary of names of processes that contain a parameter whose names match
        *parameter*, mapped to category names. Categories can optionally be filtered through
        *category*. Both *parameter* and *category* can be a string, a pattern, or sequence of them.

        When *flat* is *True*, a flat, unique list of process names is returned.

        :param parameter: A string, pattern, or sequence of them to match parameter names.
        :param category: A string, pattern, or sequence of them to match category names.
        :param match_mode: Either ``any`` or ``all`` to control the parameter matching behavior (see
            :py:func:`pattern_matcher`).
        :param category_match_mode: Either ``any`` or ``all`` to control the category matching behavior (see
            :py:func:`pattern_matcher`).
        :param flat: A boolean flag to return a flat, unique list of process names if set to *True*.
        :returns: A dictionary of process names mapped to category names, or a flat list of process names.
        """
        # rename arguments to make their meaning explicit
        parameter_pattern = parameter
        category_pattern = category

        processes = self.get_parameters(
            parameter=parameter_pattern,
            category=category_pattern,
            match_mode=match_mode,
            category_match_mode=category_match_mode,
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
        parameter: str | Sequence[str],
        process: str | Sequence[str] | None = None,
        match_mode: Callable = any,
        process_match_mode: Callable = any,
        flat: bool = True,
    ) -> list[str] | dict[str, list[str]]:
        """
        Returns a dictionary of category names mapping to process names that contain parameters
        whose names match *parameter*. Processes can optionally be filtered through *process*. Both
        *parameter* and *process* can be a string, a pattern, or sequence of them.

        When *flat* is *True*, a flat, unique list of category names is returned.

        :param parameter: A string, pattern, or sequence of them to match parameter names.
        :param process: A string, pattern, or sequence of them to match process names.
        :param match_mode: Either ``any`` or ``all`` to control the parameter matching behavior (see
            :py:func:`pattern_matcher`).
        :param process_match_mode: Either ``any`` or ``all`` to control the process matching behavior (see
            :py:func:`pattern_matcher`).
        :param flat: A boolean flag to return a flat, unique list of category names if set to *True*.
        :returns: A dictionary of category names mapped to process names, or a flat list of category names.
        """
        # rename arguments to make their meaning explicit
        parameter_pattern = parameter
        process_pattern = process

        categories = self.get_parameters(
            parameter=parameter_pattern,
            process=process_pattern,
            match_mode=match_mode,
            process_match_mode=process_match_mode,
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
        parameter: str | Sequence[str],
        match_mode: Callable = any,
    ) -> list[str]:
        """
        Returns a list of names of parameter groups that contain a parameter whose name matches
        *parameter*. *parameter* can be a string, a pattern, or sequence of them.

        :param parameter: A string, pattern, or sequence of them to match parameter names.
        :param match_mode: Either ``any`` or ``all`` to control the parameter matching behavior (see
            :py:func:`pattern_matcher`).
        :returns: A list of names of parameter groups containing the matching parameter.
        """
        # rename arguments to make their meaning explicit
        parameter_pattern = parameter

        match = pattern_matcher(parameter_pattern, mode=match_mode)
        return [
            group.name
            for group in self.parameter_groups
            if any(map(match, group.parameter_names))
        ]

    #
    # removal of empty and dangling objects
    #

    def cleanup(
        self,
        keep_parameters: str | Sequence[str] | None = None,
    ) -> None:
        """
        Cleans the internal model structure by removing empty and dangling objects by calling
        :py:meth:`remove_empty_categories`, :py:meth:`remove_dangling_parameters_from_groups`
        (receiving *keep_parameters*), and :py:meth:`remove_empty_parameter_groups` in that order.

        :param keep_parameters: A string, pattern, or sequence of them to specify parameters to keep.
        """
        self.remove_empty_categories()
        self.remove_dangling_parameters_from_groups(keep_parameters=keep_parameters)
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

    def remove_dangling_parameters_from_groups(
        self,
        keep_parameters: str | Sequence[str] | None = None,
    ) -> None:
        """
        Removes names of parameters from parameter groups that are not assigned to any process in any category.

        :param keep_parameters: A string, pattern, or sequence of them to specify parameters to keep.
        """
        # get a list of all parameters
        parameter_names = self.get_parameters("*", flat=True)

        # get set of parameters to keep
        _keep_parameters = law.util.make_set(keep_parameters) if keep_parameters else set()

        # go through groups and remove dangling parameters
        for group in self.parameter_groups:
            group.parameter_names[:] = [
                parameter_name
                for parameter_name in group.parameter_names
                if (
                    parameter_name in parameter_names or
                    law.util.multi_match(parameter_name, _keep_parameters, mode=any)
                )
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
        process: str | Sequence[str] | None = None,
        category: str | Sequence[str] | None = None,
        match_mode: Callable = any,
        category_match_mode: Callable = any,
    ) -> Generator[tuple[DotDict, DotDict], None, None]:
        """
        Generator that iteratively yields all processes whose names match *process*, optionally
        in all categories whose names match *category*. The yielded value is a 2-tuple containing
        the category name and the process object.

        :param process: A string, pattern, or sequence of them to match process names.
        :param category: A string, pattern, or sequence of them to match category names.
        :param match_mode: Either ``any`` or ``all`` to control the process matching behavior (see
            :py:func:`pattern_matcher`).
        :param category_match_mode: Either ``any`` or ``all`` to control the category matching behavior (see
            :py:func:`pattern_matcher`).
        :returns: A generator yielding 2-tuples of category name and process object.
        """
        processes = self.get_processes(
            process=process,
            category=category,
            match_mode=match_mode,
            category_match_mode=category_match_mode,
        )
        for category_name, processes in processes.items():
            for process in processes:
                yield (category_name, process)

    def iter_parameters(
        self,
        parameter: str | Sequence[str] | None = None,
        process: str | Sequence[str] | None = None,
        category: str | Sequence[str] | None = None,
        match_mode: Callable = any,
        category_match_mode: Callable = any,
        process_match_mode: Callable = any,
    ) -> Generator[tuple[DotDict, DotDict, DotDict], None, None]:
        """
        Generator that iteratively yields all parameters whose names match *parameter*, optionally
        in all processes and categories whose names match *process* and *category*. The yielded
        value is a 3-tuple containing the category name, the process name, and the parameter object.

        :param parameter: A string, pattern, or sequence of them to match parameter names.
        :param process: A string, pattern, or sequence of them to match process names.
        :param category: A string, pattern, or sequence of them to match category names.
        :param match_mode: Either ``any`` or ``all`` to control the parameter matching behavior (see
            :py:func:`pattern_matcher`).
        :param category_match_mode: Either ``any`` or ``all`` to control the category matching behavior (see
            :py:func:`pattern_matcher`).
        :param process_match_mode: Either ``any`` or ``all`` to control the process matching behavior (see
            :py:func:`pattern_matcher`).
        :returns: A generator yielding 3-tuples of category name, process name, and parameter object.
        """
        parameters = self.get_parameters(
            parameter=parameter,
            process=process,
            category=category,
            match_mode=match_mode,
            category_match_mode=category_match_mode,
            process_match_mode=process_match_mode,
        )
        for category_name, parameters in parameters.items():
            for process_name, parameters in parameters.items():
                for parameter in parameters:
                    yield (category_name, process_name, parameter)

    #
    # other helpers
    #

    def scale_process(
        self,
        scale: int | float,
        process: str | Sequence[str] | None = None,
        category: str | Sequence[str] | None = None,
        match_mode: Callable = any,
        category_match_mode: Callable = any,
    ) -> bool:
        """
        Sets the scale attribute of all processes whose names match *process*, optionally in all
        categories whose names match *category*, to *scale*.

        :param scale: The scale value to set for the matching processes.
        :param process: A string, pattern, or sequence of them to match process names.
        :param category: A string, pattern, or sequence of them to match category names.
        :param match_mode: Either ``any`` or ``all`` to control the process matching behavior (see
            :py:func:`pattern_matcher`).
        :param category_match_mode: Either ``any`` or ``all`` to control the category matching behavior (see
            :py:func:`pattern_matcher`).
        :returns: *True* if at least one process was found and scaled, *False* otherwise.
        """
        found = False
        for _, process in self.iter_processes(
            process=process,
            category=category,
            match_mode=match_mode,
            category_match_mode=category_match_mode,
        ):
            process.scale = float(scale)
            found = True
        return found


# shorthand
inference_model = InferenceModel.inference_model
