# coding: utf-8

"""
Generic tools and base tasks that are defined along typical objects in an analysis.
"""

from __future__ import annotations

import os
import abc
import enum
import importlib
import itertools
import inspect
import functools
import collections
import copy
import subprocess
from dataclasses import dataclass, field

import luigi
import law
import order as od

from columnflow.columnar_util import mandatory_coffea_columns, Route, ColumnCollection
from columnflow.util import get_docs_url, is_regex, prettify, DotDict
from columnflow.types import Sequence, Callable, Any, T


logger = law.logger.get_logger(__name__)
logger_dev = law.logger.get_logger(f"{__name__}-dev")

# default analysis and config related objects
default_analysis = law.config.get_expanded("analysis", "default_analysis")
default_config = law.config.get_expanded("analysis", "default_config")
default_dataset = law.config.get_expanded("analysis", "default_dataset")
default_repr_max_len = law.config.get_expanded_int("analysis", "repr_max_len")
default_repr_max_count = law.config.get_expanded_int("analysis", "repr_max_count")
default_repr_hash_len = law.config.get_expanded_int("analysis", "repr_hash_len")

# placeholder to denote a default value that is resolved dynamically
RESOLVE_DEFAULT = "DEFAULT"


class Requirements(DotDict):
    """
    Container for task-level requirements of different tasks.

    Can be initialized with other :py:class:`Requirement` instances and additional keyword arguments ``kwargs``,
    which are added.
    """

    def __init__(self, *others, **kwargs) -> None:
        super().__init__()

        # add others and kwargs
        for reqs in others + (kwargs,):
            self.update(reqs)


class OutputLocation(enum.Enum):
    """
    Output location flag.
    """

    config = "config"
    local = "local"
    wlcg = "wlcg"
    wlcg_mirrored = "wlcg_mirrored"


@dataclass
class TaskShifts:
    """
    Container for *local* and *upstream* shifts at a point in the task graph.
    """
    # NOTE: maybe these should be a dict of sets (one set per config) to allow for different shifts
    # per config

    local: set[str] = field(default_factory=set)
    upstream: set[str] = field(default_factory=set)


class BaseTask(law.Task):

    task_namespace = law.config.get_expanded("analysis", "cf_task_namespace", "cf")

    # container for upstream requirements for convenience
    reqs = Requirements()

    def get_params_dict(self) -> dict[str, Any]:
        return {
            attr: getattr(self, attr)
            for attr, param in self.get_params()
            if isinstance(param, luigi.Parameter)
        }


class AnalysisTask(BaseTask, law.SandboxTask):

    analysis = luigi.Parameter(
        default=default_analysis,
        description=f"name of the analysis; default: '{default_analysis}'",
    )
    version = luigi.Parameter(
        description="mandatory version that is encoded into output paths",
    )
    notify_slack = law.slack.NotifySlackParameter(significant=False)
    notify_mattermost = law.mattermost.NotifyMattermostParameter(significant=False)
    notify_custom = law.NotifyCustomParameter(significant=False)

    allow_empty_sandbox = True
    sandbox = None

    message_cache_size = 25
    local_workflow_require_branches = False
    output_collection_cls = law.SiblingFileCollection

    # defaults for targets
    default_store = "$CF_STORE_LOCAL"
    default_wlcg_fs = law.config.get_expanded("target", "default_wlcg_fs", "wlcg_fs")
    default_output_location = "config"

    exclude_params_index = {"user"}
    exclude_params_req = {"user", "notify_slack", "notify_mattermost", "notify_custom"}
    exclude_params_repr = {"user", "notify_slack", "notify_mattermost", "notify_custom"}
    exclude_params_branch = {"user"}
    exclude_params_workflow = {"user", "notify_slack", "notify_mattermost", "notify_custom"}

    # cached and parsed sections of the law config for faster lookup
    _cfg_outputs_dict = None
    _cfg_versions_dict = None
    _cfg_resources_dict = None

    @classmethod
    def modify_param_values(cls, params: dict[str, Any]) -> dict[str, Any]:
        params = super().modify_param_values(params)
        params = cls.resolve_param_values(params)
        return params

    @classmethod
    def resolve_param_values(cls, params: dict[str, Any]) -> dict[str, Any]:
        # store a reference to the analysis inst
        if "analysis_inst" not in params and "analysis" in params:
            params["analysis_inst"] = cls.get_analysis_inst(params["analysis"])

        return params

    @classmethod
    def get_analysis_inst(cls, analysis: str) -> od.Analysis:
        # prepare names
        if "." not in analysis:
            raise ValueError(f"invalid analysis format: {analysis}")
        module_id, name = analysis.rsplit(".", 1)

        # import the module
        try:
            mod = importlib.import_module(module_id)
        except ImportError as e:
            raise ImportError(f"cannot import analysis module {module_id}: {e}")

        # get the analysis instance
        analysis_inst = getattr(mod, name, None)
        if analysis_inst is None:
            raise Exception(f"module {module_id} does not contain analysis instance {name}")

        return analysis_inst

    @classmethod
    def req_params(cls, inst: AnalysisTask, **kwargs) -> dict[str, Any]:
        """
        Returns parameters that are jointly defined in this class and another task instance of some
        other class. The parameters are used when calling ``Task.req(self)``.
        """
        # always prefer certain parameters given as task family parameters (--TaskFamily-parameter)
        _prefer_cli = law.util.make_set(kwargs.get("_prefer_cli", [])) | {
            "version", "workflow", "job_workers", "poll_interval", "walltime", "max_runtime",
            "retries", "acceptance", "tolerance", "parallel_jobs", "shuffle_jobs", "htcondor_cpus",
            "htcondor_gpus", "htcondor_memory", "htcondor_disk", "htcondor_pool", "pilot",
        }
        kwargs["_prefer_cli"] = _prefer_cli

        # build the params
        params = super().req_params(inst, **kwargs)

        # when not explicitly set in kwargs and no global value was defined on the cli for the task
        # family, evaluate and use the default value
        if (
            isinstance(getattr(cls, "version", None), luigi.Parameter) and
            "version" not in kwargs and
            not law.parser.global_cmdline_values().get(f"{cls.task_family}_version") and
            cls.task_family != law.parser.root_task_cls().task_family
        ):
            default_version = cls.get_default_version(inst, params)
            if default_version and default_version != law.NO_STR:
                params["version"] = default_version

        return params

    @classmethod
    def _structure_cfg_items(cls, items: list[tuple[str, Any]]) -> dict[str, Any]:
        if not items:
            return {}

        # apply brace expansion to keys
        items = sum((
            [(_key, value) for _key in law.util.brace_expand(key)]
            for key, value in items
        ), [])

        # breakup keys at double underscores and create a nested dictionary
        items_dict = {}
        for key, value in items:
            if not value:
                continue
            d = items_dict
            parts = key.split("__")
            for i, part in enumerate(parts):
                if i < len(parts) - 1:
                    # fill intermediate structure
                    if part not in d:
                        d[part] = {}
                    elif not isinstance(d[part], dict):
                        d[part] = {"*": d[part]}
                    d = d[part]
                else:
                    # assign value to the last nesting level
                    if part in d and isinstance(d[part], dict):
                        d[part]["*"] = value
                    else:
                        d[part] = value

        return items_dict

    @classmethod
    def _get_cfg_outputs_dict(cls) -> dict[str, Any]:
        if cls._cfg_outputs_dict is None and law.config.has_section("outputs"):
            # collect config item pairs
            skip_keys = {"wlcg_file_systems", "lfn_sources"}
            items = [
                (key, law.config.get_expanded("outputs", key, None, split_csv=True))
                for key, value in law.config.items("outputs")
                if value and key not in skip_keys
            ]
            cls._cfg_outputs_dict = cls._structure_cfg_items(items)

        return cls._cfg_outputs_dict

    @classmethod
    def _get_cfg_versions_dict(cls) -> dict[str, Any]:
        if cls._cfg_versions_dict is None and law.config.has_section("versions"):
            # collect config item pairs
            items = [
                (key, value)
                for key, value in law.config.items("versions")
                if value
            ]
            cls._cfg_versions_dict = cls._structure_cfg_items(items)

        return cls._cfg_versions_dict

    @classmethod
    def _get_cfg_resources_dict(cls) -> dict[str, Any]:
        if cls._cfg_resources_dict is None and law.config.has_section("resources"):
            # helper to split resource values into key-value pairs themselves
            def parse(key: str, value: str) -> tuple[str, list[tuple[str, Any]]]:
                params = []
                for part in value.split(","):
                    part = part.strip()
                    if not part:
                        continue
                    if "=" not in part:
                        logger.warning_once(
                            f"invalid_resource_{key}",
                            f"resource for key {key} contains invalid instruction {part}, skipping",
                        )
                        continue
                    param, value = (s.strip() for s in part.split("=", 1))
                    params.append((param, value))
                return key, params

            # collect config item pairs
            items = [
                parse(key, value)
                for key, value in law.config.items("resources")
                if value and not key.startswith("_")
            ]
            cls._cfg_resources_dict = cls._structure_cfg_items(items)

        return cls._cfg_resources_dict

    @classmethod
    def get_default_version(cls, inst: AnalysisTask, params: dict[str, Any]) -> str | None:
        """
        Determines the default version for instances of *this* task class when created through :py:meth:`req` from
        another task *inst* given parameters *params*.

        :param inst: The task instance from which *this* task should be created via :py:meth:`req`.
        :param params: The parameters that are passed to the task instance.
        :return: The default version, or *None* if no default version can be defined.
        """
        # get different attributes by which the default version might be looked up
        keys = cls.get_config_lookup_keys(params)

        # forward to lookup implementation
        version = cls._get_default_version(inst, params, keys)

        # after a version is found, it can still be an exectuable taking the same arguments
        return version(cls, inst, params) if callable(version) else version

    @classmethod
    def _get_default_version(
        cls,
        inst: AnalysisTask,
        params: dict[str, Any],
        keys: law.util.InsertableDict,
    ) -> str | None:
        # try to lookup the version in the analysis's auxiliary data
        analysis_inst = getattr(inst, "analysis_inst", None)
        if analysis_inst:
            version = cls._dfs_key_lookup(keys, analysis_inst.x("versions", {}))
            if version:
                return version

        # try to find it in the analysis section in the law config
        if law.config.has_section("versions"):
            versions_dict = cls._get_cfg_versions_dict()
            if versions_dict:
                version = cls._dfs_key_lookup(keys, versions_dict)
                if version:
                    return version

        # no default version found
        return None

    @classmethod
    def get_config_lookup_keys(
        cls,
        inst_or_params: AnalysisTask | dict[str, Any],
    ) -> law.util.InsertiableDict:
        """
        Returns a dictionary with keys that can be used to lookup state specific values in a config or dictionary, such
        as default task versions or output locations.

        :param inst_or_params: The tasks instance or its parameters.
        :return: A dictionary with keys that can be used for nested lookup.
        """
        keys = law.util.InsertableDict()

        # add the analysis name
        analysis = (
            inst_or_params.get("analysis")
            if isinstance(inst_or_params, dict)
            else getattr(inst_or_params, "analysis", None)
        )
        if analysis not in {law.NO_STR, None, ""}:
            prefix = "ana"
            keys[prefix] = f"{prefix}_{analysis}"

        # add the task family
        prefix = "task"
        keys[prefix] = f"{prefix}_{cls.task_family}"

        # for backwards compatibility, add the task family again without the prefix
        # (TODO: this should be removed in the future)
        keys[f"{prefix}_compat"] = cls.task_family

        return keys

    @classmethod
    def _dfs_key_lookup(
        cls,
        keys: law.util.InsertableDict[str, str | Sequence[str]] | Sequence[str | Sequence[str]],
        nested_dict: dict[str, Any],
        empty_value: Any = None,
    ) -> str | Callable | None:
        # opinionated nested dictionary lookup alongside in ordered sequence of (optional) keys,
        # that allows for patterns in the keys and, interpreting the nested dict as a tree, finds
        # matches in a depth-first (dfs) manner
        if not nested_dict:
            return empty_value

        # the keys to use for the lookup are the flattened values of the keys dict
        flat_keys = law.util.flatten(keys.values() if isinstance(keys, dict) else keys)

        # start tree traversal using a queue lookup consisting of names and values of tree nodes,
        # as well as the remaining keys (as a deferred function) to compare for that particular path
        lookup = collections.deque([tpl + ((lambda: flat_keys.copy()),) for tpl in nested_dict.items()])
        while lookup:
            pattern, obj, keys_func = lookup.popleft()

            # create the copy of comparison keys on demand
            # (the original sequence is living once on the previous stack until now)
            _keys = keys_func()

            # check if the pattern matches any key
            regex = is_regex(pattern)
            for i, key in enumerate(_keys):
                if law.util.multi_match(key, pattern, regex=regex):
                    # for a limited time, show a deprecation warning when the old task family key was matched
                    # (old = no "task_" prefix)
                    # TODO: remove once deprecated
                    if "task_compat" in keys and key == keys["task_compat"]:
                        docs_url = get_docs_url(
                            "user_guide",
                            "best_practices.html",
                            anchor="selecting-output-locations",
                        )
                        logger.warning_once(
                            "dfs_lookup_old_task_key",
                            f"during the lookup of a pinned location, version or resource value of a '{cls.__name__}' "
                            f"task, an entry matched based on the task family '{key}' that misses the new 'task_' "
                            "prefix; please update the pinned entries in your law.cfg file by adding the 'task_' "
                            f"prefix to entries that contain the task family, e.g. 'task_{key}: VALUE'; support for "
                            f"missing prefixes will be removed in a future version; see {docs_url} for more info",
                        )
                    # remove the matched key from remaining lookup keys
                    _keys.pop(i)
                    # when obj is not a dict, we found the value
                    if not isinstance(obj, dict):
                        return obj
                    # go one level deeper and stop the current iteration
                    keys_func = (lambda _keys: (lambda: _keys.copy()))(_keys)
                    lookup.extendleft(tpl + (keys_func,) for tpl in reversed(obj.items()))
                    break

        # at this point, no value could be found
        return empty_value

    @classmethod
    def get_array_function_dict(cls, params: dict[str, Any]) -> dict[str, Any]:
        if "analysis_inst" in params:
            analysis_inst = params["analysis_inst"]
        else:
            analysis_inst = cls.get_analysis_inst(params["analysis"])

        return {"analysis_inst": analysis_inst}

    @classmethod
    def find_config_objects(
        cls,
        names: str | Sequence[str] | set[str],
        container: od.UniqueObject | Sequence[od.UniqueObject],
        object_cls: od.UniqueObjectMeta,
        groups_str: str | None = None,
        accept_patterns: bool = True,
        deep: bool = False,
        strict: bool = False,
        multi_strategy: str = "first",
    ) -> list[str] | dict[od.UniqueObject, list[str]]:
        """
        Returns all names of objects of type *object_cls* known to a *container* (e.g. :py:class:`od.Analysis` or
        :py:class:`od.Config`) that match *names*. A name can also be a pattern to match if *accept_patterns* is *True*,
        or, when given, the key of a mapping named *group_str* in the container auxiliary data that matches group names
        to object names.

        When *deep* is *True* the lookup of objects in the *container* is recursive. When *strict* is *True*, an error
        is raised if no matches are found for any of the *names*.

        *container* can also refer to a sequence of container objects. If this is the case, the default object retrieval
        is performed for all of them and the resulting values can be handled with five different strategies, controlled
        via *multi_strategy*:

            - ``"first"``: The first resolved name is returned.
            - ``"same"``: The resolved names are forced to be identical and an exception is raised if they differ. The
                first resolved value is returned.
            - ``"union"``: The set union of all resolved names is returned in a list.
            - ``"intersection"``: The set intersection of all resolved names is returned in a list.
            - ``"all"``: The resolved values are returned in a dictionary mapped to their respective container.

        Example:

        .. code-block:: python

            find_config_objects(names=["st_tchannel_*"], container=config_inst, object_cls=od.Dataset)
            # -> ["st_tchannel_t", "st_tchannel_tbar"]
        """
        # when the container is a sequence, find objects per container and apply the multi_strategy
        if isinstance(container, (list, tuple)):
            if multi_strategy not in (strategies := {"first", "same", "union", "intersection", "all"}):
                raise ValueError(f"invalid multi_strategy: {multi_strategy}, must be one of {','.join(strategies)}")

            all_object_names = {
                _container: cls.find_config_objects(
                    names=names,
                    container=_container,
                    object_cls=object_cls,
                    groups_str=groups_str,
                    accept_patterns=accept_patterns,
                    deep=deep,
                    strict=strict,
                )
                for _container in container
            }

            if multi_strategy == "all":
                return all_object_names
            if multi_strategy == "first":
                return all_object_names[container[0]]
            if multi_strategy == "union":
                return list(set.union(*map(set, all_object_names.values())))
            if multi_strategy == "intersection":
                return list(set.intersection(*map(set, all_object_names.values())))
            # "same", so check that values are identical
            first = all_object_names[container[0]]
            if not all(all_object_names[c] == first for c in container[1:]):
                raise ValueError(
                    f"different objects found across containers looking for '{object_cls}' objects '{names}':\n"
                    f"{prettify(all_object_names)}",
                )
            return first

        # prepare value caching
        singular = object_cls.cls_name_singular
        plural = object_cls.cls_name_plural
        _cache: dict[str, set[str]] = {}

        def get_all_object_names() -> set[str]:
            if "all_object_names" not in _cache:
                if deep:
                    _cache["all_object_names"] = {obj.name for obj, _, _ in getattr(container, f"walk_{plural}")()}
                else:
                    _cache["all_object_names"] = set(getattr(container, plural).names())
            return _cache["all_object_names"]

        def has_obj(name: str) -> bool:
            if "has_obj_func" not in _cache:
                kwargs = {}
                if object_cls in container._deep_child_classes:
                    kwargs["deep"] = deep
                _cache["has_obj_func"] = functools.partial(getattr(container, f"has_{singular}"), **kwargs)
            return _cache["has_obj_func"](name)

        object_names = []
        lookup = law.util.make_list(names)
        missing = set()
        while lookup:
            name = lookup.pop(0)
            if has_obj(name):
                # known object
                object_names.append(name)
            elif groups_str and name in (object_groups := container.x(groups_str, {})):
                # a key in the object group dict
                lookup.extend(list(object_groups[name]))
            elif accept_patterns:
                # must eventually be a pattern, perform an object traversal
                found = []
                for _name in sorted(get_all_object_names()):
                    if law.util.multi_match(_name, name):
                        found.append(_name)
                if not found:
                    missing.add(name)
                object_names.extend(found)

        if missing and strict:
            missing_str = ",".join(sorted(missing))
            raise ValueError(f"names/patterns did not yield any matches: {missing_str}")

        return law.util.make_unique(object_names)

    @classmethod
    def resolve_config_default(
        cls,
        *,
        param: Any,
        task_params: dict[str, Any],
        container: str | od.AuxDataMixin | Sequence[od.AuxDataMixin],
        default_str: str | None = None,
        multi_strategy: str = "first",
    ) -> Any | list[Any] | dict[od.AuxDataMixin, Any]:
        """
        Resolves a given parameter value *param*, checks if it should be placed with a default value when empty, and in
        this case, does the actual default value resolution.

        This resolution is triggered only in case *param* refers to :py:attr:`RESOLVE_DEFAULT`, a 1-tuple containing
        this attribute, or *None*. If so, the default is identified via the *default_str* from an
        :py:class:`order.AuxDataMixin` *container* and points to an auxiliary that can be either a string or a function.
        In the latter case, it is called with the task class, the container instance, and all task parameters. Note that
        when no *container* is given, *param* is returned unchanged.

        *container* can also refer to a sequence of :py:class:`order.AuxDataMixin` objects. If this is the case, the
        default resolution is performed for all of them and the resulting values can be handled with five different
        strategies, controlled via *multi_strategy*:

            - ``"first"``: The first resolved value is returned.
            - ``"same"``: The resolved values are forced to be identical and an exception is raised if they differ. The
                first resolved value is returned.
            - ``"union"``: The set union of all resolved values is returned in a list.
            - ``"intersection"``: The set intersection of all resolved values is returned in a list.
            - ``"all"``: The resolved values are returned in a dictionary mapped to their respective container.

        Example:

        .. code-block:: python

            # assuming this is your config
            config_inst = od.Config(
                id=1,
                name="my_config",
                aux={
                    "default_selector": "my_selector",
                },
            )

            # and these are the task parameters
            params = {
                "config_inst": config_inst,
            }

            AnalysisTask.resolve_config_default(
                param=RESOLVE_DEFAULT,
                task_params=params,
                container=config_inst,  # <-- same as passing the "config_inst" key of params
                default_str="default_selector",
            )
            # -> "my_selector"

        Example where the default points to a function:

        .. code-block:: python

            def default_selector(task_cls, config_inst, task_params) -> str:
                # determine the selector based on dynamic conditions
                return "my_other_selector

            config_inst = od.Config(
                id=1,
                name="my_config",
                aux={
                    "default_selector": default_selector,  # <-- function
                },
            )

           AnalysisTask.resolve_config_default(
                param=RESOLVE_DEFAULT,
                task_params=params,
                container=config_inst,
                default_str="default_selector",
            )
            # -> "my_other_selector"
        """
        if multi_strategy not in (strategies := {"first", "same", "union", "intersection", "all"}):
            raise ValueError(f"invalid multi_strategy: {multi_strategy}, must be one of {','.join(strategies)}")

        # check if the parameter value is to be resolved
        resolve_default = param in (None, RESOLVE_DEFAULT, (RESOLVE_DEFAULT,))

        return_single_value = True if param is None or isinstance(param, str) else False

        # interpret missing parameters (e.g. NO_STR) as None
        # (special case: an empty string is usually an active decision, but counts as missing too)
        if law.is_no_param(param) or resolve_default or param == "":
            param = None

        # get the container inst (typically a config_inst or analysis_inst)
        if isinstance(container, str):
            container = task_params.get(container)
            if not container:
                return param

        # actual resolution
        params: dict[od.AuxDataMixin, Any]
        if resolve_default:
            params = {}
            for _container in law.util.make_list(container):
                _param = param
                # expand default when container is set
                if _container and default_str:
                    _param = _container.x(default_str, None)
                    # allow default to be a function, taking task parameters as input
                    if isinstance(_param, Callable):
                        _param = _param(cls, _container, task_params)
                    # handle empty values and return type
                    if not return_single_value:
                        _param = () if _param is None else law.util.make_tuple(_param)
                    elif isinstance(_param, (list, tuple)):
                        _param = _param[0] if _param else None

                params[_container] = _param
        else:
            params = {_container: param for _container in law.util.make_list(container)}

        # handle values
        if not isinstance(container, (list, tuple)):
            return params[container]
        if multi_strategy == "all":
            return params
        if multi_strategy == "first":
            return params[container[0]]
        # NOTE: in there two strategies, we loose all order information
        if multi_strategy == "union":
            return list(set.union(*map(set, params.values())))
        if multi_strategy == "intersection":
            return list(set.intersection(*map(set, params.values())))
        # "same", so check that values are identical
        first = params[container[0]]
        if not all(params[c] == first for c in container[1:]):
            default_str_repr = f" for '{default_str}'" if default_str else ""
            raise ValueError(f"multiple default values found{default_str_repr} in {container}: {params}")
        return first

    @classmethod
    def resolve_config_default_and_groups(
        cls,
        *,
        param: Any,
        task_params: dict[str, Any],
        container: str | od.AuxDataMixin | Sequence[od.AuxDataMixin],
        groups_str: str,
        default_str: str | None = None,
        multi_strategy: str = "first",
        debug=False,
    ) -> Any | list[Any] | dict[od.AuxDataMixin, Any]:
        """
        This method is similar to :py:meth:`~.resolve_config_default` in that it checks if a parameter value *param* is
        empty and should be replaced with a default value. All arguments except for *groups_str* are forwarded to this
        method.

        What this method does in addition is that it checks if the values contained in *param* (after default value
        resolution) refers to a group of values identified via the *groups_str* from the :py:class:`order.AuxDataMixin`
        *container* that maps a string to a tuple of strings. If it does, each value in *param* that refers to a group
        is expanded by the actual group values.

        Example:

        .. code-block:: python

            # assuming this is your config
            config_inst = od.Config(
                id=1,
                name="my_config",
                aux={
                    "default_producer": "my_producers",
                    "producer_groups": {
                        "my_producers": ["producer_1", "producer_2"],
                        "my_other_producers": ["my_producers", "producer_3", "producer_4"],
                    },
                },
            )

            # and these are the task parameters
            params = {
                "config_inst": config_inst,
            }

            AnalysisTask.resolve_config_default_and_groups(
                param=RESOLVE_DEFAULT,
                task_params=params,
                container=config_inst,  # <-- same as passing the "config_inst" key of params
                default_str="default_producer",
                groups_str="producer_groups",
            )
            # -> ["producer_1", "producer_2"]

        Example showing recursive group expansion:

        .. code-block:: python

            # assuming config_inst and params are the same as above

            AnalysisTask.resolve_config_default_and_groups(
                param="my_other_producers",  # <-- points to a group that contains another group
                task_params=params,
                container=config_inst,
                default_str="default_producer",  # <-- not used as param is set explicitly
                groups_str="producer_groups",
            )
            # -> ["producer_1", "producer_2", "producer_3", "producer_4"]
        """
        if multi_strategy not in (strategies := {"first", "same", "union", "intersection", "all"}):
            raise ValueError(f"invalid multi_strategy: {multi_strategy}, must be one of {','.join(strategies)}")

        # get the container
        if isinstance(container, str):
            container = task_params.get(container, None)
        if not container:
            return param
        containers = law.util.make_list(container)

        # resolve the parameter
        params: dict[od.AuxDataMixin, Any] = cls.resolve_config_default(
            param=param,
            task_params=task_params,
            container=containers,
            default_str=default_str,
            multi_strategy="all",
        )
        if not params:
            return param

        # expand groups recursively
        values = {}
        for _container, _param in params.items():
            if not (param_groups := _container.x(groups_str, {})):
                values[_container] = law.util.make_tuple(_param)
                continue
            lookup = collections.deque(law.util.make_list(_param))
            handled_groups = set()
            _values = []
            while lookup:
                value = lookup.popleft()
                if value in param_groups:
                    if value in handled_groups:
                        raise Exception(
                            f"definition of '{groups_str}' contains circular references involving group '{value}'",
                        )
                    lookup.extendleft(law.util.make_list(param_groups[value]))
                    handled_groups.add(value)
                else:
                    _values.append(value)
            values[_container] = tuple(_values)

        # handle values
        if not isinstance(container, (list, tuple)):
            return values[container]
        if multi_strategy == "all":
            return values
        if multi_strategy == "first":
            return values[container[0]]
        if multi_strategy == "union":
            return list(set.union(*map(set, values.values())))
        if multi_strategy == "intersection":
            return list(set.intersection(*map(set, values.values())))
        # "same", so check that values are identical
        first = values[container[0]]
        if not all(values[c] == first for c in container[1:]):
            default_str_repr = f" for '{default_str}'" if default_str else ""
            raise ValueError(
                f"multiple default values found{default_str_repr} after expanding groups '{groups_str}' in "
                f"{containers}: {values}",
            )
        return first

    @classmethod
    def build_repr(
        cls,
        objects: Any | Sequence[Any],
        *,
        sep: str = "__",
        prepend_count: bool = False,
        max_len: int = default_repr_max_len,
        max_count: int = default_repr_max_count,
        hash_len: int = default_repr_hash_len,
    ) -> str:
        """
        Generic method to construct a string representation given a single or a sequece of *objects*.

        :param objects: The object or objects to be represented.
        :param sep: The separator used to join the objects.
        :param prepend_count: When *True*, the number of objects is prepended to the string, followed by *sep*.
        :param max_len: The maximum length of the string. If exceeded, the string is truncated and hashed.
        :param max_count: The maximum number of objects to include in the string. Additional objects are hashed, but
            only if the resulting representation length does not exceed *max_len*. If so, the overall truncation and
            hashing is applied instead.
        :param hash_len: The length of the hash that is appended to the string when it is truncated.
        :return: The string representation.
        """
        if 0 < max_len < hash_len:
            raise ValueError(f"max_len must be greater than hash_len: {max_len} <= {hash_len}")

        # join objects when a sequence is given
        if isinstance(objects, (list, tuple)):
            r = f"{len(objects)}{sep}" if prepend_count else ""
            # truncate when requested and the expected length will not exceed max_len, in which case the overall
            # truncation applies the hashing
            if (
                0 < max_count < len(objects) and
                not (0 < max_len < (len(r) + sum(map(len, objects[:max_count])) + len(sep) * max_count + hash_len))
            ):
                r += sep.join(objects[:max_count])
                r += f"{sep}{law.util.create_hash(objects[max_count:], l=hash_len)}"
            else:
                r += sep.join(objects)
        else:
            r = str(objects)

        # handle overall truncation
        if max_len > 0 and len(r) > max_len:
            r = f"{r[:max_len - hash_len - len(sep)]}{sep}{law.util.create_hash(r, l=hash_len)}"

        return r

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

        # store the analysis instance
        self.analysis_inst = self.get_analysis_inst(self.analysis)

        # cached values added and accessed by cached_value()
        self._cached_values = {}

    def cached_value(self, key: str, func: Callable[[], T]) -> T:
        """
        Upon first invocation, the function *func* is called and its return value is stored under *key* in
        :py:attr:`_cached_values`. Subsequent calls with the same *key* return the cached value.

        :param key: The key under which the value is stored.
        :param func: The function that is called to generate the value.
        :return: The cached value.
        """
        if key not in self._cached_values:
            self._cached_values[key] = func()
        return self._cached_values[key]

    def reset_sandbox(self, sandbox: str) -> None:
        """
        Resets the sandbox to a new *sandbox* value.
        """
        # do nothing if the value actualy does not change
        if self.sandbox == sandbox:
            return

        # change it and rebuild the sandbox inst when already initialized
        self.sandbox = sandbox
        if self._sandbox_initialized:
            self._initialize_sandbox(force=True)

    def store_parts(self) -> law.util.InsertableDict:
        """
        Returns a :py:class:`law.util.InsertableDict` whose values are used to create a store path. For instance, the
        parts ``{"keyA": "a", "keyB": "b", 2: "c"}`` lead to the path "a/b/c". The keys can be used by subclassing tasks
        to overwrite values.

        :return: Dictionary with parts that will be translated into an output directory path.
        """
        parts = law.util.InsertableDict()

        # add the analysis name
        parts["analysis"] = self.analysis_inst.name

        # in this base class, just add the task class name
        parts["task_family"] = self.task_family

        # add the version when set
        if self.version is not None:
            parts["version"] = self.version

        return parts

    def _get_store_parts_modifier(
        self,
        modifier: str | Callable[[AnalysisTask, dict], dict],
    ) -> Callable[[AnalysisTask, dict], dict] | None:
        if isinstance(modifier, str):
            # interpret it as a name of an entry in the store_parts_modifiers aux entry
            modifier = self.analysis_inst.x("store_parts_modifiers", {}).get(modifier)

        return modifier if callable(modifier) else None

    def local_path(
        self,
        *path,
        store_parts_modifier: str | Callable[[AnalysisTask, dict], dict] | None = None,
        **kwargs,
    ) -> str:
        """ local_path(*path, store=None, fs=None, store_parts_modifier=None)
        Joins path fragments from *store* (defaulting to :py:attr:`default_store`), :py:meth:`store_parts` and *path*
        and returns the joined path. In case a *fs* is defined, it should refer to the config section of a local file
        system, and consequently, *store* is not prepended to the returned path as the resolution of absolute paths is
        handled by that file system.
        """
        # if no fs is set, determine the main store directory
        parts = ()
        if not kwargs.pop("fs", None):
            store = kwargs.get("store") or self.default_store
            parts += (store,)

        # get and optional modify the store parts
        store_parts = self.store_parts()
        store_parts_modifier = self._get_store_parts_modifier(store_parts_modifier)
        if callable(store_parts_modifier):
            store_parts = store_parts_modifier(self, store_parts)

        # concatenate all parts that make up the path and join them
        parts += tuple(store_parts.values()) + path
        path = os.path.join(*map(str, parts))

        return path

    def local_target(
        self,
        *path,
        store_parts_modifier: str | Callable[[AnalysisTask, dict], dict] | None = None,
        **kwargs,
    ) -> law.LocalTarget:
        """ local_target(*path, dir=False, store=None, fs=None, store_parts_modifier=None, **kwargs)
        Creates either a local file or directory target, depending on *dir*, forwarding all *path* fragments, *store*
        and *fs* to :py:meth:`local_path` and all *kwargs* the respective target class.
        """
        _dir = kwargs.pop("dir", False)
        store = kwargs.pop("store", None)
        fs = kwargs.get("fs", None)

        # select the target class
        cls = law.LocalDirectoryTarget if _dir else law.LocalFileTarget

        # create the local path
        path = self.local_path(*path, store=store, fs=fs, store_parts_modifier=store_parts_modifier)

        # create the target instance and return it
        return cls(path, **kwargs)

    def wlcg_path(
        self,
        *path,
        store_parts_modifier: str | Callable[[AnalysisTask, dict], dict] | None = None,
    ) -> str:
        """
        Joins path fragments from *store_parts()* and *path* and returns the joined path.

        The full URI to the target is not considered as it is usually defined in ``[wlcg_fs]`` sections in the law
        config and hence subject to :py:func:`wlcg_target`.
        """
        # get and optional modify the store parts
        store_parts = self.store_parts()
        store_parts_modifier = self._get_store_parts_modifier(store_parts_modifier)
        if callable(store_parts_modifier):
            store_parts = store_parts_modifier(self, store_parts)

        # concatenate all parts that make up the path and join them
        parts = tuple(store_parts.values()) + path
        path = os.path.join(*map(str, parts))

        return path

    def wlcg_target(
        self,
        *path,
        store_parts_modifier: str | Callable[[AnalysisTask, dict], dict] | None = None,
        **kwargs,
    ) -> law.wclg.WLCGTarget:
        """ wlcg_target(*path, dir=False, fs=default_wlcg_fs, store_parts_modifier=None, **kwargs)
        Creates either a remote WLCG file or directory target, depending on *dir*, forwarding all *path* fragments to
        :py:meth:`wlcg_path` and all *kwargs* the respective target class. When *None*, *fs* defaults to the
        *default_wlcg_fs* class level attribute.
        """
        _dir = kwargs.pop("dir", False)
        if not kwargs.get("fs"):
            kwargs["fs"] = self.default_wlcg_fs

        # select the target class
        cls = law.wlcg.WLCGDirectoryTarget if _dir else law.wlcg.WLCGFileTarget

        # create the local path
        path = self.wlcg_path(*path, store_parts_modifier=store_parts_modifier)

        # create the target instance and return it
        return cls(path, **kwargs)

    def target(self, *path, **kwargs) -> law.LocalTarget | law.wlcg.WLCGTarget | law.MirroredTarget:
        """ target(*path, location=None, **kwargs)
        """
        # get the default location
        location = kwargs.pop("location", self.default_output_location)

        # parse it and obtain config values if necessary
        if isinstance(location, str):
            location = OutputLocation[location]
        if location == OutputLocation.config:
            lookup_keys = self.get_config_lookup_keys(self)
            outputs_dict = self._get_cfg_outputs_dict()
            location = copy.deepcopy(self._dfs_key_lookup(lookup_keys, outputs_dict))
            if not location:
                self.logger.debug(
                    f"no option 'outputs::{self.task_family}' found in law.cfg to obtain target "
                    "location, falling back to 'local'",
                )
                location = ["local"]
            location[0] = OutputLocation[location[0]]
        location = law.util.make_list(location)

        # forward to correct function
        if location[0] == OutputLocation.local:
            # get other options
            loc, store_parts_modifier = (location[1:] + [None, None])[:2]
            loc_key = "fs" if (loc and law.config.has_section(loc)) else "store"
            kwargs.setdefault(loc_key, loc)
            kwargs.setdefault("store_parts_modifier", store_parts_modifier)
            return self.local_target(*path, **kwargs)

        if location[0] == OutputLocation.wlcg:
            # get other options
            fs, store_parts_modifier = (location[1:] + [None, None])[:2]
            kwargs.setdefault("fs", fs)
            kwargs.setdefault("store_parts_modifier", store_parts_modifier)
            return self.wlcg_target(*path, **kwargs)

        if location[0] == OutputLocation.wlcg_mirrored:
            # get other options
            loc, wlcg_fs, store_parts_modifier = (location[1:] + [None, None, None])[:3]
            kwargs.setdefault("store_parts_modifier", store_parts_modifier)
            # create the wlcg target
            wlcg_kwargs = kwargs.copy()
            wlcg_kwargs.setdefault("fs", wlcg_fs)
            wlcg_target = self.wlcg_target(*path, **wlcg_kwargs)
            # TODO: add rule for falling back to wlcg target?
            # create the local target
            local_kwargs = kwargs.copy()
            loc_key = "fs" if (loc and law.config.has_section(loc)) else "store"
            local_kwargs.setdefault(loc_key, loc)
            local_target = self.local_target(*path, **local_kwargs)
            # build the mirrored target from these two
            mirrored_target_cls = (
                law.MirroredFileTarget
                if isinstance(local_target, law.LocalFileTarget)
                else law.MirroredDirectoryTarget
            )
            # whether to wait for local synchrnoization (for debugging purposes)
            local_sync = law.util.flag_to_bool(os.getenv("CF_MIRRORED_TARGET_LOCAL_SYNC", "true"))
            # create and return the target
            return mirrored_target_cls(
                path=local_target.abspath,
                remote_target=wlcg_target,
                local_target=local_target,
                local_sync=local_sync,
            )

        raise Exception(f"cannot determine output location based on '{location}'")

    def get_parquet_writer_opts(self, repeating_values: bool = False) -> dict[str, Any]:
        """
        Returns an option dictionary that can be passed as *writer_opts* to :py:meth:`~law.pyarrow.merge_parquet_task`,
        for instance, at the end of chunked processing steps that produce a single parquet file. See
        :py:class:`~pyarrow.parquet.ParquetWriter` for valid options.

        This method can be overwritten in subclasses to customize the exact behavior.

        :param repeating_values: Whether the values to be written have predominantly repeating values, in which case
            differnt compression and encoding strategies are followed.
        :return: A dictionary with options that can be passed to parquet writer objects.
        """
        # use dict encoding if values are repeating
        dict_encoding = bool(repeating_values)

        # build and return options
        return {
            "compression": "ZSTD",
            "compression_level": 1,
            "use_dictionary": dict_encoding,
            # ensure that after merging, the resulting parquet structure is the same as that of the
            # input files, e.g. do not switch from "*.list.item.*" to "*.list.element*." structures,
            # see https://github.com/scikit-hep/awkward/issues/3331 and
            # https://github.com/apache/arrow/issues/31731
            "use_compliant_nested_type": False,
        }


class ConfigTask(AnalysisTask):

    config = luigi.Parameter(
        default=default_config,
        description=f"name of the analysis config to use; default: '{default_config}'",
    )
    configs = law.CSVParameter(
        default=(default_config,),
        description=f"comma-separated names of analysis configs to use; default: '{default_config}'",
        brace_expand=True,
    )
    known_shifts = luigi.Parameter(
        default=None,
        visibility=luigi.parameter.ParameterVisibility.PRIVATE,
    )

    exclude_params_req = {"known_shifts"}
    exclude_params_sandbox = {"known_shifts"}
    exclude_params_remote_workflow = {"known_shifts"}
    exclude_params_index = {"known_shifts"}
    exclude_params_repr = {"known_shifts"}

    # the field in the store parts behind which the new part is inserted
    # added here for subclasses that typically refer to the store part added by _this_ class
    config_store_anchor = "config"

    @classmethod
    def modify_task_attributes(cls) -> None:
        """
        Hook that is called by law's task register meta class right after subclass creation to update class-level
        attributes.
        """
        super().modify_task_attributes()

        # single/multi config adjustments in case the switch has been specified
        if isinstance(cls.single_config, bool):
            remove_attr = "configs" if cls.has_single_config() else "config"
            if getattr(cls, remove_attr, law.no_value) != law.no_value:
                setattr(cls, remove_attr, None)

    @abc.abstractproperty
    def single_config(cls) -> bool:
        # flag that should be set to True or False by classes that should be instantiated
        # (this is wrapped into an abstract instance property as a safe-guard against instantiation of a misconfigured
        # subclass, but when actually specified, this is to be realized as a boolean class attribute or property)
        ...

    @classmethod
    def has_single_config(cls) -> bool:
        """
        Returns whether the class is configured to use a single config.

        :raises AttributeError: When the class does not specify the *single_config* attribute.
        :return: *True* if the class uses a single config, *False* otherwise.
        """
        single_config = cls.single_config
        if not isinstance(single_config, bool):
            raise AttributeError(f"unspecified 'single_config' attribute in {cls}: {single_config}")
        return single_config

    @classmethod
    def ensure_single_config(cls, value: bool, *, attr: str | None = None) -> None:
        """
        Ensures that the :py:attr:`single_config` flag of this task is set to *value* by raising an exception if it is
        not. This method is typically used to guard the access to attributes. If so, *attr* is used in the exception
        message to reflect this.

        :param value: The value to compare the flag with.
        :param attr: The attribute that triggered the check.
        """
        single_config = cls.has_single_config()
        if single_config != value:
            if attr:
                s = "multiple configs" if single_config else "a single config"
                msg = f"cannot access attribute '{attr}' when task '{cls}' has {s}"
            else:
                s = "multiple configs" if value else "a single config"
                msg = f"task '{cls}' expected to use {s}"
            raise Exception(msg)

    @classmethod
    def config_mode(cls) -> str:
        """
        Returns a string representation of this task's config mode.

        :return: "single" if the task has a single config, "multi" otherwise.
        """
        return "single" if cls.has_single_config() else "multi"

    @classmethod
    def _get_config_container(cls, params: dict[str, Any]) -> od.Config | list[od.Config] | None:
        """
        Extracts the single or multiple config instances from task parameters *params*, or *None* if neither is found.

        :param params: Dictionary of task parameters.
        :return: The config instance(s) or *None*.
        """
        if cls.has_single_config():
            if (config_inst := params.get("config_inst")):
                return config_inst
        elif (config_insts := params.get("config_insts")):
            return config_insts
        return None

    @classmethod
    def resolve_param_values(cls, params: dict[str, Any]) -> dict[str, Any]:
        params = super().resolve_param_values(params)

        if (analysis_inst := params.get("analysis_inst")):
            # store a reference to the config inst(s)
            if cls.has_single_config():
                if "config_inst" not in params and "config" in params:
                    params["config_inst"] = analysis_inst.get_config(params["config"])
                    params["config_insts"] = [params["config_inst"]]
            else:
                if "config_insts" not in params and "configs" in params:
                    params["config_insts"] = list(map(analysis_inst.get_config, params["configs"]))

        # resolving of parameters that is required before ArrayFunctions etc. can be initialized
        params = cls.resolve_param_values_pre_init(params)

        # check if shifts are already known
        if params.get("known_shifts", None) is None:
            logger_dev.debug(f"{cls.task_family}: shifts unknown")

            # initialize ArrayFunctions etc. and collect known shifts
            shifts = params["known_shifts"] = TaskShifts()
            params = cls.resolve_instances(params, shifts)
            params["known_shifts"] = shifts

        # resolving of parameters that can only be performed after ArrayFunction initialization
        params = cls.resolve_param_values_post_init(params)

        # resolving of shifts
        params = cls.resolve_shifts(params)

        return params

    @classmethod
    def resolve_instances(cls, params: dict[str, Any], shifts: TaskShifts) -> dict[str, Any]:
        """
        Build the array function instances.
        For single-config/dataset tasks, resolve_instances is implemented by mixin classes such as the ProducersMixin.
        For multi-config tasks, resolve_instances from the upstream task is called for each config instance. If the
        resolve_instances function needs to be called for other combinations of parameters (e.g. per dataset), it can be
        overwritten by the task class.

        :param params: Dictionary of task parameters.
        :param shifts: Collection of local and global shifts.
        :return: Updated dictionary of task parameters.
        """
        cls.get_known_shifts(params, shifts)

        if not cls.resolution_task_cls:
            params["known_shifts"] = shifts
            return params

        logger_dev.debug(
            f"{cls.task_family}: uses ConfigTask.resolve_instances base implementation; "
            f"upsteam_task_cls was defined as {cls.resolution_task_cls}; ",
        )
        # base implementation for ConfigTasks that do not define any datasets.
        # Needed for e.g. MergeShiftedHistograms
        if cls.has_single_config():
            _params = params.copy()
            _params = cls.resolution_task_cls.resolve_instances(params, shifts)
            cls.resolution_task_cls.get_known_shifts(_params, shifts)
        else:
            for config_inst in params["config_insts"]:
                _params = {
                    **params,
                    "config_inst": config_inst,
                    "config": config_inst.name,
                }
                _params = cls.resolution_task_cls.resolve_instances(_params, shifts)
                cls.resolution_task_cls.get_known_shifts(_params, shifts)

        params["known_shifts"] = shifts

        return params

    @classmethod
    def resolve_param_values_pre_init(cls, params: dict[str, Any]) -> dict[str, Any]:
        """
        Resolve parameters before the array function instances have been initialized.

        :param params: Dictionary of task parameters.
        :return: Updated dictionary of task parameters.
        """
        return params

    @classmethod
    def resolve_param_values_post_init(cls, params: dict[str, Any]) -> dict[str, Any]:
        """
        Resolve parameters after the array function instances have been initialized.

        :param params: Dictionary of task parameters.
        :return: Updated dictionary of task parameters.
        """
        return params

    @classmethod
    def resolve_shifts(cls, params: dict[str, Any]) -> dict[str, Any]:
        """
        Resolve shifts

        :param params: Dictionary of task parameters.
        :return: Updated dictionary of task parameters.
        """
        # called within modify_param_values to resolve shifts after all other parameters have been resolved
        return params

    @classmethod
    def get_known_shifts(
        cls,
        params: dict[str, Any],
        shifts: TaskShifts,
    ) -> None:
        """
        Adjusts the local and upstream fields of the *shifts* object to include shifts implemented
        by _this_ task, and dependent shifts that are implemented by upstream tasks.

        :param params: Dictionary of task parameters.
        :param shifts: TaskShifts object to adjust.
        """
        return params

    resolution_task_cls = None

    @classmethod
    def req_params(cls, inst: law.Task, *args, **kwargs) -> dict[str, Any]:
        params = super().req_params(inst, *args, **kwargs)

        # manually add known shifts between workflows and branches
        if isinstance(inst, law.BaseWorkflow) and inst.__class__ == cls and getattr(inst, "known_shifts", None):
            params["known_shifts"] = inst.known_shifts

        return params

    @classmethod
    def _multi_sequence_repr(
        cls,
        values: Sequence[str] | Sequence[Sequence[str]],
        sort: bool = False,
    ) -> str:
        """
        Returns a string representation of a singly (for single config) or doubly (for multi config) nested sequence of
        string *values*. In the former case, the values are sorted if *sort* is *True* and formed into a representation.
        The behavior of the latter case depends on whether values are identical between configs. If they are, handle
        them as a single sequence. Otherwise, the representation consists of the number of values per config and a hash
        of the combined, flat values.

        :param values: Nested values.
        :param sort: Whether to sort the values.
        :return: A string representation.
        """
        # empty case
        if not values:
            return "none"

        # optional sorting helper
        maybe_sort = (lambda vals: sorted(vals)) if sort else (lambda vals: vals)

        # helper to perform the single representation, assuming already sorted values
        def single_repr(values: Sequence[str]) -> str:
            if not values:
                return None
            if len(values) == 1:
                return values[0]
            return f"{len(values)}_{law.util.create_hash(values)}"

        # single case
        if not isinstance(values[0], (list, tuple)):
            return single_repr(maybe_sort(values))
        # multi case with a single sequence
        if len(values) == 1:
            return single_repr(maybe_sort(values[0]))
        # multi case with identical sequences
        values = [maybe_sort(_values) for _values in values]
        if all(_values == values[0] for _values in values[1:]):
            return single_repr(values[0])
        # build full representation
        _repr = "_".join(map(str, map(len, values)))
        all_values = sum(values, [])
        return _repr + f"_{law.util.create_hash(all_values)}"

    @classmethod
    def broadcast_to_configs(cls, value: Any, name: str, n_config_insts: int) -> tuple[Any]:
        if not isinstance(value, tuple) or not value:
            value = (value,)
        if len(value) == 1:
            value *= n_config_insts
        elif len(value) != n_config_insts:
            raise ValueError(
                f"number of {name} sequences ({len(value)}) does not match number of configs "
                f"({n_config_insts})",
            )
        return value

    @classmethod
    def _get_default_version(
        cls,
        inst: AnalysisTask,
        params: dict[str, Any],
        keys: law.util.InsertableDict,
    ) -> str | None:
        # try to lookup the version in the config's auxiliary data
        if isinstance(inst, ConfigTask) and inst.has_single_config():
            version = cls._dfs_key_lookup(keys, inst.config_inst.x("versions", {}))
            if version:
                return version

        return super()._get_default_version(inst, params, keys)

    @classmethod
    def get_config_lookup_keys(
        cls,
        inst_or_params: ConfigTask | dict[str, Any],
    ) -> law.util.InsertiableDict:
        keys = super().get_config_lookup_keys(inst_or_params)

        # add the config name in front of the task family
        config = (
            inst_or_params.get("config")
            if isinstance(inst_or_params, dict)
            else getattr(inst_or_params, "config", None)
        )
        if config not in {law.NO_STR, None, ""}:
            prefix = "cfg"
            keys.insert_before("task", prefix, f"{prefix}_{config}")

        return keys

    @classmethod
    def get_array_function_dict(cls, params: dict[str, Any]) -> dict[str, Any]:
        cls.ensure_single_config(True, attr="get_array_function_dict")

        kwargs = super().get_array_function_dict(params)

        if "config_inst" in params:
            kwargs["config_inst"] = params["config_inst"]
        elif "config" in params and "analysis_inst" in kwargs:
            kwargs["config_inst"] = kwargs["analysis_inst"].get_config(params["config"])

        return kwargs

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

        # store a reference to the config instances
        self.config_insts = [
            self.analysis_inst.get_config(config)
            for config in ([self.config] if self.has_single_config() else self.configs)
        ]
        if self.has_single_config():
            self.config_inst = self.config_insts[0]

    @property
    def config_repr(self) -> str:
        return "__".join(config_inst.name for config_inst in sorted(self.config_insts, key=lambda c: c.id))

    def store_parts(self) -> law.util.InsertableDict:
        parts = super().store_parts()

        # add the config name
        parts.insert_after("task_family", "config", self.config_repr)

        return parts

    def find_keep_columns(self, collection: ColumnCollection) -> set[Route]:
        """
        Returns a set of :py:class:`Route` objects describing columns that should be kept given a
        type of column *collection*.

        :param collection: The collection to return.
        :return: A set of :py:class:`Route` objects.
        """
        columns = set()

        if collection == ColumnCollection.MANDATORY_COFFEA:
            columns |= set(Route(c) for c in mandatory_coffea_columns)

        return columns

    def _expand_keep_column(
        self,
        column:
            ColumnCollection | Route | str |
            Sequence[str | int | slice | type(Ellipsis) | list | tuple],
    ) -> set[Route]:
        """
        Expands a *column* into a set of :py:class:`Route` objects. *column* can be a :py:class:`ColumnCollection`, a
        string, or any type that is accepted by :py:class:`Route`. Collections are expanded through
        :py:meth:`find_keep_columns`.

        :param column: The column to expand.
        :return: A set of :py:class:`Route` objects.
        """
        # expand collections
        if isinstance(column, ColumnCollection):
            return self.find_keep_columns(column)

        # brace expand strings
        if isinstance(column, str):
            return set(map(Route, law.util.brace_expand(column)))

        # let Route handle it
        return {Route(column)}


class ShiftTask(ConfigTask):

    shift = luigi.Parameter(
        default="nominal",
        description="name of a systematic shift to apply; must fulfill order.Shift naming rules; "
        "default: 'nominal'",
    )
    local_shift = luigi.Parameter(default=law.NO_STR)

    # skip passing local_shift to cli completion, req params and sandboxing
    exclude_params_index = {"local_shift"}
    exclude_params_req = {"local_shift"}
    exclude_params_sandbox = {"local_shift"}
    exclude_params_remote_workflow = {"local_shift"}

    allow_empty_shift = False

    @classmethod
    def resolve_shifts(cls, params: dict[str, Any]) -> dict[str, Any]:
        params = super().resolve_shifts(params)

        if "known_shifts" not in params:
            raise Exception(f"{cls.task_family}: known shifts should be resolved before calling 'resolve_shifts'")
        known_shifts = params["known_shifts"]

        # get configs
        config_insts = params.get("config_insts")

        # require that the shift is set and known
        if (requested_shift := params.get("shift")) in (None, law.NO_STR):
            if not cls.allow_empty_shift:
                raise Exception(f"no shift found in params: {params}")
            global_shift = local_shift = law.NO_STR
        else:
            # check if the shift is known to one of the configs
            shift_defined_in_config = False
            for config_inst in config_insts:
                if requested_shift not in config_inst.shifts:
                    logger_dev.debug(f"shift {requested_shift} unknown to config {config_inst}")
                else:
                    shift_defined_in_config = True
            if not shift_defined_in_config:
                raise ValueError(f"shift {requested_shift} unknown to all configs")

            # actual shift resolution: compare the requested shift to known ones
            # local_shift -> the requested shift if implemented by the task itself, else nominal
            # shift       -> the requested shift if implemented by this task
            #                or an upsteam task (== global shift), else nominal
            global_shift = requested_shift
            if (local_shift := params.get("local_shift")) in {None, law.NO_STR}:
                # check cases
                if requested_shift in known_shifts.local:
                    local_shift = requested_shift
                elif requested_shift in known_shifts.upstream:
                    local_shift = "nominal"
                else:
                    global_shift = "nominal"
                    local_shift = "nominal"

        # store parameters
        params["shift"] = global_shift
        params["local_shift"] = local_shift

        # store references to shift instances
        if (
            params["shift"] != law.NO_STR and
            params["local_shift"] != law.NO_STR and
            (not params.get("global_shift_insts") or not params.get("local_shift_insts"))
        ):
            params["global_shift_insts"] = {}
            params["local_shift_insts"] = {}

            get_shift_or_nominal = lambda config, shift: config.get_shift(shift, default=config.get_shift("nominal"))

            for config_inst in config_insts:
                params["global_shift_insts"][config_inst] = get_shift_or_nominal(config_inst, params["shift"])
                params["local_shift_insts"][config_inst] = get_shift_or_nominal(config_inst, params["local_shift"])

            if cls.has_single_config():
                config_inst = params["config_inst"]
                params["global_shift_inst"] = params["global_shift_insts"][config_inst]
                params["local_shift_inst"] = params["local_shift_insts"][config_inst]

        return params

    @classmethod
    def get_config_lookup_keys(
        cls,
        inst_or_params: ShiftTask | dict[str, Any],
    ) -> law.util.InsertiableDict:
        keys = super().get_config_lookup_keys(inst_or_params)

        # add the (global) shift name
        shift = (
            inst_or_params.get("shift")
            if isinstance(inst_or_params, dict)
            else getattr(inst_or_params, "shift", None)
        )
        if shift not in (law.NO_STR, None, ""):
            prefix = "shift"
            keys[prefix] = f"{prefix}_{shift}"

        return keys

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

        # store references to the shift instances
        self.global_shift_insts = None
        self.local_shift_insts = None
        if self.shift not in (None, law.NO_STR) and self.local_shift not in (None, law.NO_STR):
            get = lambda c, s: c.get_shift(s if s in c.shifts else "nominal")
            self.global_shift_insts = {
                config_inst: get(config_inst, self.shift)
                for config_inst in self.config_insts
            }
            self.local_shift_insts = {
                config_inst: get(config_inst, self.local_shift)
                for config_inst in self.config_insts
            }
        if self.has_single_config():
            self.global_shift_inst = None
            self.local_shift_inst = None
            if self.global_shift_insts:
                self.global_shift_inst = self.global_shift_insts[self.config_inst]
                self.local_shift_inst = self.local_shift_insts[self.config_inst]

    def store_parts(self) -> law.util.InsertableDict:
        parts = super().store_parts()

        # add the shift name
        if self.global_shift_insts:
            parts.insert_after(self.config_store_anchor, "shift", self.shift)

        return parts


class DatasetTask(ShiftTask):

    # all dataset tasks are meant to work for a single config
    single_config = True

    dataset = luigi.Parameter(
        default=default_dataset,
        description=f"name of the dataset to process; default: '{default_dataset}'",
    )

    file_merging = None

    @classmethod
    def resolve_param_values_pre_init(cls, params: dict[str, Any]) -> dict[str, Any]:
        params = super().resolve_param_values_pre_init(params)

        # store a reference to the dataset inst
        if "dataset_inst" not in params and "config_inst" in params and "dataset" in params:
            params["dataset_inst"] = params["config_inst"].get_dataset(params["dataset"])

        return params

    @classmethod
    def resolve_param_values(cls, params: dict[str, Any]) -> dict[str, Any]:
        params = super().resolve_param_values(params)

        # also add a reference to the info instance when a global shift is defined
        if "dataset_inst" in params and "global_shift_inst" in params:
            shift_name = params["global_shift_inst"].name
            params["dataset_info_inst"] = (
                params["dataset_inst"].get_info(shift_name)
                if shift_name in params["dataset_inst"].info
                else params["dataset_inst"].get_info("nominal")
            )

        return params

    @classmethod
    def get_known_shifts(
        cls,
        params: dict[str, Any],
        shifts: TaskShifts,
    ) -> None:
        # dataset can have shifts, that are considered as upstream shifts
        super().get_known_shifts(params, shifts)

        if (dataset_inst := params.get("dataset_inst")):
            if dataset_inst.is_data:
                # clear all shifts for data
                shifts.local.clear()
                shifts.upstream.clear()
            else:
                # extend with dataset variations for mc
                shifts.upstream |= set(dataset_inst.info.keys())

    @classmethod
    def get_config_lookup_keys(
        cls,
        inst_or_params: DatasetTask | dict[str, Any],
    ) -> law.util.InsertiableDict:
        keys = super().get_config_lookup_keys(inst_or_params)

        # add the dataset name before the shift name
        dataset = (
            inst_or_params.get("dataset")
            if isinstance(inst_or_params, dict)
            else getattr(inst_or_params, "dataset", None)
        )
        if dataset not in {law.NO_STR, None, ""}:
            prefix = "dataset"
            keys.insert_before("shift", prefix, f"{prefix}_{dataset}")

        return keys

    @classmethod
    def get_array_function_dict(cls, params: dict[str, Any]) -> dict[str, Any]:
        kwargs = super().get_array_function_dict(params)

        if "dataset_inst" in params:
            kwargs["dataset_inst"] = params["dataset_inst"]
        elif "dataset" in params and "config_inst" in kwargs:
            kwargs["dataset_inst"] = kwargs["config_inst"].get_dataset(params["dataset"])

        return kwargs

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

        # store references to the dataset instance
        self.dataset_inst = self.config_inst.get_dataset(self.dataset)

        # store dataset info for the global shift
        key = (
            self.global_shift_inst.name
            if self.global_shift_inst and self.global_shift_inst.name in self.dataset_inst.info
            else "nominal"
        )
        self.dataset_info_inst = self.dataset_inst.get_info(key)

    def store_parts(self) -> law.util.InsertableDict:
        parts = super().store_parts()

        # insert the dataset
        parts.insert_after(self.config_store_anchor, "dataset", self.dataset_inst.name)

        return parts

    @property
    def file_merging_factor(self) -> int:
        """
        Returns the number of files that are handled in one branch. When the :py:attr:`file_merging`
        attribute is set to a positive integer, this value is returned. Otherwise, if the value is
        zero, the original number of files is used instead.

        Consecutive merging steps are not handled yet.
        """
        n_files = self.dataset_info_inst.n_files

        if isinstance(self.file_merging, int):
            # interpret the file_merging attribute as the merging factor itself
            # zero means "merge all in one"
            if self.file_merging < 0:
                raise ValueError(f"invalid file_merging value {self.file_merging}")
            n_merge = n_files if self.file_merging == 0 else self.file_merging
        else:
            # no merging at all
            n_merge = 1

        return n_merge

    def create_branch_map(self):
        """
        Define the branch map for when this task is used as a workflow. By default, use the merging
        information provided by :py:attr:`file_merging_factor` to return a dictionary which maps
        branches to one or more input file indices. E.g. `1 -> [3, 4, 5]` would mean that branch 1
        is simultaneously handling input file indices 3, 4 and 5.
        """
        n_merge = self.file_merging_factor
        n_files = self.dataset_info_inst.n_files

        # use iter_chunks which splits a list of length n_files into chunks of maximum size n_merge
        chunks = law.util.iter_chunks(n_files, n_merge)

        # use enumerate for simply indexing
        return dict(enumerate(chunks))


class CommandTask(AnalysisTask):
    """
    A task that provides convenience methods to work with shell commands, i.e., printing them on the
    command line and executing them with error handling.
    """

    print_command = law.CSVParameter(
        default=(),
        significant=False,
        description="print the command that this task would execute but do not run any task; this "
        "CSV parameter accepts a single integer value which sets the task recursion depth to also "
        "print the commands of required tasks (0 means non-recursive)",
    )
    custom_args = luigi.Parameter(
        default="",
        description="custom arguments that are forwarded to the underlying command; they might not "
        "be encoded into output file paths; empty default",
    )

    exclude_index = True
    exclude_params_req = {"custom_args"}
    interactive_params = AnalysisTask.interactive_params + ["print_command"]

    run_command_in_tmp = False

    def _print_command(self, args) -> None:
        max_depth = int(args[0])

        print(f"print task commands with max_depth {max_depth}")

        for dep, _, depth in self.walk_deps(max_depth=max_depth, order="pre"):
            offset = depth * ("|" + law.task.interactive.ind)
            print(offset)

            print("{}> {}".format(offset, dep.repr(color=True)))
            offset += "|" + law.task.interactive.ind

            if isinstance(dep, CommandTask):
                # when dep is a workflow, take the first branch
                text = law.util.colored("command", style="bright")
                if isinstance(dep, law.BaseWorkflow) and dep.is_workflow():
                    dep = dep.as_branch(0)
                    text += " (from branch {})".format(law.util.colored("0", "red"))
                text += ": "

                cmd = dep.build_command()
                if cmd:
                    cmd = law.util.quote_cmd(cmd) if isinstance(cmd, (list, tuple)) else cmd
                    text += law.util.colored(cmd, "cyan")
                else:
                    text += law.util.colored("empty", "red")
                print(offset + text)
            else:
                print(offset + law.util.colored("not a CommandTask", "yellow"))

    def build_command(self) -> str | list[str]:
        # this method should build and return the command to run
        raise NotImplementedError

    def touch_output_dirs(self) -> None:
        # keep track of created uris so we can avoid creating them twice
        handled_parent_uris = set()

        for outp in law.util.flatten(self.output()):
            # get the parent directory target
            parent = None
            if isinstance(outp, law.SiblingFileCollection):
                parent = outp.dir
            elif isinstance(outp, law.FileSystemFileTarget):
                parent = outp.parent

            # create it
            if parent and parent.uri() not in handled_parent_uris:
                parent.touch()
                handled_parent_uris.add(parent.uri())

    def run_command(self, cmd: str | list[str], optional: bool = False, **kwargs) -> subprocess.Popen:
        # proper command encoding
        cmd = (law.util.quote_cmd(cmd) if isinstance(cmd, (list, tuple)) else cmd).strip()

        # when no cwd was set and run_command_in_tmp is True, create a tmp dir
        if "cwd" not in kwargs and self.run_command_in_tmp:
            tmp_dir = law.LocalDirectoryTarget(is_tmp=True)
            tmp_dir.touch()
            kwargs["cwd"] = tmp_dir.abspath
        self.publish_message("cwd: {}".format(kwargs.get("cwd", os.getcwd())))

        # call it
        with self.publish_step("running '{}' ...".format(law.util.colored(cmd, "cyan"))):
            p, lines = law.util.readable_popen(cmd, shell=True, executable="/bin/bash", **kwargs)
            for line in lines:
                print(line)

        # raise an exception when the call failed and optional is not True
        if p.returncode != 0 and not optional:
            raise Exception(f"command failed with exit code {p.returncode}: {cmd}")

        return p

    @law.decorator.log
    @law.decorator.notify
    @law.decorator.safe_output
    def run(self, **kwargs):
        self.pre_run_command()

        # default run implementation
        # first, create all output directories
        self.touch_output_dirs()

        # build the command
        cmd = self.build_command()

        # run it
        self.run_command(cmd, **kwargs)

        self.post_run_command()

    def pre_run_command(self) -> None:
        return

    def post_run_command(self) -> None:
        return


def wrapper_factory(
    base_cls: law.task.base.Task,
    require_cls: AnalysisTask,
    enable: Sequence[str],
    cls_name: str | None = None,
    attributes: dict | None = None,
    docs: str | None = None,
) -> law.task.base.Register:
    """
    Factory function creating wrapper task classes, inheriting from *base_cls* and
    :py:class:`~law.task.base.WrapperTask`, that do nothing but require multiple instances of *require_cls*. Unless
    *cls_name* is defined, the name of the created class defaults to the name of *require_cls* plus "Wrapper".
    Additional *attributes* are added as class-level members when given.

    The instances of *require_cls* to be required in the :py:meth:`~.wrapper_factory.Wrapper.requires()` method can be
    controlled by task parameters. These parameters can be enabled through the string sequence *enable*, which currently
    accepts:

        - ``configs``, ``skip_configs``
        - ``shifts``, ``skip_shifts``
        - ``datasets``, ``skip_datasets``

    This allows to easily build wrapper tasks that loop over (combinations of) parameters that are either defined in the
    analysis or config, which would otherwise lead to mostly redundant code.

    Example:

    .. code-block:: python

        class MyTask(DatasetTask):
            ...

        MyTaskWrapper = wrapper_factory(
            base_cls=ConfigTask,
            require_cls=MyTask,
            enable=["datasets", "skip_datasets"],
        )

        # this allows to run (e.g.)
        # law run MyTaskWrapper --datasets st_* --skip-datasets *_tbar

    When building the requirements, the full combinatorics of parameters is considered. However, certain conditions
    apply depending on enabled features. For instance, in order to use the "configs" feature (adding a parameter
    "--configs" to the created class, allowing to loop over a list of config instances known to an analysis),
    *require_cls* must be at least a :py:class:`ConfigTask` accepting "--config" (mind the singular form), whereas
    *base_cls* must explicitly not.

    :param base_cls: Base class for this wrapper
    :param require_cls: :py:class:`~law.task.base.Task` class to be wrapped
    :param enable: Enable these parameters to control the wrapped :py:class:`~law.task.base.Task` class instance.
        Currently allowed parameters are: "configs", "skip_configs", "shifts", "skip_shifts", "datasets",
        "skip_datasets"
    :param cls_name: Name of the wrapper instance. If :py:attr:`None`, defaults to the name of the
        :py:class:`~law.task.base.WrapperTask` class + `"Wrapper"`
    :param attributes: Add these attributes as class-level members of the new :py:class:`~law.task.base.WrapperTask`
        class
    :param docs: Manually set the documentation string `__doc__` of the new :py:class:`~law.task.base.WrapperTask` class
        instance
    :raises ValueError: If a parameter provided with `enable` is not in the list of known parameters
    :raises TypeError: If any parameter in `enable` is incompatible with the :py:class:`~law.task.base.WrapperTask`
        class instance or the inheritance structure of corresponding classes
    :raises ValueError: when `configs` are enabled but not found in the analysis config instance
    :raises ValueError: when `shifts` are enabled but not found in the analysis config instance
    :raises ValueError: when `datasets` are enabled but not found in the analysis config instance
    :return: The new :py:class:`~law.task.base.WrapperTask` for the :py:class:`~law.task.base.Task` class `required_cls`
    """
    # check known features
    known_features = [
        "configs", "skip_configs",
        "shifts", "skip_shifts",
        "datasets", "skip_datasets",
    ]
    for feature in enable:
        if feature not in known_features:
            raise ValueError(
                f"unknown enabled feature '{feature}', known features are "
                f"'{','.join(known_features)}'",
            )

    # treat base_cls as a tuple
    base_classes = law.util.make_tuple(base_cls)
    base_cls = base_classes[0]

    # define wrapper feature flags
    has_configs = "configs" in enable
    has_skip_configs = has_configs and "skip_configs" in enable
    has_shifts = "shifts" in enable
    has_skip_shifts = has_shifts and "skip_shifts" in enable
    has_datasets = "datasets" in enable
    has_skip_datasets = has_datasets and "skip_datasets" in enable

    # helper to check if enabled features are compatible with required and base class
    def check_class_compatibility(name, min_require_cls, max_base_cls):
        if not issubclass(require_cls, min_require_cls):
            raise TypeError(
                f"when the '{name}' feature is enabled, require_cls must inherit from {min_require_cls}, but "
                f"{require_cls} does not",
            )
        if issubclass(base_cls, min_require_cls):
            raise TypeError(
                f"when the '{name}' feature is enabled, base_cls must not inherit from {min_require_cls}, but "
                f"{base_cls} does",
            )
        if not issubclass(max_base_cls, base_cls):
            raise TypeError(
                f"when the '{name}' feature is enabled, base_cls must be a super class of {max_base_cls}, but "
                f"{base_cls} is not",
            )

    # check classes
    if has_configs:
        check_class_compatibility("configs", ConfigTask, AnalysisTask)
    if has_shifts:
        check_class_compatibility("shifts", ShiftTask, ConfigTask)
    if has_datasets:
        check_class_compatibility("datasets", DatasetTask, ShiftTask)

    # create the class
    class Wrapper(*base_classes, law.WrapperTask):

        exclude_params_repr_empty = set()
        exclude_params_req_set = set()

        if has_configs:
            configs = law.CSVParameter(
                default=(default_config,),
                description="names or name patterns of configs to use; can also be the key of a mapping defined in the "
                f"'config_groups' auxiliary data of the analysis; default: {default_config}",
                brace_expand=True,
            )
            exclude_params_req_set.add("configs")
        if has_skip_configs:
            skip_configs = law.CSVParameter(
                default=(),
                description="names or name patterns of configs to skip after evaluating --configs; can also be the key "
                "of a mapping defined in the 'config_groups' auxiliary data of the analysis; empty default",
                brace_expand=True,
            )
            exclude_params_repr_empty.add("skip_configs")
            exclude_params_req_set.add("skip_configs")
        if has_datasets:
            datasets = law.CSVParameter(
                default=("*",),
                description="names or name patterns of datasets to use; can also be the key of a mapping defined in "
                "the 'dataset_groups' auxiliary data of the corresponding config; default: ('*',)",
                brace_expand=True,
            )
            exclude_params_req_set.add("datasets")
        if has_skip_datasets:
            skip_datasets = law.CSVParameter(
                default=(),
                description="names or name patterns of datasets to skip after evaluating --datasets; can also be the "
                "key of a mapping defined in the 'dataset_groups' auxiliary data of the corresponding config; empty "
                "default",
                brace_expand=True,
            )
            exclude_params_repr_empty.add("skip_datasets")
            exclude_params_req_set.add("skip_datasets")
        if has_shifts:
            shifts = law.CSVParameter(
                default=("nominal",),
                description="names or name patterns of shifts to use; can also be the key of a mapping defined in the "
                "'shift_groups' auxiliary data of the corresponding config; default: ('nominal',)",
                brace_expand=True,
            )
            exclude_params_req_set.add("shifts")
        if has_skip_shifts:
            skip_shifts = law.CSVParameter(
                default=(),
                description="names or name patterns of shifts to skip after evaluating --shifts; can also be the key "
                "of a mapping defined in the 'shift_groups' auxiliary data of the corresponding config; empty default",
                brace_expand=True,
            )
            exclude_params_repr_empty.add("skip_shifts")
            exclude_params_req_set.add("skip_shifts")

        def __init__(self, *args, **kwargs) -> None:
            super().__init__(*args, **kwargs)

            # store wrapper flags
            self.wrapper_require_cls = require_cls
            self.wrapper_has_configs = has_configs and self.configs
            self.wrapper_has_skip_configs = has_skip_configs
            self.wrapper_has_shifts = has_shifts and self.shifts
            self.wrapper_has_skip_shifts = has_skip_shifts
            self.wrapper_has_datasets = has_datasets and self.datasets
            self.wrapper_has_skip_datasets = has_skip_datasets

            # store wrapped fields
            self.wrapper_fields = [
                field for field in ["config", "shift", "dataset"]
                if getattr(self, f"wrapper_has_{field}s")
            ]

            # build the parameter space
            self.wrapper_parameters = self._build_wrapper_parameters()

        def _build_wrapper_parameters(self):
            # collect a list of tuples whose order corresponds to wrapper_fields
            params = []

            # get the target config instances
            if self.wrapper_has_configs:
                configs = self.find_config_objects(
                    names=self.configs,
                    container=self.analysis_inst,
                    object_cls=od.Config,
                    groups_str="config_groups",
                )
                if not configs:
                    raise ValueError(f"no configs found in analysis {self.analysis_inst} matching {self.configs}")
                if self.wrapper_has_skip_configs:
                    skip_configs = self.find_config_objects(
                        names=self.skip_configs,
                        container=self.analysis_inst,
                        object_cls=od.Config,
                        groups_str="config_groups",
                    )
                    configs = [c for c in configs if c not in skip_configs]
                    if not configs:
                        raise ValueError(
                            f"no configs found in analysis {self.analysis_inst} after skipping {self.skip_configs}",
                        )
                config_insts = list(map(self.analysis_inst.get_config, sorted(configs)))
            else:
                config_insts = [self.config_inst]

            # for the remaining fields, build the full combinatorics per config_inst
            for config_inst in config_insts:
                # sequences for building combinatorics
                prod_sequences = []
                if self.wrapper_has_configs:
                    prod_sequences.append([config_inst.name])

                # find all shifts
                if self.wrapper_has_shifts:
                    shifts = self.find_config_objects(
                        names=self.shifts,
                        container=config_inst,
                        object_cls=od.Shift,
                        groups_str="shift_groups",
                    )
                    if not shifts:
                        raise ValueError(f"no shifts found in config {config_inst} matching {self.shifts}")
                    if self.wrapper_has_skip_shifts:
                        skip_shifts = self.find_config_objects(
                            names=self.skip_shifts,
                            container=config_inst,
                            object_cls=od.Shift,
                            groups_str="shift_groups",
                        )
                        shifts = [s for s in shifts if s not in skip_shifts]
                    if not shifts:
                        raise ValueError(f"no shifts found in config {config_inst} after skipping {self.skip_shifts}")
                    # move "nominal" to the front if present
                    shifts = sorted(shifts)
                    if "nominal" in shifts:
                        shifts.insert(0, shifts.pop(shifts.index("nominal")))
                    prod_sequences.append(shifts)

                # find all datasets
                if self.wrapper_has_datasets:
                    datasets = self.find_config_objects(
                        names=self.datasets,
                        container=config_inst,
                        object_cls=od.Dataset,
                        groups_str="dataset_groups",
                    )
                    if not datasets:
                        raise ValueError(f"no datasets found in config {config_inst} matching {self.datasets}")
                    if self.wrapper_has_skip_datasets:
                        skip_datasets = self.find_config_objects(
                            names=self.skip_datasets,
                            container=config_inst,
                            object_cls=od.Dataset,
                            groups_str="dataset_groups",
                        )
                        datasets = [d for d in datasets if d not in skip_datasets]
                        if not datasets:
                            raise ValueError(
                                f"no datasets found in config {config_inst} after skipping {self.skip_datasets}",
                            )
                    prod_sequences.append(sorted(datasets))

                # add the full combinatorics
                params.extend(itertools.product(*prod_sequences))

            return params

        def requires(self) -> dict:
            # build all requirements based on the parameter space
            reqs = {}

            for values in self.wrapper_parameters:
                params = dict(zip(self.wrapper_fields, values))

                # allow custom checks and updates
                params = self.update_wrapper_params(params)
                if not params:
                    continue

                # add the requirement if not present yet
                req = self.wrapper_require_cls.req(self, **params)
                if req not in reqs.values():
                    reqs[values] = req

            return reqs

        def update_wrapper_params(self, params):
            return params

        # add additional class-level members
        if attributes:
            locals().update(attributes)

    # overwrite __module__ to point to the module of the calling stack
    frame = inspect.stack()[1]
    module = inspect.getmodule(frame[0])
    Wrapper.__module__ = module.__name__

    # overwrite __name__
    Wrapper.__name__ = cls_name or f"{require_cls.__name__}Wrapper"

    # set docs
    if docs:
        Wrapper.__docs__ = docs

    return Wrapper
