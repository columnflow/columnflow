# coding: utf-8

"""
Collection of general helpers and utilities.
"""

from __future__ import annotations

__all__ = [
    "get_root_processes_from_campaign",
    "get_datasets_from_process",
    "add_shift_aliases",
    "get_shifts_from_sources",
    "expand_shift_sources",
    "create_category_id",
    "add_category",
    "create_category_combinations",
    "verify_config_processes",
]

import re
import itertools
from collections import OrderedDict

import law
import order as od

from columnflow.util import maybe_import
from columnflow.types import Callable, Any, Sequence

ak = maybe_import("awkward")
np = maybe_import("numpy")


def get_events_from_categories(
    events: ak.Array,
    categories: Sequence[str | od.Category],
    config_inst: od.Config | None = None,
) -> ak.Array:
    """
    Helper function that returns all events from an awkward array *events* that are categorized
    into one of the leafs of one of the *categories*.

    :param events: Awkward array. Requires the 'category_ids' field to be present.
    :param categories: Sequence of category instances. Can also be a sequence of strings when passing a
        *config_inst*.
    :param config_inst: Optional config instance to load category instances.
    :raises ValueError: If "category_ids" is not present in the *events* fields.
    :return: Awkward array of all events that are categorized into one of the leafs of one of the
        *categories*
    """
    if "category_ids" not in events.fields:
        raise ValueError(
            f"{get_events_from_categories.__name__} requires the 'category_ids' field to be present",
        )

    categories = law.util.make_list(categories)
    if config_inst:
        # get category insts
        categories = [config_inst.get_category(cat) for cat in categories]

    leaf_category_insts = set.union(*map(set, (cat.get_leaf_categories() or {cat} for cat in categories)))

    # do the "or" of all leaf categories
    mask = np.zeros(len(events), dtype=bool)
    for cat in leaf_category_insts:
        cat_mask = ak.any(events.category_ids == cat.id, axis=1)
        mask = cat_mask | mask

    return events[mask]


def get_root_processes_from_campaign(campaign: od.config.Campaign) -> od.unique.UniqueObjectIndex:
    """
    Extracts all root process objects from datasets contained in an order *campaign* and returns
    them in a unique object index.

    :param campaign: :py:class:`~order.config.Campaign` object containing information
        about relevant datasets
    :return: Unique indices for :py:class:`~order.process.Process` instances of
        root processes associated with these datasets
    """
    # get all dataset processes
    processes: set[od.Process] = set.union(*map(set, (dataset.processes for dataset in campaign.datasets)))

    # get their root processes
    root_processes: set[od.Process] = set.union(*map(set, (
        (process.get_root_processes() or [process])
        for process in processes
    )))

    # create an empty index and fill subprocesses via walking
    index = od.UniqueObjectIndex(cls=od.Process)
    for root_process in root_processes:
        for process, _, _ in root_process.walk_processes(include_self=True):
            index.add(process, overwrite=True)

    return index


def get_datasets_from_process(
    config: od.config.Config,
    process: str | od.process.Process,
    strategy: str = "inclusive",
    only_first: bool = True,
    check_deep: bool = False,
) -> list[od.dataset.Dataset]:
    r"""Given a *process* and the *config* it belongs to, returns a list of order dataset objects that
    contain matching processes. This is done by walking through *process* and its child processes
    and checking whether they are contained in known datasets. *strategy* controls how possible
    ambiguities are resolved:

        - ``"all"``: The full process tree is traversed and all matching datasets are considered.
            Note that this might lead to a potential over-representation of the phase space.
        - ``"inclusive"``: If a dataset is found to match a process, its child processes are not
            checked further.
        - ``"exclusive"``: If **any** (deep) subprocess of *process* is found to be contained in a
            dataset, return datasets of subprocesses but not that of *process* itself (if any).
        - ``"exclusive_strict"``: If **all** (deep) subprocesses of *process* are found to be
            contained in a dataset, return these datasets but not that of *process* itself (if any).

    As an example, consider the process tree

    .. mermaid::
        :align: center
        :zoom:

        flowchart BT
            A[single top]
            B{s channel}
            C{t channel}
            D{tw channel}
            E(t)
            F(tbar)
            G(t)
            H(tbar)
            I(t)
            J(tbar)

            B --> A
            C --> A
            D --> A

            E --> B
            F --> B

            G --> C
            H --> C

            I --> D
            J --> D

    and datasets existing for


    1. single top - s channel - t
    2. single top - s channel - tbar
    3. single top - t channel
    4. single top - t channel - t
    5. single top - tw channel
    6. single top - tw channel - t
    7. single top - tw channel - tbar

    in the *config*. Depending on *strategy*, the returned datasets for process ``single top``are:

        - ``"all"``: ``[1, 2, 3, 4, 5, 6, 7]``. Simply all datasets matching any subprocess.
        - ``"inclusive"``: ``[1, 2, 3, 5]``. Skipping ``single top - t channel - t``,
            ``single top - tw channel - t``, and ``single top - tw channel - tbar``, since more inclusive
            datasets (``single top - t channel`` and ``single top - tw channel``) exist.
        - ``"exclusive"``: ``[1, 2, 4, 6, 7]``. Skipping ``single_top - t_channel`` and
            ``single top - tw channel`` since more exclusive datasets (``single top - t channel - t``,
            ``single top - tw channel - t``, and ``single top - tw channel - tbar``) exist.
        - ``"exclusive_strict"``: ``[1, 2, 3, 6, 7]``. Like ``"exclusive"``, but not skipping
            ``single top - t channel`` since not all subprocesses of ``t channel`` match a dataset
            (there is no ``single top - t channel - tbar`` dataset).

    In addition, two arguments configure how the check is performed whether a process is contained
    in a dataset. If *only_first* is *True*, only the first matching dataset is considered.
    Otherwise, all datasets matching a specific process are returned. For the check itself,
    *check_deep* is forwarded to :py:meth:`order.Dataset.has_process`.

    :param config: Config instance containing the information about known datasets.
    :param process: Process instance or process name for which you want to obtain
        list of datasets.
    :param strategy: controls how possible ambiguities are resolved. Choices:
        [``"all"``, ``"inclusive"``, ``"exclusive"``, ``"exclusive_strict"``]
    :param only_first: If *True*, only the first matching dataset is considered.
    :param check_deep: Forwarded to :py:meth:`order.Dataset.has_process`
    :raises ValueError: If *strategy* is not in list of allowed choices
    :return: List of datasets that correspond to *process*, depending on the
        specifics of the query
    """
    # check the strategy
    known_strategies = ["all", "inclusive", "exclusive", "exclusive_strict"]
    if strategy not in known_strategies:
        _known_strategies = ", ".join(map("'{}'".format, known_strategies))
        raise ValueError(f"unknown strategy {strategy}, known values are {_known_strategies}")

    # make sure we are dealing a process instance
    root_inst = config.get_process(process)

    # the tree traversal differs depending on the strategy, so distinguish cases
    if strategy in ["all", "inclusive"]:
        dataset_insts: list[od.Dataset] = []
        for process_inst, _, child_insts in root_inst.walk_processes(include_self=True, algo="bfs"):
            found_dataset = False

            # check datasets
            for dataset_inst in config.datasets:
                if dataset_inst.has_process(process_inst, deep=check_deep):
                    dataset_insts.append(dataset_inst)
                    found_dataset = True

                    # stop checking more datasets when only the first matters
                    if only_first:
                        break

            # in the inclusive strategy, children do not need to be traversed if a dataset was found
            if strategy == "inclusive" and found_dataset:
                del child_insts[:]

        return law.util.make_unique(dataset_insts)

    # at this point, strategy is exclusive or exclusive_strict
    dataset_insts_dict: OrderedDict[str, od.Dataset] = OrderedDict()
    for process_inst, _, child_insts in root_inst.walk_processes(include_self=True, algo="dfs_post"):
        # check if child processes have matched datasets already
        if child_insts:
            n_found = sum(int(child_inst in dataset_insts_dict) for child_inst in child_insts)
            # potentially skip the current process
            if strategy == "exclusive" and n_found:
                continue
            if strategy == "exclusive_strict" and n_found == len(child_insts):
                # add a empty list to mark this is done
                dataset_insts_dict[process_inst] = []
                continue
            # at this point, the process itself must be checked,
            # so remove potentially found datasets of children
            dataset_insts_dict = OrderedDict({
                child_inst: _dataset_insts
                for child_inst, _dataset_insts in dataset_insts_dict.items()
                if child_inst not in child_insts
            })

        # check datasets
        for dataset_inst in config.datasets:
            if dataset_inst.has_process(process_inst, deep=check_deep):
                dataset_insts_dict.setdefault(process_inst, []).append(dataset_inst)

                # stop checking more datasets when only the first matters
                if only_first:
                    break

    return sum(dataset_insts_dict.values(), [])


def add_shift_aliases(
    config: od.Config,
    shift_source: str,
    aliases: dict,
) -> None:
    """
    Extracts the two up and down shift instances from a *config* corresponding to a *shift_source*
    (i.e. the name of a shift without directions) and assigns *aliases* to their auxiliary data.

    Aliases should be given in a dictionary, mapping alias targets (keys) to sources (values). In
    both strings, template variables are injected with fields corresponding to all
    :py:class:`od.Shift` attributes, such as *name*, *id*, and *direction*.

    Example:

    .. code-block:: python

        add_shift_aliases(config, "pdf", {"pdf_weight": "pdf_weight_{direction}"})
        # adds {"pdf_weight": "pdf_weight_up"} to the "pdf_up" shift in "config"
        # plus {"pdf_weight": "pdf_weight_down"} to the "pdf_down" shift in "config"
    """
    for direction in ["up", "down"]:
        shift = config.get_shift(od.Shift.join_name(shift_source, direction))
        _aliases = shift.x("column_aliases", {})
        # format keys and values
        inject_shift = lambda s: re.sub(r"\{([^_])", r"{_\1", s).format(**shift.__dict__)
        _aliases.update({inject_shift(key): inject_shift(value) for key, value in aliases.items()})
        # extend existing or register new column aliases
        shift.x.column_aliases = _aliases


def get_shifts_from_sources(config: od.Config, *shift_sources: Sequence[str]) -> list[od.Shift]:
    """
    Takes a *config* object and returns a list of shift instances for both directions given a
    sequence *shift_sources*.
    """
    return sum(
        (
            [config.get_shift(f"{s}_{od.Shift.UP}"), config.get_shift(f"{s}_{od.Shift.DOWN}")]
            for s in shift_sources
        ),
        [],
    )


def expand_shift_sources(shifts: Sequence[str] | set[str]) -> list[str]:
    """
    Given a sequence *shifts* containing either shift names (``<source>_<direction>``) or shift
    sources, the latter ones are expanded with both possible directions and returned in a common
    list.

    Example:

    .. code-block:: python

        expand_shift_sources(["jes", "jer_up"])
        # -> ["jes_up", "jes_down", "jer_up"]
    """
    _shifts = []
    for shift in shifts:
        try:
            od.Shift.split_name(shift)
            _shifts.append(shift)
        except ValueError as e:
            if not isinstance(shift, str):
                raise e
            _shifts.extend([f"{shift}_{od.Shift.UP}", f"{shift}_{od.Shift.DOWN}"])

    return law.util.make_unique(_shifts)


def create_category_id(
    config: od.Config,
    category_name: str,
    hash_len: int = 7,
    salt: Any = None,
) -> int:
    """
    Creates a unique id for a :py:class:`order.Category` named *category_name* in a
    :py:class:`order.Config` object *config* and returns it. Internally,
    :py:func:`law.util.create_hash` is used which receives *hash_len*. In case of an unintentional
    (yet unlikely) collision of two ids, there is the option to add a custom *salt* value.

    .. note::

        Please note that the size of the returned id depends on *hash_len*. When storing the id
        subsequently in an array, please be aware that values 8 or more require a ``np.int64``.
    """
    # create the hash
    h = law.util.create_hash((config.name, config.id, category_name, salt), l=hash_len)
    h = int(h, base=16)

    # add an offset to ensure that are hashes are above a threshold
    digits = len(str(int("F" * hash_len, base=16)))
    h += int(10 ** digits)

    return h


def add_category(config: od.Config, **kwargs) -> od.Category:
    """
    Creates a :py:class:`order.Category` instance by forwarding all *kwargs* to its constructor,
    adds it to a :py:class:`order.Config` object *config* and returns it. When *kwargs* do not
    contain a field *id*, :py:func:`create_category_id` is used to create one.
    """
    if "name" not in kwargs:
        fields = ",".join(map(str, kwargs))
        raise ValueError(f"a field 'name' is required to create a category, got '{fields}'")

    if "id" not in kwargs:
        kwargs["id"] = create_category_id(config, kwargs["name"])

    return config.add_category(**kwargs)


def create_category_combinations(
    config: od.Config,
    categories: dict[str, list[od.Category]],
    name_fn: Callable[[Any], str],
    kwargs_fn: Callable[[Any], dict] | None = None,
    skip_existing: bool = True,
    skip_fn: Callable[[dict[str, od.Category], str], bool] | None = None,
) -> int:
    """
    Given a *config* object and sequences of *categories* in a dict, creates all combinations of
    possible leaf categories at different depths, connects them with parent - child relations
    (see :py:class:`order.Category`) and returns the number of newly created categories.

    *categories* should be a dictionary that maps string names to sequences of categories that
    should be combined. The names are used as keyword arguments in a callable *name_fn* that is
    supposed to return the name of newly created categories (see example below).

    Each newly created category is instantiated with this name as well as arbitrary keyword
    arguments as returned by *kwargs_fn*. This function is called with the categories (in a
    dictionary, mapped to the sequence names as given in *categories*) that contribute to the newly
    created category and should return a dictionary. If the fields ``"id"`` and ``"selection"`` are
    missing, they are filled with reasonable defaults leading to a auto-generated, deterministic id
    and a list of all parent selection statements.

    If the name of a new category is already known to *config* it is skipped unless *skip_existing*
    is *False*. In addition, *skip_fn* can be a callable that receives a dictionary mapping group
    names to categories that represents the combination of categories to be added. In case *skip_fn*
    returns *True*, the combination is skipped.

    Example:

    .. code-block:: python

        categories = {
            "lepton": [cfg.get_category("e"), cfg.get_category("mu")],
            "n_jets": [cfg.get_category("1j"), cfg.get_category("2j")],
            "n_tags": [cfg.get_category("0t"), cfg.get_category("1t")],
        }

        def name_fn(categories):
            # simple implementation: join names in defined order if existing
            return "__".join(cat.name for cat in categories.values() if cat)

        def kwargs_fn(categories):
            # return arguments that are forwarded to the category init
            # (use id "+" here which simply increments the last taken id, see order.Category)
            # (note that this is also the default)
            return {"id": "+"}

        create_category_combinations(cfg, categories, name_fn, kwargs_fn)
    """
    n_created_categories = 0
    n_groups = len(categories)
    group_names = list(categories.keys())

    # nothing to do when there are less than 2 groups
    if n_groups < 2:
        return n_created_categories

    # check functions
    if not callable(name_fn):
        raise TypeError(f"name_fn must be a function, but got {name_fn}")
    if kwargs_fn and not callable(kwargs_fn):
        raise TypeError(f"when set, kwargs_fn must be a function, but got {kwargs_fn}")

    # start combining, considering one additional groups for combinatorics at a time
    for _n_groups in range(2, n_groups + 1):

        # build all group combinations
        for _group_names in itertools.combinations(group_names, _n_groups):

            # build the product of all categories for the given groups
            _categories = [categories[group_name] for group_name in _group_names]
            for root_cats in itertools.product(*_categories):
                # build the name
                root_cats = dict(zip(_group_names, root_cats))
                cat_name = name_fn(root_cats)

                # skip when already existing
                if skip_existing and config.has_category(cat_name, deep=True):
                    continue

                # skip when manually triggered
                if callable(skip_fn) and skip_fn(root_cats):
                    continue

                # create arguments for the new category
                kwargs = kwargs_fn(root_cats) if callable(kwargs_fn) else {}
                if "id" not in kwargs:
                    kwargs["id"] = create_category_id(config, cat_name)
                if "selection" not in kwargs:
                    kwargs["selection"] = [c.selection for c in root_cats.values()]

                # create the new category
                cat = od.Category(name=cat_name, **kwargs)
                n_created_categories += 1

                # find direct parents and connect them
                for _parent_group_names in itertools.combinations(_group_names, _n_groups - 1):
                    if len(_parent_group_names) == 1:
                        parent_cat_name = root_cats[_parent_group_names[0]].name
                    else:
                        parent_cat_name = name_fn({
                            group_name: root_cats[group_name]
                            for group_name in _parent_group_names
                        })
                    parent_cat = config.get_category(parent_cat_name, deep=True)
                    parent_cat.add_category(cat)

    return n_created_categories


def verify_config_processes(config: od.Config, warn: bool = False) -> None:
    """
    Verifies for all datasets contained in a *config* object that the linked processes are covered
    by any process object registered in *config* and raises an exception if not. If *warn* is
    *True*, a warning is printed instead.
    """
    missing_pairs = []
    for dataset in config.datasets:
        for process in dataset.processes:
            if not config.has_process(process):
                missing_pairs.append((dataset, process))

    # nothing to do when nothing is missing
    if not missing_pairs:
        return

    # build the message
    msg = f"found {len(missing_pairs)} dataset(s) whose process is not registered in the '{config.name}' config:"
    for dataset, process in missing_pairs:
        msg += f"\n  dataset '{dataset.name}' -> process '{process.name}'"

    # warn or raise
    if not warn:
        raise Exception(msg)

    print(f"{law.util.colored('WARNING', 'red')}: {msg}")
