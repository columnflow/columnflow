# coding: utf-8

"""
Collection of general helpers and utilities.
"""

from __future__ import annotations

__all__ = [
    "get_root_processes_from_campaign", "add_shift_aliases", "get_shifts_from_sources",
]

import re

import order as od


def get_root_processes_from_campaign(campaign: od.Campaign) -> od.UniqueObjectIndex:
    """
    Extracts all root process objects from datasets contained in an order campaign and returns them
    in a unique object index.
    """
    # get all dataset processes
    processes = set.union(*map(set, (dataset.processes for dataset in campaign.datasets)))

    # get their root processes
    root_processes = set.union(*map(set, (process.get_root_processes() for process in processes)))

    # create an empty index and fill subprocesses via walking
    index = od.UniqueObjectIndex(cls=od.Process)
    for root_process in root_processes:
        for process, _, _ in root_process.walk_processes(include_self=True):
            index.add(process, overwrite=True)

    return index


def add_shift_aliases(
    config: od.Config,
    shift_source: str,
    aliases: dict,
    selection_dependent: bool = False,
) -> None:
    """
    Extracts the two up and down shift instances from a *config* corresponding to a *shift_source*
    (i.e. the name of a shift without directions) and assigns *aliases* to their auxiliary data.

    To mark whether these aliases influence variables that are used during the selection, the
    *selection_dependent* flag can be set.

    Aliases should be given in a dictionary, mapping alias targets (keys) to sources (values). In
    both strings, template variables are injected with fields corresponding to all
    :py:class:`od.Shift` attributes, such as *name*, *id*, and *direction*.

    Example:

    .. code-block:: python

        add_shift_aliases(config, "pdf", {"pdf_weight": "pdf_weight_{direction}"})
        # adds {"pdf_weight": "pdf_weight_up"} to the "pdf_up" shift in "config"
        # plus {"pdf_weight": "pdf_weight_down"} to the "pdf_down" shift in "config"
    """
    aux_key = "column_aliases" + ("_selection_dependent" if selection_dependent else "")
    for direction in ["up", "down"]:
        shift = config.get_shift(od.Shift.join_name(shift_source, direction))
        _aliases = shift.x(aux_key, {})
        # format keys and values
        inject_shift = lambda s: re.sub(r"\{([^_])", r"{_\1", s).format(**shift.__dict__)
        _aliases.update({inject_shift(key): inject_shift(value) for key, value in aliases.items()})
        # extend existing or register new column aliases
        shift.set_aux(aux_key, _aliases)


def get_shifts_from_sources(config: od.Config, *shift_sources: str) -> list[od.Shift]:
    """
    Takes a *config* object and returns a list of shift instances for both directions given a
    sequence *shift_sources*.
    """
    return sum(
        (
            [config.get_shift(f"{s}_up"), config.get_shift(f"{s}_down")]
            for s in shift_sources
        ),
        [],
    )
