# coding: utf-8

"""
Tools for producing new array columns (e.g. high-level variables).
"""

from typing import Optional, Union, Callable

import law

from ap.util import maybe_import
from ap.columnar_util import ArrayProducer


_producer_doc = """
    Wrapper class for functions creating new columns for (most likely) coffea nano event arrays. The
    main purpose of wrappers is to store information about required and produced columns next to the
    implementation. In addition, they have a unique name which allows for using it in a config file.

    The use of the :py:func:`producer` decorator function is recommended to create producer
    instances. Example:

    .. code-block:: python

        @producer(uses={"nJet", "Jet_pt"}, produces={"Jet_pt2"})
        def my_high_level_feature(events):
            events["Jet", "pt2"] = events.Jet.pt ** 2.0
            return events

    This will register and return an instance named "my_high_level_feature" that *uses* the "nJet"
    and "Jet_pt" columns of the array structure, and produces a new column "Jet_pt2".

    Knowledge of the columns to load (write) is especially useful when opening (saving) files and
    selecting only the content to deserialize (serialize). Both *uses* and *produces* accept not
    only strings but also previously registered instances to denote in inner dependence. Column
    names should always be given in the flat nano nomenclature (using underscores). The
    :py:attr:`used_columns` and :py:attr:`produced_columns` properties will resolve this information
    and return a set of column names. Example:

    .. code-block:: python

        @calibrator(uses={my_high_level_feature})
        def my_other_feature(events):
            events = my_high_level_feature(events)
            return events

        print(my_high_level_feature.used_columns)
        # -> {"nJet", "Jet_pt"}
        print(my_high_level_feature.produced_columns)
        # -> {"Jet_pt_jec_up"}

    For more info and documentation of attributes, see the :py:class:`ArrayProducer` base class.
"""


Producer = ArrayProducer.create_subclass("Producer", {"__doc__": _producer_doc})


def producer(
    func: Optional[Callable] = None,
    **kwargs,
) -> Union[Producer, Callable]:
    """
    Decorator for registering new producer functions. See :py:class:`Producer` for documentation.
    """
    def decorator(func):
        return Producer.new(func, **kwargs)

    return decorator(func) if func else decorator


# import all production modules
if law.config.has_option("analysis", "production_modules"):
    for mod in law.config.get_expanded("analysis", "production_modules", split_csv=True):
        maybe_import(mod.strip())
