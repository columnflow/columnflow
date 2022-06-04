# coding: utf-8

"""
Object and event calibration tools.
"""

from typing import Optional, Union, Callable

import law

from ap.util import maybe_import
from ap.columnar_util import ArrayProducer


_calibrator_doc = """
    Wrapper class for functions performing object calibration on (most likely) coffea nano event
    arrays. The main purpose of wrappers is to store information about required and produced columns
    next to the implementation. In addition, they have a unique name which allows for using it in a
    config file.

    The use of the :py:func:`calibrator` decorator function is recommended to create calibrator
    instances. Example:

    .. code-block:: python

        @calibrator(uses={"nJet", "Jet_pt"}, produces={"Jet_pt_jec_up"})
        def my_jet_calibration(events):
            events["Jet", "pt_jec_up"] = events.Jet.pt * 1.05
            return events

    This will register and return an instance named "my_jet_calibration" that *uses* the "nJet" and
    "Jet_pt" columns of the array structure, and produces a new column "Jet_pt_jec_up".

    Knowledge of the columns to load (write) is especially useful when opening (saving) files and
    selecting only the content to deserialize (serialize). Both *uses* and *produces* accept not
    only strings but also previously registered instances to denote in inner dependence. Column
    names should always be given in the flat nano nomenclature (using underscores). The
    :py:attr:`used_columns` and :py:attr:`produced_columns` properties will resolve this information
    and return a set of column names. Example:

    .. code-block:: python

        @calibrator(uses={my_jet_calibration})
        def my_event_calibration(events):
            events = my_jet_calibration(events)
            return events

        print(my_event_calibration.used_columns)
        # -> {"nJet", "Jet_pt"}
        print(my_event_calibration.produced_columns)
        # -> {"Jet_pt_jec_up"}

    For more info and documentation of attributes, see the :py:class:`ArrayProducer` base class.
"""


Calibrator = ArrayProducer.create_subclass("Calibrator", {"__doc__": _calibrator_doc})


def calibrator(
    func: Optional[Callable] = None,
    **kwargs,
) -> Union[Calibrator, Callable]:
    """
    Decorator for registering new calibrator functions. See :py:class:`Calibrator` for
    documentation.
    """
    def decorator(func):
        return Calibrator.new(func, **kwargs)

    return decorator(func) if func else decorator


# import all calibration modules
if law.config.has_option("analysis", "calibration_modules"):
    for mod in law.config.get_expanded("analysis", "calibration_modules", split_csv=True):
        maybe_import(mod.strip())
