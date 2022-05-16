# coding: utf-8

"""
Object and event calibration tools.
"""

from typing import Optional, Union, Sequence, Set, Callable

import law

from ap.util import maybe_import
from ap.columnar_util import ArrayFunction


class Calibrator(ArrayFunction):
    """
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

    The name defaults to the name of the function itself and can be altered by passing a custom
    *name*. It is used internally to store the instance in a cache from which it can be retrieved
    through the :py:meth:`get` class method.

    Knowledge of the columns to load is especially useful when opening (writing) files and selecting
    the content to deserialize (serialize). *uses* accepts not only strings but also previously
    registered instances to denote in inner dependence. Column names should always be given in the
    flat nano nomenclature (using underscores). The :py:attr:`used_columns` property will resolve
    this information and return a set of column names. Example:

    .. code-block:: python

        @calibrator(uses={my_jet_calibration})
        def my_event_calibration(events):
            events = my_jet_calibration(events)
            return events

        print(my_event_calibration.used_columns)
        # -> {"nJet", "Jet_pt"}
        print(my_event_calibration.produced_columns)
        # -> {"Jet_pt_jec_up"}

    .. py:attribute:: func
       type: callable

       The wrapped function.

    .. py:attribute:: name
       type: str

       The name of the calibrator in the instance cache.

    .. py:attribute:: uses
       type: set

       The set of column names or other calibrator instances to recursively resolve the names of
       required columns.

    .. py::attribute:: used_columns
       type: set
       read-only

       The resolved, flat set of used column names.
    """

    # create an own instance cache
    _instances = {}

    def __init__(
        self,
        func: Callable,
        name: Optional[str] = None,
        uses: Optional[Union[
            str, "Calibrator", Sequence[str], Sequence["Calibrator"], Set[str], Set["Calibrator"],
        ]] = None,
        produces: Optional[Union[
            str, "Calibrator", Sequence[str], Sequence["Calibrator"], Set[str], Set["Calibrator"],
        ]] = None,
    ):
        super(Calibrator, self).__init__(func, name=name, uses=uses)

        self.produces = law.util.make_list(produces) if produces else []

    @property
    def produced_columns(self) -> Set[str]:
        columns = set()
        for obj in self.produces:
            if isinstance(obj, Calibrator):
                columns |= obj.produced_columns
            else:
                columns.add(obj)
        return columns


def calibrator(
    func: Optional[Callable] = None,
    **kwargs
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
