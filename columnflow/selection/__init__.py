# coding: utf-8

"""
Object and event selection tools.
"""

from __future__ import annotations

import inspect
from typing import Callable, Sequence

import law
import order as od
from functools import wraps


from columnflow.util import maybe_import, DotDict, DerivableMeta
from columnflow.columnar_util import TaskArrayFunction
from columnflow.config_util import expand_shift_sources


ak = maybe_import("awkward")


class Selector(TaskArrayFunction):
    """
    Base class for all selectors.
    """

    exposed = False

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # when not exposed and call_force is not specified,
        # set it to True which prevents calls from being cached
        if self.call_force is None and not self.exposed:
            self.call_force = True

    @classmethod
    def selector(
        cls,
        func: Callable | None = None,
        bases=(),
        mc_only: bool = False,
        data_only: bool = False,
        nominal_only: bool = False,
        shifts_only: Sequence[str] | set[str] | None = None,
        **kwargs,
    ) -> DerivableMeta | Callable:
        """
        Decorator for creating a new :py:class:`~.Selector` subclass with
        additional, optional *bases* and attaching the decorated function
        to it as ``call_func``.

        When *mc_only* (*data_only*) is *True*, the calibrator is skipped and not considered by
        other calibrators, selectors and producers in case they are evalauted on a
        :py:class:`order.Dataset` (using the :py:attr:`dataset_inst` attribute) whose ``is_mc``
        (``is_data``) attribute is *False*.

        When *nominal_only* is *True* or *shifts_only* is set, the calibrator is skipped and not
        considered by other calibrators, selectors and producers in case they are evalauted on a
        :py:class:`order.Shift` (using the :py:attr:`global_shift_inst` attribute) whose name does
        not match.

        All additional *kwargs* are added as class members of the new subclasses.

        :param func: Callable that is used to perform the selections
        :param bases: Additional bases for new subclass
        :param mc_only: Flag to indicate that this Selector should only run
            on Monte Carlo Simulation
        :param data_only: Flag to indicate that this Selector should only run
            on observed data
        :return: New Selector instance
        """
        # prepare shifts_only
        if shifts_only:
            shifts_only = set(expand_shift_sources(shifts_only))

        def decorator(func: Callable) -> DerivableMeta:
            # create the class dict
            cls_dict = {
                "call_func": func,
                "mc_only": mc_only,
                "data_only": data_only,
                "nominal_only": nominal_only,
                "shifts_only": shifts_only,
            }
            cls_dict.update(kwargs)

            # get the module name
            frame = inspect.stack()[1]
            module = inspect.getmodule(frame[0])

            # get the selector name
            cls_name = cls_dict.pop("cls_name", func.__name__)

            # optionally add skip function
            if mc_only and data_only:
                raise Exception(f"selector {cls_name} received both mc_only and data_only")
            if nominal_only and shifts_only:
                raise Exception(f"selector {cls_name} received both nominal_only and shifts_only")
            if mc_only or data_only or nominal_only or shifts_only:
                if cls_dict.get("skip_func"):
                    raise Exception(
                        f"selector {cls_name} received custom skip_func, but either mc_only, "
                        "data_only, nominal_only or shifts_only are set",
                    )

                def skip_func(self):
                    # check mc_only and data_only
                    if getattr(self, "dataset_inst", None):
                        if mc_only and not self.dataset_inst.is_mc:
                            return True
                        if data_only and not self.dataset_inst.is_data:
                            return True

                    # check nominal_only
                    if getattr(self, "global_shift_inst", None):
                        if nominal_only and not self.global_shift_inst.is_nominal:
                            return True
                        if shifts_only and self.global_shift_inst.name not in shifts_only:
                            return True

                    # in all other cases, do not skip
                    return False

                cls_dict["skip_func"] = skip_func

            # create the subclass
            subclass = cls.derive(cls_name, bases=bases, cls_dict=cls_dict, module=module)

            return subclass

        return decorator(func) if func else decorator


# shorthand
selector = Selector.selector


class SelectionResult(od.AuxDataMixin):
    """
    Lightweight class that wraps selection decisions (e.g. event and object
    selection steps).

    Additionally, this class provides convenience methods to merge them or to dump
    them into an awkward array. Arbitrary, auxiliary information
    (additional arrays, or other objects) that should not be stored in dumped
    akward arrays can be placed in the *aux* dictionary
    (see :py:class:`~order.mixins.AuxDataMixin`).

    The resulting structure looks like the following example:

    .. code-block:: python

        results = {
            # arbitrary, top-level main fields
            ...

            "steps": {
                # event selection decisions from certain steps
                "jet": array_of_event_masks,
                "muon": array_of_event_masks,
                ...,
            },

            "objects": {
                # object selection decisions or indices
                # define type of this field here, define that `jet` is of
                # type `Jet`
                "Jet": {
                    "jet": array_of_jet_indices,
                },
                "Muon": {
                    "muon": array_of_muon_indices,
                },
                ...,
            },
            # additionally, you can also save auxiliary data, e.g.
            "aux": {
                # save the per-object jet selection masks
                "jet": array_of_jet_object_masks,
                # save number of jets
                "n_passed_jets": ak.num(array_of_jet_indices, axis=1),
                ...,
            },
            ...
        }

    The fields can be configured through the *main*, *steps* and *objects*
    keyword arguments. The following example creates the structure above.

    .. code-block:: python

        # combined event selection after all steps
        event_sel = reduce(and_, results.steps.values())
        res = SelectionResult(
            main={
                "event": event_sel,
            },
            steps={
                "jet": array_of_event_masks,
                "muon": array_of_event_masks,
                ...
            },
            objects={
                "Jet": {
                    "jet": array_of_jet_indices
                },
                "Muon": {
                    "muon": array, ...
                }
            }
        )
        res.to_ak()
    """

    def __init__(
        self,
        main: DotDict | dict | None = None,
        steps: DotDict | dict | None = None,
        objects: DotDict | dict | None = None,
        aux: DotDict | dict | None = None,
    ):
        super().__init__(aux=aux)

        # store fields
        self.main = DotDict.wrap(main or {})
        self.steps = DotDict.wrap(steps or {})
        self.objects = DotDict.wrap(objects or {})

    def __iadd__(self, other: SelectionResult | None) -> SelectionResult:
        """
        Adds the field of an *other* instance in-place.

        When *None*, *this* instance is returned unchanged.

        :param other: Instance of :py:class:`~.SelectionResult` to be added
            to current instance
        :raises TypeError: if *other* is not a :py:class:`~.SelectionResult`
            instance
        :return: This instance after adding operation
        """
        # do nothing if the other instance is none
        if other is None:
            return self

        # type check
        if not isinstance(other, SelectionResult):
            raise TypeError(f"cannot add '{other}' to {self.__class__.__name__} instance")

        # update fields in-place
        self.main.update(other.main)
        self.steps.update(other.steps)
        # use deep merging for objects
        law.util.merge_dicts(self.objects, other.objects, inplace=True, deep=True)
        # shallow update for aux
        self.aux.update(other.aux)

        return self

    def __add__(self, other: SelectionResult | None) -> SelectionResult:
        """
        Returns a new instance with all fields of *this* and an *other*
        instance merged.

        When *None*, a copy of *this* instance is returned.

        :param other: Instance of :py:class:`~.SelectionResult` to be added
            to current instance
        :raises TypeError: if *other* is not a :py:class:`~.SelectionResult`
            instance
        :return: This instance after adding operation
        """
        inst = self.__class__()

        # add this instance
        inst += self

        # add the other instance if not none
        if other is not None:
            if not isinstance(other, SelectionResult):
                raise TypeError(f"cannot add '{other}' to {self.__class__.__name__} instance")
            inst += other

        return inst

    def to_ak(self) -> ak.Array:
        """
        Converts the contained fields into a nested awkward array and returns it.

        The conversion is performed with multiple calls of
        :external+ak:py:func:`ak.zip`.

        :return: Transformed :py:class:`~.SelectionResult`
        """
        to_merge = {}
        if self.steps:
            to_merge["steps"] = ak.zip(self.steps)
        if self.objects:
            to_merge["objects"] = ak.zip({
                src_name: ak.zip(dst_dict, depth_limit=1)  # limit due to ragged axis 1
                for src_name, dst_dict in self.objects.items()
            })

        return ak.zip({**self.main, **to_merge})
