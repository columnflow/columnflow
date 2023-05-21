# coding: utf-8

"""
Object and event selection tools.
"""

from __future__ import annotations

import inspect
from typing import Callable

import law
import order as od
from functools import wraps


from columnflow.util import maybe_import, DotDict, DerivableMeta
from columnflow.columnar_util import TaskArrayFunction


ak = maybe_import("awkward")


class Selector(TaskArrayFunction):
    """Derivative of :py:class:`~columnflow.columnar_util.TaskArrayFunction`
    that handles selections.

    :py:class:`~.Selector` s are designed to apply
    arbitrary selection criteria. These critera can be based on already existing
    nano AOD columns, but can also involve the output of any other module,
    e.g. :py:class:`~columnflow.production.Producer`s. To reduce the need to
    run potentionally computation-expensive operations multiple times, they can
    also write new columns. Similar to :py:class:`~columnflow.production.Producer` s,
    and :py:class:`~columnflow.calibration.Calibrator` s, this new columns must
    be specified in the `produces` set.

    Apart from the awkward array, a :py:class:`~.Selector` must also return a
    :py:class:`~.SelectionResult`. This object contains boolean masks on event
    and object level that represent which objects and events pass different
    selections. These masks are saved to disc and are intended for more involved
    studies, e.g. comparisons between frameworks.

    To create a new :py:class:`~.Selector`, you can use the decorrator
    class method :py:meth:`~.Selector.selector` like this:


    .. code-block:: python

        # import the Selector class and the selector method
        from columnflow.selection import Selector, selector

        # also import the SelectionResult
        from columnflow.selection import SelectionResult

        # maybe import awkard in case this Selector is actually run
        ak = maybe_import("awkward")

        # now wrap any function with a selector
        @selector(
            # define some additional information here, e.g.
            # what columns are needed for this Selector?
            uses={
                "Jet.pt", "Jet.eta"
            },
            # does this Selector produce any columns?
            produces={}

            # pass any other variable to the selector class
            is_this_a_fun_auxiliary_variable=True

            # ...
        )
        def jet_selection(events: ak.Array) -> ak.Array, SelectionResult:
            # do something ...

    The decorrator will create a new :py:class:`~.Selector` instance with the
    name of your function. The function itself is set as the `call_func` if
    the :py:class:`~.Selector` instance. All keyword arguments specified in the
    :py:meth:`~.Selector.selector` are available as member variables of your
    new :py:class:`~.Selector` instance.

    In additional to the member variables inherited from
    :py:class:`~columnflow.columnar_util.TaskArrayFunction` class, the
    :py:class:`~.Selector` class defines the *exposed* variable. This member
    variable controls whether this :py:class:`~.Selector` instance is a
    top level Selector that can be used directly for the
    :py:class:`~columnflow.tasks.selection.SelectEvents` task.
    A top level :py:class:`~.Selector` should not need anything apart from
    the awkward array containing the events, e.g.

    .. code-block:: python

        @selector(
            # some information for Selector

            # This Selector will need some external input, see below
            # Therefore, it should not be exposed
            exposed=False
        )
        def some_internal_selector(
            events: ak.Array,
            some_additional_input: Any
        ) -> ak.Array, SelectionResult:
            result = SelectionResult()
            # do stuff with additional information
            return events, result

        @selector(
            # some information for Selector
            # e.g., if we want to use some internal Selector, make
            # sure that you have all the relevant information
            uses={
                some_internal_selector,
            },
            produces={
                some_internal_selector,
            }

            # this is our top level Selector, so we need to make it reachable
            # for the SelectEvents task
            exposed=True
        )
        def top_level_selector(events: ak.Array) -> ak.Array, SelectionResult:
            results = SelectionResult()
            # do something here

            # e.g., call the internal Selector
            additional_info = 2
            events, sub_result = self[some_internal_selector](events, additional_info)
            result += sub_result

            return events, result


    :param exposed: Member variable that controls whether this
        :py:class:`~.Selector` instance is a top level Selector that can
        be used directly for the :py:class:`~columnflow.tasks.selection.SelectEvents`
        task. Defaults to `False`.
    :type exposed: `bool`
    """

    exposed = False

    def __init__(self, *args, **kwargs):
        """Init function for Selector. Checks whether this Selector
        instance is exposed and sets the accessibility accordingly
        """
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
        **kwargs,
    ) -> DerivableMeta | Callable:
        """Decorator for creating a new :py:class:`~.Selector` subclass with
        additional, optional *bases* and attaching the decorated function
        to it as ``call_func``. When *mc_only* (*data_only*) is ``True``,
        the selector is skipped and not considered by other calibrators,
        selectors and producers in case they are evalauted on an
        :external+order:py:class:`order.dataset.Dataset` whose ``is_mc``
        attribute is ``False`` (``True``).

        All additional *kwargs* are added as class members of the new subclasses.

        :param func: Callable that is used to perform the selections, defaults to None
        :type func: Callable | None, optional
        :param bases: Additional bases for new subclass, defaults to ()
        :type bases: tuple, optional
        :param mc_only: Flag to indicate that this Selector should only run
            on Monte Carlo Simulation, defaults to False
        :type mc_only: bool, optional
        :param data_only: Flag to indicate that this Selector should only run
            on observed data, defaults to False
        :type data_only: bool, optional
        :return: New Selector instance
        :rtype: DerivableMeta | Callable
        """
        def decorator(func: Callable) -> DerivableMeta:
            @wraps(func)
            def wrapper(*args, **kwargs):
                # create the class dict
                cls_dict = {"call_func": func}
                cls_dict.update(kwargs)

                # get the module name
                frame = inspect.stack()[1]
                module = inspect.getmodule(frame[0])

                # get the selector name
                cls_name = cls_dict.pop("cls_name", func.__name__)

                # optionally add skip function
                if mc_only and data_only:
                    raise Exception(f"selector {cls_name} received both mc_only and data_only")
                if mc_only or data_only:
                    if cls_dict.get("skip_func"):
                        raise Exception(
                            f"selector {cls_name} received custom skip_func, but mc_only or data_only are set",
                        )

                    def skip_func(self):
                        # never skip when there is not dataset
                        if not getattr(self, "dataset_inst", None):
                            return False

                        return self.dataset_inst.is_mc != bool(mc_only)

                    cls_dict["skip_func"] = skip_func

                # create the subclass
                subclass = cls.derive(cls_name, bases=bases, cls_dict=cls_dict, module=module)
                subclass.__doc__ = func.__doc__
                return subclass
            return wrapper(func, **kwargs)

        return decorator(func) if func else decorator


# shorthand
selector = Selector.selector


class SelectionResult(od.AuxDataMixin):
    """Lightweight class that wraps selection decisions (e.g. event and object
    selection steps).

    Basis: :py:class:`~order.AuxDataMixin`

    Additionally, this class provides convenience methods to merge them or to dump
    them into an awkward array. Arbitrary, auxiliary information
    (additional arrays, or other objects) that should not be stored in dumped
    akward arrays can be placed in the *aux* dictionary
    (see :py:class:`order.AuxDataMixin`).

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
        Adds the field of an *other* instance in-place. When *None*, *this* instance is returned
        unchanged.
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
        Returns a new instance with all fields of *this* and an *other* instance merged. When
        *None*, a copy of *this* instance is returned.
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
