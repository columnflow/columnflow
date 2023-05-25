# coding: utf-8

"""
Tools for producing new array columns (e.g. high-level variables).
"""

from __future__ import annotations

import inspect
from typing import Callable
from functools import wraps

from columnflow.util import DerivableMeta
from columnflow.columnar_util import TaskArrayFunction


class Producer(TaskArrayFunction):
    """Derivative of :py:class:`~columnflow.columnar_util.TaskArrayFunction`
    that handles producing new columns.

    A :py:class:`~.Producer` is designed to calculate new observables and
    produce new columns for them in the input :py:class:`ak.Array`.
    The new observables can be calculated from already existing NanoAOD columns,
    or from the outputs of other :py:class:`Producer`. If run after the
    :py:class:`~columnflow.tasks.reduction.ReduceEvents` tasks, a :py:class:`Producer`
    has also access to additional fields that are added by
    :py:class:`~columnflow.selection.Selector`
    instances. As for all derivatives of
    :py:class:`~columnflow.columnar_util.TaskArrayFunction`,
    the new columns must be specified in the `produces` set.

    For example, a :py:class:`Producer` that squares the pt of jets could like
    something like this:

    .. code-block:: python

        # coding: utf-8

        # import Producer class and producer decorrator
        from columnflow.production import Producer, producer

        # use maybe import for safe imports, regardless of current position
        # in task tree structure
        from columnflow.util import maybe_import

        # load central default value (`EMPTY_FLOAT`), the Route class to form
        # column field names (`Route`) and function to add new columns
        # (`set_ak_column`)
        from columnflow.columnar_util import EMPTY_FLOAT, Route, set_ak_column

        # import numpy and awkward
        np = maybe_import("numpy")
        ak = maybe_import("awkward")

        # create short hand to add columns containing floats with float32 precision
        import functools
        set_ak_column_f32 = functools.partial(set_ak_column, value_type=np.float32)


        # call decorrator to create new Producer instance and wrap function
        # that calculates the observable with it. The name of the Producer
        # instance is the same as the name of the function
        @producer(
            # Define which columns are needed to calculate the observable.
            # In this example, we only need the pt values of the Jets
            uses={
                "Jet.pt",
            },
            # Define which columns are produced. In this case, we create a
            # new column for the squared values of the pts called "squared_pt"
            produces={
                "squared_pt",
            },
        )
        def square_pts(self: Producer, events: ak.Array, **kwargs) -> ak.Array:

            # now add the four momenta of the first two `Jet`s using the
            # `sum` function of coffea's LorentzVector class
            square_pts = np.square(events.Jet.pt)

            # now add the new column to the array *events*. The column contains
            # the mass of the summed four momenta where there are at least
            # two jets, and the default value `EMPTY_FLOAT` otherwise.
            events = set_ak_column_f32(
                events,
                "squared_pt",
                squared_pts,
            )

            return events

    The decorrator will create a new :py:class:`~.Producer` instance with the
    name of your function. The function itself is set as the `call_func` of
    the :py:class:`~.Producer` instance. All keyword arguments specified in the
    :py:meth:`~.Producer.producer` are available as member variables of your
    new :py:class:`~.Producer` instance.

    You can also combine different :py:class:`Producer` instances with each other,
    or even create completely new fields.
    For example, a :py:class:`Producer` that produces the invariant mass of
    ``Jet`` pairs could look something like this:

    .. code-block:: python

        # coding: utf-8

        # import Producer class and producer decorrator
        from columnflow.production import Producer, producer

        # use maybe import for safe imports, regardless of current position
        # in task tree structure
        from columnflow.util import maybe_import

        # load central default value (`EMPTY_FLOAT`), the Route class to form
        # column field names (`Route`) and function to add new columns
        # (`set_ak_column`)
        from columnflow.columnar_util import EMPTY_FLOAT, Route, set_ak_column

        # import additional producer to attach behavior of four momenta
        # to current array
        from columnflow.production.util import attach_coffea_behavior

        # import numpy and awkward
        np = maybe_import("numpy")
        ak = maybe_import("awkward")

        # create short hand to add columns containing floats with float32 precision
        import functools
        set_ak_column_f32 = functools.partial(set_ak_column, value_type=np.float32)


        # call decorrator to create new Producer instance and wrap function
        # that calculates the observable with it. The name of the Producer
        # instance is the same as the name of the function
        @producer(
            # Define which columns are needed to calculate the observable.
            # In this example, we use the four momenta information as well
            # as everything that the attach_coffea_behavior Producer needs
            uses={
                "nJet", "Jet.pt", "Jet.mass", "Jet.eta", "Jet.phi", "Jet.e",
                attach_coffea_behavior,
            },
            # Define which columns are produced. In this case, we create a
            # new field `hardest_dijet`, which will have one property `mass`
            produces={
                "hardest_dijet.mass",
            },
        )
        def dijet_mass(self: Producer, events: ak.Array, **kwargs) -> ak.Array:

            # attach four momenta behavior for the field `Jet`
            # for more information see documentation of attach_coffea_behavior
            events = self[attach_coffea_behavior](events, collections=["Jet"], **kwargs)

            # now add the four momenta of the first two `Jet`s using the
            # `sum` function of coffea's LorentzVector class
            dijets = events.Jet[:, :2].sum(axis=1)

            # the `add` operation is only meaningful if we have at leas two
            # jets, so create a selection accordingly
            dijet_mask = ak.num(events.Jet, axis=1) >= 2

            # now add the new column to the array *events*. The column contains
            # the mass of the summed four momenta where there are at least
            # two jets, and the default value `EMPTY_FLOAT` otherwise.
            events = set_ak_column_f32(
                events,
                "hardest_dijet.mass",
                ak.where(dijet_mask, dijets.mass, EMPTY_FLOAT),
            )

            return events

    This allows for a seemless combination of different :py:class:`Producer`
    modules, such as :py:class:`~columnflow.production.util.attach_coffea_behavior`.
    """
    @classmethod
    def producer(
        cls,
        func: Callable | None = None,
        bases=(),
        mc_only: bool = False,
        data_only: bool = False,
        **kwargs,
    ) -> DerivableMeta | Callable:
        """Decorator for creating a new :py:class:`Producer` subclass with
        additional, optional *bases* and attaching the decorated function to it
        as ``call_func``. When *mc_only* (*data_only*) is *True*, the producer
        is skipped and not considered by other calibrators, selectors and
        producers in case they are evalauted on an
        :py:class:`order.dataset.Dataset` whose ``is_mc`` attribute is *False* (*True*).

        All additional *kwargs* are added as class members of the new subclasses.

        :param func: Callable function that produces new columns, defaults to None
        :type func: Callable | None, optional
        :param bases: Additional bases for new Producer instance, defaults to ()
        :type bases: tuple, optional
        :param mc_only: boolean flag indicating that this Producer instance
            should only run on Monte Carlo simulation, defaults to False
        :type mc_only: bool, optional
        :param data_only: boolean flag indicating that this Producer instance
            should only run on observed data, defaults to False
        :type data_only: bool, optional
        :return: new Producer instance
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

                # get the producer name
                cls_name = cls_dict.pop("cls_name", func.__name__)

                # optionally add skip function
                if mc_only and data_only:
                    raise Exception(f"producer {cls_name} received both mc_only and data_only")
                if mc_only or data_only:
                    if cls_dict.get("skip_func"):
                        raise Exception(
                            f"producer {cls_name} received custom skip_func, but mc_only or data_only are set",
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
producer = Producer.producer
