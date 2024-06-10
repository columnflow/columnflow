# coding: utf-8

"""
Tools for producing new array columns (e.g. high-level variables).
"""

from __future__ import annotations

import inspect

from columnflow.types import Callable, Sequence
from columnflow.util import DerivableMeta
from columnflow.columnar_util import TaskArrayFunction
from columnflow.config_util import expand_shift_sources


class Producer(TaskArrayFunction):
    """
    Base class for all producers.
    """

    exposed = True

    @classmethod
    def producer(
        cls,
        func: Callable | None = None,
        bases: tuple = (),
        mc_only: bool = False,
        data_only: bool = False,
        nominal_only: bool = False,
        shifts_only: Sequence[str] | set[str] | None = None,
        **kwargs,
    ) -> DerivableMeta | Callable:
        """
        Decorator for creating a new :py:class:`Producer` subclass with additional, optional *bases*
        and attaching the decorated function to it as :py:meth:`~Producer.call_func`.

        When *mc_only* (*data_only*) is *True*, the producer is skipped and not considered by
        other calibrators, selectors and producers in case they are evaluated on a
        :py:class:`order.Dataset` (using the :py:attr:`dataset_inst` attribute) whose ``is_mc``
        (``is_data``) attribute is *False*.

        When *nominal_only* is *True* or *shifts_only* is set, the producer is skipped and not
        considered by other calibrators, selectors and producers in case they are evaluated on a
        :py:class:`order.Shift` (using the :py:attr:`global_shift_inst` attribute) whose name does
        not match.

        All additional *kwargs* are added as class members of the new subclasses.

        :param func: Function to be wrapped and integrated into new :py:class:`Producer` class.
        :param bases: Additional bases for the new :py:class:`Producer`.
        :param mc_only: Boolean flag indicating that this :py:class:`Producer` should only run on
            Monte Carlo simulation and skipped for real data.
        :param data_only: Boolean flag indicating that this :py:class:`Producer` should only run on
            real data and skipped for Monte Carlo simulation.
        :param nominal_only: Boolean flag indicating that this :py:class:`Producer` should only run
            on the nominal shift and skipped on any other shifts.
        :param shifts_only: Shift names that this :py:class:`Producer` should only run on, skipping
            all other shifts.
        :return: New :py:class:`Producer` subclass.
        """
        def decorator(func: Callable) -> DerivableMeta:
            # create the class dict
            cls_dict = {
                **kwargs,
                "call_func": func,
                "mc_only": mc_only,
                "data_only": data_only,
                "nominal_only": nominal_only,
                "shifts_only": shifts_only,
            }

            # get the module name
            frame = inspect.stack()[1]
            module = inspect.getmodule(frame[0])

            # get the producer name
            cls_name = cls_dict.pop("cls_name", func.__name__)

            # hook to update the class dict during class derivation
            def update_cls_dict(cls_name, cls_dict, get_attr):
                mc_only = get_attr("mc_only")
                data_only = get_attr("data_only")
                nominal_only = get_attr("nominal_only")
                shifts_only = get_attr("shifts_only")

                # prepare shifts_only
                if shifts_only:
                    shifts_only_expanded = set(expand_shift_sources(shifts_only))
                    if shifts_only_expanded != shifts_only:
                        shifts_only = shifts_only_expanded
                        cls_dict["shifts_only"] = shifts_only

                # optionally add skip function
                if mc_only and data_only:
                    raise Exception(f"producer {cls_name} received both mc_only and data_only")
                if nominal_only and shifts_only:
                    raise Exception(
                        f"producer {cls_name} received both nominal_only and shifts_only",
                    )
                if mc_only or data_only or nominal_only or shifts_only:
                    if cls_dict.get("skip_func"):
                        raise Exception(
                            f"producer {cls_name} received custom skip_func, but either mc_only, "
                            "data_only, nominal_only or shifts_only are set",
                        )

                    def skip_func(self):
                        # check mc_only and data_only
                        if getattr(self, "dataset_inst", None):
                            if mc_only and not self.dataset_inst.is_mc:
                                return True
                            if data_only and not self.dataset_inst.is_data:
                                return True

                        # check nominal_only and shifts_only
                        if getattr(self, "global_shift_inst", None):
                            if nominal_only and not self.global_shift_inst.is_nominal:
                                return True
                            if shifts_only and self.global_shift_inst.name not in shifts_only:
                                return True

                        # in all other cases, do not skip
                        return False

                    cls_dict["skip_func"] = skip_func

                return cls_dict

            cls_dict["update_cls_dict"] = update_cls_dict

            # create the subclass
            subclass = cls.derive(cls_name, bases=bases, cls_dict=cls_dict, module=module)

            return subclass

        return decorator(func) if func else decorator


# shorthand
producer = Producer.producer
