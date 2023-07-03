# coding: utf-8

"""
Tools for producing new array columns (e.g. high-level variables).
"""

from __future__ import annotations

import inspect
from typing import Callable, Sequence

from columnflow.util import DerivableMeta
from columnflow.columnar_util import TaskArrayFunction
from columnflow.config_util import expand_shift_sources


class Producer(TaskArrayFunction):

    @classmethod
    def producer(
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
        Decorator for creating a new :py:class:`Producer` subclass with additional, optional *bases*
        and attaching the decorated function to it as ``call_func``.

        When *mc_only* (*data_only*) is *True*, the calibrator is skipped and not considered by
        other calibrators, selectors and producers in case they are evalauted on a
        :py:class:`order.Dataset` (using the :py:attr:`dataset_inst` attribute) whose ``is_mc``
        (``is_data``) attribute is *False*.

        When *nominal_only* is *True* or *shifts_only* is set, the calibrator is skipped and not
        considered by other calibrators, selectors and producers in case they are evalauted on a
        :py:class:`order.Shift` (using the :py:attr:`global_shift_inst` attribute) whose name does
        not match.

        All additional *kwargs* are added as class members of the new subclasses.
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

            # get the producer name
            cls_name = cls_dict.pop("cls_name", func.__name__)

            # optionally add skip function
            if mc_only and data_only:
                raise Exception(f"producer {cls_name} received both mc_only and data_only")
            if mc_only or data_only or nominal_only:
                if cls_dict.get("skip_func"):
                    raise Exception(
                        f"producer {cls_name} received custom skip_func, but either mc_only, "
                        "data_only or nominal_only are set",
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
producer = Producer.producer
