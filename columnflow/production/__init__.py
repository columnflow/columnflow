# coding: utf-8

"""
Tools for producing new array columns (e.g. high-level variables).
"""

from __future__ import annotations

import inspect
from typing import Callable

from columnflow.util import DerivableMeta
from columnflow.columnar_util import TaskArrayFunction


class Producer(TaskArrayFunction):

    @classmethod
    def producer(
        cls,
        func: Callable | None = None,
        bases=(),
        mc_only: bool = False,
        data_only: bool = False,
        **kwargs,
    ) -> DerivableMeta | Callable:
        """
        Decorator for creating a new :py:class:`Producer` subclass with additional, optional *bases*
        and attaching the decorated function to it as ``call_func``. When *mc_only* (*data_only*) is
        *True*, the producer is skipped and not considered by other calibrators, selectors and
        producers in case they are evalauted on an :py:class:`order.Dataset` whose ``is_mc``
        attribute is *False* (*True*).

        All additional *kwargs* are added as class members of the new subclasses.
        """
        def decorator(func: Callable) -> DerivableMeta:
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

            return subclass

        return decorator(func) if func else decorator


# shorthand
producer = Producer.producer
