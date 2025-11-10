# coding: utf-8

"""
Object and event calibration tools.
"""

from __future__ import annotations

import inspect

from columnflow.types import Callable
from columnflow.util import DerivableMeta
from columnflow.columnar_util import TaskArrayFunction


class Calibrator(TaskArrayFunction):
    """
    Base class for all calibrators.
    """

    exposed = True

    # register attributes for arguments accepted by decorator
    mc_only: bool = False
    data_only: bool = False

    @classmethod
    def calibrator(
        cls,
        func: Callable | None = None,
        bases: tuple = (),
        mc_only: bool = False,
        data_only: bool = False,
        **kwargs,
    ) -> DerivableMeta | Callable:
        """
        Decorator for creating a new :py:class:`~.Calibrator` subclass with additional, optional
        *bases* and attaching the decorated function to it as ``call_func``.

        When *mc_only* (*data_only*) is *True*, the calibrator is skipped and not considered by
        other calibrators, selectors and producers in case they are evalauted on a
        :py:class:`order.Dataset` (using the :py:attr:`dataset_inst` attribute) whose ``is_mc``
        (``is_data``) attribute is *False*.

        All additional *kwargs* are added as class members of the new subclasses.

        :param func: Function to be wrapped and integrated into new :py:class:`Calibrator` class.
        :param bases: Additional bases for the new :py:class:`Calibrator`.
        :param mc_only: Boolean flag indicating that this :py:class:`Calibrator` should only run on
            Monte Carlo simulation and skipped for real data.
        :param data_only: Boolean flag indicating that this :py:class:`Calibrator` should only run
            on real data and skipped for Monte Carlo simulation.
        :return: New :py:class:`Calibrator` subclass.
        """
        def decorator(func: Callable) -> DerivableMeta:
            # create the class dict
            cls_dict = {
                **kwargs,
                "call_func": func,
                "mc_only": mc_only,
                "data_only": data_only,
            }

            # get the module name
            frame = inspect.stack()[1]
            module = inspect.getmodule(frame[0])

            # get the calibrator name
            cls_name = cls_dict.pop("cls_name", func.__name__)

            # hook to update the class dict during class derivation
            def update_cls_dict(cls_name, cls_dict, get_attr):
                mc_only = get_attr("mc_only")
                data_only = get_attr("data_only")

                # optionally add skip function
                if mc_only and data_only:
                    raise Exception(f"calibrator {cls_name} received both mc_only and data_only")
                if (mc_only or data_only) and cls_dict.get("skip_func"):
                    raise Exception(
                        f"calibrator {cls_name} received custom skip_func, but either mc_only or "
                        "data_only are set",
                    )

                if "skip_func" not in cls_dict:
                    def skip_func(self, **kwargs) -> bool:
                        # check mc_only and data_only
                        if mc_only and not self.dataset_inst.is_mc:
                            return True
                        if data_only and not self.dataset_inst.is_data:
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
calibrator = Calibrator.calibrator
