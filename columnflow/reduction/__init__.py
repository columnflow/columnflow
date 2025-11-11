# coding: utf-8

"""
Event and collection reduction objects.
"""

from __future__ import annotations

import inspect

from columnflow.calibration import TaskArrayFunctionWithCalibratorRequirements
from columnflow.util import DerivableMeta
from columnflow.types import Callable, Sequence


class Reducer(TaskArrayFunctionWithCalibratorRequirements):
    """
    Base class for all reducers.
    """

    exposed = True

    # register attributes for arguments accepted by decorator
    mc_only: bool = False
    data_only: bool = False

    @classmethod
    def reducer(
        cls,
        func: Callable | None = None,
        bases: tuple = (),
        mc_only: bool = False,
        data_only: bool = False,
        require_calibrators: Sequence[str] | set[str] | None = None,
        **kwargs,
    ) -> DerivableMeta | Callable:
        """
        Decorator for creating a new :py:class:`~.Reducer` subclass with additional, optional *bases* and attaching the
        decorated function to it as ``call_func``.

        When *mc_only* (*data_only*) is *True*, the reducer is skipped and not considered by other task array functions
        in case they are evalauted on a :py:class:`order.Dataset` (using the :py:attr:`dataset_inst` attribute) whose
        ``is_mc`` (``is_data``) attribute is *False*.

        All additional *kwargs* are added as class members of the new subclasses.

        :param func: Function to be wrapped and integrated into new :py:class:`Reducer` class.
        :param bases: Additional bases for the new reducer.
        :param mc_only: Boolean flag indicating that this reducer should only run on Monte Carlo simulation and skipped
            for real data.
        :param data_only: Boolean flag indicating that this reducer should only run on real data and skipped for Monte
            Carlo simulation.
        :param require_calibrators: Sequence of names of calibrators to add to the requirements.
        :return: New reducer subclass.
        """
        def decorator(func: Callable) -> DerivableMeta:
            # create the class dict
            cls_dict = {
                **kwargs,
                "call_func": func,
                "mc_only": mc_only,
                "data_only": data_only,
                "require_calibrators": require_calibrators,
            }

            # get the module name
            frame = inspect.stack()[1]
            module = inspect.getmodule(frame[0])

            # get the reducer name
            cls_name = cls_dict.pop("cls_name", func.__name__)

            # hook to update the class dict during class derivation
            def update_cls_dict(cls_name, cls_dict, get_attr):
                mc_only = get_attr("mc_only")
                data_only = get_attr("data_only")

                # optionally add skip function
                if mc_only and data_only:
                    raise Exception(f"reducer {cls_name} received both mc_only and data_only")
                if (mc_only or data_only) and cls_dict.get("skip_func"):
                    raise Exception(
                        f"reducer {cls_name} received custom skip_func, but either mc_only or data_only are set",
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
reducer = Reducer.reducer
