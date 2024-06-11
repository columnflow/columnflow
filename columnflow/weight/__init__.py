# coding: utf-8

"""
Tools for producing new columns to be used as event or object weights.
"""

from __future__ import annotations

import inspect

from columnflow.types import Callable
from columnflow.util import DerivableMeta
from columnflow.columnar_util import TaskArrayFunction


class WeightProducer(TaskArrayFunction):
    """
    Base class for all weight producers, i.e., functions that produce and return a single column
    that is meant to be used as a per-event or per-object weight.
    """

    exposed = True

    @classmethod
    def weight_producer(
        cls,
        func: Callable | None = None,
        bases: tuple = (),
        mc_only: bool = False,
        data_only: bool = False,
        **kwargs,
    ) -> DerivableMeta | Callable:
        """
        Decorator for creating a new :py:class:`WeightProducer` subclass with additional, optional
        *bases* and attaching the decorated function to it as :py:meth:`~WeightProducer.call_func`.

        When *mc_only* (*data_only*) is *True*, the weight producer is skipped and not considered by
        other calibrators, selectors and producers in case they are evaluated on a
        :py:class:`order.Dataset` (using the :py:attr:`dataset_inst` attribute) whose ``is_mc``
        (``is_data``) attribute is *False*.

        When *nominal_only* is *True* or *shifts_only* is set, the producer is skipped and not
        considered by other calibrators, selectors and producers in case they are evaluated on a
        :py:class:`order.Shift` (using the :py:attr:`global_shift_inst` attribute) whose name does
        not match.

        All additional *kwargs* are added as class members of the new subclasses.

        :param func: Function to be wrapped and integrated into new :py:class:`WeightProducer`
            class.
        :param bases: Additional bases for the new :py:class:`WeightProducer`.
        :param mc_only: Boolean flag indicating that this :py:class:`WeightProducer` should only run
            on Monte Carlo simulation and skipped for real data.
        :param data_only: Boolean flag indicating that this :py:class:`WeightProducer` should only
            run on real data and skipped for Monte Carlo simulation.
        :return: New :py:class:`WeightProducer` subclass.
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

            # get the producer name
            cls_name = cls_dict.pop("cls_name", func.__name__)

            # hook to update the class dict during class derivation
            def update_cls_dict(cls_name, cls_dict, get_attr):
                mc_only = get_attr("mc_only")
                data_only = get_attr("data_only")

                # optionally add skip function
                if mc_only and data_only:
                    raise Exception(
                        f"weight producer {cls_name} received both mc_only and data_only",
                    )
                if mc_only or data_only:
                    if cls_dict.get("skip_func"):
                        raise Exception(
                            f"weight producer {cls_name} received custom skip_func, but either "
                            "mc_only or data_only are set",
                        )

                    def skip_func(self):
                        # check mc_only and data_only
                        if getattr(self, "dataset_inst", None):
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
weight_producer = WeightProducer.weight_producer
