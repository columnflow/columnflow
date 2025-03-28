# coding: utf-8

"""
Tools for producing histograms and event-wise weights.
"""

from __future__ import annotations

import inspect

import law
import order as od

from columnflow.types import Callable
from columnflow.util import DerivableMeta, maybe_import
from columnflow.columnar_util import TaskArrayFunction
from columnflow.types import Any


hist = maybe_import("hist")


class HistProducer(TaskArrayFunction):
    """
    Base class for all histogram producers, i.e., functions that control the creation of histograms, event weights, and
    optional post-processing.

    .. py:attribute:: create_hist_func

        type: callable

        The registered function performing the custom histogram creation.

    .. py:attribute:: fill_hist_func

        type: callable

        The registered function performing the custom histogram filling.

    .. py:attribute:: post_process_hist_func

        type: callable

        The registered function for performing an optional post-processing of histograms before they are saved.

    .. py:attribute:: post_process_merged_hist_func

        type: callable

        The registered function for performing an optional post-processing of histograms after they are merged.
    """

    # class-level attributes as defaults
    create_hist_func = None
    fill_hist_func = None
    post_process_hist_func = None
    post_process_merged_hist_func = None
    exposed = True

    @classmethod
    def hist_producer(
        cls,
        func: Callable | None = None,
        bases: tuple = (),
        mc_only: bool = False,
        data_only: bool = False,
        **kwargs,
    ) -> DerivableMeta | Callable:
        """
        Decorator for creating a new :py:class:`HistProducer` subclass with additional, optional *bases* and attaching
        the decorated function to it as :py:meth:`~HistProducer.call_func`.

        When *mc_only* (*data_only*) is *True*, the hist producer is skipped and not considered by other task array
        functions in case they are evaluated on a :py:class:`order.Dataset` (using the :py:attr:`dataset_inst`
        attribute) whose ``is_mc`` (``is_data``) attribute is *False*.

        All additional *kwargs* are added as class members of the new subclasses.

        :param func: Function to be wrapped and integrated into new :py:class:`HistProducer` class.
        :param bases: Additional bases for the new hist producer.
        :param mc_only: Boolean flag indicating that this hist producer should only run on Monte Carlo simulation and
            skipped for real data.
        :param data_only: Boolean flag indicating that this hist producer should only run on real data and skipped for
            Monte Carlo simulation.
        :return: New hist producer subclass.
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
                    raise Exception(f"hist producer {cls_name} received both mc_only and data_only")

                if mc_only or data_only:
                    if cls_dict.get("skip_func"):
                        raise Exception(
                            f"hist producer {cls_name} received custom skip_func, but either mc_only or data_only "
                            "are set",
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

    @classmethod
    def create_hist(cls, func: Callable[[dict], None]) -> None:
        """
        Decorator to wrap a function *func* that should be registered as :py:meth:`create_hist_func`. The function
        should accept two arguments:

            - *variables*, a list of :py:class:`order.Variable` instances (usually one).
            - *task*, the invoking task instance.

        The decorator does not return the wrapped function.
        """
        cls.create_hist_func = func

    @classmethod
    def fill_hist(cls, func: Callable[[dict], None]) -> None:
        """
        Decorator to wrap a function *func* that should be registered as :py:meth:`fill_hist_func`. The function should
        accept three arguments:

            - *h*, the histogram to fill.
            - *data*, a dictionary with data to fill.
            - *task*, the invoking task instance.

        The decorator does not return the wrapped function.
        """
        cls.fill_hist_func = func

    @classmethod
    def post_process_hist(cls, func: Callable[[dict], None]) -> None:
        """
        Decorator to wrap a function *func* that should be registered as :py:meth:`post_process_hist_func`. The function
        should accept two arguments:

            - *h*, the histogram to post process.
            - *task*, the invoking task instance.

        The decorator does not return the wrapped function.
        """
        cls.post_process_hist_func = func

    @classmethod
    def post_process_merged_hist(cls, func: Callable[[dict], None]) -> None:
        """
        Decorator to wrap a function *func* that should be registered as :py:meth:`post_process_merged_hist_func`. The
        function should accept two arguments:

            - *h*, the histogram to post process.
            - *task*, the invoking task instance.

        The decorator does not return the wrapped function.
        """
        cls.post_process_merged_hist_func = func

    def __init__(
        self,
        *args,
        create_hist_func: Callable | law.NoValue | None = law.no_value,
        fill_hist_func: Callable | law.NoValue | None = law.no_value,
        post_process_hist_func: Callable | law.NoValue | None = law.no_value,
        post_process_merged_hist_func: Callable | law.NoValue | None = law.no_value,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)

        # add class-level attributes as defaults for unset arguments (no_value)
        if create_hist_func == law.no_value:
            create_hist_func = self.__class__.create_hist_func
        if fill_hist_func == law.no_value:
            fill_hist_func = self.__class__.fill_hist_func
        if post_process_hist_func == law.no_value:
            post_process_hist_func = self.__class__.post_process_hist_func
        if post_process_merged_hist_func == law.no_value:
            post_process_merged_hist_func = self.__class__.post_process_merged_hist_func

        # when custom funcs are passed, bind them to this instance
        if create_hist_func:
            self.create_hist_func = create_hist_func.__get__(self, self.__class__)
        if fill_hist_func:
            self.fill_hist_func = fill_hist_func.__get__(self, self.__class__)
        if post_process_hist_func:
            self.post_process_hist_func = post_process_hist_func.__get__(self, self.__class__)
        if post_process_merged_hist_func:
            self.post_process_merged_hist_func = post_process_merged_hist_func.__get__(self, self.__class__)

    def run_create_hist(self, variables: list[od.Variable], task: law.Task) -> hist.Histogram:
        """
        Invokes the :py:meth:`create_hist_func` of this instance and returns its result, forwarding all arguments.
        """
        return self.create_hist_func(variables, task=task)

    def run_fill_hist(self, h: hist.Histogram, data: dict[str, Any], task: law.Task) -> None:
        """
        Invokes the :py:meth:`fill_hist_func` of this instance and returns its result, forwarding all arguments.
        """
        return self.fill_hist_func(h, data, task=task)

    def run_post_process_hist(self, h: hist.Histogram, task: law.Task) -> hist.Histogram:
        """
        Invokes the :py:meth:`post_process_hist_func` of this instance and returns its result, forwarding all arguments.
        """
        if not callable(self.post_process_hist_func):
            return h
        return self.post_process_hist_func(h, task=task)

    def run_post_process_merged_hist(self, h: hist.Histogram, task: law.Task) -> hist.Histogram:
        """
        Invokes the :py:meth:`post_process_merged_hist_func` of this instance and returns its result, forwarding all
        arguments.
        """
        if not callable(self.post_process_merged_hist_func):
            return h
        return self.post_process_merged_hist_func(h, task=task)


# shorthand
hist_producer = HistProducer.hist_producer
