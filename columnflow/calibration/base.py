from __future__ import annotations

import inspect
from typing import Callable
from functools import wraps

from columnflow.util import DerivableMeta
from columnflow.columnar_util import TaskArrayFunction


class Calibrator(TaskArrayFunction):
    """Base class for all calibrations

    :param TaskArrayFunction: Base class :py:class:`TaskArrayFunction`
    :type TaskArrayFunction: :py:class:`TaskArrayFunction`
    :raises Exception: if errors occur within the decorator, see :py:meth:`calibrator`
    """

    @classmethod
    def calibrator(
        cls,
        func: Callable | None = None,
        bases=(),
        mc_only: bool = False,
        data_only: bool = False,
        **kwargs,
    ) -> DerivableMeta | Callable:
        """
        Decorator for creating a new :py:class:`~.Calibrator` subclass with additional, optional
        *bases* and attaching the decorated function to it as ``call_func``. When *mc_only*
        (*data_only*) is *True*, the calibrator is skipped and not considered by other calibrators,
        selectors and producers in case they are evalauted on an
        :external+order:py:class:`order.dataset.Dataset` whose ``is_mc`` attribute is
        ``False`` (``True``).

        All additional *kwargs* are added as class members of the new subclasses.
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

                # get the calibrator name
                cls_name = cls_dict.pop("cls_name", func.__name__)

                # optionally add skip function
                if mc_only and data_only:
                    raise Exception(f"calibrator {cls_name} received both mc_only and data_only")
                if mc_only or data_only:
                    if cls_dict.get("skip_func"):
                        raise Exception(
                            f"calibrator {cls_name} received custom skip_func, but mc_only or data_only are set",
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
calibrator = Calibrator.calibrator