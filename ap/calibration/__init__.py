# coding: utf-8

"""
Object and event calibration tools.
"""

from typing import Optional, Union, Sequence, Set, Callable

import law


#: registered calibration functions, mapped to their names
calibration_functions = {}


def calibration_function(
    func: Optional[Callable] = None,
    *,
    name: Optional[str] = None,
    uses: Optional[Union[Sequence[str], Set[str], Callable, Sequence[Callable]]] = None,
) -> Callable:
    """
    Decorator for register a new calibration function. TODO
    """
    def decorator(func):
        # define the name and make sure it is not taken yet
        _name = name or func.__name__
        if _name in calibration_functions:
            raise ValueError(f"a calibration function named '{_name}' was already registered")

        # define columns to load
        load_columns = set()
        if uses:
            for obj in law.util.make_list(uses):
                # obj can itself be a calibration function, or one or multiple columns to load
                load_columns |= set(obj.load_columns if callable(obj) else law.util.make_list(obj))

        # store attributes directly on the function
        func.name = _name
        func.load_columns = load_columns

        # store it
        calibration_functions[_name] = func

        return func

    return decorator(func) if func else decorator


# import all calibration modules
import ap.calibration.test  # noqa
