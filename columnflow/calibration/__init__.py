# coding: utf-8

"""
Object and event calibration tools. For more information, see :ref:`base:summary`.
For convenience, :py:class:`~columnflow.calibration.base.Calibrator` and
:py:meth:`~columnflow.calibration.base.calibrator` can be imported directly, e.g.

.. code-block:: python

    from columnflow.calibration import Calibrator, calibrator
"""

__all__ = ["Calibrator", "calibrator"]

from .base import Calibrator, calibrator
