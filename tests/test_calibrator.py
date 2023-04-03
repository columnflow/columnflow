__all__ = ["CalibratorUtilTest"]

import unittest

from columnflow.util import maybe_import
# from columnflow.calibration import util as calib_util

np = maybe_import("numpy")
ak = maybe_import("awkward")
dak = maybe_import("dask_awkward")
coffea = maybe_import("coffea")


class CalibratorUtilTest(unittest.TestCase):
    def __init__(self):
        self.random = np.array([5.])
