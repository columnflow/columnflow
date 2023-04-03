__all__ = ["CalibratorUtilTest"]

import unittest

from columnflow.util import maybe_import
from columnflow.calibration import util as calib_util

np = maybe_import("numpy")
ak = maybe_import("awkward")
dak = maybe_import("dask_awkward")
coffea = maybe_import("coffea")


class CalibratorUtilTest(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.jer_example = ak.Array([0.101, 0.118, 0.114, 0.129, 0.131, 0.143, 0.201, 0.264])
        self.rand_gen = np.random.Generator(np.random.SFC64([312004]))

    def test_ak_random(self):

        true_output = ak.Array([0.113, 0.0422, 0.298, -0.0233, -0.0865, -0.123, -0.201, -0.0599])

        this_output = calib_util.ak_random(0, self.jer_example, rand_func=self.rand_gen.normal)

        self.assertIsInstance(this_output, ak.Array)
        self.assertListEqual(
            this_output.to_list(),
            true_output.to_list(),
        )
