import unittest
import os
from pathlib import Path

this_dir = os.path.dirname(os.path.abspath(__file__))

from columnflow.util import maybe_import
from columnflow.production.cms.supercluster_eta import photon_sceta, electron_sceta

np = maybe_import("numpy")
ak = maybe_import("awkward")


class SCEtaProducerTests(unittest.TestCase):

    @classmethod
    def setUpClass(self):
        self.photon_producer = photon_sceta()
        self.electron_producer = electron_sceta()
        ref_data_path = Path(os.path.join(
            this_dir, "data", "test_prod_sceta.json",
        ))

        self.ref_data = ak.from_json(ref_data_path)

    def test_photon_sceta(self):
        foo = self.photon_producer(self.ref_data)

        def cap_digits_in_list(in_list, n=2):
            round_lambda = lambda x: round(x, n)
            return [list(map(round_lambda, x)) for x in in_list]

        self.assertListEqual(
            cap_digits_in_list(foo.Photon.superclusterEta.to_list()),
            cap_digits_in_list(self.ref_data.Photon.superclusterEta.to_list()),
        )
