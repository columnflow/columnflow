import unittest

from columnflow.util import maybe_import
from columnflow.production.cms.supercluster_eta import photon_sceta, electron_sceta 

import order as od

np = maybe_import("numpy")
ak = maybe_import("awkward")

class SCEtaProducerTests(unittest.TestCase):

    @classmethod
    def setUpClass(self):
        self.photon_producer = photon_sceta()
        self.electron_producer = electron_sceta()
        ref_data_path = ak.from_json("tests/data/supercluster_eta.json")

        self.ref_data = ak.from_json(ref_data_path)

    def test_photon_sceta(self):
        foo = self.photon_producer(self.ref_data)
        self.assertListEqual(
            foo.Photon.superclusterEta.tolist(),
            self.ref_data.Photon.superclusterEta.tolist())
