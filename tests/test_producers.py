import unittest

from columnflow.util import maybe_import
from columnflow.production.cms.supercluster_eta import photon_sceta, electron_sceta 

import order as od

np = maybe_import("numpy")
ak = maybe_import("awkward")

class SCEtaProducerTests(unittest.TestCase):
    def __init__(self):
        self.photon_producer = photon_sceta()
        self.electron_producer = electron_sceta()
        reference = ak.from_json('tests/data/supercluster_eta.json')

    def test_photon_sceta(self):
        