import unittest
import os

from itertools import chain, combinations

this_dir = os.path.dirname(os.path.abspath(__file__))

from columnflow.calibration.cms.egamma import EGammaCorrectionConfig, egamma_scale_corrector
from columnflow.util import maybe_import, DotDict
from columnflow.types import Any, Iterable
from columnflow.production import Producer

np = maybe_import("numpy")
ak = maybe_import("awkward")


"""
from columnflow.production.cms.supercluster_eta import photon_sceta, electron_sceta
from pathlib import Path

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
        from IPython import embed
        embed(header="test_photon_sceta")
        def cap_digits_in_list(in_list, n=2):
            round_lambda = lambda x: round(x, n)
            return [list(map(round_lambda, x)) for x in in_list]

        self.assertListEqual(
            cap_digits_in_list(foo.Photon.superclusterEta.to_list()),
            cap_digits_in_list(self.ref_data.Photon.superclusterEta.to_list()),
        )"""


class EGammaCalibratorTest(unittest.TestCase):

    @classmethod
    def setUpClass(self):

        self.necessary_keywords: dict[str, Any] = {
            "source_field": "foo",
            "get_correction_file": lambda x: "foo",
            "get_scale_config": lambda x: EGammaCorrectionConfig(),
            "dataset_inst": DotDict.wrap({"is_mc": False}),
        }

    def test_egamma_scale_corrector(self):
        # make sure that the egamma scale corrector is abstract and
        # doesn't work without the necessary properties

        with self.assertRaises(TypeError):
            egamma_scale_corrector()

        def powerset(iterable: Iterable[Any]) -> Iterable[Any]:
            """
            powerset([1,2,3]) --> () (1,) (2,) (3,) (1,2) (1,3) (2,3) (1,2,3)
            """
            s = list(iterable)
            return chain.from_iterable(combinations(s, r) for r in range(len(s)))

        for subset in powerset(self.necessary_keywords):
            with self.subTest(subset=subset):
                with self.assertRaises(TypeError):
                    foo: Producer = egamma_scale_corrector.derive(
                        "foo", cls_dict={
                            x: self.necessary_keywords[x] for x in subset
                        },
                    )
                    foo()

        # test that the corrector works with the necessary properties
        foo: Producer = egamma_scale_corrector.derive(
            "foo", cls_dict=self.necessary_keywords,
        )
        from IPython import embed
        embed(header="test_egamma_scale_corrector")
        foo()
