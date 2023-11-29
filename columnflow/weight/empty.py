# coding: utf-8

"""
Empty event weight producer.
"""

from columnflow.weight import WeightProducer, weight_producer
from columnflow.util import maybe_import

np = maybe_import("numpy")
ak = maybe_import("awkward")


@weight_producer
def empty(self: WeightProducer, events: ak.Array, **kwargs) -> ak.Array:
    # simply return ones
    return ak.Array(np.ones(len(events), dtype=np.float32))
