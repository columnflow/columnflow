# coding: utf-8

"""
Column production methods related to the PDF weights.
"""

import functools

import law

from columnflow.production import Producer, producer
from columnflow.util import maybe_import
from columnflow.columnar_util import set_ak_column


np = maybe_import("numpy")
ak = maybe_import("awkward")

logger = law.logger.get_logger(__name__)

# helper
set_ak_column_f32 = functools.partial(set_ak_column, value_type=np.float32)


@producer(
    uses={"LHEPdfWeight"},
    produces={
        "pdf_weight", "pdf_weight_up", "pdf_weight_down",
    },
    # only run on mc
    mc_only=True,
)
def pdf_weights(self: Producer, events: ak.Array, **kwargs) -> ak.Array:
    """
    Producer that determines the pdf up and down variations on an event-by-event basis.
    This producer assumes that the nominal entry is always the first LHEPdfWeight value and
    that the nominal weight is already included in the LHEWeight.
    Can only be called with MC datasets.

    Resources:

       - https://arxiv.org/pdf/1510.03865.pdf
    """
    # check for the correct amount of weights
    n_weights = ak.num(events.LHEPdfWeight, axis=1)
    bad_mask = (n_weights != 101) & (n_weights != 103)
    if ak.any(bad_mask):
        bad_values = set(n_weights[bad_mask])
        raise Exception(
            "the number of LHEPdfWeights is expected to be 101 or 103, but also found values " +
            f"{bad_values} in dataset {self.dataset_inst.name}",
        )

    # log a message if nominal != 1
    pdf_weight_nominal = events.LHEPdfWeight[:, 0]
    if ak.any(pdf_weight_nominal != 1):
        bad_values = set(pdf_weight_nominal[pdf_weight_nominal != 1])
        logger.debug(
            "The nominal LHEPdfWeight is expected to be 1 but also found values " +
            f"{bad_values} in dataset {self.dataset_inst.name}. All variations will be " +
            "normalized to the nominal LHEPdfWeight and it is assumed that the nominal " +
            "weight is already included in the LHEWeight.",
        )

    # normalize all weights by the nominal one, assumed to be the first value
    pdf_weights = events.LHEPdfWeight[:, 1:101] / pdf_weight_nominal
    pdf_weights = ak.sort(pdf_weights, axis=1)

    # PDF uncertainty as half the width of the central 68% CL
    stddev = (pdf_weights[:, 83] - pdf_weights[:, 15]) / 2

    # store columns
    events = set_ak_column_f32(events, "pdf_weight", ak.ones_like(events.event))
    events = set_ak_column_f32(events, "pdf_weight_up", 1 + stddev)
    events = set_ak_column_f32(events, "pdf_weight_down", 1 - stddev)

    if ak.any(pdf_weight_nominal == 0):
        # set all pdf weights to 0 when the nominal pdf weight is 0
        invalid_pdf_weight = (pdf_weight_nominal == 0)
        logger.warning(
            f"In dataset {self.dataset_inst.name}, {ak.sum(invalid_pdf_weight)} nominal " +
            "LHEPdfWeights with values 0 have been found. The nominal/up/down pdf_weight "
            "columns have been set to 0 for these events.",
        )
        set_ak_column(events, "pdf_weight", ak.where(invalid_pdf_weight, 0, events.pdf_weight))
        set_ak_column(events, "pdf_weight_up", ak.where(invalid_pdf_weight, 0, events.pdf_weight_up))
        set_ak_column(events, "pdf_weight_down", ak.where(invalid_pdf_weight, 0, events.pdf_weight_down))

    return events
