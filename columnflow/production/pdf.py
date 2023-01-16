# coding: utf-8

"""
Column production methods related to the PDF weights.
"""


from columnflow.production import Producer, producer
from columnflow.util import maybe_import
from columnflow.columnar_util import set_ak_column

ak = maybe_import("awkward")


@producer(
    uses={"LHEPdfWeight"},
    produces={
        "pdf_weight", "pdf_weight_up", "pdf_weight_down",
    },
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

    # stop here for data
    if self.dataset_inst.is_data:
        raise ValueError("attempt to determine pdf variations in data")

    n_weights = ak.num(events.LHEPdfWeight, axis=1)
    if ak.any(n_weights != 103) or ak.any(n_weights != 101):
        raise Exception(
            f"Number of LHEPdfWeights ({n_weights}) is not as expected (103 or 101) "
            f"in dataset {self.dataset_inst.name}",
        )

    # first LHEPdfWeight value: nominal weight
    if ak.any(events.LHEPdfWeight[:, 0] != 1):
        print(
            "The first entry of the LHEPdfWeight is not 1, but it is assumed "
            "that it is the nominal entry, which is already included in the "
            "LHEWeight.",
        )

    # the following 100 LHEPdfWeight values: pdf variations
    pdfweights = events.LHEPdfWeight[:, 1:101]
    pdfweights = ak.sort(pdfweights, axis=1)

    # PDF uncertainty as 68% CL
    stddev = (pdfweights[:, 83] - pdfweights[:, 15]) / 2

    # NOTE: use mean value as nominal pdf weight? or remove the necessity of adding this nominal weight?
    events = set_ak_column(events, "pdf_weight", ak.ones_like(events.event))
    events = set_ak_column(events, "pdf_weight_up", 1 + stddev)
    events = set_ak_column(events, "pdf_weight_down", 1 - stddev)

    return events
