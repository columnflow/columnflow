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
    determines the pdf up and down shifts
    Documentation: https://arxiv.org/pdf/1510.03865.pdf
    """

    # stop here for data
    if self.dataset_inst.is_data:
        return events

    N_pdfweights = ak.num(events.LHEPdfWeight, axis=1)
    if ak.any(N_pdfweights != 103) or ak.any(N_pdfweights != 101):
        raise Exception(f"Number of LHEPdfWeights ({N_pdfweights}) is not "
                        f"as expected (103 or 101) in dataset {self.dataset_inst.name}")

    # first 101 LHEPdfWeight values: pdf variations
    # NOTE: we should find out which weight corresponds to the nominal weight (should always be 1)
    pdfweights = events.LHEPdfWeight[:, :101]
    pdfweights = ak.sort(pdfweights)

    # PDF uncertainty by calculating the variance (underestimates uncertainty?)
    # var = ak.var(events.LHEPdfWeight, axis=1)
    # PDF uncertainty as 68% CL
    var = (pdfweights[:, 83] - pdfweights[:, 15]) / 2

    mean = ak.mean(events.LHEPdfWeight, axis=1)

    # NOTE: use mean value as nominal pdf weight? or remove the necessity of adding this nominal weight?
    events = set_ak_column(events, "pdf_weight", ak.ones_like(events.event))
    events = set_ak_column(events, "pdf_weight_down", mean + var)
    events = set_ak_column(events, "pdf_weight_up", mean - var)

    return events
