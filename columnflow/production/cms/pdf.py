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
def pdf_weights(
    self: Producer,
    events: ak.Array,
    outlier_threshold: float = 0.5,
    outlier_mode: str = "keep",
    outlier_message_type: str = "warning",
    **kwargs,
) -> ak.Array:
    """
    Producer that determines the pdf up and down variations on an event-by-event basis.
    This producer assumes that the nominal entry is always the first LHEPdfWeight value and
    that the nominal weight is already included in the LHEWeight.
    Can only be called with MC datasets.

    The *outlier_mode* defines the prodcedure of how to handle events with a pdf uncertainty above
    the *outlier_threshold*. Supported modes are:

        - ``"keep"``: events are kept unmodified
        - ``"remove"``: pdf weight nominal/up/down are all set to 0, essentially removing the event

    The *outlier_message_type* allows to define the type of message that is given for datasets with outliers:

        - ``"ignore"``: no message is given
        - ``"info"``: a `logger.info` message is given
        - ``"debug"``: a `logger.debug` message is given
        - ``"warning"``: a `logger.warning` message is given
        - ``"raise"``: an Exception is raised

    Resources:

       - https://arxiv.org/pdf/1510.03865.pdf
    """

    assert outlier_mode in ("keep", "remove")
    assert outlier_message_type in ("ignore", "info", "debug", "warning", "raise")

    # check for the correct amount of weights
    n_weights = ak.num(events.LHEPdfWeight, axis=1)
    bad_mask = (n_weights != 101) & (n_weights != 103)
    if ak.any(bad_mask):
        bad_values = set(n_weights[bad_mask])
        raise Exception(
            "the number of LHEPdfWeights is expected to be 101 or 103, but also found values "
            f"{bad_values} in dataset {self.dataset_inst.name}",
        )

    # log a message if nominal != 1
    pdf_weight_nominal = events.LHEPdfWeight[:, 0]
    if ak.any(pdf_weight_nominal != 1):
        bad_values = set(pdf_weight_nominal[pdf_weight_nominal != 1])
        logger.debug(
            "The nominal LHEPdfWeight is expected to be 1 but also found values "
            f"{bad_values} in dataset {self.dataset_inst.name}. All variations will be "
            "normalized to the nominal LHEPdfWeight and it is assumed that the nominal "
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

    if ak.any(outlier_mask := (stddev > outlier_threshold)):
        # catch events with large pdf variations
        occurances = ak.sum(outlier_mask)
        frac = occurances / ak.count(stddev, axis=0) * 100
        msg = (
            f"In dataset {self.dataset_inst.name}, there are {occurances} ({frac:.2f}%) "
            "entries with pdf uncertainty above 50%"
        )

        if outlier_mode == "remove":
            # set all pdf weights to 0 when the *outlier_threshold* is passed
            events = set_ak_column_f32(events, "pdf_weight", ak.where(outlier_mask, 0, events.pdf_weight))
            events = set_ak_column_f32(events, "pdf_weight_up", ak.where(outlier_mask, 0, events.pdf_weight_up))
            events = set_ak_column_f32(events, "pdf_weight_down", ak.where(outlier_mask, 0, events.pdf_weight_down))

            msg += ". The nominal/up/down pdf_weight columns have been set to 0 for these events."

        if outlier_message_type == "raise":
            raise Exception(msg)

        msg_func = {
            "ignore": None,
            "info": logger.info,
            "warning": logger.warning,
            "debug": logger.debug,
            "raise": None,
        }[outlier_message_type]
        if msg_func:
            msg_func(msg)

    if ak.any(invalid_pdf_weight := (pdf_weight_nominal == 0)):
        # set all pdf weights to 0 when the nominal pdf weight is 0
        logger.warning(
            f"In dataset {self.dataset_inst.name}, {ak.sum(invalid_pdf_weight)} nominal "
            "LHEPdfWeights with values 0 have been found. The nominal/up/down pdf_weight "
            "columns have been set to 0 for these events.",
        )
        events = set_ak_column_f32(events, "pdf_weight", ak.where(invalid_pdf_weight, 0, events.pdf_weight))
        events = set_ak_column_f32(events, "pdf_weight_up", ak.where(invalid_pdf_weight, 0, events.pdf_weight_up))
        events = set_ak_column_f32(events, "pdf_weight_down", ak.where(invalid_pdf_weight, 0, events.pdf_weight_down))

    return events
