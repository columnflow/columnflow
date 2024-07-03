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
    outlier_action: str = "ignore",
    outlier_log_mode: str = "warning",
    **kwargs,
) -> ak.Array:
    """
    Producer that determines the pdf up and down variations on an event-by-event basis.
    This producer assumes that the nominal entry is always the first LHEPdfWeight value and
    that the nominal weight is already included in the LHEWeight.
    Can only be called with MC datasets.

    The *outlier_action* defines the procedure of how to handle events with a pdf
    uncertainty above the *outlier_threshold*. Supported modes are:

        - ``"ignore"``: events are kept unmodified
        - ``"remove"``: pdf weight nominal/up/down are all set to 0
        - ``"raise"``: an exception is raised

    Additionally, the verbosity of the procedure can be set with *outlier_log_mode*,
    which offers the following options:

        - ``"none"``: no message is given
        - ``"info"``: a `logger.info` message is given
        - ``"debug"``: a `logger.debug` message is given
        - ``"warning"``: a `logger.warning` message is given

    Resources:

        - https://arxiv.org/pdf/1510.03865.pdf
    """

    known_actions = ("ignore", "remove", "raise")
    if outlier_action not in known_actions:
        raise ValueError(
            f"unknown outlier_action '{outlier_action}', "
            f"known values are {','.join(known_actions)}",
        )
    known_log_modes = ("none", "info", "debug", "warning")
    if outlier_log_mode not in known_log_modes:
        raise ValueError(
            f"unknown outlier_log_mode '{outlier_log_mode}', "
            f"known values are {','.join(known_log_modes)}",
        )

    # check for the correct amount of weights
    n_weights = ak.num(events.LHEPdfWeight, axis=1)
    bad_mask = (n_weights != 101) & (n_weights != 103)

    # write ones in case there are no weights at all
    if ak.all(bad_mask):
        logger.warning("no valid 'LHEPdfWeight' found, saving ones for 'pdf_weights'")
        ones = np.ones(len(events), dtype=np.float32)
        for postfix in ["", "_up", "_down"]:
            events = set_ak_column_f32(events, f"pdf_weight{postfix}", ones)
        return events

    # complain when the number of weights is unexpected
    if ak.any(bad_mask):
        bad_values = ",".join(map(str, set(n_weights[bad_mask])))
        frac = ak.sum(bad_mask) / len(events) * 100
        logger.warning(
            "the number of LHEPdfWeights is expected to be 101 or 103, but also found values "
            f"'{bad_values}' in dataset {self.dataset_inst.name}, will set pdf weights to 1 for "
            f"these events ({frac:.2f}%)",
        )

    # log a message if nominal != 1
    pdf_weight_nominal = events.LHEPdfWeight[~bad_mask, 0]
    if ak.any(pdf_weight_nominal != 1):
        bad_values = ",".join(map(str, set(pdf_weight_nominal[pdf_weight_nominal != 1])))
        logger.debug(
            "the nominal LHEPdfWeight is expected to be 1 but also found values "
            f"'{bad_values}' in dataset {self.dataset_inst.name}; all variations will be "
            "normalized to the nominal LHEPdfWeight and it is assumed that the nominal "
            "weight is already included in the LHEWeight.",
        )

    # normalize all weights by the nominal one, assumed to be the first value
    pdf_weights = events.LHEPdfWeight[~bad_mask, 1:] / pdf_weight_nominal

    # PDF uncertainty as half the width of the central 68% CL
    pdf_weights = ak.sort(pdf_weights, axis=1)
    stddev = np.zeros(len(events), dtype=np.float32)
    stddev[~bad_mask] = (pdf_weights[:, 83] - pdf_weights[:, 15]) / 2

    # store columns
    events = set_ak_column_f32(events, "pdf_weight", np.ones(len(events), dtype=np.float32))
    events = set_ak_column_f32(events, "pdf_weight_up", 1 + stddev)
    events = set_ak_column_f32(events, "pdf_weight_down", 1 - stddev)

    outlier_mask = (stddev > outlier_threshold)
    if ak.any(outlier_mask):
        # catch events with large pdf variations
        occurances = ak.sum(outlier_mask)
        frac = occurances / len(stddev) * 100
        msg = (
            f"in dataset {self.dataset_inst.name}, there are {occurances} ({frac:.2f}%) "
            "entries with pdf uncertainty above 50%"
        )

        if outlier_action == "remove":
            # set all pdf weights to 0 when the *outlier_threshold* is passed
            events = set_ak_column_f32(events, "pdf_weight", ak.where(outlier_mask, 0, events.pdf_weight))
            events = set_ak_column_f32(events, "pdf_weight_up", ak.where(outlier_mask, 0, events.pdf_weight_up))
            events = set_ak_column_f32(events, "pdf_weight_down", ak.where(outlier_mask, 0, events.pdf_weight_down))

            msg += "; the nominal/up/down pdf_weight columns have been set to 0 for these events"
        elif outlier_action == "raise":
            raise Exception(msg)

        msg_func = {
            "none": lambda msg: None,
            "info": logger.info,
            "warning": logger.warning,
            "debug": logger.debug,
        }[outlier_log_mode]
        msg_func(msg)

    invalid_pdf_weight = (pdf_weight_nominal == 0)
    if ak.any(invalid_pdf_weight):
        # set all pdf weights to 0 when the nominal pdf weight is 0
        logger.warning(
            f"in dataset {self.dataset_inst.name}, {ak.sum(invalid_pdf_weight)} nominal "
            "LHEPdfWeights with values 0 have been found; the nominal/up/down pdf_weight "
            "columns have been set to 0 for these events",
        )
        events = set_ak_column_f32(events, "pdf_weight", ak.where(invalid_pdf_weight, 0, events.pdf_weight))
        events = set_ak_column_f32(events, "pdf_weight_up", ak.where(invalid_pdf_weight, 0, events.pdf_weight_up))
        events = set_ak_column_f32(events, "pdf_weight_down", ak.where(invalid_pdf_weight, 0, events.pdf_weight_down))

    return events
