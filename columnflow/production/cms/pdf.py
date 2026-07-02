# coding: utf-8

"""
Column production methods related to the PDF weights.
"""

from __future__ import annotations

import functools

import law

from columnflow.production import Producer, producer
from columnflow.util import maybe_import, StrEnum
from columnflow.columnar_util import set_ak_column, full_like, fill_at

np = maybe_import("numpy")
ak = maybe_import("awkward")


logger = law.logger.get_logger(__name__)

# helpers
set_ak_column_f32 = functools.partial(set_ak_column, value_type=np.float32)
fill_at_f32 = functools.partial(fill_at, value_type=np.float32)


def _raise_unknown_action(attr: str, action: str, known_actions: tuple[str]) -> None:
    if action not in known_actions:
        raise ValueError(f"unknown {attr} '{action}', known values are {','.join(known_actions)}")


class PDFWeightOutput(StrEnum):
    """
    Flag to denote which type of weights to output in the :py:class:`pdf_weights` producer. Options:

        - ``combined``: Produces `pdf_weight{,_up,_down}`, applying some algorithm per event to combine all weights into
            an up/down variation pair.
        - ``raw``: Produces all `pdf_weights_{hessian,alphas}`.
    """

    combined = "combined"
    raw = "raw"


@producer(
    uses={"LHEPdfWeight"},
    # which types of weights to produce (influences produced columns, defined in init)
    pdf_weight_output=PDFWeightOutput.raw,
    # only run on mc
    mc_only=True,
)
def pdf_weights(
    self: Producer,
    events: ak.Array,
    invalid_weights_action: str = "raise",
    outlier_threshold: float = 0.5,
    outlier_action: str = "ignore",
    outlier_log_mode: str = "warning",
    **kwargs,
) -> ak.Array:
    """
    Producer that determines and stores pdf weights with different methods.

    The producer assumes that the nominal entry is always the first LHEPdfWeight value and that the nominal weight is
    already included in the LHEWeight. It can only be called with MC datasets.

    The *invalid_weights_action* defines the procedure of how to handle events with a an unexpected number of pdf
    weights (not 101 or 103). Supported modes are:

        - ``"raise"``: an exception is raised
        - ``"ignore"``: nominal weight is set to one, up/down weights too if not storing all weights
            and an empty vector for all weights otherwise

    The *outlier_action* defines the procedure of how to handle events with a pdf uncertainty above the
    *outlier_threshold*. Supported modes are:

        - ``"ignore"``: events are kept unmodified
        - ``"remove"``: pdf weight nominal/up/down are all set to 0
        - ``"raise"``: an exception is raised

    Additionally, the verbosity of the procedure can be set with *outlier_log_mode*, which offers the following options:

        - ``"none"``: no message is given
        - ``"info"``: a `logger.info` message is given
        - ``"debug"``: a `logger.debug` message is given
        - ``"warning"``: a `logger.warning` message is given

    Resources:
        - https://arxiv.org/pdf/1510.03865.pdf
    """
    _raise_unknown_action("invalid_weights_action", invalid_weights_action, ("ignore", "raise"))
    _raise_unknown_action("outlier_action", outlier_action, ("ignore", "remove", "raise"))
    _raise_unknown_action("outlier_log_mode", outlier_log_mode, ("none", "info", "debug", "warning"))

    # check for the correct amount of weights
    n_weights = ak.num(events.LHEPdfWeight, axis=1)
    invalid_mask = (n_weights != 101) & (n_weights != 103)

    # handle invalid number of weights when configured to raise
    if invalid_weights_action == "raise" and ak.any(invalid_mask):
        bad_values = ",".join(map(str, set(n_weights[invalid_mask])))
        frac = ak.mean(invalid_mask)
        raise ValueError(
            f"the number of LHEPdfWeights is expected to be 101 or 103, but also found numbers of '{bad_values}' in "
            f"{frac * 100:.1f}% of events in dataset {self.dataset_inst.name}",
        )

    # write ones in case there are no weights at all
    ones = np.ones(len(events), dtype=np.float32)
    empty = full_like(events.LHEPdfWeight[:, :0], 0)
    if ak.all(invalid_mask):
        logger.warning("no 'LHEPdfWeight' vector with correct length found, setting weights to 1")
        if self.pdf_weight_output == PDFWeightOutput.combined:
            events = set_ak_column_f32(events, "pdf_weight", ones)
            events = set_ak_column_f32(events, "pdf_weight_up", ones)
            events = set_ak_column_f32(events, "pdf_weight_down", ones)
        else:
            events = set_ak_column_f32(events, "pdf_weights_hessian", empty)
            events = set_ak_column_f32(events, "pdf_weights_alphas", empty)
        return events

    # complain when the number of weights is unexpected
    if ak.any(invalid_mask):
        bad_values = ",".join(map(str, set(n_weights[invalid_mask])))
        frac = ak.sum(invalid_mask) / len(events) * 100
        logger.warning(
            f"the number of LHEPdfWeights is expected to be 101 or 103, but also found values '{bad_values}' in "
            f"dataset '{self.dataset_inst.name}', will set pdf weights to 1 for these events ({frac:.2f}%)",
        )

    # log a message if nominal != 1
    pdf_weight_nominal = ak.fill_none(ak.pad_none(events.LHEPdfWeight, 1, axis=1), 1)
    pdf_weight_nominal = ak.without_parameters(ak.values_astype(pdf_weight_nominal, np.float32))[:, 0]
    if ak.any(pdf_weight_nominal != 1):
        bad_values = ",".join(map(str, set(pdf_weight_nominal[pdf_weight_nominal != 1])))
        logger.debug(
            f"the nominal LHEPdfWeight is expected to be 1 but also found values '{bad_values}' in dataset "
            f"'{self.dataset_inst.name}'; all variations will be normalized to the nominal LHEPdfWeight and it is "
            "assumed that the nominal weight is already included in the LHEWeight",
        )

    # normalize all weights by the nominal one
    pdf_weights = ak.where(invalid_mask, empty, ak.without_parameters(events.LHEPdfWeight[:, 1:]))
    pdf_weights = pdf_weights / pdf_weight_nominal

    if self.pdf_weight_output == PDFWeightOutput.combined:
        # store the nominal weight which is always 1 after normalization
        events = set_ak_column_f32(events, "pdf_weight", ones)

        # no treatment of alphas weights for the combined treatment, so only consider the first 100 variations
        pdf_weights = pdf_weights[:, :100]

        # PDF uncertainty as half the width of the central 68% CL
        pdf_weights = ak.sort(pdf_weights, axis=1)
        stddev = (pdf_weights[:, 83:84] - pdf_weights[:, 15:16]) / 2
        stddev = ak.fill_none(ak.pad_none(stddev, 1, axis=1, clip=True), 0)[:, 0]
        stddev = ak.values_astype(ak.where(invalid_mask, 0, stddev), np.float32)

        # store columns
        events = set_ak_column_f32(events, "pdf_weight_up", 1 + stddev)
        events = set_ak_column_f32(events, "pdf_weight_down", 1 - stddev)

        # handle outliers by identifying large, relative variations
        if ak.any(outlier_mask := stddev > outlier_threshold):
            occurances = ak.sum(outlier_mask)
            frac = occurances / len(stddev) * 100
            msg = (
                f"in dataset {self.dataset_inst.name}, there are {occurances} ({frac:.2f}%) entries with pdf "
                f"uncertainty above {outlier_threshold * 100:.0f}%"
            )
            if outlier_action == "raise":
                raise Exception(msg)
            if outlier_action == "remove":
                # set all pdf weights to 0 when the *outlier_threshold* is passed
                events = fill_at_f32(events, outlier_mask, "pdf_weight", 0)
                events = fill_at_f32(events, outlier_mask, "pdf_weight_up", 0)
                events = fill_at_f32(events, outlier_mask, "pdf_weight_down", 0)
                msg += "; the nominal/up/down pdf_weight columns have been set to 0 for these events"
            # trigger a specific log
            msg_func = {
                "none": lambda msg: None,
                "info": logger.info,
                "warning": logger.warning,
                "debug": logger.debug,
            }[outlier_log_mode]
            msg_func(msg)

        # handle invalid values
        if ak.any(invalid_pdf_weight := pdf_weight_nominal == 0):
            # set all pdf weights to 0 when the nominal pdf weight is 0
            logger.warning(
                f"in dataset {self.dataset_inst.name}, {ak.sum(invalid_pdf_weight)} nominal LHEPdfWeights with values "
                "0 have been found; the nominal/up/down pdf_weight columns have been set to 0 for these events",
            )
            events = fill_at_f32(events, invalid_pdf_weight, "pdf_weight", 0)
            events = fill_at_f32(events, invalid_pdf_weight, "pdf_weight_up", 0)
            events = fill_at_f32(events, invalid_pdf_weight, "pdf_weight_down", 0)

    else:  # raw
        # mask to select events with alphas weights
        has_alphas = n_weights == 103

        # log if events have pdf weights but miss alpha_s
        if ak.any(~has_alphas):
            frac_abs = float(ak.mean(has_alphas)) * 100
            frac_rel = float(ak.mean(has_alphas[~invalid_mask])) * 100
            logger.warning(
                f"in dataset {self.dataset_inst.name}, only {frac_abs:.2f}% of the events have valid alpha_s weights "
                f"({frac_rel:.2f}% of those with valid pf weights);  issing ones will be filled with 1",
            )

        # store hessian weights
        events = set_ak_column_f32(events, "pdf_weights_hessian", pdf_weights[:, :100])

        # create and store alphas weights, filled with 1 when missing
        alphas_weights = ak.where(
            has_alphas,
            pdf_weights[:, 100:102],
            full_like(events.LHEPdfWeight[:, :2], 1.0, dtype=np.float32),
        )[:, [0, 1]]
        events = set_ak_column_f32(events, "pdf_weights_alphas", alphas_weights)

    return events


@pdf_weights.init
def pdf_weight_init(self: Producer, **kwargs) -> None:
    super(pdf_weights, self).init_func(**kwargs)

    # add produced columns
    if self.pdf_weight_output == PDFWeightOutput.combined:
        self.produces.add(r"pdf_weight{,_up,_down}")
    else:  # raw
        self.produces.add(r"pdf_weights_{hessian,alphas}")


pdf_weights_raw = pdf_weights.derive("pdf_weights_raw", cls_dict={
    "pdf_weight_output": PDFWeightOutput.raw,
})
