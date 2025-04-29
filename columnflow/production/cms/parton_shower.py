# coding: utf-8

"""
Producers for storing parton shower weights.
"""

from __future__ import annotations

from columnflow.production import producer, Producer
from columnflow.columnar_util import set_ak_column, full_like
from columnflow.util import maybe_import, DotDict

ak = maybe_import("awkward")
np = maybe_import("numpy")


@producer(
    uses={"PSWeight"},
    produces={"{isr,fsr}_weight{,_up,_down}"},
    # only run on mc
    mc_only=True,
    # indices where to find weight variations in PSWeight
    indices=DotDict(
        isr_weight_up=0,
        fsr_weight_up=1,
        isr_weight_down=2,
        fsr_weight_down=3,
    ),
)
def ps_weights(
    self: Producer,
    events: ak.Array,
    invalid_weights_action: str = "raise",
    **kwargs,
) -> ak.Array:
    """
    Producer that reads out parton shower uncertainties on an event-by-event basis.

    The *invalid_weights_action* defines the procedure of how to handle events with missing or an unexpected number of
    weights. Supported modes are:

        - ``"raise"``: An exception is raised.
        - ``"ignore_one"``: Ignores cases where only a single weight is present and a weight of one is stored for all
            variations.
        - ``"ignore"``: Stores a weight of one for all missing weight variataions.

    Resources:
        - https://cms-nanoaod-integration.web.cern.ch/integration/master/mc94X_doc.html
    """
    known_actions = {"raise", "ignore_one", "ignore"}
    if invalid_weights_action not in known_actions:
        raise ValueError(
            f"unknown invalid_weights_action '{invalid_weights_action}', known values are {','.join(known_actions)}",
        )

    # setup nominal weights
    ones = np.ones(len(events), dtype=np.float32)
    events = set_ak_column(events, "fsr_weight", ones)
    events = set_ak_column(events, "isr_weight", ones)

    # check if weight variations are missing and if needed, pad them
    indices = self.indices
    ps_weights = events.PSWeight
    num_weights = ak.num(ps_weights, axis=1)
    max_index = max(indices.values())
    if ak.any(bad_mask := num_weights <= max_index):
        msg = ""
        if invalid_weights_action == "ignore":
            # pad weights
            ps_weights = ak.fill_none(ak.pad_none(ps_weights, max_index + 1, axis=1), 1.0, axis=1)
        elif invalid_weights_action == "ignore_one":
            # special treatment if there is only one weight
            if ak.all(num_weights == 1):
                ps_weights = full_like(ps_weights, 1.0)
                indices = {column: 0 for column in indices}
            else:
                msg = f"at least {max_index + 1} or exactly one"
        else:  # raise
            msg = f"at least {max_index + 1}"
        if msg:
            bad_values = ",".join(map(str, set(num_weights[bad_mask])))
            raise ValueError(
                f"the number of PSWeight values is expected to be {msg}, but also found numbers of '{bad_values}' in "
                f"{ak.mean(bad_mask) * 100:.1f}% of events in dataset {self.dataset_inst.name}",
            )

    # now loop through the names and save the respective normalized PSWeights
    for column, index in indices.items():
        events = set_ak_column(events, column, ps_weights[:, index])

    return events
