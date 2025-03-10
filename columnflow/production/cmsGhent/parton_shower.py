from columnflow.production import producer, Producer
from columnflow.util import maybe_import, DotDict
from columnflow.production.cms.scale import set_ak_column_f32

ak = maybe_import("awkward")
np = maybe_import("numpy")


@producer(
    uses={
        "PSWeight",
    },
    produces={
        "fsr_weight", "fsr_weight_up", "fsr_weight_down",
        "isr_weight", "isr_weight_up", "isr_weight_down",
    },
    # only run on mc
    mc_only=True,
    indices=DotDict(
        isr_weight_up=0,
        fsr_weight_up=1,
        isr_weight_down=2,
        fsr_weight_down=3,
    ),
)
def ps_weights(self: Producer, events: ak.Array, **kwargs) -> ak.Array:
    """
    Producer that reads out parton shower uncertainties on an event-by-event basis.

    Resources:
        - https://cms-nanoaod-integration.web.cern.ch/integration/master/mc94X_doc.html
    """

    # setup nominal weights
    ones = np.ones(len(events), dtype=np.float32)
    events = set_ak_column_f32(events, "fsr_weight", ones)
    events = set_ak_column_f32(events, "isr_weight", ones)

    # now loop through the names and save the respective normalized PSWeights
    for column, index in self.indices.items():
        events = set_ak_column_f32(events, column, events.PSWeight[:, index])
    return events