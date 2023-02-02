# coding: utf-8

"""
Trigger selection methods.
"""

from columnflow.selection import Selector, SelectionResult, selector
from columnflow.util import maybe_import
from columnflow.columnar_util import set_ak_column


np = maybe_import("numpy")
ak = maybe_import("awkward")


@selector(
    uses={
        # nano columns
        "nTrigObj", "TrigObj.id", "TrigObj.pt", "TrigObj.eta", "TrigObj.phi", "TrigObj.filterBits",
    },
    produces={
        # new columns
        "trigger_ids",
    },
    exposed=True,
)
def trigger_selection(
    self: Selector,
    events: ak.Array,
    **kwargs,
) -> tuple[ak.Array, SelectionResult]:
    """
    HLT trigger path selection.
    """
    any_fired = False
    trigger_data = []
    trigger_ids = []

    # index of TrigObj's to repeatedly convert masks to indices
    index = ak.local_index(events.TrigObj)

    for trigger in self.config_inst.x.triggers:
        # skip the trigger if it does not apply to the dataset
        if not trigger.applies_to_dataset(self.dataset_inst):
            continue

        # get bare decisions
        fired = events.HLT[trigger.hlt_field] == 1
        any_fired = any_fired | fired

        # get trigger objects for fired events per leg
        leg_masks = []
        all_legs_match = True
        for leg in trigger.legs:
            # start with a True mask
            leg_mask = abs(events.TrigObj.id) >= 0
            # pdg id selection
            if leg.pdg_id is not None:
                leg_mask = leg_mask & (abs(events.TrigObj.id) == leg.pdg_id)
            # pt cut
            if leg.min_pt is not None:
                leg_mask = leg_mask & (events.TrigObj.pt >= leg.min_pt)
            # trigger bits match
            if leg.trigger_bits is not None:
                # OR across bits themselves, AND between all decision in the list
                for bits in leg.trigger_bits:
                    leg_mask = leg_mask & ((events.TrigObj.filterBits & bits) > 0)
            leg_masks.append(index[leg_mask])
            # at least one object must match this leg
            all_legs_match = all_legs_match & ak.any(leg_mask, axis=1)

        # final trigger decision
        fired_and_all_legs_match = fired & all_legs_match

        # store all intermediate results for subsequent selectors
        trigger_data.append((trigger, fired_and_all_legs_match, leg_masks))

        # store the trigger id
        ids = np.where(np.asarray(fired_and_all_legs_match), np.float32(trigger.id), None)
        trigger_ids.append(ak.singletons(ak.Array(ids)))

    # store the fired trigger ids
    trigger_ids = ak.concatenate(trigger_ids, axis=1)
    events = set_ak_column(events, "trigger_ids", trigger_ids, value_type=np.int32)

    return events, SelectionResult(
        steps={
            "trigger": any_fired,
        },
        aux={
            "trigger_data": trigger_data,
        },
    )


@trigger_selection.init
def trigger_selection_init(self: Selector) -> None:
    if getattr(self, "dataset_inst", None) is None:
        return

    # full used columns
    self.uses |= {
        trigger.name
        for trigger in self.config_inst.x.triggers
        if trigger.applies_to_dataset(self.dataset_inst)
    }
