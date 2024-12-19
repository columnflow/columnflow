# coding: utf-8

"""
Jet-related quantities
"""

from __future__ import annotations

from columnflow.production import Producer, producer
from columnflow.util import maybe_import
from columnflow.columnar_util import set_ak_column

np = maybe_import("numpy")
ak = maybe_import("awkward")
coffea = maybe_import("coffea")


@producer(
    # uses, produces are specified in the init function
    jet_name="FatJet",
    subjet_name="SubJet",
    output_column="msoftdrop",
)
def msoftdrop(
    self: Producer,
    events: ak.Array,
    **kwargs,
) -> ak.Array:
    """
    Recalculates the softdrop mass for a given jet collection by computing the four-vector
    sum of the corresponding subjets.

    The *jet_name* parameter indicates the jet collection for which to recompute the softdrop mass.
    and the *subjet_name* parameter indicates the name of the corresponding subjet collection.
    The fields ``<jet_name>.subJetIdx1`` and ``<jet_name>.subJetIdx2`` must exist in the input array
    and yield the indices of the subjets matched to the main jets.
    """
    # retrieve input collections
    jet = events[self.jet_name]
    subjet = events[self.subjet_name]

    valid_subjets = []
    for subjet_idx_column in self.subjet_idx_columns:
        # mask negative subjet indices (= no subjet)
        subjet_idx = jet[subjet_idx_column]
        valid_subjet_idxs = ak.mask(subjet_idx, subjet_idx >= 0)

        # pad list of subjets to prevent index error on lookup
        padded_subjet = ak.pad_none(subjet, ak.max(valid_subjet_idxs) + 1)

        # retrieve subjets for each jet
        valid_subjet = padded_subjet[valid_subjet_idxs]
        valid_subjets.append(ak.singletons(valid_subjet, axis=-1))

    # merge lists for all sibjet index columns
    valid_subjets = ak.concatenate(valid_subjets, axis=-1)

    # attach coffea behavior so we can do LV arithmetic
    valid_subjets = ak.with_name(
        valid_subjets,
        "PtEtaPhiMLorentzVector",
        behavior=coffea.nanoevents.NanoAODSchema.behavior(),
    )

    # recompute softdrop mass from LV sum
    valid_subjets_sum = valid_subjets.sum(axis=-1)
    msoftdrop = valid_subjets_sum.mass

    # set softdrop mass to -1 for jets that do not have
    # exactly 2 valid subjets
    n_valid_subjets = ak.num(valid_subjets, axis=-1)
    msoftdrop_masked = ak.where(
        n_valid_subjets == 2,
        msoftdrop,
        -1,
    )

    # store the softdrop mass in the specified output column
    events = set_ak_column(events, f"{self.jet_name}.{self.output_column}", msoftdrop_masked, value_type=np.float32)

    # return the event array
    return events


@msoftdrop.init
def msoftdrop_init(self: Producer, **kwargs) -> None:
    """
    Dynamically add `uses` and `produces`
    """
    # input columns
    self.uses |= {
        f"{collection}.{var}"
        for collection in (self.jet_name, self.subjet_name)
        for var in ("pt", "eta", "phi", "mass")
    }

    self.subjet_idx_columns = ["subJetIdx1", "subJetIdx2"]
    self.uses |= {
        f"{self.jet_name}.{subjet_idx_column}"
        for subjet_idx_column in self.subjet_idx_columns
    }

    # outputs
    self.produces = {f"{self.jet_name}.{self.output_column}"}
