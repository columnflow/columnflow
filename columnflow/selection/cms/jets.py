# coding: utf-8

"""
Selection modules for jets.
"""

from __future__ import annotations

import law
import math

from columnflow.selection import Selector, SelectionResult, selector
from columnflow.util import maybe_import, InsertableDict
from columnflow.columnar_util import set_ak_column, flat_np_view, optional_column as optional

np = maybe_import("numpy")
ak = maybe_import("awkward")


logger = law.logger.get_logger(__name__)


@selector(
    uses={
        "Jet.{pt,eta,phi,mass,jetId,chEmEF}", optional("Jet.puId"),
        "Muon.{pt,eta,phi,mass,isPFcand}",
    },
    produces={"Jet.veto_map_mask"},
    get_veto_map_file=(lambda self, external_files: external_files.jet_veto_map),
)
def jet_veto_map(
    self: Selector,
    events: ak.Array,
    **kwargs,
) -> tuple[ak.Array, SelectionResult]:
    """
    Selector that applies the Jet Veto Map to the jets and stores the result as a new column ``Jet.veto_maps``.
    Additionally, the ``jet_veto_map`` step is added to the SelectionResult that masks events containing
    jets from the veto map, which is the recommended way to use the veto map.
    For users that only want to remove the jets from the veto map, the ``veto_map_jets`` object
    is added to the SelectionResult.

    Requires an external file in the config
    under ``jet_veto_map``:

    .. code-block:: python

        cfg.x.external_files = DotDict.wrap({
            "jet_veto_map": ("/afs/cern.ch/user/m/mfrahm/public/mirrors/jsonpog-integration-a332cfa/POG/JME/2022_Summer22EE/jetvetomaps.json.gz", "v1"),  # noqa
        })

    *get_veto_map_file* can be adapted in a subclass in case it is stored differently in the external files.

    documentation: https://cms-jerc.web.cern.ch/Recommendations/#jet-veto-maps
    """
    jet = events.Jet
    muon = events.Muon[events.Muon.isPFcand]

    # loose jet selection
    jet_mask = (
        (jet.pt > 15) &
        (jet.jetId >= 2) &  # tight id
        (jet.chEmEF < 0.9) &
        ak.all(events.Jet.metric_table(muon) >= 0.2, axis=2)
    )

    # apply loose Jet puId in Run 2 to jets with pt below 50 GeV
    if self.config_inst.campaign.x.run == 2:
        jet_pu_mask = (events.Jet.puId >= 4) | (events.Jet.pt >= 50)
        jet_mask = jet_mask & jet_pu_mask

    jet_phi = jet.phi
    jet_eta = jet.eta

    # for some reason, math.pi is not included in the ranges, so we need to subtract a small number
    pi = math.pi - 1e-10

    # values outside [-pi, pi] are not included, so we need to clip the phi values
    phi_outside_range = abs(jet.phi) > pi
    if ak.any(phi_outside_range):
        # warn in severe cases
        if ak.any(severe := abs(jet_phi[phi_outside_range]) >= 3.15):
            logger.warning(
                f"found {ak.sum(severe)} jet(s) across {ak.sum(ak.any(severe, axis=1))} event(s) "
                "with phi values outside [-pi, pi] that will be clipped",
            )
        jet_phi = ak.where(
            np.abs(jet.phi) > pi,
            jet.phi - 2 * pi * np.sign(jet.phi),
            jet.phi,
        )

    # values outside [-5.19, 5.19] are not included, so we need to clip the eta values
    eta_outside_range = np.abs(jet.eta) > 5.19
    if ak.any(eta_outside_range):
        jet_eta = ak.where(
            np.abs(jet.eta) > 5.19,
            5.19 * np.sign(jet.eta),
            jet.eta,
        )
        logger.warning(
            f"Jet eta values {jet.eta[eta_outside_range][ak.any(eta_outside_range, axis=1)]} outside [-5.19, 5.19] "
            f"({ak.sum(eta_outside_range)} in total) "
            f"detected and set to {jet_eta[eta_outside_range][ak.any(eta_outside_range, axis=1)]}",
        )

    # evalute the veto map only for selected jets
    variable_map = {
        "type": self.veto_map_name,
        "eta": jet_eta[jet_mask],
        "phi": jet_phi[jet_mask],
    }
    inputs = [variable_map[inp.name] for inp in self.veto_map.inputs]
    # the map value is 0.0 for good jets, so define a mask that is True when a jet is vetoed
    veto_mask_sel = self.veto_map(*inputs) != 0

    # optionally fold with negated mask
    if self.negated_veto_map_name:
        variable_map["type"] = self.negated_veto_map_name
        inputs = [variable_map[inp.name] for inp in self.veto_map.inputs]
        veto_mask_sel = veto_mask_sel & ~(self.veto_map(*inputs) != 0)

    # insert back into full jet mask in-place
    flat_jet_mask = flat_np_view(jet_mask)
    flat_jet_mask[flat_jet_mask] = ak.flatten(veto_mask_sel)

    # store the per-jet veto mask for further processing
    # note: to be consistent with conventions, the exported values should be True for passing jets
    events = set_ak_column(events, "Jet.veto_map_mask", ~jet_mask)

    # create the selection result, letting events pass if no jets are vetoed
    results = SelectionResult(
        steps={"jet_veto_map": ~ak.any(jet_mask, axis=1)},
    )

    return events, results


@jet_veto_map.requires
def jet_veto_map_requires(self: Selector, reqs: dict) -> None:
    if "external_files" in reqs:
        return

    from columnflow.tasks.external import BundleExternalFiles
    reqs["external_files"] = BundleExternalFiles.req(self.task)


@jet_veto_map.setup
def jet_veto_map_setup(
    self: Selector,
    reqs: dict,
    inputs: dict,
    reader_targets: InsertableDict,
) -> None:
    bundle = reqs["external_files"]

    # create the corrector
    import correctionlib
    correctionlib.highlevel.Correction.__call__ = correctionlib.highlevel.Correction.evaluate
    correction_set = correctionlib.CorrectionSet.from_string(
        self.get_veto_map_file(bundle.files).load(formatter="gzip").decode("utf-8"),
    )
    keys = list(correction_set.keys())
    if len(keys) != 1:
        raise ValueError(f"Expected exactly one correction in the file, got {len(keys)}")
    self.veto_map = correction_set[keys[0]]

    # name of the veto map
    self.veto_map_name = "jetvetomap"

    # for the 2023 postBPix campaign, the additional negated bpix mask must be applied on top
    # see https://cms-jerc.web.cern.ch/Recommendations/#jet-veto-maps
    self.negated_veto_map_name = ""
    if (
        self.config_inst.campaign.x.year == 2023 and
        self.config_inst.campaign.x.postfix.lower() == "bpix"
    ):
        self.negated_veto_map_name = f"{self.veto_map_name}_bpix"
