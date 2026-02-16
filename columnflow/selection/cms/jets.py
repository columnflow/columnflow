# coding: utf-8

"""
Selection modules for jets.
"""

from __future__ import annotations

import law
import math

from columnflow.selection import Selector, SelectionResult, selector
from columnflow.util import maybe_import, load_correction_set, DotDict
from columnflow.columnar_util import set_ak_column, flat_np_view, layout_ak_array
from columnflow.types import Any

np = maybe_import("numpy")
ak = maybe_import("awkward")


logger = law.logger.get_logger(__name__)


@selector(
    # all used columns are registered in init below
    produces={"Jet.veto_map_mask"},
    use_lepton_veto_id=None,  # depends on the "run" info of the underlying campaign of the config
    get_veto_map_file=(lambda self, external_files: external_files.jet_veto_map),
)
def jet_veto_map(
    self: Selector,
    events: ak.Array,
    **kwargs,
) -> tuple[ak.Array, SelectionResult]:
    """
    Selector that applies the jet veto map to the jets and stores the result as a new column ``Jet.veto_map_mask``.
    Additionally, the ``jet_veto_map`` step is added to the SelectionResult that masks events containing jets from the
    veto map, which is the recommended way to use the veto map.

    A minimal selection is applied to jets that are fed into the veto map check as documented in [1]. However, this
    recommendation depends on the data taking period and uses either the proximity to muons in the event (run 2), or the
    so-called "tightLepVeto" (run 3). When *use_lepton_veto_id* is *None*, the decision is based on the ``run``
    auxiliary field of the campaign of the config. Otherwise, when *True*, the "tightLepVeto" is used.

    Requires an external file in the config under ``jet_veto_map``:

    .. code-block:: python

        cfg.x.external_files = DotDict.wrap({
            "jet_veto_map": ("/afs/cern.ch/user/m/mfrahm/public/mirrors/jsonpog-integration-a332cfa/POG/JME/2022_Summer22EE/jetvetomaps.json.gz", "v1"),  # noqa
        })

    *get_veto_map_file* can be adapted in a subclass in case it is stored differently in the external files.

    Resources:

        1. https://cms-jerc.web.cern.ch/Recommendations/#jet-veto-maps
        2. https://cms-talk.web.cern.ch/t/updated-jet-selection-criterion-for-jet-veto-map/130527
    """
    # jet selection
    jet = events.Jet
    jet_mask = (
        (jet.pt > 15) &
        ((jet.chEmEF + jet.neEmEF) < 0.9)
    )

    # fold in veto id or manually filter against muons
    if self.use_lepton_veto_id:
        jet_mask = jet_mask & (jet.jetId & (1 << 2))  # third bit is tightLepVeto
    else:
        muon = events.Muon[events.Muon.isPFcand]
        jet_mask = jet_mask & (
            (jet.jetId & (1 << 1)) &  # second bit is tight
            ak.all(events.Jet.metric_table(muon) >= 0.2, axis=2)
        )

    # apply loose Jet puId in Run 2 to jets with pt below 50 GeV
    if self.config_inst.campaign.x.run == 2:
        jet_mask = jet_mask & (
            (events.Jet.puId >= 4) |
            (events.Jet.pt >= 50)
        )

    # for some reason, math.pi is not included in the ranges, so we need to subtract a small number
    pi = math.pi - 1e-10

    # values outside [-pi, pi] are not included, so we need to clip the phi values
    jet_phi = jet.phi
    phi_outside_range = abs(jet_phi) > pi
    if ak.any(phi_outside_range):
        # warn in severe cases
        if ak.any(severe := abs(jet_phi[phi_outside_range]) >= 3.15):
            logger.warning(
                f"found {ak.sum(severe)} jet(s) across {ak.sum(ak.any(severe, axis=1))} event(s) "
                "with phi values outside [-pi, pi] that will be clipped",
            )
        jet_phi = ak.where(
            np.abs(jet_phi) > pi,
            jet_phi - 2 * pi * np.sign(jet_phi),
            jet_phi,
        )

    # values outside [-5.19, 5.19] are not included, so we need to clip the eta values
    jet_eta = jet.eta
    eta_outside_range = np.abs(jet_eta) > 5.19
    if ak.any(eta_outside_range):
        logger.warning(
            f"jet eta values {jet_eta[eta_outside_range][ak.any(eta_outside_range, axis=1)]} outside [-5.19, 5.19] "
            f"({ak.sum(eta_outside_range)} in total) detected and set to "
            f"{jet_eta[eta_outside_range][ak.any(eta_outside_range, axis=1)]}",
        )
        jet_eta = ak.where(
            np.abs(jet_eta) > 5.19,
            5.19 * np.sign(jet_eta),
            jet_eta,
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

    # combine again with jet mask
    flat_jet_mask = flat_np_view(jet_mask, copy=True)
    flat_jet_mask[flat_jet_mask] = ak.flatten(veto_mask_sel)
    jet_mask = layout_ak_array(flat_jet_mask, events.Jet)

    # store the per-jet veto mask for further processing
    # note: to be consistent with conventions, the exported values should be True for passing jets
    events = set_ak_column(events, "Jet.veto_map_mask", ~jet_mask)

    # create the selection result, letting events pass if no jets are vetoed
    results = SelectionResult(
        steps={"jet_veto_map": ~ak.any(jet_mask, axis=1)},
    )

    return events, results


@jet_veto_map.init
def get_veto_map_init(self: Selector) -> None:
    # get the default for use_lepton_veto_id
    if self.use_lepton_veto_id is None:
        self.use_lepton_veto_id = self.config_inst.campaign.x.run == 3

    # always read specific jet columns
    self.uses.add("Jet.{pt,eta,phi,mass,jetId,chEmEF,neEmEF}")

    # read puId in run 2 for additional cut
    if self.config_inst.campaign.x.run == 2:
        self.uses.add("Jet.puId")

    # read muon columns when not using the veto id
    if not self.use_lepton_veto_id:
        self.uses.add("Muon.{pt,eta,phi,mass,isPFcand}")


@jet_veto_map.requires
def jet_veto_map_requires(
    self: Selector,
    task: law.Task,
    reqs: dict[str, DotDict[str, Any]],
    **kwargs,
) -> None:
    if "external_files" in reqs:
        return

    from columnflow.tasks.external import BundleExternalFiles
    reqs["external_files"] = BundleExternalFiles.req(task)


@jet_veto_map.setup
def jet_veto_map_setup(
    self: Selector,
    task: law.Task,
    reqs: dict[str, DotDict[str, Any]],
    inputs: dict[str, Any],
    reader_targets: law.util.InsertableDict,
    **kwargs,
) -> None:
    # create the corrector
    map_file = self.get_veto_map_file(reqs["external_files"].files)
    correction_set = load_correction_set(map_file)
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
