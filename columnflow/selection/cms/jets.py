# coding: utf-8

"""
Selection modules for jets.
"""

from __future__ import annotations

from collections import defaultdict
from typing import Tuple

import law

from columnflow.util import maybe_import, InsertableDict
from columnflow.columnar_util import set_ak_column, optional_column as optional
from columnflow.selection import Selector, SelectionResult, selector

np = maybe_import("numpy")
ak = maybe_import("awkward")

logger = law.logger.get_logger(__name__)


@selector(
    uses={
        "Jet.{pt,eta,phi,mass,jetId,chEmEF}",
        "Muon.{pt,eta,phi,mass,isPFcand}",
        optional("Jet.puId"),
    },
    produces={"Jet.veto_maps"},
    get_veto_map_file=(lambda self, external_files: external_files.jet_veto_map),
    exposed=True,
)
def jet_veto_map(
    self: Selector,
    events: ak.Array,
    stats: defaultdict,
    **kwargs,
) -> Tuple[ak.Array, SelectionResult]:
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
            "jet_veto_map": ("/afs/cern.ch/user/m/mfrahm/public/mirrors/jsonpog-integration-a332cfa/POG/JME/2022_Summer22EE/jetvetomaps.json.gz", "v1"), # noqa
        })

    *get_veto_map_file* can be adapted in a subclass in case it is stored differently in the external files.

    documentation: https://cms-jerc.web.cern.ch/Recommendations/#jet-veto-maps
    """

    jet = events.Jet
    muon = events.Muon[events.Muon.isPFcand]

    jet_mask = (
        (jet.pt > 15) &
        (jet.jetId >= 2) &
        (jet.chEmEF < 0.9) &
        (ak.all(events.Jet.metric_table(muon) >= 0.2, axis=2))
    )
    # apply loose Jet puId in Run 2 to jets with pt below 50 GeV
    if self.config_inst.campaign.x.year <= 2018:
        jet_pu_mask = (events.Jet.puId >= 4) | (events.Jet.pt > 50)
        jet_mask = jet_mask & jet_pu_mask

    variable_map = {
        "type": "jetvetomap",
        "eta": jet.eta,
        "phi": jet.phi,
    }

    inputs = [variable_map[inp.name] for inp in self.veto_map.inputs]

    # apply the veto map
    veto_map_result = ak.where(
        jet_mask,
        self.veto_map(*inputs),
        -1,
    )

    # get the Jet veto mask (events containing such a jet should be vetoed)
    veto_map_jet_mask = (veto_map_result > 0)

    if "postBPix" in self.config_inst.campaign.name:
        # in postBPix, we need to run the veto map with type=jetvetomap_bpix and subtract this from
        # the result of the nominal jet veto map
        raise NotImplementedError("Jet Veto Map for 2023 postBPix not implemented yet")

    # add the veto map result to the events
    events = set_ak_column(events, "Jet.veto_maps", veto_map_result)
    results = SelectionResult(
        steps={"jet_veto_map": ak.sum(veto_map_jet_mask, axis=1) >= 1},
        objects={"Jet": {"veto_map_jets": veto_map_jet_mask}},
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
    if not len(keys) == 1:
        raise ValueError(f"Expected exactly one correction in the file, got {len(keys)}")

    self.veto_map = correction_set[keys[0]]
