# coding: utf-8

"""
B-tag selection methods.
"""

from __future__ import annotations

import dataclasses

from columnflow.selection import Selector, selector
from columnflow.columnar_util import TAFConfig
from columnflow.util import maybe_import, DotDict
from columnflow.types import TYPE_CHECKING

np = maybe_import("numpy")
ak = maybe_import("awkward")

if TYPE_CHECKING:
    hist = maybe_import("hist")


@dataclasses.dataclass
class BTagWPCountConfig(TAFConfig):
    # name of the jet collection
    jet_name: str = "Jet"
    # name of the b-tag score column
    btag_column: str = "btagUParTAK4B"
    # mapping of working point names and thresholds
    # ! values are merely examples and should be overwritten
    btag_wps: dict[str, float] = dataclasses.field(default_factory=lambda: {
        "loose": 0.0246,
        "medium": 0.1272,
        "tight": 0.4648,
        "xtight": 0.6298,
        "xxtight": 0.9739,
    })
    # edges for histogram binning
    # ! values are merely examples and should be overwritten
    pt_edges: tuple[float, ...] = (0, 20, 30, 50, 70, 100, 140, 200, 300, 600, 10_000)
    abs_eta_edges: tuple[float, ...] = (0.0, 1.0, 1.5, 2.0, 5.0)
    # key of the histogram to save in selector hists
    hist_key: str = "btag_wp_counts"


@selector(
    # function to configure how to retrieve the BTagWPCountConfig
    get_btag_wp_count_config=(lambda self: self.config_inst.x.btag_wp_count_config),
    # unexpose so that it cannot be called from the command line
    exposed=False,
)
def fill_btag_wp_count_hists(
    self: Selector,
    events: ak.Array,
    event_mask: ak.Array,
    jet_mask: ak.Array,
    hists: DotDict[str, hist.Hist],
    **kwargs,
) -> None:
    """
    Selector that fills numbers of selected jets passing different b-tagging working points into histograms. There is no
    return value as the created histogram is stored in *hists* in-place.

    The counts can be used later on to compute b-tagging efficiencies, required by scale factor calculations such as
    https://btv-wiki.docs.cern.ch/PerformanceCalibration/fixedWPSFRecommendations/#b-tagging-efficiencies-in-simulation.
    """
    import hist

    # select jets and reduce to selected events only
    jets = events[self.cfg.jet_name][jet_mask][event_mask]

    # flatten necessary columns
    pt = ak.flatten(jets.pt, axis=None)
    abs_eta = abs(ak.flatten(jets.eta, axis=None))
    flavor = ak.flatten(jets.hadronFlavour, axis=None)
    btag_score = ak.flatten(jets[self.cfg.btag_column], axis=None)

    # create empty histogram
    h = hist.Hist(
        hist.axis.Variable(self.cfg.pt_edges, name="pt", label="pt"),
        hist.axis.Variable(self.cfg.abs_eta_edges, name="abs_eta", label="abs_eta"),
        hist.axis.IntCategory(categories=[], growth=True, name="flavor", label="flavor"),
        hist.axis.StrCategory(["total"] + list(self.wp_ranges.keys()), name="wp_bin", label="wp_bin"),
        storage=hist.storage.Double(),
    )

    # fill total wp bin
    h.fill(pt=pt, abs_eta=abs_eta, flavor=flavor, wp_bin="total")

    # fill wp bins
    for wp_bin, (lower, upper) in self.wp_ranges.items():
        mask = np.ones(len(pt), dtype=bool)
        if lower is not None:
            mask = mask & (btag_score >= lower)
        if upper is not None:
            mask = mask & (btag_score < upper)
        h.fill(pt=pt[mask], abs_eta=abs_eta[mask], flavor=flavor[mask], wp_bin=wp_bin)

    # store the histogram
    hists[self.cfg.hist_key] = h


@fill_btag_wp_count_hists.init
def fill_btag_wp_count_hists_init(self: Selector) -> None:
    # retrieve and store the config
    self.cfg: BTagWPCountConfig = self.get_btag_wp_count_config()

    # add used columns
    self.uses.add(f"{self.cfg.jet_name}.{{pt,eta,phi,mass,hadronFlavour,{self.cfg.btag_column}}}")

    # compute working point ranges, starting with the lowest bin ranging from "veto" to the first wp entry
    self.wp_ranges = {}
    for lower_key, upper_key in zip([None] + list(self.cfg.btag_wps.keys()), list(self.cfg.btag_wps.keys()) + [None]):
        self.wp_ranges[lower_key or "veto"] = (
            self.cfg.btag_wps.get(lower_key, float("-inf")),
            self.cfg.btag_wps.get(upper_key, float("inf")),
        )
