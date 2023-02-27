# coding: utf-8

"""
Selectors for applying golden JSON in data.
"""

from __future__ import annotations

from columnflow.selection import Selector, selector
from columnflow.util import maybe_import


ak = maybe_import("awkward")
np = maybe_import("numpy")
sp = maybe_import("scipy")
maybe_import("scipy.sparse")


@selector(
    uses={"run", "luminosityBlock"},
    # function to determine the golden lumi file
    get_lumi_file=(lambda external_files: external_files.lumi.golden),
)
def json_filter(
    self: Selector,
    events: ak.Array,
    data_only=True,
    **kwargs,
) -> ak.Array:
    """
    Select only events from certified luminosity blocks included in the "golden" JSON. This filter
    can only be applied in recorded data.

    By default, the JSON file should specified in the config as an external file under
    ``lumi.golden``:

    .. code-block:: python

        cfg.x.external_files = DotDict.wrap({
            "lumi": {
                "golden": "/afs/cern.ch/cms/CAF/CMSCOMM/COMM_DQM/certification/Collisions17/13TeV/Legacy_2017/Cert_294927-306462_13TeV_UL2017_Collisions17_GoldenJSON.txt",  # noqa
            },
        })

    *get_lumi_file* can be adapted in a subclass in case it is stored differently in the external
    files.
    """
    lookup_result = self.run_ls_lookup[events.run, events.luminosityBlock].todense()
    return np.squeeze(np.array(lookup_result))


@json_filter.requires
def json_filter_requires(self: Selector, reqs: dict) -> None:
    """
    Add external files bundle as a task requirement.
    """
    if "external_files" in reqs:
        return

    from columnflow.tasks.external import BundleExternalFiles
    reqs["external_files"] = BundleExternalFiles.req(self.task)


@json_filter.setup
def json_filter_setup(self: Selector, reqs: dict, inputs: dict) -> None:
    """
    Load golden JSON and set up run/luminosity block lookup table.
    """
    bundle = reqs["external_files"]

    # import the correction sets from the external file
    json = self.get_lumi_file(bundle.files).load(formatter="json")

    # determine range of run/luminosity block values
    max_ls = max(ls for ls_ranges in json.values() for ls in ak.ravel(ls_ranges))
    max_run = max(map(int, json.keys()))

    # build lookup table
    self.run_ls_lookup = sp.sparse.lil_matrix((max_run + 1, max_ls + 1), dtype=bool)
    for run, ls_ranges in json.items():
        run = int(run)
        for ls_range in ls_ranges:
            for ls in range(ls_range[0], ls_range[1] + 1):
                self.run_ls_lookup[run, ls] = True
