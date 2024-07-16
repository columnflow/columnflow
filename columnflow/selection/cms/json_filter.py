# coding: utf-8

"""
Selectors for applying golden JSON in data.
"""

from __future__ import annotations

from columnflow.selection import Selector, selector, SelectionResult
from columnflow.util import maybe_import, InsertableDict, DotDict

ak = maybe_import("awkward")
np = maybe_import("numpy")
sp = maybe_import("scipy")
maybe_import("scipy.sparse")


def get_lumi_file_default(self, external_files: DotDict) -> str:
    """
    Function to load path or url to golden json files.

    By default, the path is extracted from the current *config_inst*, which
    should have a *external_files* in the auxiliary information block.
    The path or url is extracted with

    .. code-block:: python

        external_files.lumi.golden

    :param external_files: Config containing the information about the path
        or url to the golden json file containing good lumi sections.
    :return: path or url to golden json file.
    """
    return external_files.lumi.golden


@selector(
    uses={"run", "luminosityBlock"},
    # function to determine the golden lumi file
    get_lumi_file=get_lumi_file_default,
)
def json_filter(
    self: Selector,
    events: ak.Array,
    data_only: bool = True,
    **kwargs,
) -> tuple[ak.Array, SelectionResult]:
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

    :param events: Array containing events in the NanoAOD format
    :param data_only: boolean flag to indicate that this selector should only run on observed data,
        defaults to True
    :return: Tuple containing the events array and a
        :py:class:`~columnflow.selection.SelectionResult` with a "json" field in its "steps" data
        representing a boolean mask to accept or reject given events
    """
    # handle out-of-bounds values
    run_out_of_bounds = (events.run >= self.run_ls_lookup.shape[0])
    ls_out_of_bounds = (events.luminosityBlock >= self.run_ls_lookup.shape[1])
    out_of_bounds = (run_out_of_bounds | ls_out_of_bounds)

    run = ak.where(out_of_bounds, 0, events.run)
    ls = ak.where(out_of_bounds, 0, events.luminosityBlock)

    # look up json filter decision
    lookup_result = self.run_ls_lookup[run, ls].todense()

    # remove extra dimensions
    lookup_result = np.squeeze(np.asarray(lookup_result))

    # reject out-ouf-bounds entries
    lookup_result = ak.where(out_of_bounds, False, lookup_result)

    return events, SelectionResult(steps={"json": lookup_result})


@json_filter.requires
def json_filter_requires(self: Selector, reqs: dict) -> None:
    if "external_files" in reqs:
        return

    from columnflow.tasks.external import BundleExternalFiles
    reqs["external_files"] = BundleExternalFiles.req(self.task)


@json_filter.setup
def json_filter_setup(
    self: Selector,
    reqs: dict,
    inputs: dict,
    reader_targets: InsertableDict,
) -> None:
    """
    Setup function for :py:class:`json_filter`. Load golden JSON and set up run/luminosity block
    lookup table.

    :param reqs: Contains requirements for this task
    :param inputs: Additional inputs, currently not used
    :param reader_targets: Additional targets, currently not used
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
