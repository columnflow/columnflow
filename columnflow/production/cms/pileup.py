# coding: utf-8

"""
Column production methods related to pileup weights.
"""

import functools

import law

from columnflow.production import Producer, producer
from columnflow.util import maybe_import, InsertableDict
from columnflow.columnar_util import set_ak_column

np = maybe_import("numpy")
ak = maybe_import("awkward")


# helper
set_ak_column_f32 = functools.partial(set_ak_column, value_type=np.float32)


logger = law.logger.get_logger(__name__)


@producer(
    uses={"Pileup.nTrueInt"},
    produces={"pu_weight", "pu_weight_minbias_xs_up", "pu_weight_minbias_xs_down"},
    # only run on mc
    mc_only=True,
    # function to determine the correction file
    get_pileup_file=(lambda self, external_files: external_files.pu_sf),
)
def pu_weight(self: Producer, events: ak.Array, **kwargs) -> ak.Array:
    """
    Based on the number of primary vertices, assigns each event pileup weights using correctionlib.
    """
    # compute the indices for looking up weights
    indices = events.Pileup.nTrueInt.to_numpy().astype("int32") - 1

    # map the variable names from the corrector to our columns
    variable_map = {
        "NumTrueInteractions": indices,
    }

    for column_name, syst in (
        ("pu_weight", "nominal"),
        ("pu_weight_minbias_xs_up", "up"),
        ("pu_weight_minbias_xs_down", "down"),
    ):
        # get the inputs for this type of variation
        variable_map_syst = {**variable_map, "weights": syst}
        inputs = [variable_map_syst[inp.name] for inp in self.pileup_corrector.inputs]

        # evaluate and store the produced column
        pu_weight = self.pileup_corrector.evaluate(*inputs)
        events = set_ak_column(events, column_name, pu_weight, value_type=np.float32)

    return events


@pu_weight.requires
def pu_weight_requires(self: Producer, reqs: dict) -> None:
    """
    Adds the requirements needed the underlying task to derive the pileup weights into *reqs*.
    """
    if "external_files" in reqs:
        return

    from columnflow.tasks.external import BundleExternalFiles
    reqs["external_files"] = BundleExternalFiles.req(self.task)


@pu_weight.setup
def pu_weight_setup(
    self: Producer,
    reqs: dict,
    inputs: dict,
    reader_targets: InsertableDict,
) -> None:
    """
    Loads the pileup calculator from the external files bundle and saves them in the
    py:attr:`pileup_corrector` attribute for simpler access in the actual callable.
    """
    bundle = reqs["external_files"]

    # create the corrector
    import correctionlib
    correctionlib.highlevel.Correction.__call__ = correctionlib.highlevel.Correction.evaluate
    correction_set = correctionlib.CorrectionSet.from_string(
        self.get_pileup_file(bundle.files).load(formatter="gzip").decode("utf-8"),
    )

    # check
    if len(correction_set.keys()) != 1:
        raise Exception("Expected exactly one type of pileup correction")

    corrector_name = list(correction_set.keys())[0]
    self.pileup_corrector = correction_set[corrector_name]


@producer(
    uses={"Pileup.nTrueInt"},
    produces={"pu_weight", "pu_weight_minbias_xs_up", "pu_weight_minbias_xs_down"},
    # only run on mc
    mc_only=True,
)
def pu_weights_from_columnflow(self: Producer, events: ak.Array, **kwargs) -> ak.Array:
    """
    Based on the number of primary vertices, assigns each event pileup weights using the profile
    of pileup ratios at the py:attr:`pu_weights` attribute provided by the requires and setup
    functions below.
    """
    # compute the indices for looking up weights
    indices = events.Pileup.nTrueInt.to_numpy().astype("int32") - 1
    max_bin = len(self.pu_weights) - 1
    indices[indices > max_bin] = max_bin

    # save the weights
    events = set_ak_column_f32(events, "pu_weight", self.pu_weights.nominal[indices])
    events = set_ak_column_f32(events, "pu_weight_minbias_xs_up", self.pu_weights.minbias_xs_up[indices])
    events = set_ak_column_f32(events, "pu_weight_minbias_xs_down", self.pu_weights.minbias_xs_down[indices])

    return events


@pu_weights_from_columnflow.requires
def pu_weights_from_columnflow_requires(self: Producer, reqs: dict) -> None:
    """
    Adds the requirements needed the underlying task to derive the pileup weights into *reqs*.
    """
    if "pu_weights" in reqs:
        return

    from columnflow.tasks.cms.external import CreatePileupWeights
    reqs["pu_weights"] = CreatePileupWeights.req(self.task)


@pu_weights_from_columnflow.setup
def pu_weights_from_columnflow_setup(
    self: Producer,
    reqs: dict,
    inputs: dict,
    reader_targets: InsertableDict,
) -> None:
    """
    Loads the pileup weights added through the requirements and saves them in the
    py:attr:`pu_weights` attribute for simpler access in the actual callable.
    """
    self.pu_weights = ak.zip(inputs["pu_weights"].load(formatter="json"))
