# coding: utf-8

"""
Calibration methods.
"""

from columnflow.calibration import Calibrator, calibrator
from columnflow.calibration.cms.jets import jec, jer
from columnflow.production.cms.seeds import deterministic_seeds
from columnflow.util import maybe_import

from __cf_short_name_lc__.calibration.jet import jec_nominal

ak = maybe_import("awkward")


@calibrator(
    uses={deterministic_seeds},
    produces={deterministic_seeds},
)
def default(self: Calibrator, events: ak.Array, **kwargs) -> ak.Array:
    events = self[deterministic_seeds](events, **kwargs)

    if self.dataset_inst.is_data:
        events = self[jec_nominal](events, **kwargs)
    else:
        events = self[jec](events, **kwargs)

    return events


@default.init
def default_init(self: Calibrator) -> None:
    if not getattr(self, "dataset_inst", None):
        return

    if self.dataset_inst.is_data:
        calibrators = {jec_nominal}
    else:
        calibrators = {jec}

    self.uses |= calibrators
    self.produces |= calibrators


@calibrator(
    uses={deterministic_seeds},
    produces={deterministic_seeds},
)
def skip_jecunc(self: Calibrator, events: ak.Array, **kwargs) -> ak.Array:
    """ only uses jec_nominal for test purposes """
    events = self[deterministic_seeds](events, **kwargs)

    if self.dataset_inst.is_data:
        events = self[jec_nominal](events, **kwargs)
    else:
        events = self[jec_nominal](events, **kwargs)
        events = self[jer](events, **kwargs)

    return events


@skip_jecunc.init
def skip_jecunc_init(self: Calibrator) -> None:
    if not getattr(self, "dataset_inst", None):
        return

    if self.dataset_inst.is_data:
        calibrators = {jec_nominal}
    else:
        calibrators = {jec_nominal, jer}

    self.uses |= calibrators
    self.produces |= calibrators
