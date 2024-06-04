# from collections import defaultdict

from columnflow.production import Producer, producer
from columnflow.util import maybe_import, InsertableDict
from columnflow.columnar_util import set_ak_column
from law import LocalFileTarget

ak = maybe_import("awkward")
np = maybe_import("numpy")


@producer(
    uses={"event", "run", "luminosityBlock"},
    produces={"veto"},
    exposed=False,
    get_veto_file=(lambda self, external_files: external_files.veto),
)
def veto_events(
        self: Producer,
        events: ak.Array,
        file: LocalFileTarget = None,
        **kwargs,
) -> ak.Array:
    """
    Produces a mask vetoing certain events from being processed. Outputs a SelectionResult
    with attributes *veto* (containing a mask selecting the vetoed events) and with the *event*
    attribute initialized with a mask selecting non-vetoed events. If *file* is provided, it checks only
    events contained within this file, or events not designated to any file.

    The events that are vetoed need to be specified  from ``config_inst``,
    which must contain the keyword ``veto`` in the auxiliary information. This can look
    like this:

    .. code-block:: python

        # cfg is the current config instance
        cfg.x.veto = config.x.veto = {
            "dy_lep_m10to50_amcatnlo" : [
                {
                    "event": 33098036,
                    "luminosityBlock": 20170,
                    "run": 1,
                    ** optionally **
                    "file": "/store/mc/RunIISummer20UL18NanoAODv9/DYJetsToLL_M-10to50_TuneCP5_13TeV-amcatnloFXFX-pythia8/NANOAODSIM/106X_upgrade2018_realistic_v16_L1v1-v1/50000/296CA60E-0122-2F4F-8B04-17DCF5E3E062.root"  # noqa
                }
            ]
        }

    """

    veto = np.full_like(events.event, False, dtype=bool)
    for veto_event in self.veto_list:
        if file is None or "file" not in veto_event or file.path == veto_event["file"]:
            veto = veto | (
                (events.event == veto_event["event"]) &
                (events.run == veto_event["run"]) &
                (events.luminosityBlock == veto_event["luminosityBlock"])
            )

    events = set_ak_column(events, "veto", veto)

    return events


@veto_events.setup
def veto_events_setup(
        self: Producer,
        reqs: dict,
        inputs: dict,
        reader_targets: InsertableDict,
) -> None:
    """
    Loads the event veto file from the external files bundle and saves them in the
    py:attr:`veto_list` attribute for simpler access in the actual callable.
    """
    veto_dict = self.config_inst.aux.get("veto", {})
    self.veto_list = veto_dict.get(self.dataset_inst.name, [])
