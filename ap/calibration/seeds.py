# coding: utf-8

"""
Methods related to creating event and object seeds during calibration.
"""

import hashlib

from ap.calibration import calibrator
from ap.util import maybe_import, primes
from ap.columnar_util import Route, get_ak_indices, set_ak_column, has_ak_column

np = maybe_import("numpy")
ak = maybe_import("awkward")


@calibrator(
    uses={
        # global columns for event seed
        "event", "nGenJet", "nGenPart", "nJet", "nPhoton", "nMuon", "nElectron", "nTau", "nSV",
        # first-object columns for event seed
        "Tau.jetIdx", "Tau.decayMode",
        "Muon.jetIdx", "Muon.nStations",
        "Jet.nConstituents", "Jet.nElectrons", "Jet.nMuons",
    },
    produces={"deterministic_seed"},
)
def deterministic_event_seeds(events, create_seed, **kwargs):
    # start with a seed of the event number itself as the most sensitive integer
    seed = create_seed(events.event)

    # get global integers
    global_fields = ["nGenJet", "nGenPart", "nJet", "nPhoton", "nMuon", "nElectron", "nTau", "nSV"]
    for i, f in enumerate(global_fields, 3):
        seed = seed + primes[i] * (events[f] if f in events.fields else ak.num(events[f[1:]]))

    # get first-object integers
    object_fields = [
        "Tau.jetIdx", "Tau.decayMode", "Muon.jetIdx", "Muon.nStations", "Jet.nConstituents",
        "Jet.nElectrons", "Jet.nMuons",
    ]
    for i, f in enumerate(object_fields, i + 1):
        values = events[Route(f).fields]
        seed = seed + primes[i] * (ak.fill_none(ak.firsts(values, axis=1), -1) + 1)

    # create and store them
    seed = ak.Array(create_seed(seed))
    set_ak_column(events, "deterministic_seed", seed)

    # uniqueness test across the chunk for debugging
    # n_events = len(seed)
    # n_seeds = len(set(seed))
    # match_text = "yes" if n_events == n_seeds else "NO !!!"
    # print(f"events: {n_events}, unique seeds: {n_seeds}, match: {match_text}")

    return events


@deterministic_event_seeds.setup
def deterministic_event_seeds_setup(self, task, inputs, call_kwargs, **kwargs):
    # define the vectorized seed creation function once
    # strategy:
    #   1. gather a selection of unambiguous integer features
    #   2. multiply them with a vector of primes
    #   3. use the resulting integer as an input to sha256 and hex-digest the result
    #   4. reverse it and int-cast the leading 16 characters, leading to a 64 bit int
    # the method below performs steps 3 and 4 while 1 and 2 should be done outside
    def create_seed(n: int) -> int:
        return int(hashlib.sha256(bytes(str(n), "utf-8")).hexdigest()[:-16:-1], base=16)

    # store a vectorized version in the call_kwargs
    call_kwargs["create_seed"] = np.vectorize(create_seed, otypes=[np.uint64])


@calibrator(
    uses={deterministic_event_seeds, "nJet"},
    produces={"Jet.deterministic_seed"},
)
def deterministic_jet_seeds(events, create_seed, **kwargs):
    # create the event seeds if not already present
    if not has_ak_column(events, "deterministic_seed"):
        events = deterministic_event_seeds(events, create_seed=create_seed, **kwargs)

    # create the per jet seeds
    jet_seed = events.deterministic_seed + primes[18] * get_ak_indices(events.Jet)
    np_jet_seed = np.asarray(ak.flatten(jet_seed))
    np_jet_seed[:] = create_seed(np_jet_seed)

    # store them
    set_ak_column(events, "Jet.deterministic_seed", jet_seed)

    # uniqueness test across all jets in the chunk for debugging
    # n_jets = ak.sum(ak.num(events.Jet, axis=1))
    # n_seeds = len(set(np_jet_seed))
    # match_text = "yes" if n_jets == n_seeds else "NO !!!"
    # print(f"jets: {n_jets}, unique seeds: {n_seeds}, match: {match_text}")

    return events


@calibrator(
    uses={deterministic_event_seeds, deterministic_jet_seeds},
    produces={deterministic_event_seeds, deterministic_jet_seeds},
)
def deterministic_seeds(events, **kwargs):
    # create the event seeds
    events = deterministic_event_seeds(events, **kwargs)

    # create the jet seeds
    events = deterministic_jet_seeds(events, **kwargs)

    return events
