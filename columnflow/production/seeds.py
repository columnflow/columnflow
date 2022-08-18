# coding: utf-8

"""
Methods related to creating event and object seeds.
"""

import hashlib

from columnflow.production import Producer, producer
from columnflow.util import maybe_import, primes
from columnflow.columnar_util import Route, set_ak_column

np = maybe_import("numpy")
ak = maybe_import("awkward")


@producer(
    uses={
        # global columns for event seed
        "run", "luminosityBlock", "event", "nGenJet", "nGenPart", "nJet", "nPhoton", "nMuon",
        "nElectron", "nTau", "nSV",
        # first-object columns for event seed
        "Tau.jetIdx", "Tau.decayMode",
        "Muon.jetIdx", "Muon.nStations",
        "Jet.nConstituents", "Jet.nElectrons", "Jet.nMuons",
    },
    produces={"deterministic_seed"},
)
def deterministic_event_seeds(self: Producer, events: ak.Array, **kwargs) -> ak.Array:
    """
    Produces deterministic event seeds and stores them in *events* which is also returned.

    Strategy:
      1. gather a selection of unambiguous integer features
      2. multiply them with a vector of primes
      3. use the resulting integer as an input to sha256 and hex-digest the result
      4. reverse it and int-cast the leading 16 characters, leading to a 64 bit int
    """
    # start with a seed of the event number itself as the most sensitive integer
    # and define the offset of the first prime to use
    seed = self.create_seed(events.event)
    prime_offset = 3

    # get global integers, with a slight difference between data and mc
    global_fields = ["nGenJet", "nGenPart"] if self.dataset_inst.is_mc else ["run", "luminosityBlock"]
    global_fields.extend(["nJet", "nPhoton", "nMuon", "nElectron", "nTau", "nSV"])
    for i, f in enumerate(global_fields, prime_offset):
        seed = seed + primes[i] * (events[f] if f in events.fields else ak.num(events[f[1:]]))

    # get first-object integers
    object_fields = [
        "Tau.jetIdx", "Tau.decayMode", "Muon.jetIdx", "Muon.nStations", "Jet.nConstituents",
        "Jet.nElectrons", "Jet.nMuons",
    ]
    for i, f in enumerate(object_fields, prime_offset + len(global_fields)):
        values = events[Route(f).fields]
        seed = seed + primes[i] * (ak.fill_none(ak.firsts(values, axis=1), -1) + 1)

    # create and store them
    seed = ak.Array(self.create_seed(seed))
    set_ak_column(events, "deterministic_seed", seed)

    # uniqueness test across the chunk for debugging
    # n_events = len(seed)
    # n_seeds = len(set(seed))
    # match_text = "yes" if n_events == n_seeds else "NO !!!"
    # print(f"events: {n_events}, unique seeds: {n_seeds}, match: {match_text}")

    return events


@deterministic_event_seeds.setup
def deterministic_event_seeds_setup(self: Producer, inputs: dict) -> None:
    """
    Setup function that defines the vectorized seed creation function once and stores it in the
    py:attr:`create_seed` attribute.
    """
    def create_seed(n: int) -> int:
        return int(hashlib.sha256(bytes(str(n), "utf-8")).hexdigest()[:-16:-1], base=16)

    # store a vectorized version
    # TODO: is there a real-vectorized version if hashlib for numpy/scipy?
    self.create_seed = np.vectorize(create_seed, otypes=[np.uint64])


@producer(
    uses={deterministic_event_seeds, "nJet"},
    produces={"Jet.deterministic_seed"},
)
def deterministic_jet_seeds(self: Producer, events: ak.Array, **kwargs) -> ak.Array:
    """
    Produces deterministic seeds for each jet and stores them in *events* which is also returned.
    The seeds are based on the event seeds produced by :py:func:`deterministic_event_seeds` which is
    also used to access the py:attr:`create_seed` function. The strategy for producing seeds is
    identical.
    """
    # create the event seeds
    self[deterministic_event_seeds](events, **kwargs)

    # create the per jet seeds
    prime_offset = 18
    jet_seed = events.deterministic_seed + (
        primes[prime_offset] * ak.values_astype(ak.local_index(events.Jet), np.uint64)
    )
    np_jet_seed = np.asarray(ak.flatten(jet_seed))
    np_jet_seed[:] = self[deterministic_event_seeds].create_seed(np_jet_seed)

    # store them
    set_ak_column(events, "Jet.deterministic_seed", jet_seed)

    # uniqueness test across all jets in the chunk for debugging
    # n_jets = ak.sum(ak.num(events.Jet, axis=1))
    # n_seeds = len(set(np_jet_seed))
    # match_text = "yes" if n_jets == n_seeds else "NO !!!"
    # print(f"jets: {n_jets}, unique seeds: {n_seeds}, match: {match_text}")

    return events


@producer(
    uses={deterministic_event_seeds, deterministic_jet_seeds},
    produces={deterministic_event_seeds, deterministic_jet_seeds},
)
def deterministic_seeds(self: Producer, events: ak.Array, **kwargs) -> ak.Array:
    """
    Wrapper producer that invokes :py:func:`deterministic_event_seeds` and
    :py:func:`deterministic_jet_seeds`.
    """
    # create the event seeds
    self[deterministic_event_seeds](events, **kwargs)

    # create the jet seeds
    self[deterministic_jet_seeds](events, **kwargs)

    return events
