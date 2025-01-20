# coding: utf-8

"""
Methods related to creating event and object seeds.
"""

from __future__ import annotations

import hashlib

import law

from columnflow.production import Producer, producer
from columnflow.util import maybe_import, primes, InsertableDict
from columnflow.columnar_util import Route, set_ak_column, optional_column as optional

np = maybe_import("numpy")
ak = maybe_import("awkward")


logger = law.logger.get_logger(__name__)


def create_seed(val: int, n_hex: int = 16) -> int:
    """
    Create a seed from an integer value by hashing it and returning the trailing 64 bit integer.
    """
    return int(hashlib.sha256(bytes(str(val), "utf-8")).hexdigest()[:-(n_hex + 1):-1], base=16)


# store a vectorized version (only interface, not actually simd'ing)
create_seed_vec = np.vectorize(create_seed, otypes=[np.uint64])


@producer(
    uses={
        # global columns for event seed
        "run", "luminosityBlock", "event",
        # all other columns to be loaded are defined through the lists below
    },
    produces={"deterministic_seed"},
    # columns of event integer fields
    event_columns=[optional("Pileup.nPU")],
    # columns to be loaded to infer counts of objects
    object_count_columns=list(map(optional, [
        "Jet.nConstituents", "FatJet.pt", "SubJet.pt", "Photon.pt", "Muon.jetIdx",
        "Electron.jetIdx", "Tau.jetIdx", "SV.pt", "GenJet.pt", "GenPart.pt",
    ])),
    # columns of object integer fields (can have overlap to object_count_columns)
    object_columns=list(map(optional, [
        "Electron.jetIdx", "Electron.seediPhiOriY",
        "Muon.jetIdx", "Muon.nStations",
        "Tau.jetIdx", "Tau.decayMode",
        "Jet.nConstituents", "Jet.nElectrons", "Jet.nMuons",
    ])),
)
def deterministic_event_seeds(self: Producer, events: ak.Array, **kwargs) -> ak.Array:
    """
    Produces deterministic event seeds and stores them in *events* which is also returned.

    Strategy:

        1. gather a selection of unambiguous integer features
        2. multiply them with a vector of primes
        3. use the resulting integer as an input to sha256 and hex-digest the result
        4. reverse it and int-cast the leading 16 characters, leading to a 64 bit int

    .. note::

        When using :py:attr:`object_columns`, the event seeds depend on the position of the
        particular objects per event. It is up to the user to bring them into the desired order
        before invoking this producer.
    """
    # started from an already hashed seed based on event, run and lumi info multiplied with primes
    seed = create_seed_vec(
        np.asarray(
            self.primes[7] * ak.values_astype(events.event, np.uint64) +
            self.primes[5] * ak.values_astype(events.run, np.uint64) +
            self.primes[3] * ak.values_astype(events.luminosityBlock, np.uint64),
        ),
        n_hex=14,
    )

    # start gathering global routes when available
    global_routes = []

    # event-based columns
    for c in self.event_columns:
        r = Route(c)
        if self.apply_route(events, r) is not None:
            global_routes.append(r)

    # add routes for counts of jagged collections
    for c in self.object_count_columns:
        r = Route(c)
        if (arr := self.apply_route(events, r)) is None:
            continue
        global_routes.append(r := Route(f"n{r[0]}"))
        events = set_ak_column(events, r, ak.num(arr, axis=1), value_type=np.uint64)

    # calculate seed from global routes
    value_offset = 3
    prime_offset = 15
    for i, r in enumerate(global_routes, value_offset):
        values = r.apply(events) + i
        primes = self.primes[(values + prime_offset) % len(self.primes)]
        seed = seed + primes * ak.values_astype(values, np.uint64)

    # get integers of objects, perform a custom hashing involving local indices,
    # then multiply with primes and add to the seed
    for i, c in enumerate(self.object_columns, value_offset):
        r = Route(c)
        if (values := self.apply_route(events, r)) is None:
            continue
        values = ak.values_astype(values, np.int64) + i
        loc = ak.local_index(values) + 1
        hashed = (
            ak.num(values, axis=-1) +
            ak.sum(values * loc, axis=-1) +
            ak.sum(values**2 * loc, axis=-1)
        )
        primes = self.primes[(hashed + prime_offset) % len(self.primes)]
        seed = seed + primes * ak.values_astype(hashed, np.uint64)

    # create and store them
    seed = ak.Array(create_seed_vec(np.asarray(seed)))
    events = set_ak_column(events, "deterministic_seed", seed, value_type=np.uint64)

    # uniqueness test across the chunk for debugging
    # n_events = len(seed)
    # n_seeds = len(set(seed))
    # match_text = "yes" if n_events == n_seeds else "NO !!!"
    # print(f"events: {n_events}, unique seeds: {n_seeds}, match: {match_text}")

    return events


@deterministic_event_seeds.init
def deterministic_event_seeds_init(self: Producer) -> None:
    """
    Producer initialization that adds columns to the set of *used* columns based on the
    *event_columns*, *object_count_columns*, and *object_columns* lists.
    """
    # add used columns
    for column in self.event_columns + self.object_count_columns + self.object_columns:
        self.uses.add(Route(column))


@deterministic_event_seeds.setup
def deterministic_event_seeds_setup(
    self: Producer,
    reqs: dict,
    inputs: dict,
    reader_targets: InsertableDict,
) -> None:
    """
    Setup function that defines conventions methods needed during the producer function.
    """
    # store primes in array
    self.primes = np.array(primes, dtype=np.uint64)

    # helper to apply a route to an array with a silent failure that only issues a warning
    def apply_route(ak_array: ak.Array, route: Route) -> ak.Array | None:
        try:
            return route.apply(ak_array)
        except ak.errors.FieldNotFoundError:
            if not route.has_tag("optional"):
                raise
            logger.warning_once(
                f"{id(self)}_{route}",
                f"optional route '{route}' not found in events chunk for seed calculation",
            )
            return None

    self.apply_route = apply_route


@producer(
    uses={"Jet.pt"},
    produces={"Jet.deterministic_seed"},
)
def deterministic_jet_seeds(self: Producer, events: ak.Array, **kwargs) -> ak.Array:
    """
    Produces deterministic seeds for each jet and stores them in *events* which is also returned.
    The jet seeds are based on the event seeds like the ones produced by
    :py:func:`deterministic_event_seeds` which is not called by this producer for the purpose of
    of modularity. The strategy for producing seeds is identical.

    .. note::

        The jet seeds depend on the position of the particular jet in the event. It is up to the
        user to bring them into the desired order before invoking this producer.
    """
    # create the seeds
    primes = self.primes[events.deterministic_seed % len(self.primes)]
    jet_seed = events.deterministic_seed + (
        primes * ak.values_astype(ak.local_index(events.Jet, axis=1) + self.primes[50], np.uint64)
    )
    np_jet_seed = np.asarray(ak.flatten(jet_seed))
    np_jet_seed[:] = create_seed_vec(np_jet_seed)

    # store them
    events = set_ak_column(events, "Jet.deterministic_seed", jet_seed, value_type=np.uint64)

    # uniqueness test across all jets in the chunk for debugging
    # n_jets = ak.sum(ak.num(events.Jet, axis=1))
    # n_seeds = len(set(np_jet_seed))
    # match_text = "yes" if n_jets == n_seeds else "NO !!!"
    # print(f"jets: {n_jets}, unique seeds: {n_seeds}, match: {match_text}")

    return events


@deterministic_jet_seeds.setup
def deterministic_jet_seeds_setup(
    self: Producer,
    reqs: dict,
    inputs: dict,
    reader_targets: InsertableDict,
) -> None:
    # store primes in array
    self.primes = np.array(primes, dtype=np.uint64)


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
    events = self[deterministic_event_seeds](events, **kwargs)

    # create the jet seeds
    events = self[deterministic_jet_seeds](events, **kwargs)

    return events
