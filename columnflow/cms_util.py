# coding: utf-8

"""
Collection of CMS specific helpers and utilities.
"""

from __future__ import annotations

__all__ = []

import os
import re
import copy
import json
import base64
import zlib
import pathlib
import dataclasses
import collections

from columnflow.util import maybe_import
from columnflow.types import TYPE_CHECKING, ClassVar, Generator, Literal

if TYPE_CHECKING:
    ak = maybe_import("awkward")


#: Default root path to CAT metadata.
cat_metadata_root = "/cvmfs/cms-griddata.cern.ch/cat/metadata"

#: Default URL of CAT metadata website.
cat_metadata_url = "https://cms-analysis-corrections.docs.cern.ch"


@dataclasses.dataclass
class CATSnapshot:
    """
    Dataclass to wrap YYYY-MM-DD stype timestamps of CAT metadata per POG stored in
    "/cvmfs/cms-griddata.cern.ch/cat/metadata". No format parsing or validation is done, leaving responsibility to the
    user.
    """
    btv: str = ""
    dc: str = ""
    egm: str = ""
    jme: str = ""
    lum: str = ""
    muo: str = ""
    tau: str = ""

    def items(self) -> Generator[tuple[str, str], None, None]:
        return ((k, getattr(self, k)) for k in self.__dataclass_fields__.keys())


@dataclasses.dataclass
class CATInfo:
    """
    Dataclass to describe and wrap information about a specific CAT-defined metadata era.

    .. code-block:: python

        CATInfo(
            run=3,
            era="22CDSep23-Summer22",
            vnano=12,
            snapshot=CATSnapshot(
                btv="2025-08-20",
                dc="2025-07-25",
                egm="2025-04-15",
                jme="2025-09-23",
                lum="2024-01-31",
                muo="2025-08-14",
                tau="2025-10-01",
            ),
            # pog-specific settings
            pog_directories={"dc": "Collisions22"},
        )
    """
    run: int
    era: str
    vnano: int
    snapshot: CATSnapshot
    # optional POG-specific overrides
    pog_eras: dict[str, str] = dataclasses.field(default_factory=dict)
    pog_directories: dict[str, str] = dataclasses.field(default_factory=dict)

    metadata_root: ClassVar[str] = cat_metadata_root
    metadata_url: ClassVar[str] = cat_metadata_url

    def get_era_directory(self, pog: str = "") -> str:
        """
        Returns the era directory name for a given *pog*.

        :param pog: The POG to get the era for. Leave empty if the common POG-unspecific directory name should be used.
        """
        pog = pog.lower()

        # use specific directory if defined
        if pog in self.pog_directories:
            return self.pog_directories[pog]

        # build common directory name from run, era, and vnano
        era = self.pog_eras.get(pog.lower(), self.era) if pog else self.era
        return f"Run{self.run}-{era}-NanoAODv{self.vnano}"

    def get_era_url(self, pog: str = "", timestamp: str = "") -> str:
        """
        Returns the URL to the era directory on the CAT metadata website. When a *pog* or *timestamp* is given, the URL
        will point to the specific POG era directory or timestamp subdirectory, respectively.

        :param pog: Optional POG name to get the era URL for.
        :param timestamp: Optional timestamp to get the URL for.
        """
        url_parts = [
            self.metadata_url,
            "corrections_era",
            self.get_era_directory(pog=pog),
        ]
        if pog:
            url_parts.append(pog.upper())
        if timestamp:
            url_parts.append(timestamp)

        return "/".join(url_parts)

    def get_file(self, pog: str, *paths: str | pathlib.Path) -> str:
        """
        Returns the full path to a specific file or directory defined by *paths* in the CAT metadata structure for a
        given *pog*.
        """
        return os.path.join(
            self.metadata_root,
            pog.upper(),
            self.get_era_directory(pog),
            getattr(self.snapshot, pog.lower()),
            *(str(p).strip("/") for p in paths),
        )


@dataclasses.dataclass
class CMSDatasetInfo:
    """
    Container to wrap a CMS dataset given by its *key* with access to its components. The key should be in the format
    ``/<name>/<campaign>-<campaign_version>-<dataset_version>/<tier>AOD<mc:sim>``.

    .. code-block:: python

        d = CMSDatasetInfo.from_key("/TTtoLNu2Q_TuneCP5_13p6TeV_powheg-pythia8/RunIII2024Summer24MiniAODv6-150X_mcRun3_2024_realistic_v2-v2/MINIAODSIM") # noqa
        print(d.name)              # TTtoLNu2Q_TuneCP5_13p6TeV_powheg-pythia8
        print(d.campaign)          # RunIII2024Summer24MiniAODv6
        print(d.campaign_version)  # 150X_mcRun3_2024_realistic_v2
        print(d.dataset_version)   # v2
        print(d.tier)              # mini (lower case)
        print(d.mc)                # True
        print(d.data)              # False
        print(d.kind)              # mc
    """
    name: str
    campaign: str
    campaign_version: str
    dataset_version: str  # this is usually the GT for MC
    tier: str
    mc: bool

    @classmethod
    def from_key(cls, key: str) -> CMSDatasetInfo:
        """
        Takes a dataset *key*, splits it into its components, and returns a new :py:class:`CMSDatasetInfo` instance.

        :param key: The dataset key:
        :return: A new instance of :py:class:`CMSDatasetInfo`.
        """
        # split
        if not (m := re.match(r"^/([^/]+)/([^/-]+)-([^/-]+)-([^/-]+)/([^/-]+)AOD(SIM)?$", key)):
            raise ValueError(f"invalid dataset key '{key}'")

        # create instance
        return cls(
            name=m.group(1),
            campaign=m.group(2),
            campaign_version=m.group(3),
            dataset_version=m.group(4),
            tier=m.group(5).lower(),
            mc=m.group(6) == "SIM",
        )

    @property
    def key(self) -> str:
        # transform back to key format
        return (
            f"/{self.name}"
            f"/{self.campaign}-{self.campaign_version}-{self.dataset_version}"
            f"/{self.tier.upper()}AOD{'SIM' if self.mc else ''}"
        )

    @property
    def data(self) -> bool:
        return not bool(self.mc)

    @data.setter
    def data(self, value: bool) -> None:
        self.mc = not bool(value)

    @property
    def kind(self) -> str:
        return "mc" if self.mc else "data"

    @kind.setter
    def kind(self, value: str) -> None:
        if (_value := str(value).lower()) not in {"mc", "data"}:
            raise ValueError(f"invalid kind '{value}', expected 'mc' or 'data'")
        self.mc = _value == "mc"

    @property
    def store_path(self) -> str:
        return (
            "/store"
            f"/{self.kind}"
            f"/{self.campaign}"
            f"/{self.name}"
            f"/{self.tier.upper()}AOD{'SIM' if self.mc else ''}"
            f"/{self.campaign_version}-{self.dataset_version}"
        )

    def copy(self, **kwargs) -> CMSDatasetInfo:
        """
        Creates a copy of this instance, allowing to override specific attributes via *kwargs*.

        :param kwargs: Attributes to override in the copy.
        :return: A new instance of :py:class:`CMSDatasetInfo`.
        """
        attrs = copy.deepcopy(self.__dict__)
        attrs.update(kwargs)
        return self.__class__(**attrs)


# pdg id's mapped to particle names
particle_names = {
    2212: "p+",
    1: "d",
    2: "u",
    3: "s",
    4: "c",
    5: "b",
    6: "t",
    11: "e-",
    12: "ve",
    13: "mu-",
    14: "vmu",
    15: "tau-",
    16: "vtau",
    21: "g",
    22: "gamma",
    23: "Z",
    24: "W+",
    25: "h",
    111: "pi0",
    211: "pi+",
    130: "K0L",
    310: "K0S",
    311: "K0",
    321: "K+",
    411: "D+",
    421: "D0",
    431: "Ds+",
    511: "B0",
    521: "B+",
    531: "Bs0",
}

# dynamically add "bar" for quarks
for p in range(1, 7):
    particle_names[-p] = f"{particle_names[p]}bar"
# dynamically flip signs for leptons
for p in [11, 13, 15]:
    particle_names[-p] = f"{particle_names[p][:-1]}+"
# just repeat neutrinos for anti-neutrinos
for p in [12, 14, 16]:
    particle_names[-p] = particle_names[p]
# change signs for W and mesons ending in "+"
for p, name in list(particle_names.items()):
    if p >= 24 and name.endswith("+"):
        particle_names[-p] = f"{name[:-1]}-"


def visualize_gen_decay(gen_part: ak.Array, output_type: Literal["text", "link"] = "link") -> str:
    """
    Given a single generator particle (in coffea nano format), this function builds a graph representation of the
    particle and its decay tree, and returns it either as a mermaid.live link or as a text representation of the graph.

    :param gen_part: A single generator particle in coffea nano format.
    :param output_type: The type of output to return. Either "text" for a text representation of the graph, or "link"
        for a mermaid.live link.
    :return: The output string.
    """
    if output_type not in (known_output_types := {"text", "link"}):
        raise ValueError(f"invalid output_type '{output_type}', expected one of {known_output_types}")

    last_num = -1

    @dataclasses.dataclass
    class Node:
        pdg_id: int
        status: int
        pt: float | None = None
        eta: float | None = None
        children: list[Node] = dataclasses.field(default_factory=list)
        _num: int | None = None
        _float_digits: int = 3

        def __post_init__(self) -> None:
            nonlocal last_num
            self._num = last_num = last_num + 1

        @property
        def name(self) -> str:
            return f"node{self._num}"

        @property
        def label(self) -> str:
            lines = []
            if self.pdg_id in particle_names:
                heading = particle_names[self.pdg_id]
                lines.append(f"id={self.pdg_id}, status={self.status}")
            else:
                heading = str(self.pdg_id)
                lines.append(f"status={self.status}")
            kin = []
            if self.pt is not None:
                kin.append(f"pt={self.pt:.{self._float_digits}f}")
            if self.eta is not None:
                kin.append(f"eta={self.eta:.{self._float_digits}f}")
            if kin:
                lines.append(", ".join(kin))
            return f"{heading}<br>{'<br>'.join('<small>' + line + '</small>' for line in lines)}"

    # build the graph representation
    root = Node(pdg_id=gen_part.pdgId, status=gen_part.status, pt=gen_part.pt, eta=gen_part.eta)
    q = collections.deque([(root, child) for child in gen_part.children])
    while q:
        parent, child = q.popleft()
        node = Node(pdg_id=child.pdgId, status=child.status, pt=child.pt, eta=child.eta)
        if parent is not None:
            parent.children.append(node)
        q.extendleft([(node, c) for c in child.children][::-1])

    # flatten into node name-label pairs and relation strings
    labels = {}
    relations = {}
    q = collections.deque([root])
    while q:
        node = q.popleft()
        labels[node.name] = node.label
        relations[node.name] = [c.name for c in node.children]
        q.extendleft(node.children[::-1])

    # convert to mermaid graph
    graph = ["graph TD"]
    for name, label in labels.items():
        graph.append(f"    {name}(\"{label}\")")
    for parent_name, child_names in relations.items():
        for child_name in child_names:
            graph.append(f"    {parent_name} --> {child_name}")
    graph_text = "\n".join(graph)

    # handle output
    if output_type == "text":
        return graph_text

    # build a mermaid.live link
    # for that, first encode the graph text
    data = json.dumps({
        "code": graph_text,
        "mermaid": json.dumps({"theme": "default"}),
    })
    encoded = base64.urlsafe_b64encode(zlib.compress(data.encode("utf-8"), level=9)).decode("utf-8")

    url = f"https://mermaid.live/edit#pako:{encoded}"
    return url
