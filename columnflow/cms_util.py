# coding: utf-8

"""
Collection of CMS specific helpers and utilities.
"""

from __future__ import annotations

__all__ = []

import os
import re
import copy
import pathlib
import dataclasses

from columnflow.types import ClassVar, Generator


#: Default root path to CAT metadata.
cat_metadata_root = "/cvmfs/cms-griddata.cern.ch/cat/metadata"


@dataclasses.dataclass
class CATSnapshot:
    """
    Dataclass to wrap YYYY-M[M]-D[D] stype timestamps of CAT metadata per POG stored in
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
                btv="2025-8-20",
                egm="2025-4-15",
                jme="2025-9-23",
                lum="2024-1-31",
                muo="2025-8-14",
                tau="2025-10-1",
            ),
        )
    """
    run: int
    era: str
    vnano: int
    snapshot: CATSnapshot

    metadata_root: ClassVar[str] = cat_metadata_root

    @property
    def key(self) -> str:
        """
        Returns the era key that is used for directory names in the CAT metadata structure.
        """
        return f"Run{self.run}-{self.era}-NanoAODv{self.vnano}"

    def get_file(self, pog: str, *paths: str | pathlib.Path) -> str:
        """
        Returns the full path to a specific file or directory defined by *paths* in the CAT metadata structure for a
        given *pog*.
        """
        return os.path.join(
            self.metadata_root,
            pog.upper(),
            self.key,
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
        if not (m := re.match(r"^/([^/]+)/([^/-]+)-([^/-]+)-([^/-]+)/(.+)AOD(SIM)?$", key)):
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
        if not isinstance(value, str):
            raise TypeError(f"expected string for kind, got '{value}' ({type(value)})")
        if (_value := value.lower()) not in {"mc", "data"}:
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
