# coding: utf-8

"""
Collection of CMS specific helpers and utilities.
"""

from __future__ import annotations

__all__ = []

import os
import dataclasses

from columnflow.types import ClassVar, Generator


cat_metadata_root = "/cvmfs/cms-griddata.cern.ch/cat/metadata"


@dataclasses.dataclass
class CATSnapshot:
    """
    Dataclass to wrap YYYY-MM-DD stype timestamps of CAT metadata per POG stored in
    "/cvmfs/cms-griddata.cern.ch/cat/metadata". No format parsing or validation is done, leaving responsibility to the
    user.
    """
    btv: str = ""
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

    METADATA_ROOT: ClassVar[str] = cat_metadata_root

    @property
    def key(self) -> str:
        """
        Returns the era key that is used for directory names in the CAT metadata structure.
        """
        return f"Run{self.run}-{self.era}-NanoAODv{self.vnano}"

    def get_file(self, pog: str, *path: str) -> str:
        """
        Returns the full path to a specific file or directory defined by *path* in the CAT metadata structure for a
        given *pog*.
        """
        return os.path.join(
            self.METADATA_ROOT,
            pog.upper(),
            self.key,
            getattr(self.snapshot, pog.lower()),
            *(p.strip("/") for p in path),
        )
