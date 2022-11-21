# coding: utf-8

import os
import re
from typing import Set

import yaml
import order as od

from columnflow.util import DotDict

thisdir = os.path.dirname(os.path.abspath(__file__))


def add_shift_config(config: od.Config) -> None:

    config.set_aux("jer", DotDict.wrap({
        "source": "https://raw.githubusercontent.com/cms-jet/JRDatabase/master/textFiles",
        "campaign": "Summer19UL17",
        "version": "JRV2",
        "jet_type": "AK4PFchs",
    }))

    # helper to add column aliases for both shifts of a source
    def add_aliases(shift_source: str, aliases: Set[str], selection_dependent: bool):
        for direction in ["up", "down"]:
            shift = config.get_shift(od.Shift.join_name(shift_source, direction))
            # format keys and values
            inject_shift = lambda s: re.sub(r"\{([^_])", r"{_\1", s).format(**shift.__dict__)
            _aliases = {inject_shift(key): inject_shift(value) for key, value in aliases.items()}
            alias_type = "column_aliases_selection_dependent" if selection_dependent else "column_aliases"
            # extend existing or register new column aliases
            shift.set_aux(alias_type, shift.get_aux(alias_type, {})).update(_aliases)

    # register shifts
    config.add_shift(name="nominal", id=0)
    config.add_shift(name="tune_up", id=1, type="shape", tags={"disjoint_from_nominal"})
    config.add_shift(name="tune_down", id=2, type="shape", tags={"disjoint_from_nominal"})
    config.add_shift(name="hdamp_up", id=3, type="shape", tags={"disjoint_from_nominal"})
    config.add_shift(name="hdamp_down", id=4, type="shape", tags={"disjoint_from_nominal"})
    config.add_shift(name="minbias_xs_up", id=7, type="shape")
    config.add_shift(name="minbias_xs_down", id=8, type="shape")
    add_aliases("minbias_xs", {"pu_weight": "pu_weight_{name}"}, selection_dependent=False)

    with open(os.path.join(thisdir, "jec_sources.yaml"), "r") as f:
        all_jec_sources = yaml.load(f, yaml.Loader)["names"]
    for jec_source in config.x.jec["uncertainty_sources"]:
        idx = all_jec_sources.index(jec_source)
        config.add_shift(name=f"jec_{jec_source}_up", id=5000 + 2 * idx, type="shape")
        config.add_shift(name=f"jec_{jec_source}_down", id=5001 + 2 * idx, type="shape")
        add_aliases(
            f"jec_{jec_source}",
            {"Jet.pt": "Jet.pt_{name}", "Jet.mass": "Jet.mass_{name}"},
            selection_dependent=True,
        )

    config.add_shift(name="jer_up", id=6000, type="shape", tags={"selection_dependent"})
    config.add_shift(name="jer_down", id=6001, type="shape", tags={"selection_dependent"})
    add_aliases("jer", {"Jet.pt": "Jet.pt_{name}", "Jet.mass": "Jet.mass_{name}"}, selection_dependent=True)
