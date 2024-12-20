# coding: utf-8

"""
Generic producers for matching between physics object collections.
"""

from __future__ import annotations

from columnflow.production import Producer, producer
from columnflow.production.util import delta_r_match_multiple, lv_mass
from columnflow.util import maybe_import
from columnflow.columnar_util import set_ak_column

np = maybe_import("numpy")
ak = maybe_import("awkward")


#
# generic delta-r index-based matching producer
#

@producer(
    # mandatory parameters to be set by derived classes
    src_name=None,  # name of the source collection
    dst_name=None,  # name of the destination collection
    output_idx_column=None,  # name of the output column
    # optional parameters
    max_dr=None,  # maximum allowed delta-R for a valid match
)
def delta_r_matcher(
    self: Producer,
    events: ak.Array,
    **kwargs,
) -> ak.Array:
    """
    """
    # check missing attributes
    missing_attrs = {
        attr for attr in ["src_name", "dst_name", "output_idx_column"]
        if getattr(self, attr, None) is None
    }
    if missing_attrs:
        missing_attrs_str = ",".join(sorted(missing_attrs))
        raise ValueError(
            f"mandatory attributes for derived {type(self)} "
            f"'{self.__name__}' not set: {missing_attrs_str}",
        )

    # retrieve input collections
    src = events[self.src_name]
    dst = events[self.dst_name]

    # ensure Lorentz vectors
    src_lvs = lv_mass(src)
    dst_lvs = lv_mass(dst)

    # perform matching
    best_match_idxs, _ = delta_r_match_multiple(dst_lvs, src_lvs, max_dr=self.max_dr, as_index=True)
    best_match_lvs, _ = delta_r_match_multiple(dst_lvs, src_lvs, max_dr=self.max_dr)

    # store the index in the specified output column
    best_match_idxs = ak.fill_none(best_match_idxs, -1)
    events = set_ak_column(events, f"{self.src_name}.{self.output_column}", best_match_idxs, value_type=np.float32)
    __import__("IPython").embed()

    # return the event array
    return events


@delta_r_matcher.init
def delta_r_matcher_init(self: Producer, **kwargs) -> None:
    """
    Dynamically add `uses` and `produces`
    """
    # input columns
    self.uses |= {
        f"{collection}.{var}"
        for collection in (self.src_name, self.dst_name)
        for var in ("pt", "eta", "phi", "mass")
    }

    # outputs
    self.produces = {f"{self.src_name}.{self.output_idx_column}"}
