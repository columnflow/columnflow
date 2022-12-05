# coding: utf-8

"""
Useful functions for use by calibrators
"""

from __future__ import annotations

from columnflow.util import maybe_import

np = maybe_import("numpy")
ak = maybe_import("awkward")


# https://github.com/scikit-hep/awkward/issues/489\#issuecomment-711090923
def ak_random(*args, rand_func):
    """
    Return an awkward array filled with random numbers. The *args* must be broadcastable
    awkward arrays and will be passed as positional arguments to *rand_func* to obtain the
    random numbers.
    """
    args = ak.broadcast_arrays(*args)

    if hasattr(args[0].layout, "offsets"):
        # convert to flat numpy arrays
        np_args = [np.asarray(a.layout.content) for a in args]

        # pass flat arrays to random function and get random values
        np_randvals = rand_func(*np_args)

        # convert back to awkward array
        return ak.Array(ak.layout.ListOffsetArray64(args[0].layout.offsets, ak.layout.NumpyArray(np_randvals)))

    # pass args directly (this may fail for some array types)
    # TODO: make function more general
    np_randvals = rand_func(*args)
    return ak.from_numpy(np_randvals)


def propagate_met(
    jet_pt1: ak.Array,
    jet_phi1: ak.Array,
    jet_pt2: ak.Array,
    jet_phi2: ak.Array,
    met_pt1: ak.Array,
    met_phi1: ak.Array,
) -> tuple[ak.Array, ak.Array]:
    """
    Helper function to compute new MET based on per-jet pts and phis
    before and after a correction.
    """

    # avoid unwanted broadcasting
    assert jet_pt1.ndim == jet_phi1.ndim
    assert jet_pt2.ndim == jet_phi2.ndim

    # build px and py sums before and after
    jet_px1 = jet_pt1 * np.cos(jet_phi1)
    jet_py1 = jet_pt1 * np.sin(jet_phi1)
    jet_px2 = jet_pt2 * np.cos(jet_phi2)
    jet_py2 = jet_pt2 * np.sin(jet_phi2)

    # sum over axis 1 when not already done
    if jet_pt1.ndim > 1:
        jet_px1 = ak.sum(jet_px1, axis=1)
        jet_py1 = ak.sum(jet_py1, axis=1)
    if jet_pt2.ndim > 1:
        jet_px2 = ak.sum(jet_px2, axis=1)
        jet_py2 = ak.sum(jet_py2, axis=1)

    # propagate to met
    met_px2 = met_pt1 * np.cos(met_phi1) - (jet_px2 - jet_px1)
    met_py2 = met_pt1 * np.sin(met_phi1) - (jet_py2 - jet_py1)

    # compute new components
    met_pt2 = (met_px2**2.0 + met_py2**2.0)**0.5
    met_phi2 = np.arctan2(met_py2, met_px2)

    return met_pt2, met_phi2
