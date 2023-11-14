# coding: utf-8

"""
Useful functions for use by calibrators
"""

from __future__ import annotations

from columnflow.types import Callable
from columnflow.columnar_util import flat_np_view, layout_ak_array
from columnflow.util import maybe_import

np = maybe_import("numpy")
ak = maybe_import("awkward")


# https://github.com/scikit-hep/awkward/issues/489\#issuecomment-711090923
def ak_random(*args, rand_func: Callable) -> ak.Array:
    """
    Return an awkward array filled with random numbers.

    The *args* must be broadcastable awkward arrays and will be passed as positional arguments to
    *rand_func* to obtain the random numbers.

    :param rand_func: Callable to generate random numbers from awkward arrays in *args*.
    :return: awkward array filled with random numbers.
    """
    args = ak.broadcast_arrays(*args)

    if hasattr(args[0].layout, "offsets"):
        # pass flat arrays to random function and get random values
        np_randvals = rand_func(*map(flat_np_view, args))

        # apply layout of first (ak) array
        return layout_ak_array(np_randvals, args[0])

    # pass args directly (this may fail for some array types)
    np_randvals = rand_func(*args)
    return ak.from_numpy(np_randvals)


def propagate_met(
    jet_pt1: (ak.Array),
    jet_phi1: ak.Array,
    jet_pt2: ak.Array,
    jet_phi2: ak.Array,
    met_pt1: ak.Array,
    met_phi1: ak.Array,
) -> tuple[ak.Array, ak.Array]:
    """
    Helper function to compute new MET based on per-jet pts and phis before and after a correction.
    Since the pts and phis parameterize the individual jets, the dimensions of the arrays
    (*jet_pt1*, *jet_phi1*) as well as (*jet_pt2*, *jet_phi2*) must be the same. The pt values are
    decomposed into their x and y components, which are then propagated to the corresponding
    contributions to the MET vector

    :param jet_pt1: transverse momentum of first jet(s)
    :param jet_phi1: azimuthal angle of first jet(s)
    :param jet_pt2: transverse momentum of second jet(s)
    :param jet_phi2: azimuthal angle of second jet(s)
    :param met_pt1: missing transverse momentum (MET)
    :param met_phi1: azimuthal angle of MET vector

    :raises AssertionError: if arrays (*jet_pt1*, *jet_phi1*) and (*jet_pt2*, *jet_phi2*) have
        different dimensions.
    :return: updated values of MET vector, i.e. missing transverse momentum and corresponding
        azimuthal angle phi.
    """
    # avoid unwanted broadcasting
    if jet_pt1.ndim != jet_phi1.ndim:
        raise Exception(
            f"dimension of jet_pt1 {jet_pt1.ndim} does not match dimension of jet_phi1 "
            f"{jet_phi1.ndim}",
        )
    if jet_pt2.ndim != jet_phi2.ndim:
        raise Exception(
            f"dimension of jet_pt2 {jet_pt2.ndim} does not match dimension of jet_phi2 "
            f"{jet_phi2.ndim}",
        )

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
