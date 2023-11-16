# coding: utf-8

"""
Distance-based methods.
"""

from __future__ import annotations

from columnflow.types import Callable, Union
from columnflow.selection import Selector, SelectionResult, selector
from columnflow.util import maybe_import

np = maybe_import("numpy")
ak = maybe_import("awkward")


def cleaning_factory(
    selector_name: str,
    to_clean: str,
    clean_against: list[str],
    metric: Union[Callable, None] = None,
) -> Selector:
    """
    Factory to generate a function with name *selector_name* that cleans the field *to_clean* in an
    array following the :external+coffea:py:class:`~coffea.nanoevents.NanoAODSchema` against the
    field(s) *clean_against*. First, the necessary column names to construct four-momenta for the
    different object fields are constructed, i.e. ``pt``, ``eta``, ``phi`` and ``e`` for the
    different objects. Finally, the actual selector function is generated, which uses these columns.

    :param selector_name: Name of the :py:class:`~columnflow.selection.Selector` class to be
        initialized.
    :param to_clean: Name of the field to be cleaned (e.g. ``"Jet"``).
    :param clean_against: Names of the fields of object to clean field *to_clean* against
        (e.g. ``["Muon"]``).
    :param metric: Function to use for the cleaning. If None, use
        :external+coffea:py:meth:`~coffea.nanoevents.methods.vector.LorentzVector.delta_r`.
    :return: Instance of :py:class:`~columnflow.selection.Selector`.
    """
    # default of the metric function is the delta_r function
    # of the coffea LorentzVectors
    if metric is None:
        metric = lambda a, b: a.delta_r(b)

    # compile the list of variables that are necessary for the four momenta
    # this list is always the same
    variables_for_lorentzvec = ["pt", "eta", "phi", "e"]

    # sum up all fields aht are to be considered, i.e. the field *to_clean*
    # and all fields in *clean_against*
    all_fields = clean_against + [to_clean]

    # construct the set of columns that is necessary for the four momenta in
    # the different fields (and thus also for the current implementation of
    # the cleaning itself) by looping through the fields and variables.

    uses = {
        f"{x}.{var}" for x in all_fields for var in variables_for_lorentzvec
    }

    # additionally, also load the lengths of the different fields
    uses |= {f"n{x}" for x in all_fields}

    # finally, construct selector function itself
    @selector(uses=uses, name=selector_name)
    def func(
        self: Selector,
        events: ak.Array,
        to_clean: str,
        clean_against: list[str],
        metric: Union[Callable, None] = metric,
        threshold: float = 0.4,
    ) -> ak.Array:
        """
        Abstract function to perform a cleaning of field *to_clean* against a (list of) field(s)
        *clean_against* based on an abitrary metric *metric* (e.g.
        :external+coffea:py:meth:`~coffea.nanoevents.methods.vector.LorentzVector.delta_r`). First
        concatenate all fields in *clean_against*, which thus includes all fields that are to be
        used for the comparison of the metric. Then construct the metric for all permutations of the
        different objects using the :external+coffea:doc:`index`
        :external+coffea:py:meth:`~coffea.nanoevents.methods.vector.LorentzVector.nearest`
        implementation. All objects in field *to_clean* are removed if the metric is below the
        *threshold*.

        :param self: :py:class:`columnflow.selection.Selector` instance into which this function is
            embedded.
        :param events: array containing events in the NanoAOD format
        param to_clean: Name of the field to be cleaned (e.g. ``"Jet"``)
        :param clean_against: Names of the fields of object to clean field *to_clean* against (e.g.
            ``["Muon"]``)
        :param metric: Function to use for the cleaning. If None, the
            :external+coffea:py:meth:`~coffea.nanoevents.methods.vector.LorentzVector.delta_r`,
            defaults to None.
        :param threshold: Threshold value for decision which objects to keep and which to reject,
            defaults to ``0.4``.
        :return: array of indices of cleaned objects, ordered according to the ``pt`` of the
            objects.
        """
        # concatenate the fields that are to be used in the construction
        # of the metric table
        summed_clean_against = ak.concatenate(
            [events[x] for x in clean_against],
            axis=1,
        )

        # load actual NanoEventArray that is to be cleaned
        to_clean_field = events[to_clean]

        # construct metric table for these objects. The metric table contains the minimal value of
        # the metric *metric* for each object in field *to_clean* w.r.t. all objects in
        # *summed_clean_against*. Thus, it has the dimensions nevents x nto_clean, where *nevents*
        # is the number of events in the current chunk of data and *nto_clean* is the length of the
        # field *to_clean*. Note that the argument *threshold* in the *nearest* function must be set
        # to None since the function will perform a selection itself to extract the nearest objects
        # (i.e. applies the selection we want here in reverse)
        _, metric = to_clean_field.nearest(
            summed_clean_against,
            metric=metric,
            return_metric=True,
            threshold=None,
        )
        # build a binary mask based on the selection threshold provided by the
        # user
        mask = metric > threshold

        # construct final result. Currently, this is the list of indices for
        # clean jets, sorted for pt
        # WARNING: this still contains the bug with the application of the mask
        #          which will be adressed in a PR in the very near future
        # TODO: return the mask itself instead of the list of indices
        sorted_list = ak.argsort(to_clean_field.pt, axis=-1, ascending=False)[mask]
        return sorted_list

    return func


delta_r_jet_lepton = cleaning_factory(
    selector_name="delta_r_jet_lepton",
    to_clean="Jet",
    clean_against=["Muon", "Electron"],
    metric=lambda a, b: a.delta_r(b),
)


@selector(uses={delta_r_jet_lepton})
def jet_lepton_delta_r_cleaning(
    self: Selector,
    events: ak.Array,
    stats: dict[str, Union[int, float]],
    threshold: float = 0.4,
    **kwargs,
) -> tuple[ak.Array, SelectionResult]:
    """
    Function to apply the selection requirements necessary for a cleaning of jets against leptons.

    The function calls the requirements to clean the field *Jet* against the concatination of the
    fields *[Muon, Electron]*, i.e. all leptons and passes the desired threshold for the selection

    :param events: Array containing events in the NanoAOD format
    :param stats: :py:class:`dictionary <dict>` containing selection stats (not used here).
    :param threshold: Threshold value for decision which objects to keep and which to reject.

    :return: Tuple containing the events array and a
        :py:class:`~columnflow.selection.SelectionResult` with indices of cleaned jets in the
        "Jet" object field.
    """
    clean_jet_indices = self[delta_r_jet_lepton](events, "Jet", ["Muon", "Electron"], threshold=threshold)

    # TODO: should not return a new object collection but an array with masks
    return events, SelectionResult(objects={"Jet": clean_jet_indices})
