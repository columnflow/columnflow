# Best practices

## Variables creation

TODO

## Calibrators

TODO

## Production of columns

TODO

## Selections

### Introduction

In columnflow, selections are defined through the {py:class}`~columnflow.selection.Selector` class.
This class allows for arbitrary selection criteria on event level as well as object level. The
results of the selection (which events or objects are to be conserved) are saved in an instance of the
{py:class}`~columnflow.selection.SelectorResult` class. Similar to
{py:class}`~columnflow.production.Producer`s, it is possible to create new columns in
{py:class}`~columnflow.selection.Selector`s. In the original columnflow setup,
{py:class}`~columnflow.selection.Selector`s are being run in the ```SelectEvents``` task.

### Create an instance of the Selector class

Similar to {py:class}`~columnflow.production.Producer`s, {py:class}`~columnflow.selection.Selector`s
need to declare which columns are to be used (produced) by the
{py:class}`~columnflow.selection.Selector` instance in order for them to taken out of the parquet files
(saved in the new parquet files). An example for this structure is given below (partially taken
from the {py:class}`~columnflow.selection.Selector` documentation.):

```python

# import the Selector class and the selector method
from columnflow.selection import Selector, selector

# also import the SelectionResult class
from columnflow.selection import SelectionResult

# maybe import awkard in case this Selector is actually run
ak = maybe_import("awkward")

# now wrap any function with a selector
@selector(
    # define some additional information here, e.g.
    # what columns are needed for this Selector?
    uses={
        "Jet.pt", "Jet.eta"
    },
    # does this Selector produce any columns?
    produces={}

    # pass any other variable to the selector class
    some_auxiliary_variable=True

    # ...
)
def jet_selection(events: ak.Array) -> ak.Array, SelectionResult:
    # do something ...
    return events, SelectionResult()
```

The structure of the arguments for the returned SelectionResult instance are discussed below. (Input the internal link here)

#### Exposed and internal Selectors

{py:class}`~columnflow.selection.Selector`s can be either available directly from the command line
or only internally, through other selectors. To make a {py:class}`~columnflow.selection.Selector`
available from the command line, it should be declared with the ```exposed=True``` argument.
To call a fully funtional {py:class}`~columnflow.selection.Selector` (in the following refered
as Selector_int) from an other {py:class}`~columnflow.selection.Selector` (in the following refered
to as Selector_ext), several steps are required:
- Selector_int should be imported in the Selector_ext script,
- The columns needed for Selector_int should be declared in the ```uses``` argument of Selector_ext
(it is possible to simply write the name of the Selector_int in the ```uses``` set, the content of
the ```uses``` set from Selector_int will be added to the ```uses``` set of Selector_ext, see below)
- Selector_int must be run in Selector_ext, e.g. with the
```self[Selector_int](events, kwargs)``` call.







```
Derivative of :py:class:`~columnflow.columnar_util.TaskArrayFunction`
    that handles selections.

    :py:class:`~.Selector` s are designed to apply
    arbitrary selection criteria. These critera can be based on already existing
    nano AOD columns, but can also involve the output of any other module,
    e.g. :py:class:`~columnflow.production.Producer`s. To reduce the need to
    run potentionally computation-expensive operations multiple times, they can
    also write new columns. Similar to :py:class:`~columnflow.production.Producer` s,
    and :py:class:`~columnflow.calibration.Calibrator` s, this new columns must
    be specified in the `produces` set.

    Apart from the awkward array, a :py:class:`~.Selector` must also return a
    :py:class:`~.SelectionResult`. This object contains boolean masks on event
    and object level that represent which objects and events pass different
    selections. These masks are saved to disc and are intended for more involved
    studies, e.g. comparisons between frameworks.

    To create a new :py:class:`~.Selector`, you can use the decorrator
    class method :py:meth:`~.Selector.selector` like this:


    .. code-block:: python

        # import the Selector class and the selector method
        from columnflow.selection import Selector, selector

        # also import the SelectionResult
        from columnflow.selection import SelectionResult

        # maybe import awkard in case this Selector is actually run
        ak = maybe_import("awkward")

        # now wrap any function with a selector
        @selector(
            # define some additional information here, e.g.
            # what columns are needed for this Selector?
            uses={
                "Jet.pt", "Jet.eta"
            },
            # does this Selector produce any columns?
            produces={}

            # pass any other variable to the selector class
            is_this_a_fun_auxiliary_variable=True

            # ...
        )
        def jet_selection(events: ak.Array) -> ak.Array, SelectionResult:
            # do something ...

    The decorrator will create a new :py:class:`~.Selector` instance with the
    name of your function. The function itself is set as the `call_func` if
    the :py:class:`~.Selector` instance. All keyword arguments specified in the
    :py:meth:`~.Selector.selector` are available as member variables of your
    new :py:class:`~.Selector` instance.

    In additional to the member variables inherited from
    :py:class:`~columnflow.columnar_util.TaskArrayFunction` class, the
    :py:class:`~.Selector` class defines the *exposed* variable. This member
    variable controls whether this :py:class:`~.Selector` instance is a
    top level Selector that can be used directly for the
    :py:class:`~columnflow.tasks.selection.SelectEvents` task.
    A top level :py:class:`~.Selector` should not need anything apart from
    the awkward array containing the events, e.g.

    .. code-block:: python

        @selector(
            # some information for Selector

            # This Selector will need some external input, see below
            # Therefore, it should not be exposed
            exposed=False
        )
        def some_internal_selector(
            events: ak.Array,
            some_additional_input: Any
        ) -> ak.Array, SelectionResult:
            result = SelectionResult()
            # do stuff with additional information
            return events, result

        @selector(
            # some information for Selector
            # e.g., if we want to use some internal Selector, make
            # sure that you have all the relevant information
            uses={
                some_internal_selector,
            },
            produces={
                some_internal_selector,
            }

            # this is our top level Selector, so we need to make it reachable
            # for the SelectEvents task
            exposed=True
        )
        def top_level_selector(events: ak.Array) -> ak.Array, SelectionResult:
            results = SelectionResult()
            # do something here

            # e.g., call the internal Selector
            additional_info = 2
            events, sub_result = self[some_internal_selector](events, additional_info)
            result += sub_result

            return events, result


    :param exposed: Member variable that controls whether this
        :py:class:`~.Selector` instance is a top level Selector that can
        be used directly for the :py:class:`~columnflow.tasks.selection.SelectEvents`
        task. Defaults to `False`.
    :type exposed: `bool`

``

### Add a step to an existent selection

### Create a new "exposed" Selector





