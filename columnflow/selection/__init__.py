# coding: utf-8

"""
Object and event selection tools.
"""

from __future__ import annotations

import copy
import inspect

import law
import order as od

from columnflow.types import Callable, Sequence, T
from columnflow.util import maybe_import, DotDict, DerivableMeta
from columnflow.columnar_util import TaskArrayFunction
from columnflow.config_util import expand_shift_sources

ak = maybe_import("awkward")
np = maybe_import("numpy")

logger = law.logger.get_logger(__name__)


class Selector(TaskArrayFunction):
    """
    Base class for all selectors.
    """

    exposed = False

    def __init__(self: Selector, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

        # when not exposed and call_force is not specified,
        # set it to True which prevents calls from being cached
        if self.call_force is None and not self.exposed:
            self.call_force = True

    @classmethod
    def selector(
        cls,
        func: Callable | None = None,
        bases=(),
        mc_only: bool = False,
        data_only: bool = False,
        nominal_only: bool = False,
        shifts_only: Sequence[str] | set[str] | None = None,
        **kwargs,
    ) -> DerivableMeta | Callable:
        """
        Decorator for creating a new :py:class:`~.Selector` subclass with additional, optional
        *bases* and attaching the decorated function to it as ``call_func``.

        When *mc_only* (*data_only*) is *True*, the selector is skipped and not considered by
        other calibrators, selectors and producers in case they are evaluated on a
        :py:class:`order.Dataset` (using the :py:attr:`dataset_inst` attribute) whose ``is_mc``
        (``is_data``) attribute is *False*.

        When *nominal_only* is *True* or *shifts_only* is set, the selector is skipped and not
        considered by other calibrators, selectors and producers in case they are evaluated on a
        :py:class:`order.Shift` (using the :py:attr:`global_shift_inst` attribute) whose name does
        not match.

        All additional *kwargs* are added as class members of the new subclasses.

        :param func: Function to be wrapped and integrated into new :py:class:`Selector` class.
        :param bases: Additional bases for the new :py:class:`Selector`.
        :param mc_only: Boolean flag indicating that this :py:class:`Selector` should only run on
            Monte Carlo simulation and skipped for real data.
        :param data_only: Boolean flag indicating that this :py:class:`Selector` should only run on
            real data and skipped for Monte Carlo simulation.
        :param nominal_only: Boolean flag indicating that this :py:class:`Selector` should only run
            on the nominal shift and skipped on any other shifts.
        :param shifts_only: Shift names that this :py:class:`Selector` should only run on,
            skipping all other shifts.
        :return: New :py:class:`Selector` subclass.
        """
        def decorator(func: Callable) -> DerivableMeta:
            # create the class dict
            cls_dict = {
                **kwargs,
                "call_func": func,
                "mc_only": mc_only,
                "data_only": data_only,
                "nominal_only": nominal_only,
                "shifts_only": shifts_only,
            }

            # get the module name
            frame = inspect.stack()[1]
            module = inspect.getmodule(frame[0])

            # get the selector name
            cls_name = cls_dict.pop("cls_name", func.__name__)

            # hook to update the class dict during class derivation
            def update_cls_dict(cls_name, cls_dict, get_attr):
                mc_only = get_attr("mc_only")
                data_only = get_attr("data_only")
                nominal_only = get_attr("nominal_only")
                shifts_only = get_attr("shifts_only")

                # prepare shifts_only
                if shifts_only:
                    shifts_only_expanded = set(expand_shift_sources(shifts_only))
                    if shifts_only_expanded != shifts_only:
                        shifts_only = shifts_only_expanded
                        cls_dict["shifts_only"] = shifts_only

                # optionally add skip function
                if mc_only and data_only:
                    raise Exception(f"selector {cls_name} received both mc_only and data_only")
                if nominal_only and shifts_only:
                    raise Exception(
                        f"selector {cls_name} received both nominal_only and shifts_only",
                    )
                if mc_only or data_only or nominal_only or shifts_only:
                    if cls_dict.get("skip_func"):
                        raise Exception(
                            f"selector {cls_name} received custom skip_func, but either mc_only, "
                            "data_only, nominal_only or shifts_only are set",
                        )

                    def skip_func(self):
                        # check mc_only and data_only
                        if getattr(self, "dataset_inst", None):
                            if mc_only and not self.dataset_inst.is_mc:
                                return True
                            if data_only and not self.dataset_inst.is_data:
                                return True

                        # check nominal_only and shifts_only
                        if getattr(self, "global_shift_inst", None):
                            if nominal_only and not self.global_shift_inst.is_nominal:
                                return True
                            if shifts_only and self.global_shift_inst.name not in shifts_only:
                                return True

                        # in all other cases, do not skip
                        return False

                    cls_dict["skip_func"] = skip_func

                return cls_dict

            cls_dict["update_cls_dict"] = update_cls_dict

            # create the subclass
            subclass = cls.derive(cls_name, bases=bases, cls_dict=cls_dict, module=module)

            return subclass

        return decorator(func) if func else decorator


# shorthand
selector = Selector.selector


class SelectionResult(od.AuxDataMixin):
    """
    Lightweight class that wraps selection decisions (e.g. event and object selection steps).

    Additionally, this class provides convenience methods to merge them or to dump them into an
    awkward array. Arbitrary, auxiliary information (additional arrays, or other objects) that
    should not be stored in dumped akward arrays can be placed in the *aux* dictionary (see
    :py:class:`~order.mixins.AuxDataMixin`).

    The resulting structure looks like the following example:

    .. code-block:: python

        results = {
            # boolean selection mask for events
            "event": selected_events_mask,

            "steps": {
                # event selection decisions from certain steps
                "jet": array_of_event_masks,
                "muon": array_of_event_masks,
                ...,
            },

            "objects": {
                # object selection decisions or indices
                "Jet": {
                    "jet": array_of_jet_indices,
                    "bjet": array_of_bjet_indices,
                },
                "Muon": {
                    "muon": array_of_muon_indices,
                },
            },

            # additionally, you can also save auxiliary data, e.g.
            "aux": {
                # save the per-object jet selection masks
                "jet": array_of_jet_object_masks,
                # save number of jets
                "n_passed_jets": ak.num(array_of_jet_indices, axis=1),
                ...,
            },

            # other arbitrary top-level fields
            ...
        }

    Specific fields can be configured through *event*, *steps*, *objects* and *aux* keyword
    arguments. All additional keyword arguments are stored as top-level fields.

    The following example creates the structure above.

    .. code-block:: python

        # combined event selection after all steps
        event_sel = reduce(and_, results.steps.values())
        res = SelectionResult(
            event=selected_event_mask,
            steps={
                "jet": array_of_event_masks,
                "muon": array_of_event_masks,
                ...
            },
            # nested mappings of source collections to target collections with different indices
            objects={
                # collections to be created from the initial "Jet" collection: "jet" and "bjet"
                # define name of new field and provide indices of the corresponding objects
                "Jet": {
                    "jet": array_of_jet_indices
                    "bjet": list_of_bjet_indices,
                },
                # collections to be created from the initial "Muon" collection: "muon"
                "Muon": {
                    "muon": array_of_selected_muon_indices,
                },
            },
            # others
            ...
        )
        res.to_ak()
    """

    @classmethod
    def _check_nones_and_convert(cls, name, mask, optiontype):
        assert not ak.any(ak.is_none(mask, axis=-1)), f"mask {name} contains None values"
        logger.info(f"mask {name} is of mixed type, but does not contain Nones: converting to pure type.")
        mask = ak.fill_none(mask, False if optiontype.content == ak.types.NumpyType("bool") else 0)
        return mask

    @classmethod
    def _nonecheck_and_uniontobool(cls, name, mask, dim=1):
        get_content = lambda a: a.type.content.content if dim == 2 else a.type.content
        c = get_content(mask)
        if is_union := isinstance(c, ak.types.UnionType):
            # this might come because 0 or 1 was used instead of True or False: try converting to booleans
            is_valid = (mask == 1) | (mask == 0) | (mask == False) | (mask == True) # noqa
            assert ak.all(is_valid), f"mask {name} contains booleans and non-binary values {mask[~is_valid]}"
            mask = ak.values_astype(mask, bool)
            # type is now still Union[...] but of only bool or only ?bool if everything is fine
            contents = get_content(mask).contents
            assert all([ut == contents[0] for ut in contents]), \
                f"mask contains values not convertible to boolean: {c.contents}"
            logger.info(f"mask {name} contains both numeric binaries and booleans. Converted to boolean.")
        if isinstance(c, ak.types.OptionType):
            mask = cls._check_nones_and_convert(name, mask, c)
        if is_union:
            # at this point, can still be Union[bool, bool, ...] > convert to bool with fill_none
            mask = ak.fill_none(mask, False)
        return mask

    @classmethod
    def check_valid_event_mask(cls, name, event_mask):
        assert isinstance(event_mask, (np.ndarray, ak.Array)), f"{name} should be array, not {type(event_mask)}"
        assert event_mask.ndim == 1, f"{name} array has illegal dimension {event_mask.ndim}"
        boolean_error = f"{name} contains %s, not booleans"
        if isinstance(event_mask, np.ndarray):
            event_mask = ak.Array(event_mask)
        event_mask = cls._nonecheck_and_uniontobool(name, event_mask, 1)
        # final check we get a boolean array
        c_prim = event_mask.type.content.primitive
        if "int" in c_prim:
            assert ak.all((event_mask == 0) | (event_mask == 1)), "event mask {name} contains non-binary integers"
            return ak.values_astype(event_mask, bool)
        assert c_prim == "bool", boolean_error % event_mask.type.content
        return event_mask

    @classmethod
    def check_valid_object_mask(cls, name, obj_mask):
        assert isinstance(obj_mask, (np.ndarray, ak.Array)), f"{name} should be array, not {type(obj_mask)}"
        assert obj_mask.ndim in {1, 2}, f"{name} array has illegal dimension {obj_mask.ndim}"
        if isinstance(obj_mask, np.ndarray):
            obj_mask = ak.Array(obj_mask)
        t = obj_mask.type
        c = t.content
        assert not isinstance(c, (ak.types.RecordType, ak.types.ScalarType)), f"{name} cannot contain illegal type"
        if obj_mask.ndim == 1:
            if isinstance(c, ak.types.OptionType):
                obj_mask = cls._check_nones_and_convert(name, obj_mask, c)
                c = obj_mask.type.content
            assert isinstance(c, ak.types.NumpyType), f"1d object mask {name} has not integers, but {c}"
            assert "int" in c.primitive, f"1d mask {name} does not contain integers but {c.primitive}"
            logger.info(f"converting 1d object mask {name} to 2d")
            return ak.from_regular(obj_mask[:, None])

        assert isinstance(c, (ak.types.ListType, ak.types.RegularType)), \
            f"{name} contains {c}. Some rows might be None"

        obj_mask = cls._nonecheck_and_uniontobool(name, obj_mask, 2)
        c = obj_mask.type.content.content
        assert isinstance(c, ak.types.NumpyType), f"{name} has illegal content {c}"
        prim = c.primitive
        assert "int" in prim or prim == "bool", f"2d mask {name} does not contain integers or booleans, but {prim}"
        if "int" in prim:
            return ak.from_regular(obj_mask)
        return obj_mask

    def __init__(
        self: SelectionResult,
        event: ak.Array | None = None,
        steps: DotDict | dict | None = None,
        objects: DotDict | dict | None = None,
        aux: DotDict | dict | None = None,
        **other,
    ) -> None:
        super().__init__(aux=aux)

        # store fields
        if event is not None:
            event = self.check_valid_event_mask("event", event)
        self.event = event
        if steps is not None:
            for step, step_mask in steps.items():
                steps[step] = self.check_valid_event_mask(step, step_mask)
        self.steps = DotDict.wrap(steps or {})
        if objects is not None:
            for src_object, dst_objects in objects.items():
                assert isinstance(dst_objects, (DotDict, dict)), "objects should be a (dot)dict of (dot)dicts"
                for dst_obj, dst_obj_mask in dst_objects.items():
                    dst_objects[dst_obj] = self.check_valid_object_mask(dst_obj, dst_obj_mask)
        self.objects = DotDict.wrap(objects or {})
        self.other = DotDict.wrap(other)

    def __iadd__(self: SelectionResult, other: SelectionResult | None) -> SelectionResult:
        """
        Adds the field of an *other* instance in-place.

        When *None*, *this* instance is returned unchanged.

        :param other: Instance of :py:class:`~.SelectionResult` to be added to current instance.
        :raises TypeError: If *other* is not a :py:class:`~.SelectionResult` instance.
        :return: This instance.
        """
        # do nothing if the other instance is none
        if other is None:
            return self

        # type check
        if not isinstance(other, SelectionResult):
            raise TypeError(f"cannot add '{other}' to {self.__class__.__name__} instance")

        # helper to create a view without behavior
        def deepcopy_without_behavior(struct: T) -> T:
            return copy.deepcopy(law.util.map_struct(
                (lambda obj: ak.Array(obj, behavior={}) if isinstance(obj, ak.Array) else obj),
                struct,
                map_list=True,
                map_tuple=True,
                map_dict=True,
            ))

        # logical AND between event masks
        if self.event is None:
            self.event = deepcopy_without_behavior(other.event)
        elif other.event is not None:
            self.event = self.event & other.event
        # update steps in-place
        self.steps.update(deepcopy_without_behavior(other.steps))
        # use deep merging for objects
        law.util.merge_dicts(
            self.objects,
            deepcopy_without_behavior(other.objects),
            inplace=True,
            deep=True,
        )
        # update other fields in-place
        self.other.update(deepcopy_without_behavior(other.other))
        # shallow update for aux
        self.aux.update(deepcopy_without_behavior(other.aux))

        return self

    def __add__(self: SelectionResult, other: SelectionResult | None) -> SelectionResult:
        """
        Returns a new instance with all fields of *this* and an *other*
        instance merged.

        When *None*, a copy of *this* instance is returned.

        :param other: Instance of :py:class:`~.SelectionResult` to be added to current instance.
        :raises TypeError: If *other* is not a :py:class:`~.SelectionResult` instance.
        :return: Copy of this instance after the "add" operation.
        """
        inst = self.__class__()

        # add this instance
        inst += self

        # add the other instance if not none
        if other is not None:
            if not isinstance(other, SelectionResult):
                raise TypeError(f"cannot add '{other}' to {self.__class__.__name__} instance")
            inst += other

        return inst

    def to_ak(self: SelectionResult) -> ak.Array:
        """
        Converts the contained fields into a nested awkward array and returns it.

        The conversion is performed with multiple calls of :external+ak:py:func:`ak.zip`.

        :raises ValueError: If the main events mask contains a type other than bool.
        :raises KeyError: If the additional top-level fields in :py:attr:`other` have a field
            "event", "step" or "objects" that might overwrite existing special fields.
        :return: :py:class:`~.SelectionResult` transformed into an awkward array.
        """
        # complain if the event mask consists of non-boolean values
        if self.event is not None and getattr(ak.type(self.event).content, "primitive", None) != "bool":
            raise ValueError(
                f"{self.__class__.__name__} event mask must be of type N * bool, "
                "but got {ak.type(self.event)}",
            )

        # prepare objects to merge
        to_merge = {}
        if self.event is not None:
            to_merge["event"] = self.event
        if self.steps:
            to_merge["steps"] = ak.zip(self.steps)
        if self.objects:
            to_merge["objects"] = ak.zip({
                src_name: ak.zip(dst_dict, depth_limit=1)  # limit due to ragged axis 1
                for src_name, dst_dict in self.objects.items()
            })

        # add other fields but verify they do not overwrite existing fields
        for key in self.other:
            if key in to_merge:
                raise KeyError(
                    f"additional top-level field '{key}' of {self.__class__.__name__} conflicts "
                    f"with existing special field '{key}'",
                )
        to_merge.update(self.other)

        return ak.zip(to_merge)
