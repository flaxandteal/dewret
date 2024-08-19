# Copyright 2014 Flax & Teal Limited. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# with WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Overarching workflow concepts.

Basic constructs for describing a workflow.
"""

from __future__ import annotations
import inspect
from collections.abc import Mapping, MutableMapping, Callable
import base64
from attrs import has as attr_has, resolve_types, fields as attrs_fields
from dataclasses import is_dataclass, fields as dataclass_fields
from collections import Counter, OrderedDict
from typing import Protocol, Any, TypeVar, Generic, cast, Literal, TypeAliasType, Annotated, Iterable, get_origin, get_args, Generator, Sized, Sequence
from uuid import uuid4
from sympy import Symbol, Expr, Basic, Tuple, Dict, nan

import logging

logger = logging.getLogger(__name__)

from .core import RawType, IterableMixin, Reference, get_configuration, Raw, IteratedGenerator, strip_annotations
from .utils import hasher, is_raw, make_traceback, is_raw_type, is_expr, Unset

T = TypeVar("T")
U = TypeVar("U")
RetType = TypeVar("RetType")

CHECK_IDS = False


class Lazy(Protocol):
    """Requirements for a lazy-evaluatable function."""

    __name__: str

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        """When called this should return a reference."""
        ...


class LazyEvaluation(Lazy, Generic[RetType]):
    """Tracks a single evaluation of a lazy function."""

    def __init__(self, fn: Callable[..., RetType]):
        """Initialize an evaluation.

        Args:
            fn: callable returning RetType, which this will return
                also from it's __call__ method for consistency.
        """
        self._fn: Callable[..., RetType] = fn
        self.__name__ = fn.__name__

    def __call__(self, *args: Any, **kwargs: Any) -> RetType:
        """Wrapper around a lazy execution.

        Captures a traceback, for debugging if this does not work.

        WARNING: this is one of the few places that we would expect
        dask distributed to break, if running outside a single process
        is attempted.
        """
        tb = make_traceback()
        result = self._fn(*args, **kwargs, __traceback__=tb)
        return result


Target = Callable[..., Any]
StepExecution = Callable[..., Lazy]
LazyFactory = Callable[[Target], Lazy]


class UnsetType(Unset, Generic[T]):
    """Unset variable with a specific type.

    Attributes:
        __type__: type of the variable.
    """

    __type__: type[T]

    def __init__(self, raw_type: type[T]):
        """Create a new Unset token of a specific type.

        Attributes:
            __type__: type of the variable.
        """
        self.__type__ = raw_type


UNSET = Unset()



class Parameter(Generic[T], Symbol):
    """Global parameter.

    Independent parameter that will be used when a task is spotted
    reaching outside its scope. This wraps the variable it uses.

    To allow for potential arithmetic operations, etc. it is a Sympy
    symbol.

    Attributes:
        __name__: name of the parameter.
        __default__: captured default value from the original value.
    """

    __name__: str
    __name_suffix__: str = ""
    __default__: T | UnsetType[T]
    __tethered__: Literal[False] | None | BaseStep | Workflow
    __fixed_type__: type[T] | Unset
    autoname: bool = False

    def __init__(
        self,
        name: str,
        default: T | UnsetType[T],
        tethered: Literal[False] | None | Step | Workflow = None,
        autoname: bool = False,
        typ: type[T] | Unset = UNSET
    ):
        """Construct a parameter.

        Args:
            name: name of the parameter.
            default: value to infer type, etc. from.
            tethered: a workflow or step that demands this parameter; None if not yet present, False if not desired.
            autoname: whether we should customize this name for uniqueness (it is not user-set).
        """
        self.__original_name__ = name

        # TODO: is using this in a step hash a risk of ambiguity? (full name is circular)
        if autoname:
            self.__name_suffix__ = f"-{uuid4()}"
        self.autoname = autoname

        self.__name__ = name
        self.__default__ = default
        self.__tethered__ = tethered
        self.__callers__: list[BaseStep] = []
        self.__fixed_type__ = typ

        if tethered and isinstance(tethered, BaseStep):
            self.register_caller(tethered)

    def is_loopable(self, typ: type):
        base = get_origin(strip_annotations(typ)[0])
        return inspect.isclass(base) and issubclass(base, Iterable) and not issubclass(base, str | bytes)

    @property
    def __type__(self):
        if self.__fixed_type__ is not UNSET:
            return self.__fixed_type__

        default = self.__default__
        if (
            default is not None
            and hasattr(default, "__type__")
            and isinstance(default.__type__, type)
        ):
            raw_type = default.__type__
        else:
            raw_type = type(default)
        return raw_type

    def __eq__(self, other):
        return hash(self) == hash(other)

    def __new__(cls, *args, **kwargs):
        instance = Expr.__new__(cls)
        instance._assumptions0 = {}
        return instance

    def __hash__(self) -> int:
        """Get a unique hash for this parameter."""
        # if self.__tethered__ is None:
        #     raise RuntimeError(
        #         f"Parameter {self.name} was never tethered but should have been"
        #     )
        return hash(self.__name__)

    def make_reference(self, **kwargs) -> "ParameterReference":
        kwargs["parameter"] = self
        kwargs.setdefault("typ", self.__type__)
        typ = kwargs["typ"]
        if self.is_loopable(typ):
            return IterableParameterReference(**kwargs)
        return ParameterReference(**kwargs)

    @property
    def default(self) -> T | UnsetType[T]:
        """Retrieve default value for this parameter, or an unset token."""
        return self.__default__

    @property
    def name(self) -> str:
        """Extended name, suitable for rendering.

        This attempts to create a unique name by tying the parameter to a step
        if the user has not explicitly provided a name, ideally the one where
        we discovered it.
        """
        tethered = self.__tethered__
        if tethered is False or tethered is None or self.autoname is False:
            return self.__name__ + self.__name_suffix__
        else:
            return f"{tethered.name}-{self.__original_name__}"

    def register_caller(self, caller: BaseStep) -> None:
        """Capture a step that uses this parameter.

        Gathers together the steps using this parameter. The first found will
        be recorded as the tethered step, and used for forming the name.
        """
        if self.__tethered__ is None:
            self.__tethered__ = caller
        self.__callers__.append(caller)

    def __getattr__(self, attr: str) -> Reference[T]:
        return getattr(self.make_reference(workflow=None), attr)


def param(
    name: str,
    default: T | UnsetType[T] | Unset = UNSET,
    tethered: Literal[False] | None | Step | Workflow = False,
    typ: type[T] | Unset = UNSET,
    autoname: bool = False,
) -> T:
    """Create a parameter.

    Will cast so it looks like the original type.

    Returns:
        Parameter class cast to the type of the supplied default.
    """
    if default is UNSET:
        if isinstance(typ, Unset):
            raise ValueError("Must provide a default or a type")
        default = UnsetType[T](typ)
    return cast(
        T, Parameter(name, default=default, tethered=tethered, autoname=autoname, typ=typ)
    )


class Task:
    """Named wrapper of a lazy-evaluatable function.

    Wraps a lazy-evaluatable function (`dewret.workflow.Lazy`) with any
    metadata needed to render it later. At present, this is the name.

    Attributes:
        name: Name of the lazy function as it will appear in the output workflow text.
        target: Callable that is wrapped.
    """

    name: str
    target: Lazy

    def __init__(self, name: str, target: Lazy):
        """Initialize the Task.

        Args:
            name: Name of wrapped function.
            target: Actual function being wrapped (optional).
        """
        self.name = name
        self.target = target

    @property
    def __name__(self) -> str:
        """Name of the task."""
        return self.name

    def __str__(self) -> str:
        """Stringify the Task, currently by returning the `name`."""
        return self.name

    def __repr__(self) -> str:
        """Represent the Task, currently by returning the `name`."""
        return self.name

    def __hash__(self) -> int:
        """Hashes for finding."""
        return hash(self.name)

    def __eq__(self, other: object) -> bool:
        """Is this the same task?

        At present, we naively compare the name and target. In
        future, this may be more nuanced.
        """
        if not isinstance(other, Task):
            return False
        return self.name == other.name and self.target == other.target


class Workflow:
    """Overarching workflow concept.

    Represents a whole workflow, as a singleton maintaining all
    state information needed ahead of rendering. It is built up
    as the lazy-evaluations are finally evaluated.

    Attributes:
        steps: the sequence of calls to lazy-evaluable functions,
            built as they are evaluated.
        tasks: the mapping of names used in the `steps` to the actual
            `Task` wrappers they represent.
        result: target reference to evaluate, if yet present.
    """

    steps: list["BaseStep"]
    tasks: MutableMapping[str, "Task"]
    result: StepReference[Any] | list[StepReference[Any]] | tuple[StepReference[Any]] | None
    _remapping: dict[str, str] | None
    _name: str | None

    def __init__(self, name: str | None = None) -> None:
        """Initialize a Workflow, by setting `steps` and `tasks` to empty containers."""
        self.steps = []
        self.tasks = {}
        self.result: StepReference[Any] | None = None
        self._remapping = None
        self._name = name

    def __str__(self) -> str:
        """Name of the workflow, if available."""
        if self._name is None:
            return super().__str__()
        return self.name

    def __repr__(self) -> str:
        if self._name:
            return self.name
        comp_tup = tuple(sorted(s.id for s in self.steps))

        return f"workflow-{hasher(comp_tup)}"

    def __hash__(self) -> int:
        """Hashes for finding."""
        return hash((
            self._name,
            tuple(self.steps),
        ))

    def __eq__(self, other: object) -> bool:
        """Is this the same workflow?

        At present, we naively compare the steps and arguments. In
        future, this may be more nuanced.
        """
        if not isinstance(other, Workflow):
            return False
        return (
            self.steps == other.steps
            and self.tasks == other.tasks
            and self.result == other.result
            and self._remapping == other._remapping
            and self._name == other._name
        )

    @property
    def has_result(self) -> bool:
        return not(self.result is None or self.result is [])

    @property
    def name(self) -> str:
        """Get the name of the workflow.

        Raises:
            NameError: if no name has been set.
        """
        if self._name is None:
            raise NameError("Can not get the name of an anonymous workflow.")
        return self._name

    def find_factories(self) -> dict[str, FactoryCall]:
        """Steps that are factory calls."""
        return {step.id: step for step in self.steps if isinstance(step, FactoryCall)}

    def find_parameters(
        self, include_factory_calls: bool = True
    ) -> set[ParameterReference]:
        """Crawl steps for parameter references.

        As the workflow does not hold its own list of parameters, this
        dynamically finds them.

        Returns:
            Set of all references to parameters across the steps.
        """
        _, references = expr_to_references(
            step.arguments for step in self.steps if (include_factory_calls or not isinstance(step, FactoryCall))
        )
        return {ref for ref in references if isinstance(ref, ParameterReference)}

    @property
    def _indexed_steps(self) -> dict[str, BaseStep]:
        """Steps mapped by ID.

        Forces generation of IDs. Note that this effectively
        freezes the steps, so it should not be used until we
        are confident the steps are all ready to be hashed.

        Returns:
            Mapping of steps by ID.
        """
        return OrderedDict(sorted(((step.id, step) for step in self.steps), key=lambda x: x[0]))

    @classmethod
    def assimilate(cls, *workflows) -> "Workflow":
        """Combine two Workflows into one Workflow.

        Takes two workflows and unifies them by combining steps
        and tasks. If it sees mismatched identifiers for the same
        component, it will error.
        This could happen if the hashing function is flawed
        or some Python magic to do with Targets being passed.

        Argument:
            left: workflow to use as base
            right: workflow to combine on top
        """
        workflows = set(workflows)
        base = next(iter(workflows))

        if len(workflows) == 1:
            return base

        names = {w._name for w in workflows if w._name}
        base._name = base._name or (names and next(iter(names))) or None

        #left_steps = left._indexed_steps
        #right_steps = right._indexed_steps
        all_steps = sum((list(w._indexed_steps.items()) for w in workflows), [])

        for _, step in all_steps:
        #for step in list(left_steps.values()) + list(right_steps.values()):
            step.set_workflow(base)

        indexed_steps = {}
        for step_id, step in all_steps:
            indexed_steps.setdefault(step_id, step)
            if step != indexed_steps[step_id]:
                raise RuntimeError(
                    f"Two steps have same ID but do not match: {step_id}"
                )

        all_tasks = sum((list(w.tasks.items()) for w in workflows), [])
        indexed_tasks = {}
        for task_id, task in all_tasks:
            indexed_tasks.setdefault(task_id, task)
            if task != indexed_tasks[task_id]:
                raise RuntimeError(f"Two tasks have same name {task_id} but do not match")

        base.steps = list(indexed_steps.values())
        base.tasks = indexed_tasks

        for step in base.steps:
            step.set_workflow(base, with_arguments=True)

        results = set((w.result for w in workflows if w.result))
        if len(results) == 1:
            result = next(iter(results))
        else:
            results = {r if isinstance(r, tuple | list) else (r,) for r in results}
            result = sum(map(list, results), [])

        if result is not None and result != []:
            unify_workflows(result, base, set_only=True)
            base.set_result(result)

        return base

    def remap(self, step_id: str) -> str:
        """Apply name simplification if requested.

        Args:
            step_id: step to check.

        Returns:
            Same ID or a remapped name.
        """
        return self._remapping.get(step_id, step_id) if self._remapping else step_id

    def simplify_ids(self, infix: list[str] | None = None) -> None:
        """Work out mapping to simple ints from hashes.

        Goes through and numbers each step by the order of use of its task.
        """
        counter = Counter[Task | Workflow]()
        self._remapping = {}
        infix_str = ("-".join(infix) + "-") if infix else ""
        for step in self.steps:
            counter[step.task] += 1
            self._remapping[step.id] = f"{step.task}-{infix_str}{counter[step.task]}"
            if isinstance(step, NestedStep):
                step.subworkflow.simplify_ids(
                    infix=(infix or []) + [str(counter[step.task])]
                )
        param_counter = Counter[str]()
        name_to_original: dict[str, str] = {}
        for name, param in {
            pr._.parameter.__name__: pr._.parameter
            for pr in self.find_parameters()
            if isinstance(pr, ParameterReference)
        }.items():
            if param.__original_name__ != name:
                param_counter[param.__original_name__] += 1
                self._remapping[param.__name__] = (
                    f"{param.__original_name__}-{param_counter[param.__original_name__]}"
                )
                name_to_original[param.__original_name__] = param.__name__
        for pname, count in param_counter.items():
            if count == 1:
                self._remapping[name_to_original[pname]] = pname

    def register_task(self, fn: Lazy) -> Task:
        """Note the existence of a lazy-evaluatable function, and wrap it as a `Task`.

        Args:
            fn: the wrapped function.

        Returns:
            A new `Task` that wraps the function, and is retained in the `Workflow.tasks`
            dict.
        """
        name = fn.__name__
        if name in self.tasks and self.tasks[name].target != fn:
            raise RuntimeError(f"Naming clash for functions: {name}")

        task = Task(name, fn)
        self.tasks[name] = task
        return task

    def add_nested_step(
        self, name: str, subworkflow: Workflow, return_type: type | None, kwargs: dict[str, Any], positional_args: dict[str, bool] | None = None
    ) -> StepReference[Any]:
        """Append a nested step.

        Calls a subworkflow.

        Args:
            name: name of the subworkflow.
            subworkflow: the subworkflow itself.
            kwargs: any key-value arguments to pass in the call.
        """
        step = NestedStep(self, name, subworkflow, kwargs)
        if positional_args is not None:
            step.positional_args = positional_args
        self.steps.append(step)
        return_type = return_type or step.return_type
        if return_type is inspect._empty:
            raise TypeError("All tasks should have a type annotation.")
        return step.make_reference(typ=return_type)

    def add_step(
        self,
        fn: Lazy,
        kwargs: dict[str, Raw | Reference],
        raw_as_parameter: bool = False,
        is_factory: bool = False,
        positional_args: dict[str, bool] | None = None
    ) -> StepReference[Any]:
        """Append a step.

        Adds a step, for running a target with key-value arguments,
        to the workflow.

        Args:
            fn: the target function to turn into a step.
            kwargs: any key-value arguments to pass in the call.
            raw_as_parameter: whether to turn any discovered raw arguments into workflow parameters.
            is_factory: whether this step should be a Factory.
        """
        task = self.register_task(fn)
        step_maker = FactoryCall if is_factory else Step
        step = step_maker(self, task, kwargs, raw_as_parameter=raw_as_parameter)
        if positional_args is not None:
            step.positional_args = positional_args
        self.steps.append(step)
        return_type = step.return_type
        if (
            return_type is inspect._empty
            and not isinstance(fn, type)
            and not inspect.isclass(fn)
        ):
            raise TypeError("All tasks should have a type annotation.")
        return step.make_reference(typ=return_type)

    @staticmethod
    def from_result(
        result: StepReference[Any] | list[StepReference[Any]] | tuple[StepReference[Any]], simplify_ids: bool = False, nested: bool = True
    ) -> Workflow:
        """Create from a desired result.

        Starts from a result, and builds a workflow to output it.
        """
        result, refs = expr_to_references(result)
        if not refs:
            raise RuntimeError(
                "Attempted to build a workflow from a return-value/result/expression with no references."
            )
        refs = list(refs)
        workflow = refs[0].__workflow__
        # Ensure that we have exactly one workflow, even if multiple results.
        for entry in refs[1:]:
            if entry.__workflow__ != workflow:
                raise RuntimeError("If multiple results, they must share a single workflow")
        workflow.set_result(result)
        if simplify_ids:
            workflow.simplify_ids()
        return workflow

    def set_result(self, result: StepReference[Any] | list[StepReference[Any]] | tuple[StepReference[Any]]) -> None:
        """Choose the result step.

        Sets a step as being the result for the entire workflow.
        When we evaluate a dynamic workflow, the engine (e.g. dask)
        creates a graph to realize the result of a single collection.
        Similarly, in the static case, we need to have a result that
        drives the calculation.

        Args:
            result: reference to the chosen step.
        """
        _, refs = expr_to_references(result)
        for entry in refs:
            if entry.__workflow__ != self:
                raise RuntimeError("Output must be from a step in this workflow.")
        self.result = result

    @property
    def result_type(self):
        if self.result is None:
            return type(None)
        if hasattr(self.result, "__type__"):
            return self.result.__type__
        # TODO: get individual types!
        return type(self.result)


class WorkflowComponent:
    """Base class for anything directly tied to an individual `Workflow`.

    Attributes:
        __workflow__: the `Workflow` that this is tied to.
    """

    __workflow__: Workflow

    def __init__(self, *args, workflow: Workflow, **kwargs):
        """Tie to a `Workflow`.

        All subclasses must call this.

        Args:
            workflow: the `Workflow` to tie to.
        """
        self.__workflow__ = workflow
        super().__init__(*args, **kwargs)


class WorkflowLinkedComponent(Protocol):
    """Protocol for objects dynamically tied to a `Workflow`."""

    @property
    def __workflow__(self) -> Workflow:
        """Workflow currently tied to.

        Usually a proxy for another object that it should
        consistently follow.

        Returns:
            workflow: the `Workflow` to tie to.
        """
        ...


class FieldableProtocol(Protocol):
    __field__: tuple[str, ...]
    __field_sep__: str

    def __init__(self, *args, field: str | None = None, **kwargs):
        super().__init__(*args, **kwargs)

    @property
    def __type__(self):
        ...

    @property
    def name(self):
        return "name"

    def __make_reference__(self, *args, **kwargs) -> "FieldableProtocol":
        ...

# Subclass Reference so that we know Reference methods/attrs are available.
class FieldableMixin:
    def __init__(self: FieldableProtocol, *args, field: str | int | tuple | None = None, **kwargs):
        self.__field__: tuple[str, ...] = (field if isinstance(field, tuple) else (field,)) if field is not None else ()
        super().__init__(*args, **kwargs)

    @property
    def __field_sep__(self) -> str:
        return get_configuration("field_separator")

    @property
    def __name__(self: FieldableProtocol) -> str:
        """Name for this step.

        May be remapped by the workflow to something nicer
        than the ID.
        """
        return super().__name__ + self.__field_suffix__

    @property
    def __field_suffix__(self) -> str:
        result = ""
        for cmpt in self.__field__:
            if isinstance(cmpt, int):
                result += f"[{cmpt}]"
            else:
                result += f"{self.__field_sep__}{cmpt}"
        return result

    def find_field(self: FieldableProtocol, field, fallback_type: type | None = None, **init_kwargs: Any) -> Reference:
        """Field within the reference, if possible.

        Returns:
            A field-specific version of this reference.
        """

        # Get new type, for the specific field.
        parent_type, _ = strip_annotations(self.__type__)
        field_type = fallback_type

        if isinstance(field, int):
            base = get_origin(parent_type)
            if not inspect.isclass(base) or not issubclass(base, Sequence):
                raise AttributeError(f"Tried to index int {field} into a non-sequence type {parent_type} (base: {base})")
            if not (field_type := get_args(parent_type)[0]):
                raise AttributeError(
                    f"Tried to index int {field} into type {parent_type} but can only do so if the first type argument "
                    f"is the element type (args: {get_args(parent_type)}"
                )
        else:
            if is_dataclass(parent_type):
                try:
                    field_type = next(iter(filter(lambda fld: fld.name == field, dataclass_fields(parent_type)))).type
                except StopIteration:
                    raise AttributeError(f"Dataclass {parent_type} does not have field {field}")
            elif attr_has(parent_type):
                resolve_types(parent_type)
                try:
                    field_type = getattr(attrs_fields(parent_type), field).type
                except AttributeError:
                    raise AttributeError(f"attrs-class {parent_type} does not have field {field}")
            # TypedDict
            elif inspect.isclass(parent_type) and issubclass(parent_type, dict) and hasattr(parent_type, "__annotations__"):
                try:
                    field_type = parent_type.__annotations__[field]
                except KeyError:
                    raise AttributeError(f"TypedDict {parent_type} does not have field {field}")
            if not field_type and get_configuration("allow_plain_dict_fields") and strip_annotations(get_origin(parent_type))[0] is dict:
                args = get_args(parent_type)
                if len(args) == 2 and args[0] is str:
                    field_type = args[1]
                else:
                    raise AttributeError(f"Can only get fields for plain dicts if annotated dict[str, TYPE]")

        if field_type:
            if not issubclass(self.__class__, Reference):
                raise TypeError("Only references can have a fieldable mixin")

            if self.__field__:
                field = tuple(list(self.__field__) + [field])

            return self.__make_reference__(typ=field_type, field=field, **init_kwargs)

        raise AttributeError(
            f"Could not determine the type for field {field} in type {parent_type}"
        )

class BaseStep(WorkflowComponent):
    """Lazy-evaluated function call.

    Individual function call to a lazy-evaluatable function, tracked
    for building up the `Workflow`.

    Attributes:
        task: the `Task` being called in this step.
        arguments: key-value pairs of arguments to this step.
    """

    _id: str | None = None
    task: Task | Workflow
    arguments: Mapping[str, Reference | Raw]
    workflow: Workflow
    positional_args: dict[str, bool] | None = None

    def __init__(
        self,
        workflow: Workflow,
        task: Task | Workflow,
        arguments: Mapping[str, Reference | Raw],
        raw_as_parameter: bool = False,
    ):
        """Initialize a step.

        Args:
            workflow: `Workflow` that this is tied to.
            task: the lazy-evaluatable function that this wraps.
            arguments: key-value pairs to pass to the function.
            raw_as_parameter: whether to turn any raw-type arguments into workflow parameters (or just keep them as default argument values).
        """
        super().__init__(workflow=workflow)
        self.task = task
        self.arguments = {}
        for key, value in arguments.items():
            if (
                isinstance(value, FactoryCall)
                or isinstance(value, Reference)
                or isinstance(value, Raw)
                or is_raw(value)
                or is_expr(value)
                or is_dataclass(value)
                or attr_has(value)
            ):
                # Avoid recursive type issues
                if (
                    not isinstance(value, Reference)
                    and not isinstance(value, FactoryCall)
                    and not isinstance(value, Raw)
                    and is_raw(value)
                ):
                    if raw_as_parameter:
                        value = param(key, value, tethered=None).make_reference(workflow=workflow)
                    else:
                        value = Raw(value)

                def _to_param_ref(value):
                    if isinstance(value, Parameter):
                        return value.make_parameter(workflow=workflow)
                value, refs = expr_to_references(value, remap=_to_param_ref)

                for ref in refs:
                    if isinstance(ref, ParameterReference):
                        parameter = ref._.parameter
                        parameter.register_caller(self)
                self.arguments[key] = value
            else:
                raise RuntimeError(
                    f"Non-references must be a serializable type: {key}>{value} {type(value)}"
                )

    def __eq__(self, other: object) -> bool:
        """Is this the same step?

        At present, we naively compare the task and arguments. In
        future, this may be more nuanced.
        """
        if not isinstance(other, BaseStep):
            return False
        return (
            self.__workflow__ is other.__workflow__
            and self.task == other.task
            and self.arguments == other.arguments
        )

    def make_reference(self, **kwargs) -> "StepReference":
        kwargs["step"] = self
        kwargs.setdefault("typ", self.return_type)
        typ = kwargs["typ"]
        base = get_origin(strip_annotations(typ)[0])
        if inspect.isclass(base) and issubclass(base, Iterable) and not issubclass(base, str | bytes):
            return IterableStepReference(**kwargs)
        return StepReference(**kwargs)

    def set_workflow(self, workflow: Workflow, with_arguments: bool = True) -> None:
        """Move the step reference to another workflow.

        This method is primarily intended to be called by a step, allowing it to
        switch to a new workflow. It also updates the workflow reference for any
        arguments that are steps themselves, if specified.

        Args:
            workflow: The new target workflow to which the step should be moved.
            with_arguments: If True, also update the workflow reference for the step's arguments.
        """
        self.__workflow__ = workflow
        if with_arguments:
            for argument in self.arguments.values():
                unify_workflows(argument, workflow, set_only=True)
        self._id = None

    @property
    def return_type(self) -> Any:
        """Take the type of the wrapped function from the target.

        Unwraps and inspects the signature, meaning that the original
        wrapped function _must_ have a typehint for the return value.

        Returns:
            Expected type of the return value.
        """
        if isinstance(self.task, Workflow):
            if self.task.result is not None:
                return self.task.result_type
            else:
                raise AttributeError(
                    "Cannot determine return type of a workflow with an unspecified result"
                )
        if isinstance(self.task.target, type):
            return self.task.target
        return inspect.signature(inspect.unwrap(self.task.target)).return_annotation

    def __hash__(self) -> int:
        return hash(self.id)

    @property
    def name(self) -> str:
        """Name for this step.

        May be remapped by the workflow to something nicer
        than the ID.
        """
        return self.__workflow__.remap(self.id)

    @property
    def id(self) -> str:
        """Consistent ID based on the value."""
        if self._id is None:
            self._id = self._generate_id()
            return self._id

        if CHECK_IDS:
            check_id = self._generate_id()
            if check_id != self._id:
                return self._id
                raise RuntimeError(
                    f"Cannot change a step after requesting its ID: {self.task}"
                )
        return self._id

    def _generate_id(self) -> str:
        """Generate the ID once."""
        components: list[str | tuple[str, str]] = [repr(self.task)]
        for key, param in self.arguments.items():
            components.append((key, repr(param)))

        comp_tup: tuple[str | tuple[str, str], ...] = tuple(sorted(components, key=lambda pair: pair[0]))

        return f"{self.task}-{hasher(comp_tup)}"


class NestedStep(BaseStep):
    """Calling out to a subworkflow.

    Type of BaseStep to call a subworkflow, which holds a reference to it.
    """

    def __init__(
        self,
        workflow: Workflow,
        name: str,
        subworkflow: Workflow,
        arguments: Mapping[str, Reference | Raw],
        raw_as_parameter: bool = False,
    ):
        """Create a NestedStep.

        Args:
            workflow: outer workflow.
            name: name of the subworkflow.
            subworkflow: inner workflow (subworkflow) itself.
            arguments: arguments provided to the step.
            raw_as_parameter: whether raw-type arguments should be made (outer) workflow parameters.
        """
        self.__subworkflow__ = subworkflow
        base_arguments = {p.name: p for p in subworkflow.find_parameters()}
        base_arguments.update(arguments)
        super().__init__(
            workflow=workflow,
            task=subworkflow,
            arguments=base_arguments,
            raw_as_parameter=raw_as_parameter,
        )

    @property
    def subworkflow(self) -> Workflow:
        """Subworkflow that is wrapped."""
        return self.__subworkflow__

    @property
    def return_type(self) -> Any:
        """Take the type of the wrapped function from the target.

        Unwraps and inspects the signature, meaning that the original
        wrapped function _must_ have a typehint for the return value.

        Returns:
            Expected type of the return value.
        """
        return super().return_type
        if self.__subworkflow__.result is None or self.__subworkflow__.result is []:
            raise RuntimeError("Can only use a subworkflow if the reference exists.")
        return self.__subworkflow__.result_type


class Step(BaseStep):
    """Regular step."""

    ...


class FactoryCall(Step):
    """Call to a factory function."""

    def __init__(
        self,
        workflow: Workflow,
        task: Task | Workflow,
        arguments: Mapping[str, Reference | Raw],
        raw_as_parameter: bool = False,
    ):
        """Initialize a step.

        Args:
            workflow: `Workflow` that this is tied to.
            task: the lazy-evaluatable function that this wraps.
            arguments: key-value pairs to pass to the function - for a factory call, these _must_ be raw.
            raw_as_parameter: whether to turn any raw-type arguments into workflow parameters (or just keep them as default argument values).
        """
        for arg in list(arguments.values()):
            if not is_raw(arg) and not (
                isinstance(arg, ParameterReference) and is_raw_type(arg.__type__)
            ):
                raise RuntimeError(
                    f"Factories must be constructed with raw types {arg} {type(arg)}"
                )
        super().__init__(workflow=workflow, task=task, arguments=arguments, raw_as_parameter=raw_as_parameter)

    @property
    def __name__(self):
        return self.name

    @property
    def __default__(self) -> Unset:
        """Dummy default property for use as property."""
        return UnsetType(self.return_type)


class ParameterReference(WorkflowComponent, FieldableMixin, Reference[U]):
    """Reference to an individual `Parameter`.

    Allows us to refer to the outputs of a `Parameter` in subsequent `Parameter`
    arguments.

    Attributes:
        parameter: `Parameter` referred to.
        workflow: Related workflow. In this case, as Parameters are generic
            but ParameterReferences are specific, this carries the actual workflow reference.

    Returns:
        Workflow that the referee is related to.
    """

    class ParameterReferenceMetadata(Generic[T]):
        parameter: Parameter[T]

        def __init__(self, parameter: Parameter[T], *args, typ: type[U] | None=None, **kwargs):
            """Initialize the reference.

            Args:
                workflow: `Workflow` that this is tied to.
                parameter: `Parameter` that this refers to.
            """
            self.parameter = parameter

        @property
        def unique_name(self) -> str:
            """Unique, machine-generated name.

            Normally this will become invisible in output, but it avoids circularity
            as a step that uses this parameter will ask for this when constructing
            its own hash, but we will normally want to use the step's name as part of
            the parameter name to distinguish from other parameters of the same name.
            """
            return self.parameter.__name__

    @property
    def __default__(self) -> T | Unset:
        """Default value of the parameter."""
        default = self._.parameter.default
        if isinstance(default, Unset):
            return default

        for field in self.__field__:
            if isinstance(default, dict) or isinstance(field, int):
                default = default[field]
            else:
                default = getattr(default, field)
        return default

    @property
    def __root_name__(self) -> str:
        """Reference based on the named step.

        May be remapped by the workflow to something nicer
        than the ID.
        """
        return self._.parameter.name

    def __init__(self, parameter: Parameter[U], *args, typ: type[U] | None=None, **kwargs):
        typ = typ or parameter.__type__
        self._ = self.ParameterReferenceMetadata(parameter, *args, typ, **kwargs)
        super().__init__(*args, typ=typ, **kwargs)

    def __getitem__(self, attr: str) -> "ParameterReference":
        try:
            return self.find_field(
                field=attr,
                workflow=self.__workflow__,
                parameter=self._.parameter
            )
        except AttributeError as exc:
            raise KeyError(
                f"Key not found in {self.__root_name__} ({type(self)}:{self.__type__}): {attr}" +
                (
                    ". This could be because you are trying to iterate/index a reference whose type is not definitely iterable - double check your typehints."
                    if isinstance(attr, int) else ""
                )
            ) from exc

    def __getattr__(self, attr: str) -> "ParameterReference":
        try:
            return self[attr]
        except KeyError as exc:
            return super().__getattribute__(attr)

    def __repr__(self) -> str:
        """Hashable reference to the step (and field)."""
        try:
            typ = self.__type__.__name__
        except AttributeError:
            typ = str(self.__type__)
        name = self._.unique_name + self.__field_suffix__
        return f"{typ}|:param:{name}"

    def __hash__(self) -> int:
        """Hash to parameter.

        Returns:
            Unique hash corresponding to the parameter.
        """
        return hash((self._.parameter, self.__field__))

    def __eq__(self, other: object) -> bool:
        """Compare two references.

        We ignore the workflow itself, as equality here is usually
        to test mergeability of the "same" reference in two workflows.

        Returns:
            True if the other parameter reference is materially the same, otherwise False.
        """
        # We are equal to a parameter if we are a direct, fieldless, reference to it.
        return (
            (isinstance(other, ParameterReference) and self._.parameter == other._.parameter and self.__field__ == other.__field__)
        )

    def __make_reference__(self, **kwargs) -> "StepReference":
        return self._.parameter.make_reference(**kwargs)

class IterableParameterReference(IterableMixin, ParameterReference[U]):
    def __iter__(self):
        inner, metadata = strip_annotations(self.__type__)
        if metadata and "AtRender" in metadata and isinstance(self.__default__, Iterable):
            yield from self.__default__
        else:
            yield from super().__iter__()

    def __inner_iter__(self) -> Generator[Any, None, None]:
        inner, metadata = strip_annotations(self.__type__)
        if self.__fixed_len__ is not None:
            yield from range(self.__fixed_len__)
        elif metadata and "Fixed" in metadata and isinstance(self.__default__, Sized):
            yield from range(len(self.__default__))
        else:
            while True:
                yield None

    def __len__(self):
        inner, metadata = strip_annotations(self.__type__)
        if metadata and "Fixed" in metadata and isinstance(self.__default__, Sized):
            return len(self.__default__)
        return super().__len__()

class StepReference(FieldableMixin, Reference[U]):
    """Reference to an individual `Step`.

    Allows us to refer to the outputs of a `Step` in subsequent `Step`
    arguments.

    Attributes:
        step: `Step` referred to.
    """

    step: BaseStep

    class StepReferenceMetadata:
        def __init__(
            self, step: BaseStep, typ: type[U] | None = None
        ):
            """Initialize the reference.

            Args:
                workflow: `Workflow` that this is tied to.
                step: `Step` that this refers to.
                typ: the type that the step will output.
                field: if provided, a specific field to pull out of an attrs result class.
            """
            self.step = step
            self._typ = typ

        @property
        def return_type(self):
            return self._typ or self.step.return_type

    _: StepReferenceMetadata

    def __init__(
        self, step: BaseStep, *args, typ: type[U] | None = None, **kwargs
    ):
        """Initialize the reference.

        Args:
            workflow: `Workflow` that this is tied to.
            step: `Step` that this refers to.
            typ: the type that the step will output.
            field: if provided, a specific field to pull out of an attrs result class.
        """
        typ = typ or step.return_type
        self._ = self.StepReferenceMetadata(step, typ=typ)
        super().__init__(*args, typ=typ, **kwargs)

    def __str__(self) -> str:
        """Global description of the reference."""
        return self.__name__

    def __repr__(self) -> str:
        """Hashable reference to the step (and field)."""
        return self._.step.id + self.__field_suffix__

    def __hash__(self) -> int:
        return hash((repr(self), id(self.__workflow__)))

    def __getitem__(self, attr: str) -> "StepReference[Any]":
        """Reference to a field within this result, if possible.

        If the result is an attrs-class or dataclass, this will pull out an individual
        field for use as input to other tasks, or global output of the workflow.

        Args:
            attr: field to pull out.

        Returns:
            Another StepReference specific to the requested field.

        Raises:
            AttributeError: if this field is not present in the dataclass.
            RuntimeError: if this field is not available, or we do not have a structured result.
        """
        try:
            return self.find_field(
                workflow=self.__workflow__, step=self._.step, field=attr
            )
        except AttributeError as exc:
            raise KeyError(
                f"Key not found in {self.__root_name__} ({type(self)}:{self.__type__}): {attr}" +
                (
                    ". This could be because you are trying to iterate/index a reference whose type is not definitely iterable - double check your typehints."
                    if isinstance(attr, int) else ""
                )
            ) from exc

    def __getattr__(self, attr: str) -> "StepReference":
        try:
            return self[attr]
        except KeyError as exc:
            try:
                return super().__getattribute__(attr)
            except AttributeError as inner_exc:
                raise inner_exc from exc

    @property
    def __type__(self) -> type:
        return self._.return_type

    @property
    def __root_name__(self) -> str:
        """Reference based on the named step.

        May be remapped by the workflow to something nicer
        than the ID.
        """
        return self._.step.name

    @property
    def __workflow__(self) -> Workflow:
        """Related workflow.

        Returns:
            Workflow that the referee is related to.
        """
        return self._.step.__workflow__

    @__workflow__.setter
    def __workflow__(self, workflow: Workflow) -> None:
        """Sets related workflow.

        Args:
            workflow: workflow to update the step
        """
        self._.step.set_workflow(workflow)

    def __make_reference__(self, **kwargs) -> "StepReference":
        return self._.step.make_reference(**kwargs)

class IterableStepReference(IterableMixin, StepReference[U]):
    def __iter__(self):
        yield IteratedGenerator(self)

def merge_workflows(*workflows: Workflow) -> Workflow:
    """Combine several workflows into one.

    Merges a series of workflows by combining steps and tasks.

    Argument:
        *workflows: series of workflows to combine.

    Returns:
        One workflow with all steps.
    """
    return Workflow.assimilate(*workflows)


def is_task(task: Lazy) -> bool:
    """Decide whether this is a task.

    Checks whether the wrapped function has the magic
    attribute `__step_expression__` set to True, which is
    done within task creation.

    Args:
        task: lazy-evaluated value, suspected to be a task.

    Returns:
        True if `task` is indeed a task.
    """
    return isinstance(task, LazyEvaluation)

def expr_to_references(expression: Any, remap: Callable[[Any], Any] | None = None) -> tuple[Basic | None, set[Reference | Parameter]]:
    to_check = []
    def _to_expr(value):
        if remap and (res := remap(value)) is not None:
            return _to_expr(res)

        if isinstance(value, Reference):
            to_check.append(value)
            return value

        if value is None:
            return None

        if isinstance(value, Symbol):
            return value
        elif isinstance(value, Basic):
            for sym in value.free_symbols:
                new_sym = _to_expr(sym)
                if new_sym != sym:
                    value = value.subs(sym, new_sym)
            return value

        if is_dataclass(value) or attr_has(value):
            if is_dataclass(value):
                fields = dataclass_fields(value)
            else:
                fields = {field for field in attrs_fields(value.__class__)}
            for field in fields:
                if hasattr(value, field.name) and isinstance((val := getattr(value, field.name)), Reference):
                    setattr(value, field.name, _to_expr(val))
            return value

        # We need to look inside a Raw, but we do not want to lose it if
        # we do not need to.
        retval = value
        if isinstance(value, Raw):
            value = value.value

        if isinstance(value, Mapping):
            dct = {key: _to_expr(val) for key, val in value.items()}
            if dct == value:
                return retval
            return value.__class__(dct)
        elif not isinstance(value, str | bytes) and isinstance(value, Iterable):
            lst = (tuple if isinstance(value, tuple) else list)(_to_expr(v) for v in value)
            if lst == value:
                return retval
            return lst
        return retval

    expression = _to_expr(expression)

    #if {sym for sym in symbols if not is_raw(sym)} != set(to_check):
    #    print(symbols, to_check, [type(r) for r in symbols])
    #    raise RuntimeError("The only symbols allowed are references (to e.g. step or parameter)")
    return expression, to_check

def unify_workflows(expression: Any, base_workflow: Workflow | None, set_only: bool = False) -> Workflow | None:
    expression, to_check = expr_to_references(expression)
    if not to_check:
        return expression, base_workflow

    # Build a unified workflow
    collected_workflow = base_workflow or next(iter(to_check)).__workflow__
    if not set_only:
        for step_result in to_check:
            new_workflow = step_result.__workflow__
            if collected_workflow != new_workflow and collected_workflow and new_workflow:
                collected_workflow = Workflow.assimilate(collected_workflow, new_workflow)

    # Make sure all the results share it
    for step_result in to_check:
        step_result.__workflow__ = collected_workflow

    return expression, collected_workflow
