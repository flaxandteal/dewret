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
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Overarching workflow concepts.

Basic constructs for describing a workflow.
"""

from __future__ import annotations
import inspect
from collections.abc import Mapping, MutableMapping, Callable
import base64
from attrs import define, has as attr_has, resolve_types, fields as attrs_fields
from dataclasses import is_dataclass, fields as dataclass_fields
from collections import Counter
from typing import Protocol, Any, TypeVar, Generic, cast, Literal
from uuid import uuid4

import logging

logger = logging.getLogger(__name__)

from .utils import hasher, RawType, is_raw, make_traceback

T = TypeVar("T")
RetType = TypeVar("RetType")


@define
class Raw:
    """Value object for any raw types.

    This is able to hash raw types consistently and provides
    a single type for validating type-consistency.

    Attributes:
        value: the real value, e.g. a `str`, `int`, ...
    """

    value: RawType

    def __hash__(self) -> int:
        """Provide a hash that is unique to the `value` member."""
        return hash(repr(self))

    def __repr__(self) -> str:
        """Convert to a consistent, string representation."""
        value: str
        if isinstance(self.value, bytes):
            value = base64.b64encode(self.value).decode("ascii")
        else:
            value = str(self.value)
        return f"{type(self.value).__name__}|{value}"


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
        return self._fn(*args, **kwargs, __traceback__=tb)


Target = Callable[..., Any]
StepExecution = Callable[..., Lazy]
LazyFactory = Callable[[Target], Lazy]


class Unset:
    """Unset variable, with no default value."""


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


class Parameter(Generic[T]):
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
    __default__: T | UnsetType[T]
    __tethered__: Literal[False] | None | BaseStep | Workflow

    autoname: bool = False

    def __init__(
        self,
        name: str,
        default: T | UnsetType[T],
        tethered: Literal[False] | None | Step | Workflow = None,
        autoname: bool = False,
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
            name = f"{name}-{uuid4()}"
        self.autoname = autoname

        self.__name__ = name
        self.__default__ = default
        self.__tethered__ = tethered
        self.__callers__: list[BaseStep] = []

        if (
            default is not None
            and hasattr(default, "__type__")
            and isinstance(default.__type__, type)
        ):
            raw_type = default.__type__
        else:
            raw_type = type(default)
        self.__type__: type[T] = raw_type

        if tethered and isinstance(tethered, BaseStep):
            self.register_caller(tethered)

    def __hash__(self) -> int:
        """Get a unique hash for this parameter."""
        if self.__tethered__ is None:
            raise RuntimeError(
                "Parameter {self.full_name} was never tethered but should have been"
            )
        return hash(self.__name__)

    @property
    def default(self) -> T | UnsetType[T]:
        """Retrieve default value for this parameter, or an unset token."""
        return self.__default__

    @property
    def full_name(self) -> str:
        """Extended name, suitable for rendering.

        This attempts to create a unique name by tying the parameter to a step
        if the user has not explicitly provided a name, ideally the one where
        we discovered it.
        """
        tethered = self.__tethered__
        if tethered is False or tethered is None or self.autoname is False:
            return self.__name__
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

    @property
    def name(self) -> str:
        """Name for this step.

        May be remapped by the workflow to something nicer
        than the ID.
        """
        return self.full_name


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
        T, Parameter(name, default=default, tethered=tethered, autoname=autoname)
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
    result: StepReference[Any] | None
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

    def __hash__(self) -> int:
        """Hashes for finding."""
        return hash(self.name)

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
    def name(self) -> str:
        """Get the name of the workflow.

        Raises:
            NameError: if no name has been set.
        """
        if self._name is None:
            raise NameError("Can not get the name of an anonymous workflow.")
        return self._name

    def find_parameters(self) -> set[ParameterReference]:
        """Crawl steps for parameter references.

        As the workflow does not hold its own list of parameters, this
        dynamically finds them.

        Returns:
            Set of all references to parameters across the steps.
        """
        return set().union(
            *(
                {
                    arg
                    for arg in step.arguments.values()
                    if isinstance(arg, ParameterReference)
                }
                for step in self.steps
            )
        )

    @property
    def _indexed_steps(self) -> dict[str, BaseStep]:
        """Steps mapped by ID.

        Forces generation of IDs. Note that this effectively
        freezes the steps, so it should not be used until we
        are confident the steps are all ready to be hashed.

        Returns:
            Mapping of steps by ID.
        """
        return {step.id: step for step in self.steps}

    @classmethod
    def assimilate(cls, left: Workflow, right: Workflow) -> "Workflow":
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
        new = cls()

        new._name = left._name or right._name

        left_steps = left._indexed_steps
        right_steps = right._indexed_steps

        for step in list(left_steps.values()) + list(right_steps.values()):
            step.set_workflow(new)
            for arg in step.arguments:
                if hasattr(arg, "__workflow__"):
                    arg.__workflow__ = new

        for step_id in left_steps.keys() & right_steps.keys():
            if left_steps[step_id] != right_steps[step_id]:
                raise RuntimeError(
                    f"Two steps have same ID but do not match: {step_id}"
                )

        for task_id in left.tasks.keys() & right.tasks.keys():
            if left.tasks[task_id] != right.tasks[task_id]:
                raise RuntimeError("Two tasks have same name but do not match")

        indexed_steps = dict(left_steps)
        indexed_steps.update(right_steps)
        new.steps += list(indexed_steps.values())
        new.tasks.update(left.tasks)
        new.tasks.update(right.tasks)

        for step in new.steps:
            step.__workflow__ = new

        # TODO: should we combine as a result array?
        result = left.result or right.result

        if result:
            new.set_result(
                StepReference(
                    new, result.step, typ=result.return_type, field=result.field
                )
            )

        return new

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
            pr.parameter.__name__: pr.parameter for pr in self.find_parameters()
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
        self, name: str, subworkflow: Workflow, kwargs: dict[str, Any]
    ) -> StepReference[Any]:
        """Append a nested step.

        Calls a subworkflow.

        Args:
            name: name of the subworkflow.
            subworkflow: the subworkflow itself.
            kwargs: any key-value arguments to pass in the call.
        """
        step = NestedStep(self, name, subworkflow, kwargs)
        self.steps.append(step)
        return_type = step.return_type
        if return_type is inspect._empty:
            raise TypeError("All tasks should have a type annotation.")
        return StepReference(self, step, return_type)

    def add_step(
        self,
        fn: Lazy,
        kwargs: dict[str, Raw | Reference],
        raw_as_parameter: bool = False,
    ) -> StepReference[Any]:
        """Append a step.

        Adds a step, for running a target with key-value arguments,
        to the workflow.

        Args:
            fn: the target function to turn into a step.
            kwargs: any key-value arguments to pass in the call.
            raw_as_parameter: whether to turn any discovered raw arguments into workflow parameters.
        """
        task = self.register_task(fn)
        step = Step(self, task, kwargs, raw_as_parameter=raw_as_parameter)
        self.steps.append(step)
        return_type = step.return_type
        if return_type is inspect._empty:
            raise TypeError("All tasks should have a type annotation.")
        return StepReference(self, step, return_type)

    @staticmethod
    def from_result(
        result: StepReference[Any], simplify_ids: bool = False, nested: bool = True
    ) -> Workflow:
        """Create from a desired result.

        Starts from a result, and builds a workflow to output it.
        """
        workflow = result.__workflow__
        workflow.set_result(result)
        if simplify_ids:
            workflow.simplify_ids()
        return workflow

    def set_result(self, result: StepReference[Any]) -> None:
        """Choose the result step.

        Sets a step as being the result for the entire workflow.
        When we evaluate a dynamic workflow, the engine (e.g. dask)
        creates a graph to realize the result of a single collection.
        Similarly, in the static case, we need to have a result that
        drives the calculation.

        Args:
            result: reference to the chosen step.
        """
        if result.step.__workflow__ != self:
            raise RuntimeError("Output must be from a step in this workflow.")
        self.result = result


class WorkflowComponent:
    """Base class for anything directly tied to an individual `Workflow`.

    Attributes:
        __workflow__: the `Workflow` that this is tied to.
    """

    __workflow__: Workflow

    def __init__(self, workflow: Workflow):
        """Tie to a `Workflow`.

        All subclasses must call this.

        Args:
            workflow: the `Workflow` to tie to.
        """
        self.__workflow__ = workflow


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


class Reference:
    """Superclass for all symbolic references to values."""

    @property
    def name(self) -> str:
        """Referral name for this reference."""
        raise NotImplementedError("Reference must provide a name")


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
        super().__init__(workflow)
        self.task = task
        self.arguments = {}
        for key, value in arguments.items():
            if isinstance(value, Reference) or isinstance(value, Raw) or is_raw(value):
                # Avoid recursive type issues
                if (
                    not isinstance(value, Reference)
                    and not isinstance(value, Raw)
                    and is_raw(value)
                ):
                    if raw_as_parameter:
                        value = ParameterReference(
                            workflow, param(key, value, tethered=None)
                        )
                    else:
                        value = Raw(value)
                if isinstance(value, ParameterReference):
                    parameter = value.parameter
                    parameter.register_caller(self)
                self.arguments[key] = value
            else:
                raise RuntimeError(
                    f"Non-references must be a serializable type: {key}>{value}"
                )

    def __eq__(self, other: object) -> bool:
        """Is this the same step?

        At present, we naively compare the task and arguments. In
        future, this may be more nuanced.
        """
        if not isinstance(other, BaseStep):
            return False
        return (
            self.__workflow__ == other.__workflow__
            and self.task == other.task
            and self.arguments == other.arguments
        )

    def set_workflow(self, workflow: Workflow) -> None:
        """Move the step reference to another workflow.

        Primarily intended to be called by its step, as a cascade.
        It will attempt to update its arguments, similarly.

        Args:
            workflow: the new target workflow.
        """
        self.__workflow__ = workflow
        for argument in self.arguments.values():
            if hasattr(argument, "__workflow__"):
                try:
                    argument.__workflow__ = workflow
                except AttributeError:
                    ...

    @property
    def return_type(self) -> Any:
        """Take the type of the wrapped function from the target.

        Unwraps and inspects the signature, meaning that the original
        wrapped function _must_ have a typehint for the return value.

        Returns:
            Expected type of the return value.
        """
        if isinstance(self.task, Workflow):
            if self.task.result:
                return self.task.result.return_type
            else:
                raise AttributeError(
                    "Cannot determine return type of a workflow with an unspecified result"
                )
        return inspect.signature(inspect.unwrap(self.task.target)).return_annotation

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

        comp_tup: tuple[str | tuple[str, str], ...] = tuple(components)

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
        super().__init__(
            workflow=workflow,
            task=subworkflow,
            arguments=arguments,
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
        if not self.__subworkflow__.result:
            raise RuntimeError("Can only use a subworkflow if the reference exists.")
        return self.__subworkflow__.result.return_type


class Step(BaseStep):
    """Regular step."""

    ...


class ParameterReference(Reference):
    """Reference to an individual `Parameter`.

    Allows us to refer to the outputs of a `Parameter` in subsequent `Parameter`
    arguments.

    Attributes:
        parameter: `Parameter` referred to.
        __workflow__: Related workflow. In this case, as Parameters are generic
            but ParameterReferences are specific, this carries the actual workflow reference.

    Returns:
        Workflow that the referee is related to.
    """

    parameter: Parameter[RawType]
    __workflow__: Workflow

    def __init__(self, __workflow__: Workflow, parameter: Parameter[RawType]):
        """Initialize the reference.

        Args:
            workflow: `Workflow` that this is tied to.
            parameter: `Parameter` that this refers to.
        """
        self.parameter = parameter
        self.__workflow__ = __workflow__

    @property
    def __type__(self) -> type:
        """Type represented by wrapped parameter."""
        return self.parameter.__type__

    def __str__(self) -> str:
        """Global description of the reference."""
        return self.parameter.full_name

    def __repr__(self) -> str:
        """Hashable reference to the step (and field)."""
        try:
            typ = self.__type__.__name__
        except AttributeError:
            typ = str(self.__type__)
        return f"{typ}|:param:{self.unique_name}"

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
    def name(self) -> str:
        """Reference based on the named step.

        May be remapped by the workflow to something nicer
        than the ID.
        """
        return self.__workflow__.remap(self.parameter.name)

    def __hash__(self) -> int:
        """Hash to parameter.

        Returns:
            Unique hash corresponding to the parameter.
        """
        return hash(self.parameter)

    def __eq__(self, other: object) -> bool:
        """Compare two references.

        We ignore the workflow itself, as equality here is usually
        to test mergeability of the "same" reference in two workflows.

        Returns:
            True if the other parameter reference is materially the same, otherwise False.
        """
        return (
            isinstance(other, ParameterReference) and self.parameter == other.parameter
        )


U = TypeVar("U")


class StepReference(Generic[U], Reference):
    """Reference to an individual `Step`.

    Allows us to refer to the outputs of a `Step` in subsequent `Step`
    arguments.

    Attributes:
        step: `Step` referred to.
    """

    step: BaseStep
    _field: str | None
    typ: type[U]

    @property
    def field(self) -> str:
        """Field within the result.

        Explicitly set field (within an attrs-class) or `out`.

        Returns:
            Field name.
        """
        return self._field or "out"

    def __init__(
        self, workflow: Workflow, step: BaseStep, typ: type[U], field: str | None = None
    ):
        """Initialize the reference.

        Args:
            workflow: `Workflow` that this is tied to.
            step: `Step` that this refers to.
            typ: the type that the step will output.
            field: if provided, a specific field to pull out of an attrs result class.
        """
        self.step = step
        self._field = field
        self.typ = typ

    def __str__(self) -> str:
        """Global description of the reference."""
        return f"{self.step.id}/{self.field}"

    def __repr__(self) -> str:
        """Hashable reference to the step (and field)."""
        return f"{self.step.id}/{self.field}"

    def __getattr__(self, attr: str) -> "StepReference"[Any]:
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
        if self._field is None:
            typ: type | None
            if attr_has(self.typ):
                resolve_types(self.typ)
                typ = getattr(attrs_fields(self.typ), attr).type
            elif is_dataclass(self.typ):
                matched = [
                    field for field in dataclass_fields(self.typ) if field.name == attr
                ]
                if not matched:
                    raise AttributeError(f"Field {attr} not present in dataclass")
                typ = matched[0].type
            else:
                typ = None

            if typ:
                return self.__class__(
                    workflow=self.__workflow__, step=self.step, typ=typ, field=attr
                )
        raise AttributeError(
            "Can only get attribute of a StepReference representing an attrs-class or dataclass"
        )

    @property
    def return_type(self) -> type[U]:
        """Type that this step reference will resolve to.

        Returns:
            Python type indicating the final result type.
        """
        return self.typ

    @property
    def name(self) -> str:
        """Reference based on the named step.

        May be remapped by the workflow to something nicer
        than the ID.
        """
        return f"{self.step.name}/{self.field}"

    @property
    def __type__(self) -> Any:
        """Type of the step's referenced value."""
        return self.step.return_type

    @property
    def __workflow__(self) -> Workflow:
        """Related workflow.

        Returns:
            Workflow that the referee is related to.
        """
        return self.step.__workflow__

    @__workflow__.setter
    def __workflow__(self, workflow: Workflow) -> None:
        self.step.set_workflow(workflow)


def merge_workflows(*workflows: Workflow) -> Workflow:
    """Combine several workflows into one.

    Merges a series of workflows by combining steps and tasks.

    Argument:
        *workflows: series of workflows to combine.

    Returns:
        One workflow with all steps.
    """
    base = list(workflows).pop()
    for workflow in workflows:
        base = Workflow.assimilate(base, workflow)
    return base


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
