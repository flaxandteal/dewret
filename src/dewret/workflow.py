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
from typing import Protocol, Any, TypeVar, Generic, cast, Literal, TypeAliasType, Annotated, Iterable, get_origin, get_args
from uuid import uuid4
from sympy import Symbol, Expr, Basic, Tuple, Dict, nan

import logging

logger = logging.getLogger(__name__)

from .core import Reference, get_configuration, RawType, Raw
from .utils import hasher, is_raw, make_traceback, is_raw_type, is_expr, Unset

T = TypeVar("T")
U = TypeVar("U")
RetType = TypeVar("RetType")


def all_references_from(value: Any):
    all_references: set = set()

    # If Raw, we examine the internal value.
    # In theory, this should not contain a reference,
    # but this makes all_references_from useful for error-checking.
    if isinstance(value, Raw):
        value = value.value

    if isinstance(value, Reference):
        all_references.add(value)
    elif isinstance(value, Basic):
        symbols = value.free_symbols
        if not all(isinstance(sym, Reference) for sym in symbols):
            raise RuntimeError("Can only use symbols that are references to e.g. step or parameter.")
        all_references |= symbols
    elif isinstance(value, Mapping):
        all_references |= all_references_from(value.keys())
        all_references |= all_references_from(value.values())
    elif isinstance(value, Iterable) and not isinstance(value, str | bytes):
        all_references |= set().union(*(all_references_from(entry) for entry in value))

    return all_references

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
        #if autoname:
        #    name = f"{name}-{uuid4()}"
        self.autoname = autoname

        self.__name__ = name
        self.__default__ = default
        self.__tethered__ = tethered
        self.__callers__: list[BaseStep] = []
        self.__fixed_type__ = typ

        if tethered and isinstance(tethered, BaseStep):
            self.register_caller(tethered)

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
        if isinstance(other, ParameterReference) and other._.parameter == self and not other.__field__:
            return True
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
        return self.name

    def __hash__(self) -> int:
        """Hashes for finding."""
        return hash(repr(self))

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
        references = all_references_from(
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
        return OrderedDict(sorted((step.id, step) for step in self.steps))

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
            for arg in step.arguments.values():
                unify_workflows(arg, new, set_only=True)

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
            step.set_workflow(new, with_arguments=True)

        if left.result == right.result:
            result = left.result
        elif not left.result:
            result = right.result
        elif not right.result:
            result = left.result
        else:
            if not isinstance(left.result, tuple | list):
                left.result = [left.result]
            if not isinstance(right.result, tuple | list):
                right.result = [right.result]
            result = list(left.result) + list(right.result)

        if result is not None and result != []:
            unify_workflows(result, new, set_only=True)
            new.set_result(result)

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
        return StepReference(step, typ=return_type)

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
        return StepReference(step, return_type)

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

    def __init__(self, *args, field: str | None = None, **kwargs):
        super().__init__(*args, **kwargs)

    @property
    def __type__(self):
        ...

    @property
    def name(self):
        return "name"

# Subclass Reference so that we know Reference methods/attrs are available.
class FieldableMixin:
    def __init__(self: FieldableProtocol, *args, field: str | None = None, **kwargs):
        self.__field__: tuple[str, ...] = tuple(field.split("/")) if field else ()
        super().__init__(*args, **kwargs)

    @property
    def __name__(self: FieldableProtocol) -> str:
        """Name for this step.

        May be remapped by the workflow to something nicer
        than the ID.
        """
        return "/".join([super().__name__] + list(self.__field__))

    def find_field(self: FieldableProtocol, field, fallback_type: type | None = None, **init_kwargs: Any) -> Reference:
        """Field within the reference, if possible.

        Returns:
            A field-specific version of this reference.
        """

        # Get new type, for the specific field.
        parent_type = self.__type__
        field_type = fallback_type

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
        if not field_type and get_configuration("allow_plain_dict_fields") and get_origin(parent_type) is dict:
            args = get_args(parent_type)
            if len(args) == 2 and args[0] is str:
                field_type = args[1]
            else:
                raise AttributeError(f"Can only get fields for plain dicts if annotated dict[str, TYPE]")

        if field_type:
            if not issubclass(self.__class__, Reference):
                raise TypeError("Only references can have a fieldable mixin")

            if self.__field__:
                field = "/".join(self.__field__) + "/" + field

            return self.__class__(typ=field_type, field=field, **init_kwargs)

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
                        value = ParameterReference(
                            workflow=workflow, parameter=param(key, value, tethered=None)
                        )
                    else:
                        value = Raw(value)

                expr, refs = expr_to_references(value, include_parameters=True)
                if expr is not None:
                    for ref in set(refs):
                        if isinstance(ref, Parameter):
                            new_ref = ParameterReference(workflow=workflow, parameter=ref)
                            expr = expr.subs(ref, new_ref)
                            refs.remove(ref)
                            refs.append(new_ref)
                    value = expr

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

    def set_workflow(self, workflow: Workflow, with_arguments: bool = True) -> None:
        """Move the step reference to a different workflow.

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
        return self._.parameter.default

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

    def __getattr__(self, attr: str) -> "ParameterReference":
        try:
            return self.find_field(
                field=attr,
                workflow=self.__workflow__,
                parameter=self._.parameter
            )
        except AttributeError as exc:
            if not "dask_graph" in str(exc):
                raise
            return super().__getattribute__(attr)

    def __getitem__(self, attr: str) -> "ParameterReference":
        return getattr(self, attr)

    def __repr__(self) -> str:
        """Hashable reference to the step (and field)."""
        try:
            typ = self.__type__.__name__
        except AttributeError:
            typ = str(self.__type__)
        name = "/".join([self._.unique_name] + list(self.__field__))
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
            (isinstance(other, Parameter) and self._.parameter == other and not self.__field__) or
            (isinstance(other, ParameterReference) and self._.parameter == other._.parameter and self.__field__ == other.__field__)
        )


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
        return "/".join([self._.step.id] + list(self.__field__))

    def __hash__(self) -> int:
        return hash((repr(self), id(self.__workflow__)))

    def __getattr__(self, attr: str) -> "StepReference[Any]":
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
            try:
                return super().__getattribute__(attr)
            except AttributeError as inner_exc:
                raise inner_exc from exc

    def __getitem__(self, attr: str) -> "StepReference":
        return getattr(self, attr)

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

def expr_to_references(expression: Any, include_parameters: bool=False) -> tuple[Basic | None, set[Reference | Parameter]]:
    if isinstance(expression, Raw) or is_raw(expression):
        return expression, set()

    if isinstance(expression, Reference):
        return expression, {expression}

    if is_dataclass(expression) or attr_has(expression):
        refs = set()
        fields = dataclass_fields(expression) if is_dataclass(expression) else {field.name for field in attrs_fields(expression)}
        for field in fields:
            if hasattr(expression, field.name) and isinstance((val := getattr(expression, field.name)), Reference):
                _, field_refs = expr_to_references(val, include_parameters=include_parameters)
                refs |= field_refs
        return expression, refs

    def _to_expr(value):
        if value is None:
            return nan
        elif hasattr(value, "__type__"):
            return value
        elif isinstance(value, Raw):
            return value.value

        if isinstance(value, Mapping):
            dct = Dict({key: _to_expr(val) for key, val in value.items()})
            return dct
        elif not isinstance(value, str | bytes) and isinstance(value, Iterable):
            return Tuple(*(_to_expr(entry) for entry in value))
        return value

    if not isinstance(expression, Basic):
        expression = _to_expr(expression)

    symbols = list(expression.free_symbols)
    to_check = [sym for sym in symbols if isinstance(sym, Reference) or (include_parameters and isinstance(sym, Parameter))]
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
