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
from collections.abc import Mapping, MutableMapping, Callable, Awaitable
from collections import OrderedDict
import base64
from attrs import define
from typing import Protocol, Any
from collections import Counter
import logging

logger = logging.getLogger(__name__)


from .utils import hasher, RawType, is_raw

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

Target = Callable[..., Any]
StepExecution = Callable[..., Lazy]
LazyFactory = Callable[[Target], Lazy]

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
        return (
            self.name == other.name and
            self.target == other.target
        )

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
    steps: list["Step"]
    tasks: MutableMapping[str, "Task"]
    result: StepReference | None
    _remapping: dict[str, str] | None

    @property
    def _indexed_steps(self) -> dict[str, Step]:
        """Steps mapped by ID.

        Forces generation of IDs. Note that this effectively
        freezes the steps, so it should not be used until we
        are confident the steps are all ready to be hashed.

        Returns:
            Mapping of steps by ID.
        """
        return {
            step.id: step for step in self.steps
        }

    @classmethod
    def assimilate(cls, left: Workflow, right: Workflow) -> "Workflow":
        """Combine Workflows into one Workflow.

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

        left_steps = left._indexed_steps
        right_steps = right._indexed_steps
        for step_id in (left_steps.keys() & right_steps.keys()):
            left_steps[step_id].__workflow__ = new
            right_steps[step_id].__workflow__ = new
            if left_steps[step_id] != right_steps[step_id]:
                raise RuntimeError(f"Two steps have same ID but do not match: {step_id}")

        for task_id in (left.tasks.keys() & right.tasks.keys()):
            if left.tasks[task_id] != right.tasks[task_id]:
                raise RuntimeError("Two tasks have same name but do not match")

        indexed_steps = dict(left_steps)
        indexed_steps.update(right_steps)
        new.steps += list(indexed_steps.values())
        new.tasks.update(left.tasks)
        new.tasks.update(right.tasks)

        for step in new.steps:
            step.__workflow__ = new

        return new

    def __init__(self) -> None:
        """Initialize a Workflow, by setting `steps` and `tasks` to empty containers."""
        self.steps = []
        self.tasks = {}
        self.result: StepReference | None = None
        self._remapping = None

    def remap(self, step_id: str) -> str:
        """Apply name simplification if requested.

        Args:
            step_id: step to check.

        Returns:
            Same ID or a remapped name.
        """
        return (
            self._remapping.get(step_id, step_id)
            if self._remapping else
            step_id
        )

    def simplify_ids(self) -> None:
        """Work out mapping to simple ints from hashes.

        Goes through and numbers each step by the order of use of its task.
        """
        counter = Counter[Task]()
        self._remapping = {}
        for step in self.steps:
            counter[step.task] += 1
            self._remapping[step.id] = f"{step.task}-{counter[step.task]}"

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

    def add_step(self, fn: Lazy, kwargs: dict[str, Raw | Reference]) -> StepReference:
        """Append a step.

        Adds a step, for running a target with key-value arguments,
        to the workflow.

        Args:
            fn: the target function to turn into a step.
            kwargs: any key-value arguments to pass in the call.
        """
        task = self.register_task(fn)
        step = Step(
            self,
            task,
            kwargs
        )
        self.steps.append(step)
        return StepReference(self, step)

    @staticmethod
    def from_result(result: StepReference, simplify_ids: bool = False) -> Workflow:
        """Create from a desired result.

        Starts from a result, and builds a workflow to output it.
        """
        step = result.step
        workflow = result.__workflow__
        workflow.set_result(result)
        if simplify_ids:
            workflow.simplify_ids()
        return workflow

    def set_result(self, result: StepReference) -> None:
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

class Step(WorkflowComponent):
    """Lazy-evaluated function call.

    Individual function call to a lazy-evaluatable function, tracked
    for building up the `Workflow`.

    Attributes:
        task: the `Task` being called in this step.
        parameters: key-value pairs of arguments to this step.
    """
    _id: str | None = None
    task: Task
    parameters: Mapping[str, Reference | Raw]

    def __init__(self, workflow: Workflow, task: Task, parameters: Mapping[str, Reference | Raw]):
        """Initialize a step.

        Args:
            workflow: `Workflow` that this is tied to.
            task: the lazy-evaluatable function that this wraps.
            parameters: key-value pairs to pass to the function.
        """
        super().__init__(workflow)
        self.task = task
        self.parameters = {}
        for key, value in parameters.items():
            if (
               isinstance(value, Reference) or
               isinstance(value, Raw) or
               is_raw(value)
            ):
                # Avoid recursive type issues
                if not isinstance(value, Reference) and not isinstance(value, Raw) and is_raw(value):
                    value = Raw(value)
                self.parameters[key] = value
            else:
                raise RuntimeError(f"Non-references must be a serializable type: {key}>{value}")

    def __eq__(self, other: object) -> bool:
        """Is this the same step?

        At present, we naively compare the task and parameters. In
        future, this may be more nuanced.
        """
        if not isinstance(other, Step):
            return False
        return (
            self.__workflow__ == other.__workflow__ and
            self.task == other.task and
            self.parameters == other.parameters
        )

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
            raise RuntimeError(f"Cannot change a step after requesting its ID: {self.task}")
        return self._id

    def _generate_id(self) -> str:
        """Generate the ID once."""
        components: list[str | tuple[str, str]] = [repr(self.task)]
        for key, param in self.parameters.items():
            components.append((key, repr(param)))

        comp_tup: tuple[str | tuple[str, str], ...] = tuple(components)

        return f"{self.task}-{hasher(comp_tup)}"

class StepReference(Reference):
    """Reference to an individual `Step`.

    Allows us to refer to the outputs of a `Step` in subsequent `Step`
    parameters.

    Attributes:
        step: `Step` referred to.
    """
    step: Step
    field: str

    def __init__(self, workflow: Workflow, step: Step):
        """Initialize the reference.

        Args:
            workflow: `Workflow` that this is tied to.
            step: `Step` that this refers to.
        """
        self.step = step
        self.field = "out"

    def __str__(self) -> str:
        """Global description of the reference."""
        return f"{self.step.id}/{self.field}"

    def __repr__(self) -> str:
        """Hashable reference to the step (and field)."""
        return f"{self.step.id}/{self.field}"

    @property
    def name(self) -> str:
        """Reference based on the named step.

        May be remapped by the workflow to something nicer
        than the ID.
        """
        return f"{self.step.name}/{self.field}"

    @property
    def __workflow__(self) -> Workflow:
        """Related workflow.

        Returns:
            Workflow that the referee is related to.
        """
        return self.step.__workflow__

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
