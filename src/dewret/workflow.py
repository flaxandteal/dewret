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

from collections.abc import Mapping, MutableMapping, Callable
import base64
from attrs import define
from typing import Protocol, Any

from .utils import hasher, RawType, is_raw

class Lazy(Protocol):
    """Requirements for a lazy-evaluatable function."""
    ...

Target = Callable[..., Any]
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
    target: Target | None

    def __init__(self, name: str, target: Target | None= None):
        """Initialize the Task.

        Argument:
            name: Name of wrapped function.
            callable: Actual function being wrapped (optional).
        """
        self.name = name
        self.target = target

    def __str__(self) -> str:
        """Stringify the Task, currently by returning the `name`."""
        return self.name

    def __repr__(self) -> str:
        """Represent the Task, currently by returning the `name`."""
        return self.name

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
    """
    steps: list["Step"]
    tasks: MutableMapping[str, "Task"]

    def __init__(self) -> None:
        """Initialize a Workflow, by setting `steps` and `tasks` to empty containers."""
        self.steps = []
        self.tasks = {}

    def register_task(self, fn: Target) -> Task:
        """Note the existence of a lazy-evaluatable function, and wrap it as a `Task`.

        Argument:
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


class WorkflowComponent:
    """Base class for anything tied to an individual `Workflow`.

    Attributes:
        __workflow__: the `Workflow` that this is tied to.
    """
    __workflow__: Workflow

    def __init__(self, workflow: Workflow):
        """Tie to a `Workflow`.

        All subclasses must call this.

        Argument:
            workflow: the `Workflow` to tie to.
        """
        self.__workflow__ = workflow

class Reference(WorkflowComponent):
    """Superclass for all symbolic references to values."""
    ...

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

        Argument:
            workflow: `Workflow` that this is tied to.
            task: the lazy-evaluatable function that this wraps.
            parameters: key-value pairs to pass to the function.
        """
        super().__init__(workflow)
        self.task = task
        self.parameters = {}
        for key, value in parameters.items():
            if isinstance(value, Reference | Raw | RawType):
                # Avoid recursive type issues
                if not isinstance(value, Reference) and not isinstance(value, Raw) and is_raw(value):
                    value = Raw(value)
                self.parameters[key] = value
            else:
                raise RuntimeError(f"Non-references must be a serializable type: {key}>{value}")

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

    def __init__(self, workflow: Workflow, step: Step):
        """Initialize the reference.

        Argument:
            workflow: `Workflow` that this is tied to.
            step: `Step` that this refers to.
        """
        super().__init__(workflow)
        self.step = step
