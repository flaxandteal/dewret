from collections.abc import Mapping, MutableMapping, Callable
import base64
from attrs import define
from typing import Protocol, Any

from .utils import hasher, RawType, is_raw

class Lazy(Protocol):
    ...

Target = Callable[..., Any]
LazyFactory = Callable[[Target], Lazy]

class Task:
    name: str
    target: Target | None

    def __init__(self, name: str, target: Target | None= None):
        self.name = name
        self.target = target

    def __str__(self) -> str:
        return self.name

    def __repr__(self) -> str:
        return self.name

class Workflow:
    steps: list["Step"]
    tasks: MutableMapping[str, "Task"]

    def __init__(self) -> None:
        self.steps = []
        self.tasks = {}

    def register_task(self, fn: Target) -> Task:
        name = fn.__name__
        if name in self.tasks and self.tasks[name].target != fn:
            raise RuntimeError(f"Naming clash for functions: {name}")

        task = Task(name, fn)
        self.tasks[name] = task
        return task


class WorkflowComponent:
    __workflow__: Workflow

    def __init__(self, workflow: Workflow):
        self.__workflow__ = workflow

class Reference(WorkflowComponent):
    ...

@define
class Raw:
    value: RawType

    def __repr__(self) -> str:
        value: str
        if isinstance(self.value, bytes):
            value = base64.b64encode(self.value).decode("ascii")
        else:
            value = str(self.value)
        return f"{type(self.value).__name__}|{value}"

class Step(WorkflowComponent):
    _id: str | None = None
    task: Task
    parameters: Mapping[str, Reference | Raw]

    def __init__(self, workflow: Workflow, task: Task, parameters: Mapping[str, Reference | Raw]):
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
        if self._id is None:
            self._id = self._generate_id()
            return self._id

        check_id = self._generate_id()
        if check_id != self._id:
            raise RuntimeError(f"Cannot change a step after requesting its ID: {self.task}")
        return self._id

    def _generate_id(self) -> str:
        components: list[str | tuple[str, str]] = [repr(self.task)]
        for key, param in self.parameters.items():
            components.append((key, repr(param)))

        comp_tup: tuple[str | tuple[str, str], ...] = tuple(components)
        return f"{self.task}-{hasher(comp_tup)}"

class StepReference(Reference):
    step: Step

    def __init__(self, workflow: Workflow, step: Step):
        super().__init__(workflow)
        self.step = step
