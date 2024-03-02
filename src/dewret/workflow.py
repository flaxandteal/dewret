from collections.abc import Mapping, MutableMapping, Callable
from attrs import define

from .utils import hasher, RawType, is_raw

class Workflow:
    steps: list["Step"]
    tasks: MutableMapping[str, "Task"]

    def __init__(self):
        self.steps = []
        self.tasks = {}

    def register_task(self, fn: Callable):
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

class Task:
    name: str
    target: Callable | None

    def __init__(self, name, target = None):
        self.name = name
        self.target = target

    def __str__(self):
        return self.name

    def __repr__(self):
        return self.name

@define
class Raw:
    value: RawType

    def __repr__(self):
        return f"{type(self.value).__name__}|{self.value}"

class Step(WorkflowComponent):
    _id: str | None = None
    task: Task
    parameters: Mapping[str, Reference | Raw]

    def __init__(self, workflow, task, parameters):
        super().__init__(workflow)
        self.task = task
        self.parameters = {}
        for key, value in parameters.items():
            if isinstance(value, Reference | Raw | RawType):
                if is_raw(value):
                    value = Raw(value)
                self.parameters[key] = value
            else:
                raise RuntimeError(f"Non-references must be a serializable type: {key}>{value}")

    @property
    def id(self):
        if self._id is None:
            self._id = self._generate_id()
            return self._id

        check_id = self._generate_id()
        if check_id != self._id:
            raise RuntimeError(f"Cannot change a step after requesting its ID: {self.task}")
        return self._id

    def _generate_id(self):
        components = (
            repr(self.task),
            *((k, repr(p)) for k, p in self.parameters.items())
        )
        return f"{self.task}-{hasher(components)}"

class StepReference(Reference):
    step: Step

    def __init__(self, workflow: Workflow, step: Step):
        super().__init__(workflow)
        self.step = step
