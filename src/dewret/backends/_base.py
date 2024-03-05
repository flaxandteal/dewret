from typing import Protocol
from dewret.workflow import LazyFactory, Lazy, Workflow, StepReference

class BackendModule(Protocol):
    lazy: LazyFactory

    def run(self, workflow: Workflow, task: Lazy) -> StepReference:
        ...
