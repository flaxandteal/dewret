from dask.delayed import delayed
from dewret.workflow import Workflow, Lazy, StepReference
from typing import Protocol, runtime_checkable

@runtime_checkable
class Delayed(Protocol):
    def compute(self, __workflow__: Workflow) -> StepReference:
        ...

lazy = delayed
def run(workflow: Workflow, task: Lazy) -> StepReference:
    if not isinstance(task, Delayed):
        raise RuntimeError("Cannot mix backends")
    return task.compute(__workflow__=workflow)
