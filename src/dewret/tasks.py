import importlib
from enum import Enum
from functools import cached_property
from collections.abc import Callable

from .workflow import Step, StepReference, Workflow

class Backend(Enum):
    DASK = "dask"

DEFAULT_BACKEND = Backend.DASK

def get_dask_backend():
    from .backends import dask
    return dask

class TaskManager:
    _backend: Backend | None = None

    def set_backend(self, backend: Backend):
        self._backend = backend

    @cached_property
    def backend(self):
        if self._backend is None:
            self.set_backend(DEFAULT_BACKEND)

        backend = importlib.import_module(f".backends.{self._backend.value}", "dewret")
        return backend

    def make_lazy(self):
        return self.backend.lazy

    def __call__(self, task, **kwargs):
        workflow = Workflow()
        return self.backend.run(workflow, task, **kwargs)

_manager = TaskManager()
lazy = _manager.make_lazy
run = _manager

def task():
    def _task(fn: Callable):
        def _fn(__workflow__: Workflow | None = None, **kwargs):
            if __workflow__ is None:
                __workflow__ = Workflow()
            task = __workflow__.register_task(fn)
            step = Step(
                __workflow__,
                task,
                kwargs
            )
            __workflow__.steps.append(step)
            return StepReference(__workflow__, step)
        return lazy()(_fn)
    return _task
