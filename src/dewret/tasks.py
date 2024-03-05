import importlib
from enum import Enum
from functools import cached_property
from collections.abc import Callable
from typing import Any

from .workflow import Step, StepReference, Workflow, Lazy, Target, LazyFactory, Reference, Raw
from .backends._base import BackendModule

class Backend(Enum):
    DASK = "dask"

DEFAULT_BACKEND = Backend.DASK

def get_dask_backend() -> BackendModule:
    from .backends import dask
    return dask

class TaskManager:
    _backend: Backend | None = None

    def set_backend(self, backend: Backend) -> Backend:
        self._backend = backend
        return self._backend

    @cached_property
    def backend(self) -> BackendModule:
        backend = self._backend
        if backend is None:
            backend = self.set_backend(DEFAULT_BACKEND)

        backend_mod = importlib.import_module(f".backends.{backend.value}", "dewret")
        return backend_mod

    def make_lazy(self) -> LazyFactory:
        return self.backend.lazy

    def __call__(self, task: Lazy, **kwargs: Any) -> StepReference:
        workflow = Workflow()
        return self.backend.run(workflow, task, **kwargs)

_manager = TaskManager()
lazy = _manager.make_lazy
run = _manager

def task() -> Callable[[Target], Lazy]:
    def _task(fn: Target) -> Lazy:
        def _fn(__workflow__: Workflow | None = None, **kwargs: Reference | Raw) -> StepReference:
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
