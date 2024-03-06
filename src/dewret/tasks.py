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

"""Abstraction layer for task operations.

Access dask, or other, backends consistently using this module. It provides
decorators and execution calls that manage tasks. Note that the `task`
decorator should be called with no arguments, and will return the appropriate
decorator for the current backend.

Typical usage example:

  @task()
  def increment(num: int) -> int:
      return num + 1
"""

import importlib
from enum import Enum
from functools import cached_property
from collections.abc import Callable
from typing import Any

from .workflow import Step, StepReference, Workflow, Lazy, Target, LazyFactory, Reference, Raw, StepExecution, Task
from .backends._base import BackendModule

class Backend(Enum):
    """Stringy enum representing available backends."""
    DASK = "dask"

DEFAULT_BACKEND = Backend.DASK

def get_dask_backend() -> BackendModule:
    """Initialize the dask backend."""
    from .backends import dask
    return dask

class TaskManager:
    """Overarching backend-agnostic task manager.

    Gatekeeps the specific backend implementation. This can be
    instantiated without choosing a backend, but the first call to
    any of its methods will concretize that choice - either as
    the default, or the backend set via `TaskManager.set_backend`.
    It cannot be changed after this point.
    """

    _backend: Backend | None = None

    def set_backend(self, backend: Backend) -> Backend:
        """Choose a backend.

        Sets the backend, provided it has not already been loaded.

        Args:
            backend: chosen backend, to override the default.

        Returns:
            Backend that was set.

        Raises:
            RuntimeError: when a backend has already been loaded.
        """
        if self._backend is not None:
            raise RuntimeError(
                f"Backend is already loaded ({self._backend}). Did an imported module use lazy/run already?"
            )
        self._backend = backend
        return self._backend

    @cached_property
    def backend(self) -> BackendModule:
        """Import backend module.

        Cached property to load the backend module, if it has not been already.

        Returns:
            Backend module for the specific choice of backend.
        """
        backend = self._backend
        if backend is None:
            backend = self.set_backend(DEFAULT_BACKEND)

        backend_mod = importlib.import_module(f".backends.{backend.value}", "dewret")
        return backend_mod

    def make_lazy(self) -> LazyFactory:
        """Get the lazy decorator for this backend.

        Returns:
            Real decorator for this backend.
        """
        return self.backend.lazy

    def __call__(self, task: Lazy, **kwargs: Any) -> Workflow:
        """Execute the lazy evalution.

        Arguments:
            task: the task to evaluate.
            **kwargs: any arguments to pass to the task.

        Returns:
            A reusable reference to this individual step.
        """
        workflow = Workflow()
        result = self.backend.run(workflow, task, **kwargs)
        return Workflow.from_result(result)

_manager = TaskManager()
lazy = _manager.make_lazy
run = _manager

def task() -> Callable[[Target], StepExecution]:
    """Decorator factory abstracting backend's own task decorator.

    Returns:
        Decorator for the current backend to mark lazy-executable tasks.
        For example:

            @task()
            def increment(num: int) -> int:
                return num + 1

        If the backend is `dask` (the default), it is will evaluate this
        as a `dask.delayed`. Note that, with any backend, dewret will
        hijack the decorator to record the attempted _evalution_ rather than
        actually _evaluating_ the lazy function. Nonetheless, this hijacking
        will still be executed with the backend's lazy executor, so
        `dask.delayed` will still be called, for example, in the dask case.
    """

    def _task(fn: Target) -> StepExecution:
        def _fn(__workflow__: Workflow | None = None, **kwargs: Reference | Raw) -> StepReference:
            if __workflow__ is None:
                __workflow__ = Workflow()
            return __workflow__.add_step(fn, kwargs)
        return lazy()(_fn)
    return _task

def set_backend(backend: Backend) -> None:
    """Choose a backend.

    Will raise an error if a backend is already chosen.
    """
    _manager.set_backend(backend)
