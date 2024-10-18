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

"""Dask backend.

Lazy-evaluation via `dask.delayed`.
"""

from dask.delayed import delayed, DelayedLeaf
from dask.config import config
from typing import Protocol, runtime_checkable, Any, cast
from concurrent.futures import ThreadPoolExecutor
from dewret.workflow import Workflow, Lazy, StepReference, Target


@runtime_checkable
class Delayed(Protocol):
    """Description of a dask `delayed`.

    Since `dask.delayed` does not have a hintable type, this
    stands in its place, making sure that all the features of a
    `dask.delayed` are available.

    More info: https://github.com/dask/dask/issues/7779
    """

    @property
    def __dask_graph__(self):  # type: ignore
        """Retrieve the dask graph."""
        ...

    def compute(self, __workflow__: Workflow | None) -> StepReference[Any]:
        """Evaluate this `dask.delayed`.

        Evaluate a delayed (dask lazy-evaluated) function. dewret
        will have replaced it with a wrapper that expects a `Workflow`
        and all arguments will already be known to the wrapped `delayed`
        so the signature here is simple.

        Args:
            __workflow__: `Workflow` that this is tied to, if applicable.

        Returns:
            Reference to the final output step.
        """
        ...


def unwrap(task: Lazy) -> Target:
    """Unwraps a lazy-evaluated function to get the function.

    In recent dask (>=2024.3) this works with inspect.wraps, but earlier
    versions do not have the `__wrapped__` property.

    Args:
        task: task to be unwrapped.

    Returns:
        Original target.

    Raises:
        RuntimeError: if the task is not a wrapped function.
    """
    if not isinstance(task, DelayedLeaf):
        raise RuntimeError("Task is not for this backend")
    if not callable(task):
        raise RuntimeError("Task is not actually a callable")
    return cast(Target, task._obj)


def is_lazy(task: Any) -> bool:
    """Checks if a task is really a lazy-evaluated function for this backend.

    Args:
        task: suspected lazy-evaluated function.

    Returns:
        True if so, False otherwise.
    """
    return isinstance(task, Delayed) or (
        isinstance(task, tuple | list) and all(is_lazy(elt) for elt in task)
    )


lazy = delayed


def run(
    workflow: Workflow | None,
    task: Lazy | list[Lazy] | tuple[Lazy, ...],
    thread_pool: ThreadPoolExecutor | None = None,
    **kwargs: Any,
) -> Any:
    """Execute a task as the output of a workflow.

    Runs a task with dask.

    Args:
        workflow: `Workflow` in which to record the execution.
        task: `dask.delayed` function, wrapped by dewret, that we wish to compute.
        thread_pool: custom thread pool for executing workflows, copies in correct values for contextvars to each thread before they are accessed by a dask worker.
        **kwargs: any configuration arguments for this backend.
    """
    # def _check_delayed was here, but we decided to delegate this to dask

    if isinstance(task, Delayed) and is_lazy(task):
        computable = task
    else:
        computable = delayed(task)
    config["pool"] = thread_pool
    result = computable.compute(__workflow__=workflow)
    return result
