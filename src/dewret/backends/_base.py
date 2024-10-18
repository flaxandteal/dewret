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

"""Backend base protocol.

Definition of a protocol that valid backend modules must fulfil.
"""

from typing import Protocol, Any
from concurrent.futures import ThreadPoolExecutor
from dewret.workflow import LazyFactory, Lazy, Workflow, StepReference, Target


class BackendModule(Protocol):
    """Requirements for a valid backend module.

    Essential criteria that a backend module must fulfil, such as a
    lazy-evaluation decorator and a task execution function.

    Attributes:
        lazy: Callable that takes a function and returns a lazy-evaluated
            version of it, appropriate to the backend.
    """

    lazy: LazyFactory

    def run(self, workflow: Workflow | None, task: Lazy | list[Lazy] | tuple[Lazy, ...], thread_pool: ThreadPoolExecutor | None=None) -> StepReference[Any] | list[StepReference[Any]] | tuple[StepReference[Any]]:
        """Execute a lazy task for this `Workflow`.

        Args:
            workflow: `Workflow` that is being executed.
            task: task that forms the output.
            thread_pool: the thread pool that should be used for this execution.

        Returns:
            Reference to the final output step.
        """
        ...

    def unwrap(self, lazy: Lazy) -> Target:
        """Unwraps a lazy-evaluated function to get the function.

        Ideally, we could use the `__wrapped__` property but not all
        workflow engines support this, and most importantly, dask has
        only done so as of 2024.03.

        Args:
            lazy: task to be unwrapped.

        Returns:
            Original target.

        Raises:
            RuntimeError: if the task is not a wrapped function.
        """
        ...

    def is_lazy(self, lazy: Any) -> bool:
        """Confirm whether this is a lazy-evaluatable function.

        Args:
            lazy: suspected lazy-evaluatable function to check.

        Returns:
            True if this is a lazy-evaluatable function for this backend, otherwise False.
        """
        ...
