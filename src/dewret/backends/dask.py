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

from dask.delayed import delayed
from dewret.workflow import Workflow, Lazy, StepReference
from typing import Protocol, runtime_checkable

@runtime_checkable
class Delayed(Protocol):
    """Description of a dask `delayed`.

    Since `dask.delayed` does not have a hintable type, this
    stands in its place, making sure that all the features of a
    `dask.delayed` are available.

    More info: https://github.com/dask/dask/issues/7779
    """

    def compute(self, __workflow__: Workflow | None) -> StepReference:
        """Evaluate this `dask.delayed`.

        Evaluate a delayed (dask lazy-evaluated) function. dewret
        will have replaced it with a wrapper that expects a `Workflow`
        and all arguments will already be known to the wrapped `delayed`
        so the signature here is simple.

        Argument:
            __workflow__: `Workflow` that this is tied to, if applicable.

        Returns:
            Reference to the final output step.
        """
        ...

lazy = delayed
def run(workflow: Workflow | None, task: Lazy) -> StepReference:
    """Execute a task as the output of a workflow.

    Runs a task with dask.

    Argument:
        workflow: `Workflow` in which to record the execution.
        task: `dask.delayed` function, wrapped by dewret, that we wish to compute.
    """
    if not isinstance(task, Delayed):
        raise RuntimeError("Cannot mix backends")
    return task.compute(__workflow__=workflow)
