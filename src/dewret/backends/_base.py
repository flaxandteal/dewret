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

from typing import Protocol
from dewret.workflow import LazyFactory, Lazy, Workflow, StepReference

class BackendModule(Protocol):
    """Requirements for a valid backend module.

    Essential criteria that a backend module must fulfil, such as a
    lazy-evaluation decorator and a task execution function.

    Attributes:
        lazy: Callable that takes a function and returns a lazy-evaluated
            version of it, appropriate to the backend.
    """
    lazy: LazyFactory

    def run(self, workflow: Workflow, task: Lazy) -> StepReference:
        """Execute a lazy task for this `Workflow`.

        Argument:
            workflow: `Workflow` that is being executed.
            task: task that forms the output.

        Returns:
            Reference to the final output step.
        """
        ...
